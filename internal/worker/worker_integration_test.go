package worker

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mariocandido/mailq/internal/backoff"
	"github.com/mariocandido/mailq/internal/classify"
	"github.com/mariocandido/mailq/internal/fakesmtp"
	"github.com/mariocandido/mailq/internal/metrics"
	"github.com/mariocandido/mailq/internal/smtpclient"
	"github.com/mariocandido/mailq/internal/store"
	"github.com/mariocandido/mailq/internal/throttle"
)

// Runs the whole pipeline (Postgres queue -> worker -> SMTP -> outcome)
// against a real database. Skipped unless MAILQ_TEST_DATABASE_URL is set.
func TestPipeline(t *testing.T) {
	url := os.Getenv("MAILQ_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("MAILQ_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	st, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool().Exec(ctx, `TRUNCATE messages, attempts, suppressions CASCADE`); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	fake := &fakesmtp.Server{FlakyFailures: 1, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go fake.Serve(ln) //nolint:errcheck

	w := &Worker{
		Store: st,
		Client: &smtpclient.Client{
			Router:  &smtpclient.StaticRouter{Routes: map[string]string{"fake.test": ln.Addr().String()}},
			Timeout: 5 * time.Second,
		},
		Classifier:   classify.Default(),
		Limiter:      throttle.New(throttle.Config{Rate: 50, Burst: 50}),
		Backoff:      backoff.Policy{Schedule: []time.Duration{200 * time.Millisecond}, MaxAge: 2 * time.Second},
		Metrics:      metrics.New(prometheus.NewRegistry()),
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		PollInterval: 50 * time.Millisecond,
	}

	recipients := []string{"ok@fake.test", "bounce@fake.test", "block@fake.test", "flaky@fake.test",
		"defer@fake.test", "x@nowhere.test"}
	var in []store.NewMessage
	for _, to := range recipients {
		in = append(in, store.NewMessage{From: "me@test.local", To: to, Domain: smtpclient.DomainOf(to), Subject: "s", Body: "b"})
	}
	msgs, err := st.Enqueue(ctx, "key-1", in)
	if err != nil {
		t.Fatal(err)
	}

	// Idempotent replay returns the same messages.
	again, err := st.Enqueue(ctx, "key-1", in)
	if err != nil {
		t.Fatal(err)
	}
	for i := range msgs {
		if msgs[i].ID != again[i].ID {
			t.Fatalf("idempotent replay created a new message for %s", msgs[i].To)
		}
	}

	wctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = w.Run(wctx); close(done) }()

	want := map[string]struct {
		status   string
		attempts int
	}{
		"ok@fake.test":     {store.StatusDelivered, 1},
		"bounce@fake.test": {store.StatusBounced, 1},
		"block@fake.test":  {store.StatusBounced, 1},
		"flaky@fake.test":  {store.StatusDelivered, 2},
		"defer@fake.test":  {store.StatusBounced, -1}, // expires; attempt count depends on timing
		"x@nowhere.test":   {store.StatusBounced, 1},
	}

	deadline := time.Now().Add(15 * time.Second)
	got := map[string]store.Message{}
	for time.Now().Before(deadline) {
		allFinal := true
		for _, m := range msgs {
			cur, _, err := st.Get(ctx, m.ID)
			if err != nil {
				t.Fatal(err)
			}
			got[cur.To] = cur
			if cur.Status != store.StatusDelivered && cur.Status != store.StatusBounced {
				allFinal = false
			}
		}
		if allFinal {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	<-done

	for to, w := range want {
		m := got[to]
		if m.Status != w.status || (w.attempts > 0 && m.Attempts != w.attempts) {
			t.Errorf("%s: status=%s attempts=%d last_error=%q, want %s/%d", to, m.Status, m.Attempts, m.LastError, w.status, w.attempts)
		}
	}
	if got["defer@fake.test"].Attempts < 2 {
		t.Errorf("defer@ should have been retried before expiring, attempts=%d", got["defer@fake.test"].Attempts)
	}

	// Only the hard bounce is suppressed; the policy block is not.
	sup, err := st.Suppressions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sup) != 1 || sup[0].Recipient != "bounce@fake.test" {
		t.Fatalf("suppressions = %+v, want only bounce@fake.test", sup)
	}
	again, err = st.Enqueue(ctx, "", in[1:2])
	if err != nil {
		t.Fatal(err)
	}
	if again[0].Status != store.StatusSuppressed {
		t.Fatalf("new message to a suppressed recipient has status %s", again[0].Status)
	}
	if fake.Delivered() != 2 {
		t.Errorf("fake server accepted %d messages, want 2", fake.Delivered())
	}
}
