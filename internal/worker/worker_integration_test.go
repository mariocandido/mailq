package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

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

// A burst to one slow domain must wait in memory, not bounce back and forth
// through Postgres, and must not hold up mail for other domains.
func TestThrottledBurstWaitsInMemory(t *testing.T) {
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
	fake := &fakesmtp.Server{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go fake.Serve(ln) //nolint:errcheck

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	w := &Worker{
		Store: st,
		Client: &smtpclient.Client{
			Router: &smtpclient.StaticRouter{Routes: map[string]string{
				"slow.test": ln.Addr().String(), "fast.test": ln.Addr().String(),
			}},
			Timeout: 5 * time.Second,
		},
		Classifier: classify.Default(),
		Limiter: throttle.New(throttle.Config{
			Rate: 100, Burst: 100, Overrides: map[string]float64{"slow.test": 10},
		}),
		Backoff:          backoff.Default(),
		Metrics:          m,
		Log:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		Concurrency:      8,
		MaxHeldPerDomain: 4,
		PollInterval:     20 * time.Millisecond,
	}

	// 20 messages to slow.test (10/s, burst of 100 tokens drained below)
	// queued before 5 to fast.test.
	var in []store.NewMessage
	for i := range 20 {
		in = append(in, store.NewMessage{From: "me@test.local", To: fmt.Sprintf("s%d@slow.test", i), Domain: "slow.test", Subject: "s", Body: "b"})
	}
	if _, err := st.Enqueue(ctx, "", in); err != nil {
		t.Fatal(err)
	}
	in = nil
	for i := range 5 {
		in = append(in, store.NewMessage{From: "me@test.local", To: fmt.Sprintf("f%d@fast.test", i), Domain: "fast.test", Subject: "s", Body: "b"})
	}
	if _, err := st.Enqueue(ctx, "", in); err != nil {
		t.Fatal(err)
	}
	// Drain slow.test's burst so that it really is throttled at 10/s.
	w.Limiter.OnRateLimited("slow.test")
	w.Limiter.OnRateLimited("slow.test") // now 2.5/s

	wctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = w.Run(wctx); close(done) }()

	count := func(domain string) int {
		var n int
		if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM messages WHERE domain = $1 AND status = 'delivered'`, domain).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// fast.test is not stuck behind slow.test's line.
	deadline := time.Now().Add(3 * time.Second)
	for count("fast.test") < 5 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := count("fast.test"); n != 5 {
		t.Fatalf("fast.test delivered %d of 5 while slow.test was throttled", n)
	}
	if n := count("slow.test"); n >= 20 {
		t.Fatalf("slow.test delivered %d already; the test needs it to be throttled", n)
	}

	deadline = time.Now().Add(20 * time.Second)
	for count("slow.test") < 20 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	<-done

	if n := count("slow.test"); n != 20 {
		t.Fatalf("slow.test delivered %d of 20", n)
	}
	if n := testutil.ToFloat64(m.Requeued.WithLabelValues("slow.test", "lease")); n != 0 {
		t.Errorf("%v messages went back to Postgres, want 0", n)
	}
	var attempts int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 25 || fake.Delivered() != 25 {
		t.Errorf("attempts=%d delivered=%d, want 25 each (throttling must not cost attempts)", attempts, fake.Delivered())
	}
}
