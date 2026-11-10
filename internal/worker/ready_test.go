package worker

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/mariocandido/mailq/internal/store"
)

func msg(domain string, i int) store.Message {
	return store.Message{ID: fmt.Sprintf("%s-%d", domain, i), Domain: domain}
}

// fakeAcquire grants up to n[domain] slots and then refuses with wait.
func fakeAcquire(n map[string]int, wait time.Duration) acquireFunc {
	return func(d string) (func(), time.Duration, bool) {
		if n[d] <= 0 {
			return nil, wait, false
		}
		n[d]--
		return func() {}, 0, true
	}
}

func ids(gs []grant) []string {
	var out []string
	for _, g := range gs {
		out = append(out, g.m.ID)
	}
	slices.Sort(out)
	return out
}

func TestReadyKeepsRefusedMessagesInOrder(t *testing.T) {
	q := newReadyQueue()
	now := time.Unix(0, 0)
	for i := range 5 {
		q.Push(msg("a.test", i), now)
	}
	throttled := 0
	slots := map[string]int{"a.test": 2}

	got := q.Ready(now, 10, fakeAcquire(slots, time.Second), func(string) { throttled++ })
	if want := []string{"a.test-0", "a.test-1"}; !slices.Equal(ids(got), want) {
		t.Fatalf("first round = %v, want %v", ids(got), want)
	}
	if q.Len() != 3 || throttled != 1 {
		t.Fatalf("len=%d throttled=%d, want 3 and 1", q.Len(), throttled)
	}

	// Before the wait is over the domain is not even asked.
	slots["a.test"] = 10
	if got := q.Ready(now.Add(500*time.Millisecond), 10, fakeAcquire(slots, time.Second), func(string) { throttled++ }); len(got) != 0 {
		t.Fatalf("domain asked again before its wait: %v", ids(got))
	}
	if d, ok := q.NextWake(now.Add(500 * time.Millisecond)); !ok || d != 500*time.Millisecond {
		t.Fatalf("NextWake = %v %v, want 500ms", d, ok)
	}

	// After the wait the rest goes out, oldest first.
	got = q.Ready(now.Add(time.Second), 10, fakeAcquire(slots, time.Second), func(string) { throttled++ })
	if want := []string{"a.test-2", "a.test-3", "a.test-4"}; !slices.Equal(ids(got), want) {
		t.Fatalf("second round = %v, want %v", ids(got), want)
	}
	if q.Len() != 0 || throttled != 1 {
		t.Fatalf("len=%d throttled=%d, want 0 and 1 (each message counted once)", q.Len(), throttled)
	}
	if _, ok := q.NextWake(now); ok {
		t.Fatal("empty queue should have nothing to wake for")
	}
}

func TestThrottledDomainDoesNotBlockOthers(t *testing.T) {
	q := newReadyQueue()
	now := time.Unix(0, 0)
	for i := range 3 {
		q.Push(msg("slow.test", i), now)
		q.Push(msg("fast.test", i), now)
	}
	got := q.Ready(now, 10, fakeAcquire(map[string]int{"fast.test": 10}, time.Second), func(string) {})
	if want := []string{"fast.test-0", "fast.test-1", "fast.test-2"}; !slices.Equal(ids(got), want) {
		t.Fatalf("got %v, want every fast.test message", ids(got))
	}
}

func TestReadyRespectsLimit(t *testing.T) {
	q := newReadyQueue()
	now := time.Unix(0, 0)
	for i := range 5 {
		q.Push(msg("a.test", i), now)
	}
	if got := q.Ready(now, 2, fakeAcquire(map[string]int{"a.test": 10}, 0), func(string) {}); len(got) != 2 || q.Len() != 3 {
		t.Fatalf("got %d, len %d; want 2 and 3", len(got), q.Len())
	}
}

func TestFullExpiredAndDrain(t *testing.T) {
	q := newReadyQueue()
	t0 := time.Unix(0, 0)
	q.Push(msg("a.test", 0), t0)
	q.Push(msg("a.test", 1), t0.Add(10*time.Second))
	q.Push(msg("b.test", 0), t0)

	if full := q.Full(2); !slices.Equal(full, []string{"a.test"}) {
		t.Fatalf("Full(2) = %v, want [a.test]", full)
	}

	exp := q.Expired(t0.Add(30*time.Second), 30*time.Second)
	var got []string
	for _, m := range exp {
		got = append(got, m.ID)
	}
	slices.Sort(got)
	if want := []string{"a.test-0", "b.test-0"}; !slices.Equal(got, want) {
		t.Fatalf("Expired = %v, want %v", got, want)
	}
	if q.Len() != 1 {
		t.Fatalf("len after Expired = %d, want 1", q.Len())
	}

	if rest := q.Drain(); len(rest) != 1 || rest[0].ID != "a.test-1" || q.Len() != 0 {
		t.Fatalf("Drain = %v, len %d", rest, q.Len())
	}
}
