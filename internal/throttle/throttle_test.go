package throttle

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTest(cfg Config) (*Limiter, *clock) {
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	l := New(cfg)
	l.now = c.now
	return l, c
}

func TestBurstThenRate(t *testing.T) {
	l, c := newTest(Config{Rate: 2, Burst: 3})
	for i := range 3 {
		if rel, _, ok := l.Acquire("a.com"); !ok {
			t.Fatalf("acquire %d within burst failed", i)
		} else {
			rel()
		}
	}
	_, wait, ok := l.Acquire("a.com")
	if ok || wait != 500*time.Millisecond {
		t.Fatalf("expected to wait 500ms, got ok=%v wait=%v", ok, wait)
	}
	c.advance(500 * time.Millisecond)
	if _, _, ok := l.Acquire("a.com"); !ok {
		t.Fatal("token should have refilled")
	}
}

func TestDomainsAreIndependent(t *testing.T) {
	l, _ := newTest(Config{Rate: 1, Burst: 1})
	if _, _, ok := l.Acquire("a.com"); !ok {
		t.Fatal("a.com first acquire")
	}
	if _, _, ok := l.Acquire("b.com"); !ok {
		t.Fatal("b.com must not be affected by a.com")
	}
}

func TestMaxConcurrent(t *testing.T) {
	l, _ := newTest(Config{Rate: 100, Burst: 100, MaxConcurrent: 2})
	r1, _, _ := l.Acquire("a.com")
	r2, _, _ := l.Acquire("a.com")
	if _, _, ok := l.Acquire("a.com"); ok {
		t.Fatal("third concurrent delivery should be refused")
	}
	r1()
	r1() // double release must be harmless
	if _, _, ok := l.Acquire("a.com"); !ok {
		t.Fatal("slot should be free after release")
	}
	r2()
}

func TestAIMD(t *testing.T) {
	l, _ := newTest(Config{Rate: 8, Burst: 8, MinRate: 1, Step: 1})
	l.OnRateLimited("a.com")
	if r := l.Rates()["a.com"]; r != 4 {
		t.Fatalf("rate after one 421: %v, want 4", r)
	}
	l.OnRateLimited("a.com")
	l.OnRateLimited("a.com")
	l.OnRateLimited("a.com")
	if r := l.Rates()["a.com"]; r != 1 {
		t.Fatalf("rate must not go below MinRate: %v", r)
	}
	if _, _, ok := l.Acquire("a.com"); ok {
		t.Fatal("burst must be drained after a rate-limit response")
	}
	for range 20 {
		l.OnSuccess("a.com")
	}
	if r := l.Rates()["a.com"]; r != 8 {
		t.Fatalf("rate must recover up to the ceiling only: %v", r)
	}
}

func TestOverrides(t *testing.T) {
	l, _ := newTest(Config{Rate: 10, Overrides: map[string]float64{"slow.com": 1}})
	l.Acquire("slow.com")
	l.Acquire("fast.com")
	if r := l.Rates(); r["slow.com"] != 1 || r["fast.com"] != 10 {
		t.Fatalf("rates: %v", r)
	}
}
