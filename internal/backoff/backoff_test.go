package backoff

import (
	"testing"
	"time"
)

func TestDelayFollowsScheduleAndRepeatsLast(t *testing.T) {
	p := Policy{Schedule: []time.Duration{time.Second, 5 * time.Second, time.Minute}}
	want := []time.Duration{time.Second, 5 * time.Second, time.Minute, time.Minute, time.Minute}
	for i, w := range want {
		if got := p.Delay(i+1, 0.5); got != w {
			t.Errorf("attempt %d: got %v, want %v", i+1, got, w)
		}
	}
}

func TestJitterBounds(t *testing.T) {
	p := Policy{Schedule: []time.Duration{10 * time.Second}, Jitter: 0.2}
	if got := p.Delay(1, 0); got != 8*time.Second {
		t.Errorf("min jitter: got %v", got)
	}
	if got := p.Delay(1, 0.999999); got < 11900*time.Millisecond || got > 12*time.Second {
		t.Errorf("max jitter: got %v", got)
	}
}

func TestNextExpires(t *testing.T) {
	p := Policy{Schedule: []time.Duration{time.Minute}, MaxAge: 10 * time.Minute}
	queued := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	if next, ok := p.Next(1, queued, queued.Add(5*time.Minute), 0.5); !ok || !next.Equal(queued.Add(6*time.Minute)) {
		t.Errorf("expected retry at +6m, got %v %v", next, ok)
	}
	if _, ok := p.Next(9, queued, queued.Add(9*time.Minute+30*time.Second), 0.5); ok {
		t.Error("expected expiry once the retry would exceed MaxAge")
	}
}

func TestParseSchedule(t *testing.T) {
	got, err := ParseSchedule(" 2s, 1m ,1h")
	if err != nil || len(got) != 3 || got[1] != time.Minute {
		t.Fatalf("got %v, %v", got, err)
	}
	for _, bad := range []string{"", "1x", "-1s", "0s"} {
		if _, err := ParseSchedule(bad); err == nil {
			t.Errorf("ParseSchedule(%q) should fail", bad)
		}
	}
}
