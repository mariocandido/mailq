// Package backoff computes when a deferred message should be retried.
package backoff

import (
	"fmt"
	"strings"
	"time"
)

// Policy is a fixed retry schedule with jitter and a maximum message age.
// Attempt n (1-based) waits Schedule[n-1]; after the schedule runs out the
// last value repeats. Once a retry would land after MaxAge (measured from
// when the message was queued) the message expires and is bounced.
type Policy struct {
	Schedule []time.Duration
	MaxAge   time.Duration
	Jitter   float64 // 0.2 means +/-20%
}

// Default mirrors common MTA behaviour: quick first retries, then hours,
// giving up after a day.
func Default() Policy {
	return Policy{
		Schedule: []time.Duration{
			time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute,
			time.Hour, 2 * time.Hour, 4 * time.Hour,
		},
		MaxAge: 24 * time.Hour,
		Jitter: 0.2,
	}
}

// Delay returns the wait after the given attempt. rnd must be in [0,1).
// Jitter spreads retries so that a provider outage does not turn into a
// synchronized retry storm when it recovers.
func (p Policy) Delay(attempt int, rnd float64) time.Duration {
	if len(p.Schedule) == 0 {
		return time.Minute
	}
	i := min(max(attempt-1, 0), len(p.Schedule)-1)
	d := p.Schedule[i]
	if p.Jitter > 0 {
		d = time.Duration(float64(d) * (1 + p.Jitter*(2*rnd-1)))
	}
	return d
}

// Next returns the next attempt time, or ok=false when the message expired.
func (p Policy) Next(attempt int, queuedAt, now time.Time, rnd float64) (next time.Time, ok bool) {
	next = now.Add(p.Delay(attempt, rnd))
	if p.MaxAge > 0 && next.Sub(queuedAt) > p.MaxAge {
		return time.Time{}, false
	}
	return next, true
}

// ParseSchedule parses "1m,5m,15m".
func ParseSchedule(s string) ([]time.Duration, error) {
	var out []time.Duration
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		d, err := time.ParseDuration(part)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid retry delay %q", part)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty retry schedule")
	}
	return out, nil
}
