// Package throttle limits how fast mailq talks to each destination domain.
//
// Every domain gets a token bucket (rate + burst) and a cap on concurrent
// connections. The rate adapts with AIMD, like TCP congestion control: it is
// halved whenever the domain pushes back with a rate-limit response and grows
// back slowly after each successful delivery, never above its ceiling.
package throttle

import (
	"sync"
	"time"
)

// Config holds the per-domain limits.
type Config struct {
	Rate          float64            // messages/second ceiling for a domain
	Burst         float64            // bucket size
	MaxConcurrent int                // simultaneous deliveries per domain (0 = unlimited)
	MinRate       float64            // floor when backing off
	Step          float64            // additive increase per success
	Overrides     map[string]float64 // per-domain rate ceilings
}

// Limiter is safe for concurrent use.
type Limiter struct {
	mu      sync.Mutex
	cfg     Config
	now     func() time.Time
	domains map[string]*bucket
}

type bucket struct {
	ceiling  float64
	rate     float64
	tokens   float64
	last     time.Time
	inflight int
}

const (
	minWait         = 100 * time.Millisecond
	concurrencyWait = 250 * time.Millisecond
)

// New creates a limiter, filling in sane defaults.
func New(cfg Config) *Limiter {
	if cfg.Rate <= 0 {
		cfg.Rate = 5
	}
	if cfg.Burst < 1 {
		cfg.Burst = max(1, cfg.Rate)
	}
	if cfg.MinRate <= 0 {
		cfg.MinRate = 0.1
	}
	if cfg.Step <= 0 {
		cfg.Step = cfg.Rate / 20
	}
	return &Limiter{cfg: cfg, now: time.Now, domains: map[string]*bucket{}}
}

func (l *Limiter) get(domain string) *bucket {
	b, ok := l.domains[domain]
	if !ok {
		ceiling := l.cfg.Rate
		if r, ok := l.cfg.Overrides[domain]; ok && r > 0 {
			ceiling = r
		}
		b = &bucket{ceiling: ceiling, rate: ceiling, tokens: l.cfg.Burst, last: l.now()}
		l.domains[domain] = b
	}
	return b
}

func (b *bucket) refill(now time.Time, burst float64) {
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(burst, b.tokens+elapsed*b.rate)
		b.last = now
	}
}

// Acquire asks to send one message to domain. When ok is false the caller
// should come back after wait. When ok is true the caller must call release
// once the delivery attempt is over.
func (l *Limiter) Acquire(domain string) (release func(), wait time.Duration, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.get(domain)
	b.refill(l.now(), l.cfg.Burst)

	if l.cfg.MaxConcurrent > 0 && b.inflight >= l.cfg.MaxConcurrent {
		return nil, concurrencyWait, false
	}
	if b.tokens < 1 {
		wait = time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		return nil, max(wait, minWait), false
	}

	b.tokens--
	b.inflight++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			b.inflight--
			l.mu.Unlock()
		})
	}, 0, true
}

// OnSuccess slowly raises the domain's rate back towards its ceiling.
func (l *Limiter) OnSuccess(domain string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.get(domain)
	b.rate = min(b.ceiling, b.rate+l.cfg.Step)
}

// OnRateLimited halves the domain's rate and drains its burst.
func (l *Limiter) OnRateLimited(domain string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.get(domain)
	b.rate = max(l.cfg.MinRate, b.rate/2)
	b.tokens = min(b.tokens, 0)
}

// Rates returns the current rate of every domain seen so far.
func (l *Limiter) Rates() map[string]float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]float64, len(l.domains))
	for d, b := range l.domains {
		out[d] = b.rate
	}
	return out
}
