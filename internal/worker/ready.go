package worker

import (
	"time"

	"github.com/mariocandido/mailq/internal/store"
)

// readyQueue holds claimed messages in memory, per destination domain, until
// the domain's throttle lets them through.
//
// Without it, a message refused by the throttle has to go back to Postgres
// and be claimed again later: under heavy throttling that is a write and a
// claim per message every few hundred milliseconds. Holding it here costs
// nothing; the lease it got when claimed keeps other workers away.
//
// Only the worker's Run loop touches it, so it needs no locking.
type readyQueue struct {
	byDomain  map[string][]*held
	notBefore map[string]time.Time // throttle said "come back later"
	n         int
}

type held struct {
	m       store.Message
	since   time.Time // when it was claimed; the lease started then
	waiting bool      // already counted as throttled once
}

type grant struct {
	m       store.Message
	since   time.Time
	release func()
}

type acquireFunc func(domain string) (release func(), wait time.Duration, ok bool)

func newReadyQueue() *readyQueue {
	return &readyQueue{byDomain: map[string][]*held{}, notBefore: map[string]time.Time{}}
}

func (q *readyQueue) Len() int { return q.n }

// Push adds a freshly claimed message at the back of its domain's line.
func (q *readyQueue) Push(m store.Message, claimedAt time.Time) {
	q.byDomain[m.Domain] = append(q.byDomain[m.Domain], &held{m: m, since: claimedAt})
	q.n++
}

// Ready takes up to limit messages whose domain throttle grants a slot now.
// Each domain is served in FIFO order; a domain that refuses is not asked
// again until the wait it gave has passed. onThrottled is called once per
// message the first time it has to wait.
func (q *readyQueue) Ready(now time.Time, limit int, acquire acquireFunc, onThrottled func(domain string)) []grant {
	var out []grant
	// Map order is random, which is enough to keep one busy domain from
	// always being served first.
	for d, line := range q.byDomain {
		if limit <= 0 {
			break
		}
		if now.Before(q.notBefore[d]) {
			continue
		}
		for len(line) > 0 && limit > 0 {
			release, wait, ok := acquire(d)
			if !ok {
				q.notBefore[d] = now.Add(wait)
				if !line[0].waiting {
					line[0].waiting = true
					onThrottled(d)
				}
				break
			}
			out = append(out, grant{m: line[0].m, since: line[0].since, release: release})
			line = line[1:]
			limit--
			q.n--
		}
		q.set(d, line)
	}
	return out
}

// Wake lets domain be asked again right away, e.g. because one of its
// deliveries just finished and freed a session slot.
func (q *readyQueue) Wake(domain string) { delete(q.notBefore, domain) }

// NextWake says how long until some held message may be ready.
func (q *readyQueue) NextWake(now time.Time) (time.Duration, bool) {
	var best time.Duration
	found := false
	for d := range q.byDomain {
		w := max(q.notBefore[d].Sub(now), 0)
		if !found || w < best {
			best, found = w, true
		}
	}
	return best, found
}

// Expired removes and returns messages held for longer than maxAge, so they
// can be handed back to Postgres before their lease runs out.
func (q *readyQueue) Expired(now time.Time, maxAge time.Duration) []store.Message {
	var out []store.Message
	for d, line := range q.byDomain {
		kept := line[:0]
		for _, h := range line {
			if now.Sub(h.since) >= maxAge {
				out = append(out, h.m)
				q.n--
			} else {
				kept = append(kept, h)
			}
		}
		q.set(d, kept)
	}
	return out
}

// Drain empties the queue, e.g. on shutdown.
func (q *readyQueue) Drain() []store.Message {
	var out []store.Message
	for d, line := range q.byDomain {
		for _, h := range line {
			out = append(out, h.m)
		}
		q.set(d, nil)
	}
	q.n = 0
	return out
}

// Full lists the domains that already have at least limit messages held.
// The worker stops claiming those until the line gets shorter.
func (q *readyQueue) Full(limit int) []string {
	var out []string
	for d, line := range q.byDomain {
		if len(line) >= limit {
			out = append(out, d)
		}
	}
	return out
}

func (q *readyQueue) set(domain string, line []*held) {
	if len(line) == 0 {
		delete(q.byDomain, domain)
		delete(q.notBefore, domain)
		return
	}
	q.byDomain[domain] = line
}
