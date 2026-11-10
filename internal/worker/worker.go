// Package worker pulls due messages from the store, delivers them within the
// per-domain limits and records the outcome.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/mariocandido/mailq/internal/backoff"
	"github.com/mariocandido/mailq/internal/classify"
	"github.com/mariocandido/mailq/internal/metrics"
	"github.com/mariocandido/mailq/internal/smtpclient"
	"github.com/mariocandido/mailq/internal/store"
	"github.com/mariocandido/mailq/internal/throttle"
)

// Worker is the delivery loop.
type Worker struct {
	Store        *store.Store
	Client       *smtpclient.Client
	Classifier   *classify.Classifier
	Limiter      *throttle.Limiter
	Backoff      backoff.Policy
	Metrics      *metrics.Metrics
	Log          *slog.Logger
	Concurrency  int           // simultaneous deliveries overall
	Lease        time.Duration // how long a claimed message stays reserved
	PollInterval time.Duration // sleep when the queue is empty

	// Messages refused by the throttle wait in memory instead of going back
	// to Postgres. MaxHeld bounds them overall; once a domain has
	// MaxHeldPerDomain waiting, no more of its messages are claimed.
	MaxHeld          int
	MaxHeldPerDomain int
}

func (w *Worker) defaults() {
	if w.Concurrency <= 0 {
		w.Concurrency = 16
	}
	if w.Lease <= 0 {
		w.Lease = 2*w.Client.Timeout + 10*time.Second
	}
	if w.PollInterval <= 0 {
		w.PollInterval = 250 * time.Millisecond
	}
	if w.MaxHeld <= 0 {
		w.MaxHeld = 4 * w.Concurrency
	}
	if w.MaxHeldPerDomain <= 0 {
		w.MaxHeldPerDomain = w.Concurrency
	}
	if w.Log == nil {
		w.Log = slog.Default()
	}
}

// Run blocks until ctx is cancelled, then hands held messages back to the
// queue and waits for in-flight deliveries to finish so that no SMTP session
// is cut in the middle.
func (w *Worker) Run(ctx context.Context) error {
	w.defaults()
	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()

	q := newReadyQueue()
	// Finished deliveries report their domain here: a freed session slot
	// means that domain may send again before the throttle's estimated wait.
	finished := make(chan string, w.Concurrency)
	defer func() {
		for _, m := range q.Drain() {
			w.requeue(ctx, m, "shutdown")
		}
		w.Metrics.Held.Set(0)
	}()

	// A held message is handed back well before its lease ends, so that the
	// delivery still has time to finish while the lease is ours.
	maxHold := w.Lease / 2

	for ctx.Err() == nil {
		drainFinished(finished, q)
		for _, m := range q.Expired(time.Now(), maxHold) {
			w.requeue(ctx, m, "lease")
		}
		launched := w.dispatch(ctx, q, sem, &wg, finished)

		claimed := 0
		free, room := cap(sem)-len(sem), w.MaxHeld-q.Len()
		if free > 0 && room > 0 {
			msgs, err := w.Store.Claim(ctx, min(free, room), w.Lease, q.Full(w.MaxHeldPerDomain))
			if err != nil {
				if ctx.Err() == nil {
					w.Log.Error("claim failed", "err", err)
					sleep(ctx, time.Second)
				}
				continue
			}
			now := time.Now()
			for _, m := range msgs {
				q.Push(m, now)
			}
			claimed = len(msgs)
			launched += w.dispatch(ctx, q, sem, &wg, finished)
		}
		w.Metrics.Held.Set(float64(q.Len()))

		if launched > 0 || claimed > 0 {
			continue
		}
		wait := w.PollInterval
		if free == 0 {
			wait = 20 * time.Millisecond
		}
		if d, ok := q.NextWake(time.Now()); ok && d < wait {
			wait = max(d, 5*time.Millisecond)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-t.C:
		case d := <-finished:
			q.Wake(d)
		}
		t.Stop()
	}
	return nil
}

func drainFinished(finished <-chan string, q *readyQueue) {
	for {
		select {
		case d := <-finished:
			q.Wake(d)
		default:
			return
		}
	}
}

// dispatch starts a delivery for every held message the throttle lets
// through, as long as there are free delivery slots.
func (w *Worker) dispatch(ctx context.Context, q *readyQueue, sem chan struct{}, wg *sync.WaitGroup, finished chan<- string) int {
	grants := q.Ready(time.Now(), cap(sem)-len(sem), w.Limiter.Acquire, func(d string) {
		w.Metrics.Throttled.WithLabelValues(d).Inc()
	})
	for _, g := range grants {
		sem <- struct{}{} // never blocks: Ready returned at most the free slots
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w.process(ctx, g.m, g.since.Add(w.Lease))
			g.release()
			select {
			case finished <- g.m.Domain:
			default: // the loop is already awake with others to handle
			}
		}()
	}
	return len(grants)
}

// requeue gives a claimed message back to Postgres without counting an
// attempt, so that any worker can pick it up right away.
func (w *Worker) requeue(ctx context.Context, m store.Message, reason string) {
	w.Metrics.Requeued.WithLabelValues(m.Domain, reason).Inc()
	if err := w.Store.Reschedule(context.WithoutCancel(ctx), m.ID, time.Now()); err != nil {
		w.Log.Error("requeue failed", "id", m.ID, "err", err)
	}
}

// process makes one delivery attempt and records it. leaseEnd is when the
// message's lease runs out; the SMTP session must be over by then, or another
// worker could start delivering the same message.
func (w *Worker) process(ctx context.Context, m store.Message, leaseEnd time.Time) {
	// Detach from ctx: on shutdown we still finish the attempt and record it.
	dctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), leaseEnd)
	defer cancel()

	start := time.Now()
	res := w.Client.Deliver(dctx, smtpclient.Message{
		ID: m.ID, From: m.From, To: m.To, Subject: m.Subject, Body: m.Body,
	})
	elapsed := time.Since(start)

	var v classify.Verdict
	if errors.Is(res.Err, smtpclient.ErrNoRoute) {
		v = classify.Verdict{Class: classify.Permanent, Reason: classify.ReasonNoRoute}
	} else {
		v = w.Classifier.Classify(m.Domain, res.Response)
	}

	attempt := store.Attempt{
		At: start, Host: res.Host, Code: res.Response.Code, Enhanced: res.Response.Enhanced,
		Response: describe(res), Class: string(v.Class), Reason: string(v.Reason),
		DurationMS: elapsed.Milliseconds(),
	}
	out := store.Outcome{Attempt: attempt, NextAttemptAt: time.Now()}
	attemptNo := m.Attempts + 1

	switch v.Class {
	case classify.Delivered:
		out.Status = store.StatusDelivered
		w.Limiter.OnSuccess(m.Domain)
	case classify.Transient:
		if v.Reason == classify.ReasonRateLimited {
			w.Limiter.OnRateLimited(m.Domain)
		}
		if next, ok := w.Backoff.Next(attemptNo, m.CreatedAt, time.Now(), rand.Float64()); ok {
			out.Status, out.NextAttemptAt, out.LastError = store.StatusDeferred, next, attempt.Response
		} else {
			out.Status = store.StatusBounced
			out.LastError = fmt.Sprintf("expired after %d attempts, last: %s", attemptNo, attempt.Response)
		}
	case classify.Permanent:
		out.Status, out.LastError = store.StatusBounced, attempt.Response
		if v.Suppress {
			out.SuppressReason = string(v.Reason)
		}
	}

	w.Metrics.Attempts.WithLabelValues(m.Domain, string(v.Class), string(v.Reason)).Inc()
	w.Metrics.Latency.WithLabelValues(m.Domain).Observe(elapsed.Seconds())

	// Recording the outcome gets its own timeout: a delivery that used up the
	// lease must still be written down.
	cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer ccancel()
	if err := w.Store.Complete(cctx, m, out); err != nil {
		w.Log.Error("complete failed", "id", m.ID, "err", err)
		return
	}
	w.Log.Info("attempt",
		"id", m.ID, "to", m.To, "attempt", attemptNo, "status", out.Status,
		"code", attempt.Code, "enhanced", attempt.Enhanced, "reason", v.Reason,
		"host", res.Host, "ms", attempt.DurationMS, "next", out.NextAttemptAt.Format(time.RFC3339))
}

func describe(res smtpclient.Result) string {
	if res.Response.Code != 0 {
		return strings.TrimSpace(fmt.Sprintf("%d %s %s", res.Response.Code, res.Response.Enhanced, res.Response.Text))
	}
	if res.Err != nil {
		return res.Err.Error()
	}
	return "unknown error"
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
