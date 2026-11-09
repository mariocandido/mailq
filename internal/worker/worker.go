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
	if w.Log == nil {
		w.Log = slog.Default()
	}
}

// Run blocks until ctx is cancelled, then waits for in-flight deliveries to
// finish so that no SMTP session is cut in the middle.
func (w *Worker) Run(ctx context.Context) error {
	w.defaults()
	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()

	for ctx.Err() == nil {
		free := cap(sem) - len(sem)
		if free == 0 {
			sleep(ctx, 20*time.Millisecond)
			continue
		}
		msgs, err := w.Store.Claim(ctx, free, w.Lease)
		if err != nil {
			if ctx.Err() == nil {
				w.Log.Error("claim failed", "err", err)
				sleep(ctx, time.Second)
			}
			continue
		}
		if len(msgs) == 0 {
			sleep(ctx, w.PollInterval)
			continue
		}
		for _, m := range msgs {
			release, wait, ok := w.Limiter.Acquire(m.Domain)
			if !ok {
				w.Metrics.Throttled.WithLabelValues(m.Domain).Inc()
				if err := w.Store.Reschedule(context.WithoutCancel(ctx), m.ID, time.Now().Add(wait)); err != nil {
					w.Log.Error("reschedule failed", "id", m.ID, "err", err)
				}
				continue
			}
			sem <- struct{}{} // never blocks: we claimed at most `free` messages
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				defer release()
				w.process(ctx, m)
			}()
		}
	}
	return nil
}

func (w *Worker) process(ctx context.Context, m store.Message) {
	// Detach from ctx: on shutdown we still finish the attempt and record it.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.Lease)
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

	if err := w.Store.Complete(dctx, m, out); err != nil {
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
