// Command mailq runs the HTTP API and the delivery workers.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/mariocandido/mailq/internal/api"
	"github.com/mariocandido/mailq/internal/backoff"
	"github.com/mariocandido/mailq/internal/classify"
	"github.com/mariocandido/mailq/internal/metrics"
	"github.com/mariocandido/mailq/internal/smtpclient"
	"github.com/mariocandido/mailq/internal/store"
	"github.com/mariocandido/mailq/internal/throttle"
	"github.com/mariocandido/mailq/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

type config struct {
	DatabaseURL string
	HTTPAddr    string
	Helo        string
	Routes      map[string]string
	AllowMX     bool
	TLSInsecure bool
	Concurrency int
	SMTPTimeout time.Duration
	Backoff     backoff.Policy
	Throttle    throttle.Config
}

func loadConfig() (config, error) {
	var c config
	var err error
	c.DatabaseURL = env("MAILQ_DATABASE_URL", "postgres://mailq:mailq@localhost:5432/mailq?sslmode=disable")
	c.HTTPAddr = env("MAILQ_HTTP_ADDR", ":8080")
	c.Helo = env("MAILQ_HELO", "mailq.local")
	if c.Routes, err = parseMap(env("MAILQ_ROUTES", "mailpit.test=localhost:1025,fake.test=localhost:2525")); err != nil {
		return c, fmt.Errorf("MAILQ_ROUTES: %w", err)
	}
	c.AllowMX = env("MAILQ_ALLOW_MX", "false") == "true"
	c.TLSInsecure = env("MAILQ_TLS_INSECURE", "false") == "true"
	if c.Concurrency, err = strconv.Atoi(env("MAILQ_CONCURRENCY", "16")); err != nil {
		return c, fmt.Errorf("MAILQ_CONCURRENCY: %w", err)
	}
	if c.SMTPTimeout, err = time.ParseDuration(env("MAILQ_SMTP_TIMEOUT", "30s")); err != nil {
		return c, fmt.Errorf("MAILQ_SMTP_TIMEOUT: %w", err)
	}

	c.Backoff = backoff.Default()
	if s := os.Getenv("MAILQ_RETRY_SCHEDULE"); s != "" {
		if c.Backoff.Schedule, err = backoff.ParseSchedule(s); err != nil {
			return c, fmt.Errorf("MAILQ_RETRY_SCHEDULE: %w", err)
		}
	}
	if s := os.Getenv("MAILQ_MAX_AGE"); s != "" {
		if c.Backoff.MaxAge, err = time.ParseDuration(s); err != nil {
			return c, fmt.Errorf("MAILQ_MAX_AGE: %w", err)
		}
	}

	if c.Throttle.Rate, err = strconv.ParseFloat(env("MAILQ_DOMAIN_RATE", "5"), 64); err != nil {
		return c, fmt.Errorf("MAILQ_DOMAIN_RATE: %w", err)
	}
	if c.Throttle.Burst, err = strconv.ParseFloat(env("MAILQ_DOMAIN_BURST", "5"), 64); err != nil {
		return c, fmt.Errorf("MAILQ_DOMAIN_BURST: %w", err)
	}
	if c.Throttle.MaxConcurrent, err = strconv.Atoi(env("MAILQ_DOMAIN_CONCURRENCY", "4")); err != nil {
		return c, fmt.Errorf("MAILQ_DOMAIN_CONCURRENCY: %w", err)
	}
	overrides, err := parseMap(env("MAILQ_DOMAIN_RATES", ""))
	if err != nil {
		return c, fmt.Errorf("MAILQ_DOMAIN_RATES: %w", err)
	}
	c.Throttle.Overrides = map[string]float64{}
	for d, v := range overrides {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return c, fmt.Errorf("MAILQ_DOMAIN_RATES: %s: %w", d, err)
		}
		c.Throttle.Overrides[d] = f
	}
	return c, nil
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)
	lim := throttle.New(cfg.Throttle)

	wk := &worker.Worker{
		Store: st,
		Client: &smtpclient.Client{
			Router:      &smtpclient.StaticRouter{Routes: cfg.Routes, AllowMX: cfg.AllowMX},
			HeloName:    cfg.Helo,
			Timeout:     cfg.SMTPTimeout,
			TLSInsecure: cfg.TLSInsecure,
		},
		Classifier:  classify.Default(),
		Limiter:     lim,
		Backoff:     cfg.Backoff,
		Metrics:     m,
		Log:         log,
		Concurrency: cfg.Concurrency,
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           (&api.Server{Store: st, Metrics: m, Gatherer: reg, Log: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go refreshGauges(ctx, st, lim, m, log)

	workerDone := make(chan error, 1)
	go func() { workerDone <- wk.Run(ctx) }()
	httpErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	log.Info("mailq started", "http", cfg.HTTPAddr, "routes", cfg.Routes, "allow_mx", cfg.AllowMX,
		"retry_schedule", fmt.Sprint(cfg.Backoff.Schedule), "max_age", cfg.Backoff.MaxAge.String())

	select {
	case <-ctx.Done():
	case err := <-httpErr:
		stop()
		return err
	}

	log.Info("shutting down: finishing in-flight deliveries")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	select {
	case <-workerDone:
	case <-shutdownCtx.Done():
		log.Warn("gave up waiting for in-flight deliveries; their leases will expire and be retried")
	}
	return nil
}

func refreshGauges(ctx context.Context, st *store.Store, lim *throttle.Limiter, m *metrics.Metrics, log *slog.Logger) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		depth, err := st.QueueDepth(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("queue depth failed", "err", err)
			}
			continue
		}
		for _, s := range []string{store.StatusQueued, store.StatusSending, store.StatusDeferred,
			store.StatusDelivered, store.StatusBounced, store.StatusSuppressed} {
			m.Queue.WithLabelValues(s).Set(float64(depth[s]))
		}
		for d, r := range lim.Rates() {
			m.DomainRate.WithLabelValues(d).Set(r)
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// parseMap parses "a=b,c=d".
func parseMap(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("invalid entry %q, expected key=value", pair)
		}
		out[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}
	return out, nil
}
