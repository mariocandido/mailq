// Command fakesmtp runs a misbehaving SMTP server for local testing.
// See internal/fakesmtp for the recipient conventions.
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/mariocandido/mailq/internal/fakesmtp"
)

func main() {
	addr := flag.String("addr", ":2525", "listen address")
	rate := flag.Int("rate", 5, "accepted recipients per second before answering 421 (0 = unlimited)")
	flaky := flag.Int("flaky", 2, "failures before a flaky* recipient is accepted")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv := &fakesmtp.Server{RatePerSec: *rate, FlakyFailures: *flaky, Log: log}
	log.Info("fakesmtp listening", "addr", *addr, "rate", *rate, "flaky", *flaky)
	if err := srv.ListenAndServe(*addr); err != nil {
		log.Error("fakesmtp stopped", "err", err)
		os.Exit(1)
	}
}
