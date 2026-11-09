// Package fakesmtp is a tiny SMTP server that misbehaves on purpose, so that
// every delivery path of mailq can be exercised locally without touching the
// real internet. Behaviour depends on the recipient's local part:
//
//	bounce*   550 5.1.1 user unknown         (hard bounce, suppressed)
//	block*    550 5.7.1 rejected as spam     (policy block, not suppressed)
//	full*     452 4.2.2 mailbox full         (soft bounce, retried)
//	defer*    451 4.3.0 temporary failure    (retried until it expires)
//	flaky*    451 the first N times, then accepted
//	anything else is accepted
//
// When more than RatePerSec recipients are accepted within one second the
// server answers 421 4.7.28, like Gmail does, and closes the connection.
package fakesmtp

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server is the fake SMTP server.
type Server struct {
	RatePerSec    int // 0 disables the rate limit
	FlakyFailures int // failures before a flaky* recipient is accepted
	Log           *slog.Logger

	mu        sync.Mutex
	flaky     map[string]int
	accepted  []time.Time
	delivered atomic.Int64
}

// Delivered returns how many messages were accepted (DATA completed).
func (s *Server) Delivered() int64 { return s.delivered.Load() }

func (s *Server) log() *slog.Logger {
	if s.Log == nil {
		return slog.Default()
	}
	return s.Log
}

// ListenAndServe listens on addr and serves until the listener fails.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until it is closed.
func (s *Server) Serve(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	tp := textproto.NewConn(c)
	reply := func(format string, args ...any) { _ = tp.PrintfLine(format, args...) }

	reply("220 fakesmtp ESMTP ready")
	var from string
	var rcpts []string
	for {
		_ = c.SetDeadline(time.Now().Add(time.Minute))
		line, err := tp.ReadLine()
		if err != nil {
			return
		}
		cmd, arg, _ := strings.Cut(line, " ")
		switch strings.ToUpper(cmd) {
		case "EHLO":
			reply("250-fakesmtp greets %s", arg)
			reply("250-8BITMIME")
			reply("250 SIZE 10485760")
		case "HELO":
			reply("250 fakesmtp")
		case "MAIL":
			from, rcpts = extractAddr(arg), nil
			reply("250 2.1.0 OK")
		case "RCPT":
			addr := extractAddr(arg)
			code, text := s.decide(addr)
			reply("%d %s", code, text)
			if code == 421 {
				return // 421 means the server closes the channel
			}
			if code == 250 {
				rcpts = append(rcpts, addr)
			}
		case "DATA":
			if len(rcpts) == 0 {
				reply("554 5.5.1 Error: no valid recipients")
				continue
			}
			reply("354 End data with <CR><LF>.<CR><LF>")
			data, err := tp.ReadDotBytes()
			if err != nil {
				return
			}
			s.delivered.Add(int64(len(rcpts)))
			s.log().Info("fakesmtp accepted", "from", from, "to", rcpts, "bytes", len(data))
			reply("250 2.0.0 OK queued as fake-%d", time.Now().UnixNano())
			rcpts = nil
		case "RSET":
			from, rcpts = "", nil
			reply("250 2.0.0 OK")
		case "NOOP":
			reply("250 2.0.0 OK")
		case "QUIT":
			reply("221 2.0.0 Bye")
			return
		default:
			reply("502 5.5.2 Command not recognized")
		}
	}
}

func (s *Server) decide(addr string) (int, string) {
	local, _, _ := strings.Cut(strings.ToLower(addr), "@")
	switch {
	case strings.HasPrefix(local, "bounce"):
		return 550, fmt.Sprintf("5.1.1 <%s>: Recipient address rejected: User unknown", addr)
	case strings.HasPrefix(local, "block"):
		return 550, "5.7.1 Message rejected due to spam content policy"
	case strings.HasPrefix(local, "full"):
		return 452, "4.2.2 Mailbox full, try again later"
	case strings.HasPrefix(local, "defer"):
		return 451, "4.3.0 Temporary local problem, try again later"
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.HasPrefix(local, "flaky") {
		if s.flaky == nil {
			s.flaky = map[string]int{}
		}
		if s.flaky[addr] < s.FlakyFailures {
			s.flaky[addr]++
			return 451, fmt.Sprintf("4.4.2 Temporary failure %d/%d, try again later", s.flaky[addr], s.FlakyFailures)
		}
	}
	if s.RatePerSec > 0 {
		now := time.Now()
		cut := 0
		for cut < len(s.accepted) && now.Sub(s.accepted[cut]) > time.Second {
			cut++
		}
		s.accepted = s.accepted[cut:]
		if len(s.accepted) >= s.RatePerSec {
			return 421, "4.7.28 Our system has detected an unusual rate of unsolicited mail, try again later"
		}
		s.accepted = append(s.accepted, now)
	}
	return 250, "2.1.5 OK"
}

// extractAddr turns "TO:<a@b> BODY=8BITMIME" into "a@b".
func extractAddr(arg string) string {
	if i := strings.IndexByte(arg, '<'); i >= 0 {
		if j := strings.IndexByte(arg[i:], '>'); j > 0 {
			return arg[i+1 : i+j]
		}
	}
	_, v, _ := strings.Cut(arg, ":")
	return strings.TrimSpace(v)
}
