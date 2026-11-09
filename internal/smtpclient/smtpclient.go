// Package smtpclient delivers one message to the mail server responsible for
// the recipient's domain and reports exactly what that server answered.
package smtpclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"

	"github.com/mariocandido/mailq/internal/classify"
)

// ErrNoRoute means there is no server to deliver to for a domain.
var ErrNoRoute = errors.New("no route to domain")

// Router returns the servers (host:port) to try for a domain, in order.
type Router interface {
	Hosts(ctx context.Context, domain string) ([]string, error)
}

// StaticRouter uses fixed routes first and, when allowed, falls back to the
// domain's MX records. MX delivery is off by default so that a local demo can
// never send real mail by accident.
type StaticRouter struct {
	Routes   map[string]string // domain -> host:port
	AllowMX  bool
	Port     string // port used for MX hosts, default "25"
	Resolver *net.Resolver
}

// Hosts implements Router.
func (r *StaticRouter) Hosts(ctx context.Context, domain string) ([]string, error) {
	domain = strings.ToLower(domain)
	if h, ok := r.Routes[domain]; ok {
		return []string{h}, nil
	}
	if !r.AllowMX {
		return nil, fmt.Errorf("%w: %s (MX delivery disabled)", ErrNoRoute, domain)
	}
	res := r.Resolver
	if res == nil {
		res = net.DefaultResolver
	}
	port := r.Port
	if port == "" {
		port = "25"
	}

	mxs, err := res.LookupMX(ctx, domain)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			// RFC 5321 5.1: no MX records means the domain itself is the
			// implicit MX, as long as it resolves.
			if _, herr := res.LookupHost(ctx, domain); herr == nil {
				return []string{net.JoinHostPort(domain, port)}, nil
			}
			return nil, fmt.Errorf("%w: %s: %v", ErrNoRoute, domain, err)
		}
		return nil, err // temporary DNS failure: retry later
	}
	// RFC 7505 null MX: the domain explicitly accepts no mail.
	if len(mxs) == 1 && (mxs[0].Host == "." || mxs[0].Host == "") {
		return nil, fmt.Errorf("%w: %s publishes a null MX", ErrNoRoute, domain)
	}
	hosts := make([]string, 0, len(mxs))
	for _, mx := range mxs { // already sorted by preference
		hosts = append(hosts, net.JoinHostPort(strings.TrimSuffix(mx.Host, "."), port))
	}
	return hosts, nil
}

// Client delivers messages over SMTP.
type Client struct {
	Router      Router
	HeloName    string
	Timeout     time.Duration // whole SMTP session per host
	TLSInsecure bool          // skip certificate checks on STARTTLS (testing only)
}

// Message is what gets delivered.
type Message struct {
	ID, From, To, Subject, Body string
}

// Result describes one delivery attempt. Response.Code is 0 when no server
// answered; Err then explains why.
type Result struct {
	Host     string
	Response classify.Response
	Err      error
}

// DomainOf returns the lower-cased domain of an address.
func DomainOf(addr string) string {
	if i := strings.LastIndexByte(addr, '@'); i >= 0 {
		return strings.ToLower(addr[i+1:])
	}
	return ""
}

func (c *Client) helo() string {
	if c.HeloName == "" {
		return "localhost"
	}
	return c.HeloName
}

func (c *Client) timeout() time.Duration {
	if c.Timeout <= 0 {
		return 30 * time.Second
	}
	return c.Timeout
}

// Deliver tries each host for the recipient's domain until one answers.
// Once a server gives an SMTP reply, that reply is final for this attempt;
// other MX hosts are only tried when a host cannot be reached at all.
func (c *Client) Deliver(ctx context.Context, m Message) Result {
	hosts, err := c.Router.Hosts(ctx, DomainOf(m.To))
	if err != nil {
		return Result{Err: err}
	}
	var last Result
	for _, h := range hosts {
		last = c.deliverTo(ctx, h, m)
		if last.Response.Code != 0 || ctx.Err() != nil {
			return last
		}
	}
	return last
}

func (c *Client) deliverTo(ctx context.Context, addr string, m Message) Result {
	res := Result{Host: addr}

	dialer := net.Dialer{Timeout: c.timeout()}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		res.Err = err
		return res
	}
	defer conn.Close()

	deadline := time.Now().Add(c.timeout())
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)

	host, _, _ := net.SplitHostPort(addr)
	cl, err := smtp.NewClient(conn, host) // reads the 220 greeting
	if err != nil {
		return withErr(res, err)
	}
	defer cl.Close()

	if err := cl.Hello(c.helo()); err != nil {
		return withErr(res, err)
	}
	// Opportunistic TLS, as done between MTAs.
	if ok, _ := cl.Extension("STARTTLS"); ok {
		if err := cl.StartTLS(&tls.Config{ServerName: host, InsecureSkipVerify: c.TLSInsecure}); err != nil {
			return withErr(res, err)
		}
	}
	if err := cl.Mail(m.From); err != nil {
		return withErr(res, err)
	}
	if err := cl.Rcpt(m.To); err != nil {
		return withErr(res, err)
	}
	w, err := cl.Data()
	if err != nil {
		return withErr(res, err)
	}
	if _, err := w.Write(Build(m, c.helo(), time.Now())); err != nil {
		return withErr(res, err)
	}
	if err := w.Close(); err != nil { // reads the reply to the final "."
		return withErr(res, err)
	}
	_ = cl.Quit()

	res.Response = classify.Response{Code: 250, Enhanced: "2.0.0", Text: "message accepted"}
	return res
}

// withErr turns an SMTP error reply into a Response, or keeps a network
// error as Err.
func withErr(res Result, err error) Result {
	var tp *textproto.Error
	if errors.As(err, &tp) {
		enh, text := classify.ParseEnhanced(tp.Msg)
		res.Response = classify.Response{Code: tp.Code, Enhanced: enh, Text: text}
		return res
	}
	res.Err = err
	return res
}

// Build renders a plain-text RFC 5322 message.
func Build(m Message, helo string, now time.Time) []byte {
	var b bytes.Buffer
	header := func(k, v string) { fmt.Fprintf(&b, "%s: %s\r\n", k, v) }
	header("From", m.From)
	header("To", m.To)
	header("Subject", mime.QEncoding.Encode("utf-8", m.Subject))
	header("Date", now.UTC().Format(time.RFC1123Z))
	header("Message-ID", fmt.Sprintf("<%s@%s>", m.ID, helo))
	header("MIME-Version", "1.0")
	header("Content-Type", `text/plain; charset="utf-8"`)
	header("Content-Transfer-Encoding", "quoted-printable")
	b.WriteString("\r\n")
	qp := quotedprintable.NewWriter(&b)
	_, _ = qp.Write([]byte(m.Body))
	_ = qp.Close()
	return b.Bytes()
}
