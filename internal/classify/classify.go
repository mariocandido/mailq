// Package classify turns SMTP replies into delivery verdicts: delivered,
// transient (retry later) or permanent (stop trying).
//
// The enhanced status code (RFC 3463) is preferred over the basic reply code
// because mailbox providers frequently use the same basic code (e.g. 550) for
// very different situations (unknown user vs. spam block).
package classify

import (
	"regexp"
	"strings"
)

// Class is the coarse outcome of a delivery attempt.
type Class string

const (
	Delivered Class = "delivered"
	Transient Class = "transient"
	Permanent Class = "permanent"
)

// Reason explains a verdict in more detail. It drives side effects such as
// slowing a domain down (rate_limited) or suppressing a recipient
// (user_unknown).
type Reason string

const (
	ReasonOK          Reason = "ok"
	ReasonRateLimited Reason = "rate_limited"
	ReasonMailboxFull Reason = "mailbox_full"
	ReasonTemporary   Reason = "temporary_failure"
	ReasonConnection  Reason = "connection_error"
	ReasonUserUnknown Reason = "user_unknown"
	ReasonPolicy      Reason = "policy_block"
	ReasonPermanent   Reason = "permanent_failure"
	ReasonNoRoute     Reason = "no_route"
)

// Response is what the remote server said. Code is 0 when no SMTP reply was
// obtained at all (DNS failure, connection refused, timeout...).
type Response struct {
	Code     int
	Enhanced string // e.g. "5.1.1"
	Text     string
}

// Verdict is the decision taken for one attempt.
type Verdict struct {
	Class    Class
	Reason   Reason
	Suppress bool // add the recipient to the suppression list
}

// Rule lets provider-specific responses override the generic logic.
type Rule struct {
	Domain   string // recipient domain; empty matches any domain
	Contains string // case-insensitive substring of "enhanced text"
	Verdict  Verdict
}

// DefaultRules encode a few well known provider responses.
var DefaultRules = []Rule{
	// Gmail: "421 4.7.28 ... unusual rate of unsolicited mail"
	{Domain: "gmail.com", Contains: "4.7.28", Verdict: Verdict{Class: Transient, Reason: ReasonRateLimited}},
	// Outlook / Hotmail: IP reputation based throttling.
	{Domain: "outlook.com", Contains: "4.7.650", Verdict: Verdict{Class: Transient, Reason: ReasonRateLimited}},
	{Domain: "hotmail.com", Contains: "4.7.650", Verdict: Verdict{Class: Transient, Reason: ReasonRateLimited}},
	// Yahoo: "421 4.7.0 [TSS04] Messages ... temporarily deferred".
	{Domain: "yahoo.com", Contains: "TSS04", Verdict: Verdict{Class: Transient, Reason: ReasonRateLimited}},
}

// Classifier applies rules first and then generic RFC based logic.
type Classifier struct {
	rules []Rule
}

// New returns a classifier with the given provider rules.
func New(rules ...Rule) *Classifier { return &Classifier{rules: rules} }

// Default returns a classifier with DefaultRules.
func Default() *Classifier { return New(DefaultRules...) }

var enhancedRe = regexp.MustCompile(`^[245]\.\d{1,3}\.\d{1,3}\b`)

// ParseEnhanced splits "5.1.1 User unknown" into ("5.1.1", "User unknown").
func ParseEnhanced(msg string) (enhanced, text string) {
	msg = strings.TrimSpace(msg)
	if m := enhancedRe.FindString(msg); m != "" {
		return m, strings.TrimSpace(msg[len(m):])
	}
	return "", msg
}

// Classify decides what to do with a response from a server for domain.
func (c *Classifier) Classify(domain string, r Response) Verdict {
	hay := strings.ToLower(r.Enhanced + " " + r.Text)
	if r.Code != 0 {
		for _, rule := range c.rules {
			if rule.Domain != "" && !strings.EqualFold(rule.Domain, domain) {
				continue
			}
			if strings.Contains(hay, strings.ToLower(rule.Contains)) {
				return rule.Verdict
			}
		}
	}

	switch {
	case r.Code >= 200 && r.Code < 300:
		return Verdict{Class: Delivered, Reason: ReasonOK}
	case r.Code >= 400 && r.Code < 500:
		return transient(r, hay)
	case r.Code >= 500 && r.Code < 600:
		return permanent(r, hay)
	}
	// No SMTP reply at all: network problem, always worth retrying.
	return Verdict{Class: Transient, Reason: ReasonConnection}
}

func transient(r Response, hay string) Verdict {
	switch {
	// 421 means "service not available, closing channel" and is what most
	// providers use to push back. 4.7.x is "security or policy status", which
	// in practice is mostly throttling and greylisting: slowing down is the
	// safe reaction in both cases.
	case r.Code == 421, strings.HasPrefix(r.Enhanced, "4.7."),
		containsAny(hay, "rate limit", "rate-limit", "too many", "throttl", "unusual rate"):
		return Verdict{Class: Transient, Reason: ReasonRateLimited}
	case r.Enhanced == "4.2.2", containsAny(hay, "mailbox full", "over quota", "quota exceeded"):
		return Verdict{Class: Transient, Reason: ReasonMailboxFull}
	}
	return Verdict{Class: Transient, Reason: ReasonTemporary}
}

// Enhanced codes that say the recipient address itself is bad.
var badRecipient = map[string]bool{
	"5.1.1":  true, // bad destination mailbox
	"5.1.2":  true, // bad destination system (domain)
	"5.1.3":  true, // bad destination mailbox address syntax
	"5.1.6":  true, // mailbox has moved
	"5.1.10": true, // recipient address has null MX
}

func permanent(r Response, hay string) Verdict {
	switch {
	// A full mailbox is usually temporary even when reported with 5xx, so it
	// is treated as a soft bounce and retried until the message expires.
	case r.Enhanced == "5.2.2", containsAny(hay, "mailbox full", "over quota", "quota exceeded"):
		return Verdict{Class: Transient, Reason: ReasonMailboxFull}
	case badRecipient[r.Enhanced],
		r.Enhanced == "" && containsAny(hay, "user unknown", "no such user", "does not exist", "unknown recipient", "recipient not found"):
		return Verdict{Class: Permanent, Reason: ReasonUserUnknown, Suppress: true}
	// Policy blocks are about the sender/IP/content, not the recipient, so
	// the recipient is NOT suppressed.
	case strings.HasPrefix(r.Enhanced, "5.7."), containsAny(hay, "spam", "policy", "blocked", "blocklist", "blacklist"):
		return Verdict{Class: Permanent, Reason: ReasonPolicy}
	}
	return Verdict{Class: Permanent, Reason: ReasonPermanent}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
