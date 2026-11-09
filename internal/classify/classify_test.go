package classify

import "testing"

func TestParseEnhanced(t *testing.T) {
	cases := []struct{ in, enh, text string }{
		{"5.1.1 <a@b>: User unknown", "5.1.1", "<a@b>: User unknown"},
		{"4.7.28 Our system has detected", "4.7.28", "Our system has detected"},
		{"Mailbox unavailable", "", "Mailbox unavailable"},
		{"  2.0.0 OK", "2.0.0", "OK"},
		{"5.1.10 null MX", "5.1.10", "null MX"},
	}
	for _, c := range cases {
		enh, text := ParseEnhanced(c.in)
		if enh != c.enh || text != c.text {
			t.Errorf("ParseEnhanced(%q) = (%q, %q), want (%q, %q)", c.in, enh, text, c.enh, c.text)
		}
	}
}

func TestClassify(t *testing.T) {
	c := Default()
	cases := []struct {
		name     string
		domain   string
		resp     Response
		class    Class
		reason   Reason
		suppress bool
	}{
		{"accepted", "x.com", Response{250, "2.0.0", "OK"}, Delivered, ReasonOK, false},
		{"no reply", "x.com", Response{}, Transient, ReasonConnection, false},
		{"421 closing", "x.com", Response{421, "", "Service not available"}, Transient, ReasonRateLimited, false},
		{"gmail rate", "gmail.com", Response{421, "4.7.28", "unusual rate"}, Transient, ReasonRateLimited, false},
		{"451 with 4.7.x", "x.com", Response{451, "4.7.1", "greylisted"}, Transient, ReasonRateLimited, false},
		{"yahoo TSS04", "yahoo.com", Response{451, "4.7.0", "[TSS04] deferred"}, Transient, ReasonRateLimited, false},
		{"mailbox full 452", "x.com", Response{452, "4.2.2", "Mailbox full"}, Transient, ReasonMailboxFull, false},
		{"mailbox full 552 is soft", "x.com", Response{552, "5.2.2", "Mailbox full"}, Transient, ReasonMailboxFull, false},
		{"temp failure", "x.com", Response{451, "4.3.0", "local problem"}, Transient, ReasonTemporary, false},
		{"user unknown", "x.com", Response{550, "5.1.1", "User unknown"}, Permanent, ReasonUserUnknown, true},
		{"user unknown no enhanced", "x.com", Response{550, "", "No such user here"}, Permanent, ReasonUserUnknown, true},
		{"spam block", "x.com", Response{550, "5.7.1", "rejected as spam"}, Permanent, ReasonPolicy, false},
		{"550 5.7.1 mentioning user", "x.com", Response{550, "5.7.1", "user unknown? no, blocked"}, Permanent, ReasonPolicy, false},
		{"bad sender is not the recipient's fault", "x.com", Response{550, "5.1.8", "bad sender"}, Permanent, ReasonPermanent, false},
		{"generic 554", "x.com", Response{554, "5.0.0", "transaction failed"}, Permanent, ReasonPermanent, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := c.Classify(tc.domain, tc.resp)
			if v.Class != tc.class || v.Reason != tc.reason || v.Suppress != tc.suppress {
				t.Errorf("got %+v, want class=%s reason=%s suppress=%v", v, tc.class, tc.reason, tc.suppress)
			}
		})
	}
}

func TestRulesAreDomainScoped(t *testing.T) {
	c := New(Rule{Domain: "example.com", Contains: "magic", Verdict: Verdict{Class: Permanent, Reason: ReasonPolicy}})
	if v := c.Classify("other.com", Response{451, "4.3.0", "magic"}); v.Reason != ReasonTemporary {
		t.Errorf("rule leaked to another domain: %+v", v)
	}
	if v := c.Classify("EXAMPLE.com", Response{451, "4.3.0", "Magic"}); v.Reason != ReasonPolicy {
		t.Errorf("rule not applied: %+v", v)
	}
}
