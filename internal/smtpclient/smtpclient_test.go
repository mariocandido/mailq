package smtpclient

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mariocandido/mailq/internal/fakesmtp"
)

func startFake(t *testing.T, srv *fakesmtp.Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln) //nolint:errcheck
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

func newClient(addr string) *Client {
	return &Client{
		Router:   &StaticRouter{Routes: map[string]string{"fake.test": addr}},
		HeloName: "test.local",
		Timeout:  5 * time.Second,
	}
}

func TestDeliverResponses(t *testing.T) {
	srv := &fakesmtp.Server{FlakyFailures: 1}
	c := newClient(startFake(t, srv))
	ctx := context.Background()

	cases := []struct {
		to       string
		code     int
		enhanced string
	}{
		{"ok@fake.test", 250, "2.0.0"},
		{"bounce@fake.test", 550, "5.1.1"},
		{"block@fake.test", 550, "5.7.1"},
		{"full@fake.test", 452, "4.2.2"},
		{"defer@fake.test", 451, "4.3.0"},
		{"flaky@fake.test", 451, "4.4.2"},
		{"flaky@fake.test", 250, "2.0.0"}, // second try succeeds
	}
	for _, tc := range cases {
		res := c.Deliver(ctx, Message{ID: "1", From: "me@test.local", To: tc.to, Subject: "hi", Body: "body"})
		if res.Response.Code != tc.code || res.Response.Enhanced != tc.enhanced {
			t.Errorf("%s: got %+v (err %v), want %d %s", tc.to, res.Response, res.Err, tc.code, tc.enhanced)
		}
	}
	if got := srv.Delivered(); got != 2 {
		t.Errorf("fake server accepted %d messages, want 2", got)
	}
}

func TestRateLimit421(t *testing.T) {
	c := newClient(startFake(t, &fakesmtp.Server{RatePerSec: 2}))
	var codes []int
	for range 3 {
		res := c.Deliver(context.Background(), Message{ID: "1", From: "me@test.local", To: "x@fake.test", Subject: "s", Body: "b"})
		codes = append(codes, res.Response.Code)
	}
	if codes[0] != 250 || codes[1] != 250 || codes[2] != 421 {
		t.Fatalf("codes %v, want [250 250 421]", codes)
	}
}

func TestNoRoute(t *testing.T) {
	c := newClient("127.0.0.1:1")
	res := c.Deliver(context.Background(), Message{To: "a@elsewhere.test"})
	if !errors.Is(res.Err, ErrNoRoute) {
		t.Fatalf("want ErrNoRoute, got %v", res.Err)
	}
}

func TestConnectionRefusedIsNetworkError(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more
	res := newClient(addr).Deliver(context.Background(), Message{From: "a@b.c", To: "x@fake.test"})
	if res.Response.Code != 0 || res.Err == nil {
		t.Fatalf("want a network error, got %+v", res)
	}
}

func TestBuild(t *testing.T) {
	raw := string(Build(Message{ID: "abc", From: "a@x.test", To: "b@y.test", Subject: "Olá", Body: "linha 1\nção"},
		"mailq.local", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
	for _, want := range []string{
		"Message-ID: <abc@mailq.local>\r\n",
		"Subject: =?utf-8?q?Ol=C3=A1?=\r\n",
		"Content-Transfer-Encoding: quoted-printable\r\n",
		"\r\n\r\nlinha 1",
		"=C3=A7=C3=A3o",
	} {
		if !strings.Contains(raw, want) {
			t.Errorf("message missing %q:\n%s", want, raw)
		}
	}
}
