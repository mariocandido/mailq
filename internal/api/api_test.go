package api

import "testing"

func TestValidate(t *testing.T) {
	ok := SendRequest{From: "Me <me@x.test>", To: []string{"A@Fake.Test", "a@fake.test", "b@y.test"}, Subject: "s", Text: "t"}
	msgs, err := Validate(ok)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("duplicates must collapse: %+v", msgs)
	}
	if msgs[0].To != "a@fake.test" || msgs[0].Domain != "fake.test" || msgs[0].From != "me@x.test" {
		t.Fatalf("normalisation: %+v", msgs[0])
	}

	bad := []SendRequest{
		{From: "nope", To: []string{"a@b.c"}, Text: "t"},
		{From: "a@b.c", To: nil, Text: "t"},
		{From: "a@b.c", To: []string{"not an address"}, Text: "t"},
		{From: "a@b.c", To: []string{"a@b.c"}, Subject: "x\r\nBcc: evil@x", Text: "t"},
		{From: "a@b.c", To: []string{"a@b.c"}, Text: "  "},
	}
	for i, r := range bad {
		if _, err := Validate(r); err == nil {
			t.Errorf("case %d should fail: %+v", i, r)
		}
	}
}
