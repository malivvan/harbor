package ledger

import (
	"encoding/json"
	"strings"
	"testing"
)

// The output format is a wire contract: signatures are taken over these bytes, so
// a change here invalidates every signature produced by an earlier build. These
// tests pin the exact bytes.

func mustBytes(t *testing.T, v any) string {
	t.Helper()
	b, err := canonicalBytes(v)
	if err != nil {
		t.Fatalf("canonicalBytes: %v", err)
	}
	return string(b)
}

func TestSortedKeysNoWhitespace(t *testing.T) {
	got := mustBytes(t, map[string]any{"b": 1, "a": 2, "c": 3})
	want := `{"a":2,"b":1,"c":3}`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestNestedSortingIsRecursive(t *testing.T) {
	got := mustBytes(t, map[string]any{
		"z": map[string]any{"y": 1, "x": 2},
		"a": []any{
			map[string]any{"q": 1, "p": 2},
			"plain",
		},
	})
	want := `{"a":[{"p":2,"q":1},"plain"],"z":{"x":2,"y":1}}`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// TestNumberTextIsPreserved: 1, 1.0 and 1e0 are the same float64 but must not be
// silently rewritten, or a signature could be computed over a different value than
// the one that was sent.
func TestNumberTextIsPreserved(t *testing.T) {
	cases := []struct{ in, want string }{
		{`{"n":1}`, `{"n":1}`},
		{`{"n":1.0}`, `{"n":1.0}`},
		{`{"n":1e0}`, `{"n":1e0}`},
		{`{"n":12345678901234567890}`, `{"n":12345678901234567890}`},
		{`{"n":-0.000001}`, `{"n":-0.000001}`},
	}
	for _, c := range cases {
		got, err := fromRaw(json.RawMessage(c.in))
		if err != nil {
			t.Fatalf("fromRaw(%s): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("fromRaw(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestScalarsAndNull(t *testing.T) {
	cases := []struct{ in, want string }{
		{`null`, `null`},
		{`true`, `true`},
		{`false`, `false`},
		{`"hi"`, `"hi"`},
		{`[]`, `[]`},
		{`{}`, `{}`},
	}
	for _, c := range cases {
		got, err := fromRaw(json.RawMessage(c.in))
		if err != nil {
			t.Fatalf("fromRaw(%s): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("fromRaw(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestUnicodeAndEscaping locks the string escaping so signatures over non-ASCII
// content stay stable.
func TestUnicodeAndEscaping(t *testing.T) {
	got := mustBytes(t, map[string]any{"k": "héllo — 世界 \"quoted\"\n"})
	want := `{"k":"héllo — 世界 \"quoted\"\n"}`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// TestStructOfStructs is the realistic case: signing a struct, not a map.
func TestStructOfStructs(t *testing.T) {
	type inner struct {
		Beta  string `json:"beta"`
		Alpha int    `json:"alpha"`
	}
	type outer struct {
		Zulu  inner   `json:"zulu"`
		Alpha []int   `json:"alpha"`
		Ptr   *string `json:"ptr"`
	}
	got := mustBytes(t, outer{Zulu: inner{Beta: "b", Alpha: 1}, Alpha: []int{3, 1, 2}})
	// Struct field order follows the JSON tags' declaration order for structs,
	// but object keys of a decoded map are sorted; the decode step normalises both.
	want := `{"alpha":[3,1,2],"ptr":null,"zulu":{"alpha":1,"beta":"b"}}`
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// TestDeterministicAcrossRuns is the whole point: repeated encodings of the same
// value must be identical even though Go map iteration is randomised.
func TestDeterministicAcrossRuns(t *testing.T) {
	v := map[string]any{}
	for i := 0; i < 40; i++ {
		v[string(rune('a'+i%26))+string(rune('0'+i%10))] = i
	}
	first := mustBytes(t, v)
	for i := 0; i < 50; i++ {
		if got := mustBytes(t, v); got != first {
			t.Fatalf("run %d differs:\n first: %s\n got:   %s", i, first, got)
		}
	}
}

// TestPreEncodedInputIsCanonicalised: a json.RawMessage with unsorted keys must
// not pass through untouched.
func TestPreEncodedInputIsCanonicalised(t *testing.T) {
	raw := json.RawMessage(`{"z":1,"a":2}`)
	got, err := fromRaw(raw)
	if err != nil {
		t.Fatalf("fromRaw: %v", err)
	}
	if string(got) != `{"a":2,"z":1}` {
		t.Fatalf("got %s, want %s", got, `{"a":2,"z":1}`)
	}
}

func TestInvalidInputErrors(t *testing.T) {
	if _, err := canonicalBytes(func() {}); err == nil {
		t.Fatal("Bytes of an unmarshallable value: want an error")
	}
	if _, err := fromRaw(json.RawMessage(`{`)); err == nil {
		t.Fatal("fromRaw of invalid JSON: want an error")
	}
}

// TestNoTrailingNewline guards the exact wire shape.
func TestNoTrailingNewline(t *testing.T) {
	got := mustBytes(t, map[string]any{"a": 1})
	if strings.HasSuffix(got, "\n") {
		t.Fatalf("output ends with a newline: %q", got)
	}
}
