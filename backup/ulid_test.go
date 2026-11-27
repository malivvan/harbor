package backup

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewShape(t *testing.T) {
	s := newULID()
	if len(s) != ulidSize {
		t.Fatalf("len = %d, want %d (%q)", len(s), ulidSize, s)
	}
	if !validULID(s) {
		t.Fatalf("validULID(%q) = false", s)
	}
	// The encoder must emit only canonical upper-case alphabet characters.
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune(ulidAlphabet, rune(s[i])) {
			t.Fatalf("character %q at %d is not in the alphabet", s[i], i)
		}
	}
}

func TestTimeRoundTrip(t *testing.T) {
	cases := []time.Time{
		time.UnixMilli(0).UTC(),
		time.Date(2026, 11, 23, 1, 2, 3, 0, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC),
	}
	for _, want := range cases {
		s := newULIDAt(want)
		got, err := ulidTime(s)
		if err != nil {
			t.Fatalf("ulidTime(%q): %v", s, err)
		}
		if !got.Equal(want.Truncate(time.Millisecond)) {
			t.Errorf("ulidTime(newULIDAt(%v)) = %v", want, got)
		}
	}
}

// TestLexicographicOrderByTime is the property every caller relies on: ids sort
// by creation time as plain strings.
func TestLexicographicOrderByTime(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var prev string
	for i := 0; i < 100; i++ {
		cur := newULIDAt(base.Add(time.Duration(i) * time.Millisecond))
		if prev != "" && cur <= prev {
			t.Fatalf("id at step %d (%q) does not sort after %q", i, cur, prev)
		}
		prev = cur
	}
}

func TestNewIsUniqueAndConcurrent(t *testing.T) {
	const n = 2000
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[string]struct{}, n)
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			s := newULID()
			mu.Lock()
			defer mu.Unlock()
			if _, dup := seen[s]; dup {
				t.Errorf("duplicate ULID %q", s)
			}
			seen[s] = struct{}{}
		}()
	}
	wg.Wait()
}

func TestValidRejects(t *testing.T) {
	bad := []string{
		"",
		"too-short",
		strings.Repeat("0", ulidSize-1),
		strings.Repeat("0", ulidSize+1),
		strings.Repeat("U", ulidSize), // U is excluded from Crockford base32
		strings.Repeat("!", ulidSize),
	}
	for _, s := range bad {
		if validULID(s) {
			t.Errorf("validULID(%q) = true, want false", s)
		}
	}
}

func TestValidAcceptsLowerCase(t *testing.T) {
	s := strings.ToLower(newULID())
	if !validULID(s) {
		t.Fatalf("validULID(%q) = false, want true (lower case input is accepted)", s)
	}
	if _, err := ulidTime(s); err != nil {
		t.Fatalf("ulidTime(%q): %v", s, err)
	}
}

func TestTimeRejects(t *testing.T) {
	if _, err := ulidTime("short"); err == nil {
		t.Fatal("Time on a short string: want an error")
	}
	if _, err := ulidTime("!!!!!!!!!!"); err == nil {
		t.Fatal("Time on invalid characters: want an error")
	}
}
