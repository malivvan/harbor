// Package id generates ULID identifiers.
//
// A ULID is a 26-character Crockford-base32 string holding a 48-bit millisecond
// timestamp followed by 80 bits of randomness. Because the timestamp is the high
// order part of the encoding, ULIDs sort lexicographically by creation time —
// which makes them usable as database keys for both tempest's record store and any
// application built on it.
//
// The package makes no network calls and has no dependencies beyond the standard
// library. New and NewAt are safe for concurrent use.
package id

import (
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

// alphabet is Crockford base32: the digits and letters that are not confusable
// with 0/1/I/L/O/U are omitted, or mapped, so a hand-transcribed ULID stays
// unambiguous.
const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

const (
	// Size is the length of an encoded ULID.
	Size = 26
	// timeChars is how many characters encode the 48-bit timestamp.
	timeChars = 10
	// randomChars is how many characters encode the 80 random bits.
	randomChars = 16
	// randomBytes is randomChars*5/8.
	randomBytes = 10
)

// ErrInvalid reports a string that is not a well-formed ULID.
var ErrInvalid = errors.New("id: invalid ULID")

// New returns a ULID for the current time.
func New() string {
	return NewAt(time.Now())
}

// NewAt returns a ULID whose timestamp part is t, truncated to milliseconds.
//
// Two calls with the same t are ordered only by their random part, so a caller
// that needs strict within-millisecond ordering should sort on a secondary key.
// For the common case — one id per record, ordered by creation — the millisecond
// prefix is enough.
func NewAt(t time.Time) string {
	ms := uint64(t.UnixMilli())

	out := make([]byte, Size)

	// Timestamp: 48 bits most-significant-first across 10 characters.
	for i := timeChars - 1; i >= 0; i-- {
		out[i] = alphabet[ms&0x1F]
		ms >>= 5
	}

	// Randomness: 80 bits across 16 characters, exactly 5 bits each.
	var rnd [randomBytes]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		// crypto/rand failing is a broken platform, not a caller error. Panicking
		// matches crypto/rand's own documented behaviour and keeps the hot path
		// allocation-free of error handling.
		panic(fmt.Sprintf("id: crypto/rand unavailable: %v", err))
	}
	for c := 0; c < randomChars; c++ {
		out[timeChars+c] = alphabet[bitsAt(rnd[:], c*5)]
	}

	return string(out)
}

// bitsAt reads 5 bits starting at bit position pos from a big-endian byte slice,
// returning them as a number in [0, 32).
func bitsAt(b []byte, pos int) uint8 {
	var v uint8
	for k := 0; k < 5; k++ {
		p := pos + k
		v = v<<1 | (b[p/8]>>(7-uint(p%8)))&1
	}
	return v
}

// Time returns the timestamp encoded in a ULID. Only the first 10 characters are
// read, so it works on a full ULID or on its timestamp prefix.
func Time(s string) (time.Time, error) {
	if len(s) < timeChars {
		return time.Time{}, fmt.Errorf("%w: %d characters, need at least %d", ErrInvalid, len(s), timeChars)
	}
	var ms uint64
	for i := 0; i < timeChars; i++ {
		v, ok := decode(s[i])
		if !ok {
			return time.Time{}, fmt.Errorf("%w: character %q at index %d", ErrInvalid, s[i], i)
		}
		ms = ms<<5 | uint64(v)
	}
	return time.UnixMilli(int64(ms)).UTC(), nil
}

// Valid reports whether s is a well-formed 26-character ULID.
func Valid(s string) bool {
	if len(s) != Size {
		return false
	}
	for i := 0; i < len(s); i++ {
		if _, ok := decode(s[i]); !ok {
			return false
		}
	}
	return true
}

// decode maps a Crockford-base32 character to its value. Lower case is accepted
// on input because ULIDs round-trip through systems that lower-case identifiers;
// the encoder always emits upper case.
func decode(c byte) (uint8, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'Z':
		// Crockford omits I, L, O and U; accept them as their confusable digits.
		switch c {
		case 'I', 'L':
			return 1, true
		case 'O':
			return 0, true
		case 'U':
			return 0, false
		}
		i := indexInAlphabet(c)
		if i < 0 {
			return 0, false
		}
		return uint8(i), true
	case c >= 'a' && c <= 'z':
		return decode(c - 'a' + 'A')
	}
	return 0, false
}

// indexInAlphabet returns the position of c in alphabet, or -1.
func indexInAlphabet(c byte) int {
	for i := 0; i < len(alphabet); i++ {
		if alphabet[i] == c {
			return i
		}
	}
	return -1
}
