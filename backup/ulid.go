package backup

import (
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

// ulidAlphabet is Crockford base32: the digits and letters that are not confusable
// with 0/1/I/L/O/U are omitted, or mapped, so a hand-transcribed ULID stays
// unambiguous.
const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

const (
	// ulidSize is the length of an encoded ULID.
	ulidSize = 26
	// ulidTimeChars is how many characters encode the 48-bit timestamp.
	ulidTimeChars = 10
	// ulidRandomChars is how many characters encode the 80 random bits.
	ulidRandomChars = 16
	// ulidRandomBytes is ulidRandomChars*5/8.
	ulidRandomBytes = 10
)

// errInvalidULID reports a string that is not a well-formed ULID.
var errInvalidULID = errors.New("backup: invalid ULID")

// newULID returns a ULID for the current time.
//
// A ULID is a 26-character Crockford-base32 string holding a 48-bit millisecond
// timestamp followed by 80 bits of randomness. Because the timestamp is the high
// order part of the encoding, ULIDs sort lexicographically by creation time,
// which is what makes them usable as snapshot keys.
//
// newULID and newULIDAt are safe for concurrent use and have no dependencies
// beyond the standard library.
func newULID() string {
	return newULIDAt(time.Now())
}

// newULIDAt returns a ULID whose timestamp part is t, truncated to milliseconds.
//
// Two calls with the same t are ordered only by their random part, so a caller
// that needs strict within-millisecond ordering should sort on a secondary key.
// For the common case — one id per record, ordered by creation — the millisecond
// prefix is enough.
func newULIDAt(t time.Time) string {
	ms := uint64(t.UnixMilli())

	out := make([]byte, ulidSize)

	// Timestamp: 48 bits most-significant-first across 10 characters.
	for i := ulidTimeChars - 1; i >= 0; i-- {
		out[i] = ulidAlphabet[ms&0x1F]
		ms >>= 5
	}

	// Randomness: 80 bits across 16 characters, exactly 5 bits each.
	var rnd [ulidRandomBytes]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		// crypto/rand failing is a broken platform, not a caller error. Panicking
		// matches crypto/rand's own documented behaviour and keeps the hot path
		// allocation-free of error handling.
		panic(fmt.Sprintf("backup: crypto/rand unavailable: %v", err))
	}
	for c := 0; c < ulidRandomChars; c++ {
		out[ulidTimeChars+c] = ulidAlphabet[ulidBitsAt(rnd[:], c*5)]
	}

	return string(out)
}

// ulidBitsAt reads 5 bits starting at bit position pos from a big-endian byte slice,
// returning them as a number in [0, 32).
func ulidBitsAt(b []byte, pos int) uint8 {
	var v uint8
	for k := 0; k < 5; k++ {
		p := pos + k
		v = v<<1 | (b[p/8]>>(7-uint(p%8)))&1
	}
	return v
}

// ulidTime returns the timestamp encoded in a ULID. Only the first 10 characters
// are read, so it works on a full ULID or on its timestamp prefix.
func ulidTime(s string) (time.Time, error) {
	if len(s) < ulidTimeChars {
		return time.Time{}, fmt.Errorf("%w: %d characters, need at least %d", errInvalidULID, len(s), ulidTimeChars)
	}
	var ms uint64
	for i := 0; i < ulidTimeChars; i++ {
		v, ok := ulidDecode(s[i])
		if !ok {
			return time.Time{}, fmt.Errorf("%w: character %q at index %d", errInvalidULID, s[i], i)
		}
		ms = ms<<5 | uint64(v)
	}
	return time.UnixMilli(int64(ms)).UTC(), nil
}

// validULID reports whether s is a well-formed 26-character ULID.
func validULID(s string) bool {
	if len(s) != ulidSize {
		return false
	}
	for i := 0; i < len(s); i++ {
		if _, ok := ulidDecode(s[i]); !ok {
			return false
		}
	}
	return true
}

// ulidDecode maps a Crockford-base32 character to its value. Lower case is accepted
// on input because ULIDs round-trip through systems that lower-case identifiers;
// the encoder always emits upper case.
func ulidDecode(c byte) (uint8, bool) {
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
		i := ulidIndexInAlphabet(c)
		if i < 0 {
			return 0, false
		}
		return uint8(i), true
	case c >= 'a' && c <= 'z':
		return ulidDecode(c - 'a' + 'A')
	}
	return 0, false
}

// ulidIndexInAlphabet returns the position of c in ulidAlphabet, or -1.
func ulidIndexInAlphabet(c byte) int {
	for i := 0; i < len(ulidAlphabet); i++ {
		if ulidAlphabet[i] == c {
			return i
		}
	}
	return -1
}
