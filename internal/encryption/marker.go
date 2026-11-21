package encryption

import (
	"encoding/binary"
	"fmt"
)

// markerVersion is the on-disk version produced by this package.
const markerVersion = byte(1)

// markerHeaderSize is magic(4) | version(1) | cipher(1) | flags(1).
const markerHeaderSize = 4 + 1 + 1 + 1

// MarkerSpec describes the framing of one store's encryption marker.
//
// Every store uses the same layout — magic | version | cipher | flags |
// [extra] | sealed canary — and differs only in its magic and in whether it
// carries an optional 8-byte field (fs chunk size, ts block duration).
type MarkerSpec struct {
	// Magic identifies the store kind, for example "FENC" or "TENC".
	Magic string

	// ExtraFieldSize is 0 or 8: the size of the optional little-endian uint64
	// field that follows the flags byte.
	ExtraFieldSize int
}

// Marker is a decoded marker.
type Marker struct {
	// Cipher is the resolved cipher suite stored in the marker.
	Cipher CipherSuite

	// Flags holds the store-specific feature bits.
	Flags uint8

	// Extra is the optional trailing field (0 when the spec has none).
	Extra uint64

	// Canary is the sealed canary used to verify the configured keys.
	Canary []byte
}

// validate reports an unusable spec.
func (s MarkerSpec) validate() error {
	if len(s.Magic) != 4 {
		return fmt.Errorf("%w: marker magic must be 4 bytes, got %d", ErrInvalidConfig, len(s.Magic))
	}
	if s.ExtraFieldSize != 0 && s.ExtraFieldSize != 8 {
		return fmt.Errorf("%w: marker extra field must be 0 or 8 bytes, got %d", ErrInvalidConfig, s.ExtraFieldSize)
	}
	return nil
}

// EncodeMarker serializes a marker.
func EncodeMarker(spec MarkerSpec, m Marker) ([]byte, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}

	raw := make([]byte, 0, markerHeaderSize+spec.ExtraFieldSize+len(m.Canary))
	raw = append(raw, spec.Magic...)
	raw = append(raw, markerVersion, byte(m.Cipher), m.Flags)
	if spec.ExtraFieldSize == 8 {
		var extra [8]byte
		binary.LittleEndian.PutUint64(extra[:], m.Extra)
		raw = append(raw, extra[:]...)
	}
	return append(raw, m.Canary...), nil
}

// DecodeMarker parses a marker written by EncodeMarker. A malformed or
// unsupported marker is reported as ErrWrongMode so a plaintext open fails
// loudly instead of misreading the store.
func DecodeMarker(spec MarkerSpec, raw []byte) (Marker, error) {
	if err := spec.validate(); err != nil {
		return Marker{}, err
	}

	fixedSize := markerHeaderSize + spec.ExtraFieldSize
	if len(raw) < fixedSize {
		return Marker{}, fmt.Errorf("%w: corrupt encryption marker", ErrWrongMode)
	}
	if string(raw[:4]) != spec.Magic {
		return Marker{}, fmt.Errorf("%w: corrupt encryption marker", ErrWrongMode)
	}
	if raw[4] != markerVersion {
		return Marker{}, fmt.Errorf("%w: unsupported encryption marker version", ErrWrongMode)
	}

	m := Marker{
		Cipher: CipherSuite(raw[5]),
		Flags:  raw[6],
		// Copy: the caller's buffer may belong to a bbolt transaction that is
		// about to end.
		Canary: append([]byte(nil), raw[fixedSize:]...),
	}
	if spec.ExtraFieldSize == 8 {
		m.Extra = binary.LittleEndian.Uint64(raw[7:15])
	}
	return m, nil
}

// VerifyCanary confirms that kc decrypts sealed back to want, which proves the
// configured keys open the store. A keychain that cannot decrypt it yields
// ErrAuthFailed.
func VerifyCanary(kc *Keychain, sealed []byte, want string) error {
	if kc == nil {
		return fmt.Errorf("%w: plaintext keychain has no canary to verify", ErrInvalidConfig)
	}

	plain, err := kc.Open(sealed)
	if err != nil {
		return err
	}
	if string(plain) != want {
		return ErrAuthFailed
	}
	return nil
}
