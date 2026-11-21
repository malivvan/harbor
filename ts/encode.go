package ts

import (
	"encoding/binary"
	"errors"
	"math"
)

const recordSize = 8

// errCorruptPoint reports a plaintext record that does not have the expected
// size. Ciphertext failures are reported as ErrAuthFailed instead.
var errCorruptPoint = errors.New("ts: corrupt point")

// encodeKey maps an int64 timestamp to 8 bytes that sort chronologically.
func encodeKey(ts int64) [recordSize]byte {
	var b [recordSize]byte
	binary.BigEndian.PutUint64(b[:], uint64(ts)^(1<<63))
	return b
}

// decodeKey is the inverse of encodeKey.
func decodeKey(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b) ^ (1 << 63))
}

// encodeValue stores a float64 as its IEEE-754 bits.
func encodeValue(v float64) [recordSize]byte {
	var b [recordSize]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
	return b
}

// decodeValue is the inverse of encodeValue.
func decodeValue(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

// sealBytes seals plain with the store's primary key, prepending a fresh nonce.
func (s *Store) sealBytes(plain []byte) ([]byte, error) {
	if s == nil {
		return nil, ErrAuthFailed
	}
	return s.enc.seal(plain)
}

// openBytes opens a sealed record. Rotation fallback keys are tried in order.
func (s *Store) openBytes(raw []byte) ([]byte, error) {
	if s == nil {
		return nil, ErrAuthFailed
	}
	return s.enc.open(raw)
}

// sealValue encodes v and seals it when encryption is enabled.
func (s *Store) sealValue(v float64) ([]byte, error) {
	raw := encodeValue(v)
	if s.enc == nil {
		return raw[:], nil
	}
	return s.sealBytes(raw[:])
}

// openValue decodes a stored value, decrypting it when necessary.
func (s *Store) openValue(raw []byte) (float64, error) {
	if s.enc == nil {
		if len(raw) != recordSize {
			return 0, errCorruptPoint
		}
		return decodeValue(raw), nil
	}

	plain, err := s.openBytes(raw)
	if err != nil {
		return 0, err
	}
	if len(plain) != recordSize {
		return 0, errCorruptPoint
	}
	return decodeValue(plain), nil
}

// decodePoint decodes a stored point. A false result means the record is not a
// point (for example a plaintext record of the wrong size) and should be
// skipped; an error means the record could not be authenticated.
func (s *Store) decodePoint(key, value []byte) (Point, bool, error) {
	if len(key) != recordSize {
		return Point{}, false, nil
	}

	if s.enc == nil {
		if len(value) != recordSize {
			return Point{}, false, nil
		}
		return Point{Timestamp: decodeKey(key), Value: decodeValue(value)}, true, nil
	}

	plain, err := s.openBytes(value)
	if err != nil {
		return Point{}, false, err
	}
	if len(plain) != recordSize {
		return Point{}, false, errCorruptPoint
	}
	return Point{Timestamp: decodeKey(key), Value: decodeValue(plain)}, true, nil
}
