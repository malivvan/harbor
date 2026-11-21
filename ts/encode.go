package ts

import (
	"encoding/binary"
	"math"
)

const recordSize = 8

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

func decodePoint(key, value []byte) (Point, bool) {
	if len(key) != recordSize || len(value) != recordSize {
		return Point{}, false
	}
	return Point{Timestamp: decodeKey(key), Value: decodeValue(value)}, true
}
