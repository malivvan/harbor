package index_test

import "encoding/binary"

// encodeTestInt encodes an int as a big-endian int64, the same shape the store
// uses for integer keys. Index tests treat values as opaque byte strings, so any
// order-preserving encoding works; a fixed-width one keeps byte order equal to
// numeric order for the non-negative values these tests cover.
func encodeTestInt(v int) ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(int64(v)))
	return buf, nil
}

// decodeTestInt reverses encodeTestInt.
func decodeTestInt(raw []byte, v *int) error {
	*v = int(int64(binary.BigEndian.Uint64(raw)))
	return nil
}
