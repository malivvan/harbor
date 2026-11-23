package vector

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// The persisted vector format is hand-written so the project does not depend
// on a code generator or an external serialization package. It is positional
// and deterministic:
//
//	StoredPoint: string(ID) | uvarint(len(Vector)) | float32[BE]... |
//	             uvarint(len(Payload)) |
//	             (string(key) | 0x00 for nil)...
//	             (string(key) | 0x01 | Value)... |
//	             uvarint(Version)
//	Value:       kind[1] | string(StringValue) | varint(IntValue) |
//	             float64[BE](DoubleValue) | bool[1] | bytes(BytesValue)
//
// Strings and byte slices are a uvarint length followed by the bytes. Payload
// keys are sorted, so the same point always produces the same bytes. Every
// length is bounds-checked before allocating, and decoding requires the input
// to be consumed exactly.

var errCorruptPoint = fmt.Errorf("vector: corrupt stored point")

// ValueSchemaFingerprint identifies the wire layout of Value. The format is
// positional, so a layout change must change this value.
const ValueSchemaFingerprint = "b4b9e1b8e7c5d4dfd8b7b7b6e1f2a3c4d5e6f708192a3b4c5d6e7f8091a2b3c4"

// StoredPointSchemaFingerprint identifies the wire layout of StoredPoint.
const StoredPointSchemaFingerprint = "7f3c2a1b0e9d8c7b6a594837261504f3e2d1c0b9a8978675645342312010ff"

func appendUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func readUvarint(data []byte) (uint64, int, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, fmt.Errorf("%w: invalid uvarint", errCorruptPoint)
	}
	return v, n, nil
}

func uvarintLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

func appendString(dst []byte, s string) []byte {
	dst = appendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func readString(data []byte) (string, int, error) {
	n, m, err := readUvarint(data)
	if err != nil {
		return "", 0, err
	}
	if n > uint64(len(data)-m) {
		return "", 0, fmt.Errorf("%w: string length %d exceeds %d remaining bytes", errCorruptPoint, n, len(data)-m)
	}
	end := m + int(n)
	return string(data[m:end]), end, nil
}

func appendBytes(dst, b []byte) []byte {
	dst = appendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func readBytes(data []byte) ([]byte, int, error) {
	n, m, err := readUvarint(data)
	if err != nil {
		return nil, 0, err
	}
	if n > uint64(len(data)-m) {
		return nil, 0, fmt.Errorf("%w: byte length %d exceeds %d remaining bytes", errCorruptPoint, n, len(data)-m)
	}
	end := m + int(n)
	if n == 0 {
		return nil, end, nil
	}
	out := make([]byte, n)
	copy(out, data[m:end])
	return out, end, nil
}

func appendVarint(dst []byte, v int64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func readVarint(data []byte) (int64, int, error) {
	v, n := binary.Varint(data)
	if n <= 0 {
		return 0, 0, fmt.Errorf("%w: invalid varint", errCorruptPoint)
	}
	return v, n, nil
}

func varintLen(v int64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutVarint(buf[:], v)
}

func valueSize(v *Value) int {
	if v == nil {
		return 1 // nil marker
	}
	// present marker + kind + string + varint + float64 + bool + bytes
	return 2 + uvarintLen(uint64(len(v.StringValue))) + len(v.StringValue) +
		varintLen(v.IntValue) + 8 + 1 +
		uvarintLen(uint64(len(v.BytesValue))) + len(v.BytesValue)
}

func appendValue(dst []byte, v Value) []byte {
	dst = append(dst, byte(v.Kind))
	dst = appendString(dst, v.StringValue)
	dst = appendVarint(dst, v.IntValue)
	var f [8]byte
	binary.BigEndian.PutUint64(f[:], math.Float64bits(v.DoubleValue))
	dst = append(dst, f[:]...)
	if v.BoolValue {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	return appendBytes(dst, v.BytesValue)
}

func decodeValue(data []byte) (Value, int, error) {
	var v Value
	if len(data) < 1 {
		return v, 0, fmt.Errorf("%w: truncated value", errCorruptPoint)
	}
	v.Kind = ValueKind(data[0])
	n := 1

	s, m, err := readString(data[n:])
	if err != nil {
		return v, 0, err
	}
	v.StringValue = s
	n += m

	iv, m, err := readVarint(data[n:])
	if err != nil {
		return v, 0, err
	}
	v.IntValue = iv
	n += m

	if len(data)-n < 8 {
		return v, 0, fmt.Errorf("%w: truncated float64", errCorruptPoint)
	}
	v.DoubleValue = math.Float64frombits(binary.BigEndian.Uint64(data[n:]))
	n += 8

	if len(data)-n < 1 {
		return v, 0, fmt.Errorf("%w: truncated bool", errCorruptPoint)
	}
	switch data[n] {
	case 0:
		v.BoolValue = false
	case 1:
		v.BoolValue = true
	default:
		return v, 0, fmt.Errorf("%w: invalid bool %d", errCorruptPoint, data[n])
	}
	n++

	b, m, err := readBytes(data[n:])
	if err != nil {
		return v, 0, err
	}
	v.BytesValue = b
	n += m
	return v, n, nil
}

func storedPointSize(p StoredPoint) int {
	size := uvarintLen(uint64(len(p.ID))) + len(p.ID)
	size += uvarintLen(uint64(len(p.Vector))) + 4*len(p.Vector)
	size += uvarintLen(uint64(len(p.Payload)))
	for k, v := range p.Payload {
		size += uvarintLen(uint64(len(k))) + len(k)
		size += valueSize(v)
	}
	return size + uvarintLen(p.Version)
}

func appendStoredPoint(dst []byte, p StoredPoint) []byte {
	dst = appendString(dst, p.ID)
	dst = appendUvarint(dst, uint64(len(p.Vector)))
	var f [4]byte
	for _, v := range p.Vector {
		binary.BigEndian.PutUint32(f[:], math.Float32bits(v))
		dst = append(dst, f[:]...)
	}

	keys := make([]string, 0, len(p.Payload))
	for k := range p.Payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	dst = appendUvarint(dst, uint64(len(keys)))
	for _, k := range keys {
		dst = appendString(dst, k)
		v := p.Payload[k]
		if v == nil {
			dst = append(dst, 0)
			continue
		}
		dst = append(dst, 1)
		dst = appendValue(dst, *v)
	}

	return appendUvarint(dst, p.Version)
}

func encodeStoredPoint(p StoredPoint) []byte {
	return appendStoredPoint(make([]byte, 0, storedPointSize(p)), p)
}

// encodePointsDigest returns a canonical encoding of a point batch, used only
// to digest a written batch for the audit trail. Points are sorted by ID and
// encoded with the same positional format as storage, so the digest is stable
// regardless of the caller's slice order.
func encodePointsDigest(points []PointStruct) []byte {
	sorted := make([]PointStruct, len(points))
	copy(sorted, points)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	out := make([]byte, 0, len(sorted)*16)
	for i := range sorted {
		stored, err := toStoredPoint(&sorted[i])
		if err != nil {
			continue
		}
		out = appendStoredPoint(out, *stored)
	}
	return out
}

func decodeStoredPoint(data []byte) (StoredPoint, error) {
	var p StoredPoint
	n := 0

	id, m, err := readString(data[n:])
	if err != nil {
		return p, err
	}
	p.ID = id
	n += m

	count, m, err := readUvarint(data[n:])
	if err != nil {
		return p, err
	}
	n += m
	if count > uint64((len(data)-n)/4) {
		return p, fmt.Errorf("%w: vector count %d exceeds input", errCorruptPoint, count)
	}
	if count > 0 {
		p.Vector = make([]float32, count)
		for i := range p.Vector {
			p.Vector[i] = math.Float32frombits(binary.BigEndian.Uint32(data[n:]))
			n += 4
		}
	}

	pcount, m, err := readUvarint(data[n:])
	if err != nil {
		return p, err
	}
	n += m
	// Each entry needs at least a uvarint key length and a marker byte.
	if pcount > uint64(len(data)-n) {
		return p, fmt.Errorf("%w: payload count %d exceeds input", errCorruptPoint, pcount)
	}
	if pcount > 0 {
		p.Payload = make(map[string]*Value, pcount)
		for i := uint64(0); i < pcount; i++ {
			key, m, err := readString(data[n:])
			if err != nil {
				return p, err
			}
			n += m
			if len(data)-n < 1 {
				return p, fmt.Errorf("%w: truncated payload marker", errCorruptPoint)
			}
			marker := data[n]
			n++
			switch marker {
			case 0:
				p.Payload[key] = nil
			case 1:
				value, m, err := decodeValue(data[n:])
				if err != nil {
					return p, err
				}
				n += m
				p.Payload[key] = &value
			default:
				return p, fmt.Errorf("%w: invalid payload marker %d", errCorruptPoint, marker)
			}
		}
	}

	version, m, err := readUvarint(data[n:])
	if err != nil {
		return p, err
	}
	p.Version = version
	n += m

	if n != len(data) {
		return p, fmt.Errorf("%w: %d trailing bytes", errCorruptPoint, len(data)-n)
	}
	return p, nil
}
