// Package binco contains a codec that encodes and decodes entities using the
// binary primitives from github.com/malivvan/binco.
//
// binco itself is schema-driven: serializers are generated per type. Storm
// stores arbitrary caller types through a codec.MarshalUnmarshaler, so this
// package provides a reflective encoder built on binco's wire primitives:
// variable-length integers for numeric values and lengths, and binco's
// composite codecs for booleans, strings and byte slices.
//
// Values are encoded positionally: struct fields in declaration order (only
// exported fields), slices and maps with a length, and pointers/interfaces with
// a nil marker. Types that implement encoding.BinaryMarshaler and
// encoding.BinaryUnmarshaler (for example time.Time) are stored using their own
// binary representation. Maps are encoded in sorted key order so the output is
// deterministic.
package binco

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/malivvan/binco"
	"github.com/malivvan/binco/composite"
	"github.com/malivvan/binco/varnum"
)

const name = "binc"

// Codec serializes objects using the binary format of the binco module.
var Codec = new(bincCodec)

type bincCodec struct{}

var (
	binaryMarshalerType   = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
)

// Marshal encodes v.
func (c bincCodec) Marshal(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, errors.New("binc: cannot encode nil value")
	}

	rv := reflect.ValueOf(v)
	// Treat pointer and value forms of the same entity the same way.
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, errors.New("binc: cannot encode nil pointer")
		}
		rv = rv.Elem()
	}

	return appendValue(nil, rv)
}

// Unmarshal decodes b into v, which must be a non-nil pointer.
func (c bincCodec) Unmarshal(b []byte, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("binc: destination must be a non-nil pointer")
	}

	n, err := readValue(b, 0, rv.Elem())
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("binc: %d trailing bytes after value", len(b)-n)
	}
	return nil
}

// Name returns the codec identifier stored in Storm metadata.
func (c bincCodec) Name() string {
	return name
}

func appendValue(dst []byte, v reflect.Value) ([]byte, error) {
	if !v.IsValid() {
		return nil, errors.New("binc: invalid value")
	}

	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return append(dst, 0), nil
		}
		return nil, errors.New("binc: non-nil interface values are not supported")
	case reflect.Pointer:
		if v.IsNil() {
			return append(dst, 0), nil
		}
		dst = append(dst, 1)
		return appendValue(dst, v.Elem())
	}

	if data, ok, err := binaryMarshal(v); ok {
		if err != nil {
			return nil, fmt.Errorf("binc: marshal %s: %w", v.Type(), err)
		}
		dst = appendBinco(dst, varnum.PositiveInt, len(data))
		return append(dst, data...), nil
	}

	switch v.Kind() {
	case reflect.Bool:
		return appendBinco(dst, composite.Bool, v.Bool()), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return appendBinco(dst, varnum.Int64, v.Int()), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return appendBinco(dst, varnum.Uint64, v.Uint()), nil

	case reflect.Float32:
		return appendBinco(dst, varnum.Float32, float32(v.Float())), nil

	case reflect.Float64:
		return appendBinco(dst, varnum.Float64, v.Float()), nil

	case reflect.String:
		return appendBinco(dst, composite.String, v.String()), nil

	case reflect.Slice:
		if v.IsNil() {
			return append(dst, 0), nil
		}
		dst = append(dst, 1)
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return appendBinco(dst, composite.ByteSlice, v.Bytes()), nil
		}
		dst = appendBinco(dst, varnum.PositiveInt, v.Len())
		var err error
		for i := 0; i < v.Len(); i++ {
			dst, err = appendValue(dst, v.Index(i))
			if err != nil {
				return nil, err
			}
		}
		return dst, nil

	case reflect.Array:
		var err error
		for i := 0; i < v.Len(); i++ {
			dst, err = appendValue(dst, v.Index(i))
			if err != nil {
				return nil, err
			}
		}
		return dst, nil

	case reflect.Map:
		return appendMap(dst, v)

	case reflect.Struct:
		var err error
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath != "" {
				continue // unexported
			}
			dst, err = appendValue(dst, v.Field(i))
			if err != nil {
				return nil, err
			}
		}
		return dst, nil

	case reflect.Complex64, reflect.Complex128:
		return nil, errors.New("binc: complex numbers are not supported")

	default:
		return nil, fmt.Errorf("binc: cannot encode kind %s", v.Kind())
	}
}

func appendMap(dst []byte, v reflect.Value) ([]byte, error) {
	if v.IsNil() {
		return append(dst, 0), nil
	}
	dst = append(dst, 1)

	type entry struct {
		encodedKey []byte
		key        reflect.Value
	}

	keys := v.MapKeys()
	entries := make([]entry, 0, len(keys))
	for _, key := range keys {
		encoded, err := appendValue(nil, key)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry{encodedKey: encoded, key: key})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].encodedKey, entries[j].encodedKey) < 0
	})

	dst = appendBinco(dst, varnum.PositiveInt, len(entries))
	var err error
	for _, e := range entries {
		dst = append(dst, e.encodedKey...)
		dst, err = appendValue(dst, v.MapIndex(e.key))
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func readValue(src []byte, off int, v reflect.Value) (int, error) {
	if off < 0 || off > len(src) {
		return 0, binco.ErrTruncated
	}

	if v.Kind() == reflect.Interface {
		flag, next, err := readFlag(src, off)
		if err != nil {
			return 0, err
		}
		if flag == 0 {
			v.SetZero()
			return next, nil
		}
		return 0, errors.New("binc: non-nil interface values are not supported")
	}

	if v.Kind() == reflect.Pointer {
		flag, next, err := readFlag(src, off)
		if err != nil {
			return 0, err
		}
		if flag == 0 {
			v.SetZero()
			return next, nil
		}
		ptr := reflect.New(v.Type().Elem())
		next, err = readValue(src, next, ptr.Elem())
		if err != nil {
			return 0, err
		}
		v.Set(ptr)
		return next, nil
	}

	if canBinaryUnmarshal(v) {
		length, next, err := readLength(src, off)
		if err != nil {
			return 0, err
		}
		if length > len(src)-next {
			return 0, binco.ErrTruncated
		}
		if err := v.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(src[next : next+length]); err != nil {
			return 0, fmt.Errorf("binc: unmarshal %s: %w", v.Type(), err)
		}
		return next + length, nil
	}

	switch v.Kind() {
	case reflect.Bool:
		value, n, err := composite.Bool.Unmarshal(src[off:])
		if err != nil {
			return 0, err
		}
		v.SetBool(value)
		return off + n, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value, n, err := varnum.Int64.Unmarshal(src[off:])
		if err != nil {
			return 0, err
		}
		if v.OverflowInt(value) {
			return 0, binco.ErrOverflow
		}
		v.SetInt(value)
		return off + n, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value, n, err := varnum.Uint64.Unmarshal(src[off:])
		if err != nil {
			return 0, err
		}
		if v.OverflowUint(value) {
			return 0, binco.ErrOverflow
		}
		v.SetUint(value)
		return off + n, nil

	case reflect.Float32:
		value, n, err := varnum.Float32.Unmarshal(src[off:])
		if err != nil {
			return 0, err
		}
		v.SetFloat(float64(value))
		return off + n, nil

	case reflect.Float64:
		value, n, err := varnum.Float64.Unmarshal(src[off:])
		if err != nil {
			return 0, err
		}
		v.SetFloat(value)
		return off + n, nil

	case reflect.String:
		value, n, err := composite.String.Unmarshal(src[off:])
		if err != nil {
			return 0, err
		}
		v.SetString(value)
		return off + n, nil

	case reflect.Slice:
		flag, next, err := readFlag(src, off)
		if err != nil {
			return 0, err
		}
		if flag == 0 {
			v.SetZero()
			return next, nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			value, n, err := composite.ByteSlice.Unmarshal(src[next:])
			if err != nil {
				return 0, err
			}
			v.SetBytes(append([]byte(nil), value...))
			return next + n, nil
		}
		length, next, err := readLength(src, next)
		if err != nil {
			return 0, err
		}
		if length > len(src)-next {
			return 0, binco.ErrTruncated
		}
		slice := reflect.MakeSlice(v.Type(), length, length)
		for i := 0; i < length; i++ {
			next, err = readValue(src, next, slice.Index(i))
			if err != nil {
				return 0, err
			}
		}
		v.Set(slice)
		return next, nil

	case reflect.Array:
		var err error
		next := off
		for i := 0; i < v.Len(); i++ {
			next, err = readValue(src, next, v.Index(i))
			if err != nil {
				return 0, err
			}
		}
		return next, nil

	case reflect.Map:
		return readMap(src, off, v)

	case reflect.Struct:
		var err error
		next := off
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath != "" {
				continue // unexported
			}
			next, err = readValue(src, next, v.Field(i))
			if err != nil {
				return 0, err
			}
		}
		return next, nil

	case reflect.Complex64, reflect.Complex128:
		return 0, errors.New("binc: complex numbers are not supported")

	default:
		return 0, fmt.Errorf("binc: cannot decode kind %s", v.Kind())
	}
}

func readMap(src []byte, off int, v reflect.Value) (int, error) {
	flag, next, err := readFlag(src, off)
	if err != nil {
		return 0, err
	}
	if flag == 0 {
		v.SetZero()
		return next, nil
	}

	length, next, err := readLength(src, next)
	if err != nil {
		return 0, err
	}
	if length > len(src)-next {
		return 0, binco.ErrTruncated
	}

	m := reflect.MakeMapWithSize(v.Type(), length)
	for i := 0; i < length; i++ {
		key := reflect.New(v.Type().Key()).Elem()
		next, err = readValue(src, next, key)
		if err != nil {
			return 0, err
		}
		value := reflect.New(v.Type().Elem()).Elem()
		next, err = readValue(src, next, value)
		if err != nil {
			return 0, err
		}
		m.SetMapIndex(key, value)
	}
	v.Set(m)
	return next, nil
}

func readFlag(src []byte, off int) (byte, int, error) {
	if off >= len(src) {
		return 0, 0, binco.ErrTruncated
	}
	return src[off], off + 1, nil
}

func readLength(src []byte, off int) (int, int, error) {
	if off < 0 || off > len(src) {
		return 0, 0, binco.ErrTruncated
	}
	length, n, err := varnum.PositiveInt.Unmarshal(src[off:])
	if err != nil {
		return 0, 0, err
	}
	return length, off + n, nil
}

// appendBinco appends the encoding of v produced by a binco serializer.
func appendBinco[T any](dst []byte, s binco.Serializer[T], v T) []byte {
	size := s.Size(v)
	off := len(dst)
	dst = append(dst, make([]byte, size)...)
	s.Marshal(v, dst[off:])
	return dst
}

// isBinaryType reports whether t has a symmetric binary representation: the
// value implements encoding.BinaryMarshaler and its pointer implements
// encoding.BinaryUnmarshaler (as time.Time does).
func isBinaryType(t reflect.Type) bool {
	return t.Implements(binaryMarshalerType) && reflect.PointerTo(t).Implements(binaryUnmarshalerType)
}

// binaryMarshal returns the binary representation of v when its type has one.
func binaryMarshal(v reflect.Value) ([]byte, bool, error) {
	if !isBinaryType(v.Type()) {
		return nil, false, nil
	}
	data, err := v.Interface().(encoding.BinaryMarshaler).MarshalBinary()
	return data, true, err
}

func canBinaryUnmarshal(v reflect.Value) bool {
	return v.CanAddr() && isBinaryType(v.Type())
}
