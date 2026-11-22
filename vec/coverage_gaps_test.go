package vec

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueGetters(t *testing.T) {
	var nilValue *Value
	require.Empty(t, nilValue.GetStringValue())
	require.Zero(t, nilValue.GetIntValue())
	require.Zero(t, nilValue.GetDoubleValue())
	require.False(t, nilValue.GetBoolValue())
	require.Nil(t, nilValue.GetBytesValue())

	v := &Value{Kind: KindString, StringValue: "s"}
	require.Equal(t, "s", v.GetStringValue())
	require.Zero(t, v.GetIntValue())

	v = &Value{Kind: KindInt, IntValue: 7}
	require.Equal(t, int64(7), v.GetIntValue())
	require.Empty(t, v.GetStringValue())

	v = &Value{Kind: KindDouble, DoubleValue: 1.5}
	require.Equal(t, 1.5, v.GetDoubleValue())
	require.Zero(t, v.GetIntValue())

	v = &Value{Kind: KindBool, BoolValue: true}
	require.True(t, v.GetBoolValue())
	require.Zero(t, v.GetDoubleValue())

	v = &Value{Kind: KindBytes, BytesValue: []byte("b")}
	require.Equal(t, []byte("b"), v.GetBytesValue())
	require.False(t, v.GetBoolValue())

	// A mismatched kind returns the zero value.
	v = &Value{Kind: KindString, IntValue: 9}
	require.Zero(t, v.GetIntValue())
}

func TestAsFloat64AndAsInt64(t *testing.T) {
	numbers := []any{
		float64(1), float32(1), int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
	}
	for _, n := range numbers {
		f, ok := asFloat64(n)
		require.True(t, ok, "%T", n)
		require.InDelta(t, 1.0, f, 1e-6)
	}
	_, ok := asFloat64("1")
	require.False(t, ok)

	for _, n := range []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1)} {
		i, ok := asInt64(n)
		require.True(t, ok, "%T", n)
		require.EqualValues(t, 1, i)
	}
	_, ok = asInt64(uint64(math.MaxInt64) + 1)
	require.False(t, ok)
	_, ok = asInt64(1.5)
	require.False(t, ok)
	_, ok = asInt64("1")
	require.False(t, ok)
}

func TestStringAndRegexHelpers(t *testing.T) {
	require.True(t, containsString("hello", "ell"))
	require.False(t, containsString("hello", "xyz"))

	re := compileRegex("^h.*o$")
	require.NotNil(t, re)
	require.True(t, re.MatchString("hello"))
	require.Same(t, re, compileRegex("^h.*o$"))

	require.Nil(t, compileRegex("["))
	require.Nil(t, compileRegex("["), "invalid patterns are cached")
	require.True(t, matchRegex("hello", "^h"))
	require.False(t, matchRegex("hello", "["))

	// Force the cache past its bound to exercise the clear branch.
	for i := 0; i < 1100; i++ {
		compileRegex(fmt.Sprintf("^pattern-%d$", i))
	}
	require.LessOrEqual(t, regexCacheSize(), 1025)
}

func TestStoredPointCodec(t *testing.T) {
	value := Value{Kind: KindString, StringValue: "payload", BytesValue: []byte{1, 2}}
	valueBytes := appendValue(nil, value)
	decodedValue, n, err := decodeValue(valueBytes)
	require.NoError(t, err)
	require.Equal(t, len(valueBytes), n)
	require.Equal(t, value, decodedValue)

	point := StoredPoint{
		ID:      "p",
		Vector:  []float32{1, 2},
		Payload: map[string]*Value{"k": {Kind: KindInt, IntValue: 1}, "nil": nil},
		Version: 3,
	}
	pointBytes := encodeStoredPoint(point)
	require.Equal(t, len(pointBytes), storedPointSize(point))
	decoded, err := decodeStoredPoint(pointBytes)
	require.NoError(t, err)
	require.Equal(t, point, decoded)

	_, err = decodeStoredPoint(nil)
	require.Error(t, err)
	_, err = decodeStoredPoint(append(pointBytes, 0))
	require.Error(t, err)
	// A truncated length must not panic or allocate wildly.
	_, err = decodeStoredPoint([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	require.Error(t, err)
}

func TestEncStateNilPassthrough(t *testing.T) {
	plain := []byte("plain")

	var nilState *encState
	sealed, err := nilState.seal(plain)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err := nilState.open(plain)
	require.NoError(t, err)
	require.Equal(t, plain, opened)

	// A non-nil state with a nil keychain is also a passthrough.
	empty := &encState{}
	sealed, err = empty.seal(plain)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err = empty.open(plain)
	require.NoError(t, err)
	require.Equal(t, plain, opened)
}

func TestMatchHelpers(t *testing.T) {
	require.True(t, matchExact("a", "a"))
	require.False(t, matchExact("a", "b"))
	require.True(t, matchExact(int64(1), 1))
	require.True(t, matchPrefix("hello", "he"))
	require.False(t, matchPrefix(123, "he"))
	require.True(t, matchRange(int64(5), &RangeValue{GTE: 1, LTE: 9}))
	require.False(t, matchRange(int64(5), &RangeValue{GTE: 6}))
	require.False(t, matchRange(int64(5), nil))
	require.True(t, matchContains("hello", "ell"))
}
