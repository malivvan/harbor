package binco

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/storm/db/codec/internal"
)

func TestBinc(t *testing.T) {
	internal.RoundtripTester(t, Codec)
}

func TestCodecName(t *testing.T) {
	require.Equal(t, "binc", Codec.Name())
}

type nested struct {
	ID   int
	Name string
}

type allTypes struct {
	Bool   bool
	Int    int
	Int8   int8
	Int16  int16
	Int32  int32
	Int64  int64
	Uint   uint
	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64
	Float  float64
	Small  float32
	Text   string
	Bytes  []byte
	Slice  []string
	Array  [3]int
	Map    map[string]int
	Ptr    *int
	Nested nested
	Empty  []int
	NoMap  map[string]string
}

func TestRoundTripAllTypes(t *testing.T) {
	seven := 7
	value := allTypes{
		Bool:   true,
		Int:    -42,
		Int8:   -8,
		Int16:  -1600,
		Int32:  -320000,
		Int64:  -64000000000,
		Uint:   42,
		Uint8:  8,
		Uint16: 1600,
		Uint32: 320000,
		Uint64: 64000000000,
		Float:  3.14159,
		Small:  1.5,
		Text:   "hello, binc",
		Bytes:  []byte{0, 1, 2, 255},
		Slice:  []string{"a", "b", "c"},
		Array:  [3]int{1, 2, 3},
		Map:    map[string]int{"b": 2, "a": 1, "c": 3},
		Ptr:    &seven,
		Nested: nested{ID: 9, Name: "nested"},
	}

	encoded, err := Codec.Marshal(value)
	require.NoError(t, err)

	var decoded allTypes
	require.NoError(t, Codec.Unmarshal(encoded, &decoded))
	require.Equal(t, value, decoded)
}

func TestNilCollectionsRoundTrip(t *testing.T) {
	value := allTypes{
		Bytes: nil,
		Slice: nil,
		Map:   nil,
		Ptr:   nil,
		Empty: []int{},
		NoMap: map[string]string{},
	}

	encoded, err := Codec.Marshal(&value)
	require.NoError(t, err)

	var decoded allTypes
	require.NoError(t, Codec.Unmarshal(encoded, &decoded))

	require.Nil(t, decoded.Bytes)
	require.Nil(t, decoded.Slice)
	require.Nil(t, decoded.Map)
	require.Nil(t, decoded.Ptr)
	require.NotNil(t, decoded.Empty)
	require.Empty(t, decoded.Empty)
	require.NotNil(t, decoded.NoMap)
	require.Empty(t, decoded.NoMap)
}

func TestTimeRoundTrip(t *testing.T) {
	type event struct {
		At   time.Time
		Name string
	}

	value := event{
		At:   time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.UTC),
		Name: "launch",
	}

	encoded, err := Codec.Marshal(value)
	require.NoError(t, err)

	var decoded event
	require.NoError(t, Codec.Unmarshal(encoded, &decoded))

	require.True(t, value.At.Equal(decoded.At), "time = %v, want %v", decoded.At, value.At)
	require.Equal(t, value.Name, decoded.Name)
}

func TestPointerEntityRoundTrip(t *testing.T) {
	value := &nested{ID: 1, Name: "pointer"}

	encoded, err := Codec.Marshal(value)
	require.NoError(t, err)

	// Pointer and value forms must produce the same encoding.
	byValue, err := Codec.Marshal(*value)
	require.NoError(t, err)
	require.Equal(t, byValue, encoded)

	decoded := new(nested)
	require.NoError(t, Codec.Unmarshal(encoded, decoded))
	require.Equal(t, value, decoded)
}

func TestMapEncodingIsDeterministic(t *testing.T) {
	value := map[string]int{"one": 1, "two": 2, "three": 3, "four": 4}

	first, err := Codec.Marshal(value)
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		next, err := Codec.Marshal(value)
		require.NoError(t, err)
		require.True(t, bytes.Equal(first, next), "map encoding must be deterministic")
	}

	var decoded map[string]int
	require.NoError(t, Codec.Unmarshal(first, &decoded))
	require.Equal(t, value, decoded)
}

func TestIntRoundTrip(t *testing.T) {
	values := []int{0, 1, -1, 2, 63, 64, 127, 128, 255, 256, -128, -129, 1 << 40, -(1 << 40)}

	for _, want := range values {
		encoded, err := Codec.Marshal(want)
		require.NoError(t, err, "value %d", want)

		var got int
		require.NoError(t, Codec.Unmarshal(encoded, &got), "value %d", want)
		require.Equal(t, want, got)
	}
}

func TestUnsupportedValues(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		_, err := Codec.Marshal(nil)
		require.Error(t, err)
	})

	t.Run("nil pointer", func(t *testing.T) {
		var p *nested
		_, err := Codec.Marshal(p)
		require.Error(t, err)
	})

	t.Run("non-nil interface", func(t *testing.T) {
		type holder struct {
			Any any
		}
		_, err := Codec.Marshal(holder{Any: 42})
		require.Error(t, err)

		encoded, err := Codec.Marshal(holder{})
		require.NoError(t, err)
		var decoded holder
		require.NoError(t, Codec.Unmarshal(encoded, &decoded))
		require.Nil(t, decoded.Any)
	})

	t.Run("complex number", func(t *testing.T) {
		_, err := Codec.Marshal(complex(1, 2))
		require.Error(t, err)
	})
}

func TestUnmarshalDestination(t *testing.T) {
	encoded, err := Codec.Marshal(1)
	require.NoError(t, err)

	var notPointer int
	require.Error(t, Codec.Unmarshal(encoded, notPointer))

	var nilPointer *int
	require.Error(t, Codec.Unmarshal(encoded, nilPointer))

	require.NoError(t, Codec.Unmarshal(encoded, new(int)))
}

func TestTrailingBytesRejected(t *testing.T) {
	encoded, err := Codec.Marshal(7)
	require.NoError(t, err)

	var got int
	require.Error(t, Codec.Unmarshal(append(encoded, 0xFF), &got))
}

func TestTruncatedInput(t *testing.T) {
	encoded, err := Codec.Marshal(nested{ID: 5, Name: "truncate me"})
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var got nested
	for i := 0; i < len(encoded); i++ {
		require.Error(t, Codec.Unmarshal(encoded[:i], &got), "truncated at %d bytes", i)
	}
}

func TestUnexportedFieldsIgnored(t *testing.T) {
	type withPrivate struct {
		Public  string
		private int
	}

	encoded, err := Codec.Marshal(withPrivate{Public: "visible", private: 13})
	require.NoError(t, err)

	var decoded withPrivate
	require.NoError(t, Codec.Unmarshal(encoded, &decoded))
	require.Equal(t, "visible", decoded.Public)
	require.Zero(t, decoded.private, "unexported fields are not encoded")
}
