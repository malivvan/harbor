package record

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// altCodec is a codec other than the default. Tests use it to provoke
// ErrDifferentCodec and to check codec inheritance; it encodes exactly like
// JSON but reports a different name.
var altCodec Codec = altJSONCodec{}

type altJSONCodec struct{}

func (altJSONCodec) Marshal(v interface{}) ([]byte, error) {
	return JSONCodec.Marshal(v)
}

func (altJSONCodec) Unmarshal(b []byte, v interface{}) error {
	return JSONCodec.Unmarshal(b, v)
}

func (altJSONCodec) Name() string {
	return "alt"
}

func TestJSONCodecRoundtrip(t *testing.T) {
	type payload struct {
		Name  string
		Count int
	}

	in := &payload{Name: "test", Count: 3}
	out := new(payload)

	encoded, err := JSONCodec.Marshal(in)
	require.NoError(t, err)
	require.NoError(t, JSONCodec.Unmarshal(encoded, out))
	require.Equal(t, in, out)
}

func TestJSONCodecName(t *testing.T) {
	require.Equal(t, "json", JSONCodec.Name(), "the name is stored in bucket metadata")
	require.Equal(t, "json", defaultCodec.Name(), "JSON is the default codec")
}

func TestAltCodecIsDistinct(t *testing.T) {
	require.NotEqual(t, JSONCodec.Name(), altCodec.Name())
}
