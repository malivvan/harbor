package compress

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"testing"
)

func newTestCodec(t *testing.T, cfg Config) *Codec {
	t.Helper()
	c, err := NewCodec(cfg)
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	return c
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	codec := newTestCodec(t, Config{})
	cases := [][]byte{
		[]byte(strings.Repeat("tempest ", 1000)),
		[]byte(`{"a":1,"b":[1,2,3],"c":"the quick brown fox jumps over the lazy dog"}`),
		[]byte("short"),
		{},
	}
	for i, src := range cases {
		encoded, err := codec.Encode(nil, src)
		if err != nil {
			t.Fatalf("case %d: Encode: %v", i, err)
		}
		decoded, err := Decode(nil, encoded)
		if err != nil {
			t.Fatalf("case %d: Decode: %v", i, err)
		}
		if !bytes.Equal(decoded, src) {
			t.Fatalf("case %d: round trip = %q, want %q", i, decoded, src)
		}
	}
}

func TestEncodeNeverGrows(t *testing.T) {
	codec := newTestCodec(t, Config{Level: 1})
	for _, size := range []int{1, 8, 15, 16, 17, 64, 1024} {
		src := make([]byte, size)
		if _, err := rand.Read(src); err != nil {
			t.Fatalf("read random: %v", err)
		}
		encoded, err := codec.Encode(nil, src)
		if err != nil {
			t.Fatalf("size %d: Encode: %v", size, err)
		}
		if len(encoded) > len(src) {
			t.Fatalf("size %d: encoded %d bytes, larger than input", size, len(encoded))
		}
		// Incompressible input is stored raw: no envelope.
		if bytes.HasPrefix(encoded, []byte(Magic)) && size <= HeaderSize {
			t.Fatalf("size %d: unexpected envelope", size)
		}
	}
}

func TestEncodeRespectsMinSize(t *testing.T) {
	codec := newTestCodec(t, Config{MinSize: 1024})
	small := []byte(strings.Repeat("a", 512))
	encoded, err := codec.Encode(nil, small)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(encoded, small) {
		t.Fatal("value below MinSize must be stored raw")
	}

	large := []byte(strings.Repeat("a", 4096))
	encoded, err = codec.Encode(nil, large)
	if err != nil {
		t.Fatalf("Encode large: %v", err)
	}
	if !bytes.HasPrefix(encoded, []byte(Magic)) {
		t.Fatal("compressible value above MinSize must carry an envelope")
	}
}

func TestDecodePlainPassthrough(t *testing.T) {
	src := []byte("plain bytes without an envelope")
	got, err := Decode(nil, src)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("Decode = %q, want passthrough", got)
	}
}

func TestDecodeRejectsBadVersion(t *testing.T) {
	codec := newTestCodec(t, Config{})
	encoded, err := codec.Encode(nil, []byte(strings.Repeat("compress me ", 100)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	encoded[offsetVersion] = 9
	if _, err := Decode(nil, encoded); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Decode bad version = %v, want ErrUnsupportedVersion", err)
	}

	encoded[offsetVersion] = Version
	encoded[offsetMode] = 7
	if _, err := Decode(nil, encoded); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Decode bad mode = %v, want ErrUnsupportedVersion", err)
	}
}

func TestDecodeRejectsTruncated(t *testing.T) {
	codec := newTestCodec(t, Config{})
	encoded, err := codec.Encode(nil, []byte(strings.Repeat("compress me ", 100)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err := Decode(nil, encoded[:HeaderSize+2]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Decode truncated = %v, want ErrCorrupt", err)
	}
}

func TestDecodeRejectsLengthMismatch(t *testing.T) {
	codec := newTestCodec(t, Config{})
	encoded, err := codec.Encode(nil, []byte(strings.Repeat("compress me ", 100)))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	binaryBigEndianPut(encoded[offsetOriginal:offsetOriginal+8], 1)
	if _, err := Decode(nil, encoded); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Decode length mismatch = %v, want ErrCorrupt", err)
	}
}

// binaryBigEndianPut writes v big-endian into b (test helper avoiding another
// import).
func binaryBigEndianPut(b []byte, v uint64) {
	for i := len(b) - 1; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
}

func TestConcurrentCodec(t *testing.T) {
	codec := newTestCodec(t, Config{})
	src := []byte(strings.Repeat("concurrent compression ", 200))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				encoded, err := codec.Encode(nil, src)
				if err != nil {
					t.Errorf("Encode: %v", err)
					return
				}
				decoded, err := Decode(nil, encoded)
				if err != nil {
					t.Errorf("Decode: %v", err)
					return
				}
				if !bytes.Equal(decoded, src) {
					t.Errorf("round trip mismatch")
					return
				}
			}
		}()
	}
	wg.Wait()
}
