// Package compress implements tempest's Zstandard value codec.
//
// Compressed values are self-describing: a value either carries a versioned
// ZCMP envelope or is stored as plain bytes. The read path always checks for the
// magic and decodes an envelope when it finds one, so a store written with
// compression enabled stays readable with compression disabled and vice versa.
//
// Compression is applied to the plaintext side of encryption (marshal →
// compress → seal), which keeps key rotation, verification and the encryption
// markers independent of it. The envelope is only emitted when it actually
// shrinks the value, so small or incompressible values never grow.
package compress

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	// Magic identifies a compressed value envelope.
	Magic = "ZCMP"
	// Version is the envelope format version this build writes and accepts.
	Version = 1
	// ModeZstd is the only encoding mode defined by version 1.
	ModeZstd = 0x01
	// HeaderSize is the fixed envelope header length: magic, version, mode,
	// two reserved bytes and the big-endian original length.
	HeaderSize = 16
	// DefaultLevel is the Zstandard compression level used when Config.Level
	// is zero.
	DefaultLevel = 3
)

const (
	offsetVersion  = 4
	offsetMode     = 5
	offsetReserved = 6
	offsetOriginal = 8
)

// Errors returned by the codec.
var (
	// ErrCorrupt reports an envelope that cannot be decoded, or whose decoded
	// length differs from the recorded original length.
	ErrCorrupt = errors.New("compress: corrupt envelope")
	// ErrUnsupportedVersion reports an envelope written by another version or
	// with an unknown encoding mode.
	ErrUnsupportedVersion = errors.New("compress: unsupported envelope version")
)

// Config selects the compression level and the minimum plaintext size worth
// compressing. A zero Config selects DefaultLevel and tries every value.
type Config struct {
	// Level is the Zstandard level; 0 selects DefaultLevel. Negative levels
	// select Zstandard's faster modes.
	Level int
	// MinSize skips values smaller than this many bytes. Zero tries every value.
	// It is a CPU optimization only: the "emit only when smaller" rule already
	// guarantees that a value never grows.
	MinSize int
}

// Codec is a pooled Zstandard codec safe for concurrent use. Stores hold one
// Codec per open and call Encode/Decode on every value they persist.
type Codec struct {
	enc     *zstd.Encoder
	minSize int
}

// NewCodec returns a codec for cfg. The returned codec is immutable and may be
// shared by every operation of a store, including through a rotation.
func NewCodec(cfg Config) (*Codec, error) {
	level := cfg.Level
	if level == 0 {
		level = DefaultLevel
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return nil, fmt.Errorf("compress: create zstd encoder: %w", err)
	}
	return &Codec{enc: enc, minSize: cfg.MinSize}, nil
}

// Encode compresses src into dst[:n]. When the compressed value (including the
// envelope header) is not smaller than src, or src is below the configured
// minimum size, it returns src unchanged with no envelope.
func (c *Codec) Encode(dst, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return src, nil
	}
	if c == nil || c.enc == nil {
		return src, nil
	}
	if c.minSize > 0 && len(src) < c.minSize {
		return src, nil
	}

	body := c.enc.EncodeAll(src, nil)
	if len(body)+HeaderSize >= len(src) {
		return src, nil
	}

	out := dst
	if cap(out) < HeaderSize+len(body) {
		out = make([]byte, HeaderSize+len(body))
	} else {
		out = out[:HeaderSize+len(body)]
	}
	copy(out[0:4], Magic)
	out[offsetVersion] = Version
	out[offsetMode] = ModeZstd
	out[offsetReserved] = 0
	out[offsetReserved+1] = 0
	binary.BigEndian.PutUint64(out[offsetOriginal:offsetOriginal+8], uint64(len(src)))
	copy(out[HeaderSize:], body)
	return out, nil
}

// Decode reverses Encode. Input without a ZCMP envelope is returned unchanged
// (aliasing src is permitted). A malformed or unsupported envelope returns
// ErrCorrupt or ErrUnsupportedVersion.
func (c *Codec) Decode(dst, src []byte) ([]byte, error) {
	return Decode(dst, src)
}

var decoderPool = sync.Pool{
	New: func() any {
		d, err := zstd.NewReader(nil)
		if err != nil {
			return nil
		}
		return d
	},
}

// Decode returns the decompressed payload of src, or src itself when it carries
// no envelope. It is a package function so a store can read compressed values
// even when it was opened with compression disabled: decoding never depends on
// configuration.
func Decode(dst, src []byte) ([]byte, error) {
	if len(src) < HeaderSize || string(src[:4]) != Magic {
		return src, nil
	}
	if src[offsetVersion] != Version || src[offsetMode] != ModeZstd {
		return nil, fmt.Errorf("%w: version %d, mode %#x", ErrUnsupportedVersion, src[offsetVersion], src[offsetMode])
	}
	want := binary.BigEndian.Uint64(src[offsetOriginal : offsetOriginal+8])
	if want > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: original length %d is not addressable", ErrCorrupt, want)
	}

	dec := decoderPool.Get()
	reader, ok := dec.(*zstd.Decoder)
	if !ok || reader == nil {
		return nil, fmt.Errorf("%w: no zstd decoder", ErrCorrupt)
	}
	defer decoderPool.Put(reader)

	out, err := reader.DecodeAll(src[HeaderSize:], dst[:0])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if uint64(len(out)) != want {
		return nil, fmt.Errorf("%w: decoded %d bytes, want %d", ErrCorrupt, len(out), want)
	}
	return out, nil
}
