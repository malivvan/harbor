package fstree

import (
	"fmt"

	"github.com/malivvan/tempest/internal/compress"
	"github.com/malivvan/tempest/internal/crypto"
	"github.com/malivvan/tempest/metrics"
	"github.com/malivvan/tempest/store/ledger"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = crypto.KeySize

// Config enables optional encryption for a Store and its file systems. A zero
// Config, or one with no encryption feature enabled, keeps the plaintext
// on-disk format.
type Config struct {
	// Root is the bucket path below which all file systems live. Each element
	// is one nested bucket name. A nil or empty Root means the database root;
	// the tempest root package passes [][]byte{[]byte(store.FSTREE)} explicitly.
	Root [][]byte

	// Secret is a raw 32-byte master key. It must be set when encryption is
	// enabled.
	Secret []byte

	// EncryptContent seals file contents (the data bucket).
	EncryptContent bool

	// EncryptMetadata seals inode blobs, hiding names, modes, sizes and times.
	EncryptMetadata bool

	// ChunkSize enables chunked content encryption with chunks of this many
	// plaintext bytes. A positive value implies EncryptContent.
	ChunkSize int

	// Compress Zstd-compresses file content and symlink targets on the
	// plaintext side of encryption. It is a write-time preference: reads
	// auto-detect the envelope, so a filesystem written compressed stays
	// readable with Compress disabled. It works with or without encryption and
	// with whole-blob or chunked content.
	Compress bool

	// CompressLevel selects the Zstandard level (0 selects the default).
	CompressLevel int

	// MinCompress skips compressing content smaller than this many bytes.
	MinCompress int

	// Dedup stores chunk content by its plaintext SHA-256, so identical chunks
	// (within a file, across files and across file systems of one store) are
	// written once and reference-counted. It implies ChunkSize (default
	// DefaultChunkSize) and therefore content encryption. The layout is
	// recorded in the filesystem marker and a mismatch fails closed.
	Dedup bool

	// Audit, when non-nil, receives an entry for every committed write.
	Audit ledger.Sink

	// Metrics, when non-nil, receives chunk/cache counters, byte counts and
	// tracing spans. A nil Observer disables instrumentation.
	Metrics metrics.Observer
}

func (c Config) encryptionEnabled() bool {
	return c.EncryptContent || c.EncryptMetadata || c.ChunkSize != 0 || c.Dedup
}

// Validate reports whether the configuration can be opened: the chunk size must
// be non-negative, and an encryption secret is required (and must have a valid
// key length) when encryption or chunking is enabled. A nil Secret is valid for
// a plaintext store.
func (c Config) Validate() error {
	if c.ChunkSize < 0 {
		return fmt.Errorf("%w: chunk size cannot be negative", ErrInvalidConfig)
	}
	if !c.encryptionEnabled() {
		return nil
	}
	_, err := crypto.ResolveKey(c.Secret)
	return err
}

// Normalize validates the configuration and returns it unchanged; fs.Config has
// no implicit defaults. Open calls Normalize, so the public validation and the
// open path cannot drift.
func (c Config) Normalize() (Config, error) {
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// encState is the shared encryption configuration of a Store. A nil encState
// means plaintext.
type encState struct {
	kc          *crypto.Keychain
	encContent  bool
	encMetadata bool
	chunkSize   int
	comp        *compress.Codec
	// dedup stores chunks content-addressed and reference-counted.
	dedup bool
}

// encrypted reports whether a real cipher is configured. A state can exist for
// compression alone, with no keychain.
func (e *encState) encrypted() bool { return e != nil && e.kc != nil }

func (e *encState) enabled() bool {
	return e != nil
}

func (e *encState) seal(plain []byte) ([]byte, error) {
	if !e.encrypted() {
		return plain, nil
	}
	return e.kc.Seal(plain)
}

func (e *encState) open(raw []byte) ([]byte, error) {
	if !e.encrypted() {
		return raw, nil
	}
	return e.kc.Open(raw)
}

func (e *encState) sealMetadata(plain []byte) ([]byte, error) {
	if e == nil || !e.encMetadata {
		return plain, nil
	}
	return e.seal(plain)
}

func (e *encState) openMetadata(raw []byte) ([]byte, error) {
	if e == nil || !e.encMetadata {
		return raw, nil
	}
	return e.open(raw)
}

func (e *encState) sealContent(plain []byte) ([]byte, error) {
	if e != nil && e.comp != nil {
		var err error
		if plain, err = e.comp.Encode(nil, plain); err != nil {
			return nil, err
		}
	}
	if e == nil || !e.encContent || !e.encrypted() {
		return plain, nil
	}
	return e.seal(plain)
}

func (e *encState) openContent(raw []byte) ([]byte, error) {
	if e == nil || !e.encContent || !e.encrypted() {
		return compress.Decode(nil, raw)
	}
	plain, err := e.open(raw)
	if err != nil {
		return nil, err
	}
	return compress.Decode(nil, plain)
}

// Marker constants. The marker lives (unencrypted) in each filesystem's state
// bucket so a plaintext open can detect an encrypted filesystem.
const (
	markerKey       = "\x00__fs_enc__"
	markerMagic     = "FENC"
	canaryPlaintext = "tempest/fs/aead/v1"
)

const (
	markerFlagContent  = 1 << 0
	markerFlagMetadata = 1 << 1
	markerFlagChunked  = 1 << 2
	markerFlagDedup    = 1 << 4
)

// markerSpec describes the on-disk marker framing shared by every store.
var markerSpec = crypto.MarkerSpec{Magic: markerMagic, ExtraFieldSize: 8}

// marker describes the encryption mode recorded in a filesystem.
type marker struct {
	encContent  bool
	encMetadata bool
	chunkSize   int
	dedup       bool
}

// flagsByte returns the marker bits for the configured state.
func (m marker) flagsByte() byte {
	var f byte
	if m.encContent {
		f |= markerFlagContent
	}
	if m.encMetadata {
		f |= markerFlagMetadata
	}
	if m.chunkSize > 0 {
		f |= markerFlagChunked
	}
	if m.dedup {
		f |= markerFlagDedup
	}
	return f
}

func (m marker) matches(e *encState) bool {
	return m.encContent == e.encContent &&
		m.encMetadata == e.encMetadata &&
		m.chunkSize == e.chunkSize &&
		m.dedup == e.dedup
}

// encodeMarker serializes the marker with a sealed canary used to verify the
// key: opening the canary with the wrong key fails with ErrAuthFailed.
func (e *encState) encodeMarker() ([]byte, error) {
	var flags byte
	if e.encContent {
		flags |= markerFlagContent
	}
	if e.encMetadata {
		flags |= markerFlagMetadata
	}
	if e.chunkSize > 0 {
		flags |= markerFlagChunked
	}
	if e.dedup {
		flags |= markerFlagDedup
	}

	canary, err := e.seal([]byte(crypto.CanaryPlaintext(canaryPlaintext, flags, uint64(e.chunkSize))))
	if err != nil {
		return nil, err
	}

	return crypto.EncodeMarker(markerSpec, crypto.Marker{
		Flags:  flags,
		Extra:  uint64(e.chunkSize),
		Canary: canary,
	})
}

// decodeMarker parses a marker and returns its sealed canary.
func decodeMarker(raw []byte) (marker, []byte, error) {
	decoded, err := crypto.DecodeMarker(markerSpec, raw)
	if err != nil {
		return marker{}, nil, err
	}

	return marker{
		encContent:  decoded.Flags&markerFlagContent != 0,
		encMetadata: decoded.Flags&markerFlagMetadata != 0,
		chunkSize:   int(decoded.Extra),
		dedup:       decoded.Flags&markerFlagDedup != 0,
	}, decoded.Canary, nil
}

// verifyCanary confirms the configured keys decrypt the stored canary.
func (e *encState) verifyCanary(canary []byte, m marker) error {
	return crypto.VerifyCanary(e.kc, canary, crypto.CanaryPlaintext(canaryPlaintext, m.flagsByte(), uint64(m.chunkSize)))
}

// DefaultChunkSize is the chunk width used for encrypted content when the
// caller does not select one. Chunking keeps writes and truncations from
// rewriting the whole file.
const DefaultChunkSize = 64 * 1024

func resolveConfig(cfg Config) (*encState, error) {
	if !cfg.encryptionEnabled() && !cfg.Compress {
		return nil, nil
	}
	if cfg.ChunkSize < 0 {
		return nil, fmt.Errorf("%w: chunk size cannot be negative", ErrInvalidConfig)
	}
	state := &encState{}
	if cfg.Compress {
		codec, err := compress.NewCodec(compress.Config{Level: cfg.CompressLevel, MinSize: cfg.MinCompress})
		if err != nil {
			return nil, err
		}
		state.comp = codec
	}
	if !cfg.encryptionEnabled() {
		return state, nil
	}
	chunkSize := cfg.ChunkSize
	if cfg.Dedup && chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	key, err := crypto.ResolveKey(cfg.Secret)
	if err != nil {
		return nil, err
	}

	kc, err := crypto.NewKeychain([][]byte{key})
	if err != nil {
		return nil, err
	}

	state.kc = kc
	state.encContent = cfg.EncryptContent || chunkSize > 0
	state.encMetadata = cfg.EncryptMetadata
	state.chunkSize = chunkSize
	state.dedup = cfg.Dedup
	return state, nil
}
