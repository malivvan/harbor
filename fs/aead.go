package fs

import (
	"fmt"

	"github.com/malivvan/harbor/audit"
	"github.com/malivvan/harbor/internal/crypto"
)

// KeySize is the required key length in bytes (256 bits).
const KeySize = crypto.KeySize

// Config enables optional encryption for a Store and its file systems. A zero
// Config, or one with no encryption feature enabled, keeps the plaintext
// on-disk format.
type Config struct {
	// Root is the bucket path below which all file systems live. Each element
	// is one nested bucket name. A nil or empty Root means the database root;
	// the harbor root package passes [][]byte{[]byte("fs")} explicitly.
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

	// Audit, when non-nil, receives an entry for every committed write.
	Audit audit.Sink
}

func (c Config) encryptionEnabled() bool {
	return c.EncryptContent || c.EncryptMetadata || c.ChunkSize != 0
}

// encState is the shared encryption configuration of a Store. A nil encState
// means plaintext.
type encState struct {
	kc          *crypto.Keychain
	encContent  bool
	encMetadata bool
	chunkSize   int
}

func (e *encState) enabled() bool {
	return e != nil
}

func (e *encState) seal(plain []byte) ([]byte, error) {
	if e == nil {
		return plain, nil
	}
	return e.kc.Seal(plain)
}

func (e *encState) open(raw []byte) ([]byte, error) {
	if e == nil {
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
	if e == nil || !e.encContent {
		return plain, nil
	}
	return e.seal(plain)
}

func (e *encState) openContent(raw []byte) ([]byte, error) {
	if e == nil || !e.encContent {
		return raw, nil
	}
	return e.open(raw)
}

// Marker constants. The marker lives (unencrypted) in each filesystem's state
// bucket so a plaintext open can detect an encrypted filesystem.
const (
	markerKey       = "\x00__fs_enc__"
	markerMagic     = "FENC"
	canaryPlaintext = "harbor/fs/aead/v1"
)

const (
	markerFlagContent  = 1 << 0
	markerFlagMetadata = 1 << 1
	markerFlagChunked  = 1 << 2
)

// markerSpec describes the on-disk marker framing shared by every store.
var markerSpec = crypto.MarkerSpec{Magic: markerMagic, ExtraFieldSize: 8}

// marker describes the encryption mode recorded in a filesystem.
type marker struct {
	encContent  bool
	encMetadata bool
	chunkSize   int
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
	return f
}

func (m marker) matches(e *encState) bool {
	return m.encContent == e.encContent &&
		m.encMetadata == e.encMetadata &&
		m.chunkSize == e.chunkSize
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
	if !cfg.encryptionEnabled() {
		return nil, nil
	}
	if cfg.ChunkSize < 0 {
		return nil, fmt.Errorf("%w: chunk size cannot be negative", ErrInvalidConfig)
	}
	chunkSize := cfg.ChunkSize

	key, err := crypto.ResolveKey(cfg.Secret)
	if err != nil {
		return nil, err
	}

	kc, err := crypto.NewKeychain([][]byte{key})
	if err != nil {
		return nil, err
	}

	return &encState{
		kc:          kc,
		encContent:  cfg.EncryptContent || chunkSize > 0,
		encMetadata: cfg.EncryptMetadata,
		chunkSize:   chunkSize,
	}, nil
}
