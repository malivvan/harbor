package fs

import (
	"encoding/binary"
	"fmt"

	"github.com/malivvan/storm/internal/encryption"
)

// CipherSuite selects the AEAD cipher used by an encrypted filesystem.
type CipherSuite = encryption.CipherSuite

// CipherEngine performs AEAD encryption and decryption.
type CipherEngine = encryption.CipherEngine

const (
	// CipherAuto selects AES-256-GCM.
	CipherAuto = encryption.CipherAuto
	// CipherAES256GCM uses AES-256-GCM.
	CipherAES256GCM = encryption.CipherAES256GCM
	// CipherXChaCha20Poly1305 uses XChaCha20-Poly1305.
	CipherXChaCha20Poly1305 = encryption.CipherXChaCha20Poly1305

	// KeySize is the required key length in bytes (256 bits).
	KeySize = encryption.KeySize
)

// Config enables optional encryption for a Store and its file systems. A nil
// Config, or one with no encryption feature enabled, keeps the plaintext
// on-disk format.
type Config struct {
	// Cipher selects the AEAD cipher. CipherAuto (the default) resolves to
	// AES-256-GCM.
	Cipher CipherSuite

	// Key is a raw 32-byte master key. Exactly one of Key and KeyProvider must
	// be set when encryption is enabled.
	Key []byte

	// KeyProvider supplies the master key instead of Key.
	KeyProvider KeyProvider

	// EncryptContent seals file contents (the data bucket).
	EncryptContent bool

	// EncryptMetadata seals inode blobs, hiding names, modes, sizes and times.
	EncryptMetadata bool

	// ChunkSize enables chunked content encryption with chunks of this many
	// plaintext bytes. A positive value implies EncryptContent.
	ChunkSize int
}

func (c *Config) encryptionEnabled() bool {
	return c != nil && (c.EncryptContent || c.EncryptMetadata || c.ChunkSize != 0)
}

// KeyProvider supplies the raw 32-byte master key used for encryption.
type KeyProvider = encryption.KeyProvider

// RawKeyProvider supplies a fixed 32-byte master key.
type RawKeyProvider = encryption.RawKeyProvider

// NewRawKeyProvider returns a provider that uses key directly.
func NewRawKeyProvider(key []byte) (*RawKeyProvider, error) {
	return encryption.NewRawKeyProvider(key)
}

// EnvKeyProvider reads a raw 32-byte master key from an environment variable.
type EnvKeyProvider = encryption.EnvKeyProvider

// NewEnvKeyProvider creates a key provider backed by an environment variable.
func NewEnvKeyProvider(envVar string) *EnvKeyProvider {
	return encryption.NewEnvKeyProvider(envVar)
}

// MultiKeyProvider supplies several keys for rotation: the first is used for
// new writes, and every key is tried when decrypting existing data.
type MultiKeyProvider = encryption.MultiKeyProvider

// NewMultiKeyProvider creates a multi-key provider. At least one provider is
// required; the first one is the primary key.
func NewMultiKeyProvider(providers ...KeyProvider) (*MultiKeyProvider, error) {
	return encryption.NewMultiKeyProvider(providers...)
}

// encState is the shared encryption configuration of a Store. A nil encState
// means plaintext.
type encState struct {
	engine      CipherEngine
	engines     []CipherEngine
	cipher      CipherSuite
	encContent  bool
	encMetadata bool
	chunkSize   int
}

func (e *encState) enabled() bool {
	return e != nil
}

func (e *encState) seal(plain []byte) ([]byte, error) {
	nonce, err := encryption.GenerateNonce(e.cipher)
	if err != nil {
		return nil, err
	}
	sealed, err := e.engine.Encrypt(nonce, plain)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(nonce)+len(sealed))
	out = append(out, nonce...)
	return append(out, sealed...), nil
}

func (e *encState) open(raw []byte) ([]byte, error) {
	if e == nil {
		return raw, nil
	}
	for _, engine := range e.engines {
		nonceSize := engine.NonceSize()
		if len(raw) < nonceSize+engine.Overhead() {
			continue
		}
		plain, err := engine.Decrypt(raw[:nonceSize], raw[nonceSize:])
		if err == nil {
			return plain, nil
		}
	}
	return nil, ErrAuthFailed
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
	if e == nil || !e.encContent || len(raw) == 0 {
		return raw, nil
	}
	return e.open(raw)
}

// Marker constants. The marker lives (unencrypted) in each filesystem's state
// bucket so a plaintext open can detect an encrypted filesystem.
const (
	markerKey       = "\x00__fs_enc__"
	markerMagic     = "FENC"
	markerVersion   = byte(1)
	canaryPlaintext = "storm/fs/aead/v1"
)

const (
	markerFlagContent  = 1 << 0
	markerFlagMetadata = 1 << 1
	markerFlagChunked  = 1 << 2
)

type marker struct {
	cipher      CipherSuite
	encContent  bool
	encMetadata bool
	chunkSize   int
}

func (m marker) matches(e *encState) bool {
	return m.cipher == e.cipher &&
		m.encContent == e.encContent &&
		m.encMetadata == e.encMetadata &&
		m.chunkSize == e.chunkSize
}

func (e *encState) encodeMarker() ([]byte, error) {
	canary, err := e.seal([]byte(canaryPlaintext))
	if err != nil {
		return nil, err
	}

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

	raw := make([]byte, 0, 4+1+1+1+8+len(canary))
	raw = append(raw, markerMagic...)
	raw = append(raw, markerVersion, byte(e.cipher), flags)
	var chunkSize [8]byte
	binary.LittleEndian.PutUint64(chunkSize[:], uint64(e.chunkSize))
	raw = append(raw, chunkSize[:]...)
	raw = append(raw, canary...)
	return raw, nil
}

func decodeMarker(raw []byte) (marker, []byte, error) {
	const fixedSize = 4 + 1 + 1 + 1 + 8
	if len(raw) < fixedSize {
		return marker{}, nil, fmt.Errorf("%w: corrupt encryption marker", ErrWrongMode)
	}
	if string(raw[:4]) != markerMagic {
		return marker{}, nil, fmt.Errorf("%w: corrupt encryption marker", ErrWrongMode)
	}
	if raw[4] != markerVersion {
		return marker{}, nil, fmt.Errorf("%w: unsupported encryption marker version", ErrWrongMode)
	}

	m := marker{
		cipher:      CipherSuite(raw[5]),
		encContent:  raw[6]&markerFlagContent != 0,
		encMetadata: raw[6]&markerFlagMetadata != 0,
	}
	if raw[6]&markerFlagChunked != 0 {
		m.chunkSize = int(binary.LittleEndian.Uint64(raw[7:15]))
	}
	return m, raw[fixedSize:], nil
}

func (e *encState) verifyCanary(canary []byte) error {
	plain, err := e.open(canary)
	if err != nil {
		return err
	}
	if string(plain) != canaryPlaintext {
		return ErrAuthFailed
	}
	return nil
}

func resolveConfig(cfg *Config) (*encState, error) {
	if !cfg.encryptionEnabled() {
		return nil, nil
	}
	if cfg.ChunkSize < 0 {
		return nil, fmt.Errorf("%w: chunk size cannot be negative", ErrInvalidConfig)
	}

	cipherSuite, err := encryption.Resolve(cfg.Cipher)
	if err != nil {
		return nil, ErrUnsupportedCipher
	}

	keys, err := encryption.ResolveKeys(cfg.Key, cfg.KeyProvider)
	if err != nil {
		return nil, err
	}

	state := &encState{
		cipher:      cipherSuite,
		encContent:  cfg.EncryptContent || cfg.ChunkSize > 0,
		encMetadata: cfg.EncryptMetadata,
		chunkSize:   cfg.ChunkSize,
		engines:     make([]CipherEngine, 0, len(keys)),
	}
	for _, key := range keys {
		engine, err := encryption.NewCipherEngine(cipherSuite, key)
		if err != nil {
			return nil, err
		}
		state.engines = append(state.engines, engine)
	}
	state.engine = state.engines[0]
	return state, nil
}
