package aeadfs

import (
	"errors"

	"github.com/malivvan/storm/internal/encryption"
)

// CipherSuite represents the encryption algorithm to use. It is an alias for
// the shared implementation in internal/encryption.
type CipherSuite = encryption.CipherSuite

const (
	// CipherAuto automatically selects the best cipher based on hardware capabilities
	CipherAuto = encryption.CipherAuto
	// CipherAES256GCM uses AES-256 with Galois/Counter Mode
	CipherAES256GCM = encryption.CipherAES256GCM
	// CipherXChaCha20Poly1305 uses XChaCha20 stream cipher with Poly1305 MAC
	CipherXChaCha20Poly1305 = encryption.CipherXChaCha20Poly1305
)

// FilenameEncryption represents the filename encryption mode
type FilenameEncryption uint8

const (
	// FilenameEncryptionNone does not encrypt filenames
	FilenameEncryptionNone FilenameEncryption = iota
	// FilenameEncryptionDeterministic uses SIV mode for deterministic encryption
	FilenameEncryptionDeterministic
	// FilenameEncryptionRandom uses random encryption with metadata database
	FilenameEncryptionRandom
)

// Config contains configuration for the encrypted filesystem
type Config struct {
	// Cipher suite to use for encryption
	Cipher CipherSuite

	// KeyProvider supplies encryption keys
	KeyProvider KeyProvider

	// FilenameEncryption mode (Phase 3 feature)
	FilenameEncryption FilenameEncryption

	// PreserveExtensions keeps file extensions visible when using filename encryption
	PreserveExtensions bool

	// MetadataPath is the path to store metadata for random filename encryption
	MetadataPath string

	// ChunkSize for streaming encryption (Phase 4 feature)
	ChunkSize int

	// EnableSeek allows seeking within encrypted files (Phase 4 feature)
	EnableSeek bool

	// Parallel controls parallel chunk processing (Phase 5 feature)
	Parallel ParallelConfig
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config cannot be nil")
	}

	// Validate KeyProvider
	if c.KeyProvider == nil {
		return errors.New("key provider cannot be nil")
	}

	// Validate CipherSuite
	if c.Cipher != CipherAES256GCM && c.Cipher != CipherXChaCha20Poly1305 && c.Cipher != CipherAuto {
		return errors.New("unsupported cipher suite")
	}

	// Validate FilenameEncryption
	if c.FilenameEncryption != FilenameEncryptionNone &&
		c.FilenameEncryption != FilenameEncryptionDeterministic &&
		c.FilenameEncryption != FilenameEncryptionRandom {
		return errors.New("unsupported filename encryption mode")
	}

	// Validate MetadataPath for random filename encryption
	if c.FilenameEncryption == FilenameEncryptionRandom && c.MetadataPath == "" {
		return errors.New("metadata path must be set when using random filename encryption")
	}

	// Validate ChunkSize
	if c.ChunkSize < 0 {
		return errors.New("chunk size cannot be negative")
	}
	if c.ChunkSize > 0 {
		// ChunkSize must be between 4KB and 16MB
		const minChunkSize = 4 * 1024         // 4KB
		const maxChunkSize = 16 * 1024 * 1024 // 16MB

		if c.ChunkSize < minChunkSize {
			return errors.New("chunk size must be at least 4 KiB when chunked mode is enabled")
		}
		if c.ChunkSize > maxChunkSize {
			return errors.New("chunk size must not exceed 16 MiB")
		}

		// Warn if not a power of 2 (not an error, but suboptimal)
		// Check if power of 2: n & (n-1) == 0
		if c.ChunkSize&(c.ChunkSize-1) != 0 {
			// Not a power of 2 - this is allowed but not recommended
			// Could log a warning here if we had a logger
		}
	}

	// Validate ParallelConfig
	if c.Parallel.Enabled {
		if c.Parallel.MaxWorkers < 0 {
			return errors.New("parallel max workers cannot be negative")
		}
		if c.Parallel.MaxWorkers > 1024 {
			return errors.New("parallel max workers must not exceed 1024")
		}
		if c.Parallel.MinChunksForParallel < 1 {
			return errors.New("parallel min chunks threshold must be at least 1")
		}
		if c.Parallel.MinChunksForParallel > 1000 {
			return errors.New("parallel min chunks threshold must not exceed 1000")
		}

		// Parallel processing requires chunked mode
		if c.ChunkSize == 0 {
			return errors.New("parallel processing requires chunked mode (ChunkSize > 0)")
		}
	}

	return nil
}

// KeyProvider supplies the raw 32-byte master key used for encryption.
// Implementations do not perform password-based key derivation.
type KeyProvider interface {
	// Key returns the [KeySize]-byte master key.
	Key() ([]byte, error)
}

// Common errors moved to errors.go for better organization
