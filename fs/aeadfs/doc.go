// Package aeadfs provides a transparent encryption layer for the AbsFs
// filesystem abstraction, enabling secure at-rest encryption with modern
// cryptographic primitives.
//
// # Overview
//
// aeadfs implements the absfs.FileSystem interface, allowing it to wrap
// any AbsFs-compatible filesystem and provide transparent encryption and
// decryption of file contents.
//
// # Supported Cipher Suites
//
// Content Encryption:
//   - AES-256-GCM: Advanced Encryption Standard with 256-bit keys and
//     Galois/Counter Mode for authenticated encryption
//   - XChaCha20-Poly1305: Modern stream cipher with Poly1305 message
//     authentication
//
// Filename Encryption:
//   - AES-SIV: Synthetic Initialization Vector mode for deterministic
//     filename encryption (RFC 5297)
//
// Both content cipher suites provide:
//   - Authenticated Encryption with Associated Data (AEAD)
//   - Protection against tampering and corruption
//   - 128-bit authentication tags
//   - Nonce/IV uniqueness guarantees
//
// The cipher primitives (suite selection, engines, and nonce generation) are
// implemented in the shared internal/encryption package, which is also used by
// the db/codec/aead codec.
//
// # Basic Usage
//
//	// Create base filesystem
//	base := osfs.New()
//
//	// Create encrypted filesystem with a raw 32-byte key
//	key := loadKey() // exactly aeadfs.KeySize bytes, from secure storage
//	provider, err := aeadfs.NewRawKeyProvider(key)
//	if err != nil {
//	    panic(err)
//	}
//	config := &aeadfs.Config{
//	    Cipher:      aeadfs.CipherAES256GCM,
//	    KeyProvider: provider,
//	}
//
//	fs, err := aeadfs.New(base, config)
//	if err != nil {
//	    panic(err)
//	}
//
//	// Use like any absfs.FileSystem
//	file, _ := fs.Create("/secret.txt")
//	file.WriteString("This will be encrypted on disk")
//	file.Close()
//
// # Filename Encryption
//
// The package supports three modes of filename encryption:
//
//	// No filename encryption (content only)
//	config := &aeadfs.Config{
//	    FilenameEncryption: aeadfs.FilenameEncryptionNone,
//	}
//
//	// Deterministic filename encryption (SIV mode)
//	config := &aeadfs.Config{
//	    FilenameEncryption: aeadfs.FilenameEncryptionDeterministic,
//	    PreserveExtensions: true, // Keep .txt, .jpg visible
//	}
//
//	// Random filename encryption with metadata database
//	config := &aeadfs.Config{
//	    FilenameEncryption: aeadfs.FilenameEncryptionRandom,
//	    MetadataPath:       "/.aeadfs-metadata.json",
//	}
//
// Deterministic mode uses AES-SIV to encrypt filenames consistently (same name
// always produces same ciphertext), preserving directory structure while hiding
// filenames. Random mode assigns UUID-based names for maximum security.
//
// # Security Considerations
//
// Protected Against:
//   - Unauthorized access to encrypted files at rest
//   - Data tampering and corruption (authenticated encryption)
//   - Known-plaintext attacks (with proper key management)
//   - Offline brute-force attacks (with strong key derivation)
//
// Not Protected Against:
//   - Memory dumps while files are decrypted in memory
//   - Side-channel attacks (timing, cache)
//   - Compromised systems with keyloggers or malware
//   - Physical access attacks on running systems
//   - Metadata leakage (file sizes, access patterns - unless filename encryption enabled)
//
// Note: When using FilenameEncryptionDeterministic or FilenameEncryptionRandom,
// filenames are encrypted, significantly reducing metadata leakage.
//
// # Key Management
//
// aeadfs does not derive keys from passwords. The encryption key is supplied
// as a raw 32-byte (256-bit) value through a [KeyProvider]:
//
//	provider, err := aeadfs.NewRawKeyProvider(key) // len(key) == aeadfs.KeySize
//
// [NewEnvKeyProvider] reads the raw key from an environment variable. Callers
// that need password-based derivation (for example PBKDF2 or Argon2id) should
// perform it outside this package and pass the resulting 32-byte key to
// [NewRawKeyProvider].
//
// # File Format
//
// Traditional (single-chunk) encrypted files:
//   - Magic bytes (4 bytes): "ENCR" (0x454E4352)
//   - Version (1 byte): File format version
//   - Cipher suite (1 byte): Identifies the encryption algorithm
//   - Nonce size (2 bytes): Length of the nonce
//   - Nonce (variable): Random nonce for encryption
//   - Ciphertext (variable): Encrypted data + authentication tag
//
// # Chunked File Format
//
// For efficient random access, files can be encrypted in chunks (enabled via
// Config.ChunkSize). Chunked files use this format:
//
//   - File Header (same as above)
//   - Chunk Index (20 KB reserved):
//   - Chunk size (4 bytes): Size of plaintext chunks
//   - Chunk count (4 bytes): Number of chunks
//   - Chunk offsets (8 bytes each): File offset for each chunk
//   - Plaintext sizes (4 bytes each): Plaintext size for each chunk
//   - Padding (fills remaining reserved space)
//   - Encrypted Chunks (variable number):
//   - Chunk header (plaintext size + nonce)
//   - Ciphertext (encrypted chunk data + auth tag)
//
// Benefits of chunked format:
//   - Efficient seeking without decrypting entire file
//   - Random access to any chunk
//   - Modify individual chunks without rewriting entire file
//   - LRU cache reduces disk I/O for frequently accessed chunks
//   - Each chunk has independent nonce for security
//
// Usage:
//
//	config := &aeadfs.Config{
//	    Cipher: aeadfs.CipherAES256GCM,
//	    KeyProvider: keyProvider,
//	    ChunkSize: 64 * 1024, // 64 KB chunks (default)
//	}
//
// # Performance
//
// The implementation uses Go's standard crypto package which includes
// hardware acceleration (AES-NI) when available. Performance characteristics:
//   - AES-256-GCM: Typically >2 GB/s with AES-NI
//   - XChaCha20-Poly1305: Typically >1 GB/s
//   - Key handling: raw 32-byte key, no runtime derivation
package aeadfs
