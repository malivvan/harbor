package cryptfs

import (
	"testing"
)

func TestCipherSuite_String(t *testing.T) {
	tests := []struct {
		cipher   CipherSuite
		expected string
	}{
		{CipherAuto, "auto"},
		{CipherAES256GCM, "aes-256-gcm"},
		{CipherXChaCha20Poly1305, "xchacha20-poly1305"},
		{CipherSuite(99), "unknown"},
	}

	for _, tt := range tests {
		result := tt.cipher.String()
		if result != tt.expected {
			t.Errorf("CipherSuite(%d).String() = %q, want %q", tt.cipher, result, tt.expected)
		}
	}
}

func TestConfig_ValidateFull(t *testing.T) {
	validKeyProvider := testProvider("password")

	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			name:      "nil config",
			config:    nil,
			expectErr: true,
		},
		{
			name: "nil key provider",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: nil,
			},
			expectErr: true,
		},
		{
			name: "invalid cipher",
			config: &Config{
				Cipher:      CipherSuite(99),
				KeyProvider: validKeyProvider,
			},
			expectErr: true,
		},
		{
			name: "invalid filename encryption",
			config: &Config{
				Cipher:             CipherAES256GCM,
				KeyProvider:        validKeyProvider,
				FilenameEncryption: FilenameEncryption(99),
			},
			expectErr: true,
		},
		{
			name: "random filename encryption without metadata path",
			config: &Config{
				Cipher:             CipherAES256GCM,
				KeyProvider:        validKeyProvider,
				FilenameEncryption: FilenameEncryptionRandom,
				MetadataPath:       "",
			},
			expectErr: true,
		},
		{
			name: "negative chunk size",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   -1,
			},
			expectErr: true,
		},
		{
			name: "chunk size too small",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   1024, // Less than 4KB
			},
			expectErr: true,
		},
		{
			name: "chunk size too large",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   32 * 1024 * 1024, // More than 16MB
			},
			expectErr: true,
		},
		{
			name: "negative parallel workers",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:    true,
					MaxWorkers: -1,
				},
			},
			expectErr: true,
		},
		{
			name: "too many parallel workers",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           2048,
					MinChunksForParallel: 2,
				},
			},
			expectErr: true,
		},
		{
			name: "parallel without chunked mode",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   0,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           4,
					MinChunksForParallel: 2,
				},
			},
			expectErr: true,
		},
		{
			name: "min chunks too low",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           4,
					MinChunksForParallel: 0,
				},
			},
			expectErr: true,
		},
		{
			name: "min chunks too high",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: validKeyProvider,
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           4,
					MinChunksForParallel: 10000,
				},
			},
			expectErr: true,
		},
		{
			name: "valid config with all options",
			config: &Config{
				Cipher:             CipherAES256GCM,
				KeyProvider:        validKeyProvider,
				FilenameEncryption: FilenameEncryptionDeterministic,
				ChunkSize:          64 * 1024,
				EnableSeek:         true,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           4,
					MinChunksForParallel: 2,
				},
			},
			expectErr: false,
		},
		{
			name: "valid config with random filename encryption",
			config: &Config{
				Cipher:             CipherAES256GCM,
				KeyProvider:        validKeyProvider,
				FilenameEncryption: FilenameEncryptionRandom,
				MetadataPath:       "/path/to/metadata",
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}
