package aeadfs

import (
	"testing"
)

// TestConfig_Validate tests the Config validation
func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
			errMsg:  "config cannot be nil",
		},
		{
			name: "nil key provider",
			config: &Config{
				Cipher: CipherAES256GCM,
			},
			wantErr: true,
			errMsg:  "key provider cannot be nil",
		},
		{
			name: "unsupported cipher",
			config: &Config{
				Cipher:      CipherSuite(99),
				KeyProvider: testProvider("test"),
			},
			wantErr: true,
			errMsg:  "unsupported cipher suite",
		},
		{
			name: "valid minimal config",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
			},
			wantErr: false,
		},
		{
			name: "negative chunk size",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   -1,
			},
			wantErr: true,
			errMsg:  "chunk size cannot be negative",
		},
		{
			name: "chunk size too small",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   1024, // 1KB - too small
			},
			wantErr: true,
			errMsg:  "chunk size must be at least 4 KiB",
		},
		{
			name: "chunk size too large",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   32 * 1024 * 1024, // 32MB - too large
			},
			wantErr: true,
			errMsg:  "chunk size must not exceed 16 MiB",
		},
		{
			name: "valid chunk size",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   64 * 1024, // 64KB
			},
			wantErr: false,
		},
		{
			name: "unsupported filename encryption",
			config: &Config{
				Cipher:             CipherAES256GCM,
				KeyProvider:        testProvider("test"),
				FilenameEncryption: FilenameEncryption(99),
			},
			wantErr: true,
			errMsg:  "unsupported filename encryption mode",
		},
		{
			name: "random filename encryption without metadata path",
			config: &Config{
				Cipher:             CipherAES256GCM,
				KeyProvider:        testProvider("test"),
				FilenameEncryption: FilenameEncryptionRandom,
				MetadataPath:       "",
			},
			wantErr: true,
			errMsg:  "metadata path must be set",
		},
		{
			name: "random filename encryption with metadata path",
			config: &Config{
				Cipher:             CipherAES256GCM,
				KeyProvider:        testProvider("test"),
				FilenameEncryption: FilenameEncryptionRandom,
				MetadataPath:       "/tmp/metadata.db",
			},
			wantErr: false,
		},
		{
			name: "parallel without chunked mode",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   0, // Traditional mode
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           4,
					MinChunksForParallel: 4,
				},
			},
			wantErr: true,
			errMsg:  "parallel processing requires chunked mode",
		},
		{
			name: "negative max workers",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           -1,
					MinChunksForParallel: 4,
				},
			},
			wantErr: true,
			errMsg:  "parallel max workers cannot be negative",
		},
		{
			name: "too many workers",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           2000,
					MinChunksForParallel: 4,
				},
			},
			wantErr: true,
			errMsg:  "parallel max workers must not exceed 1024",
		},
		{
			name: "zero min chunks",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           4,
					MinChunksForParallel: 0,
				},
			},
			wantErr: true,
			errMsg:  "parallel min chunks threshold must be at least 1",
		},
		{
			name: "too many min chunks",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   64 * 1024,
				Parallel: ParallelConfig{
					Enabled:              true,
					MaxWorkers:           4,
					MinChunksForParallel: 2000,
				},
			},
			wantErr: true,
			errMsg:  "parallel min chunks threshold must not exceed 1000",
		},
		{
			name: "valid parallel config",
			config: &Config{
				Cipher:      CipherAES256GCM,
				KeyProvider: testProvider("test"),
				ChunkSize:   64 * 1024,
				Parallel:    DefaultParallelConfig(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Config.Validate() expected error containing %q, got nil", tt.errMsg)
				} else if tt.errMsg != "" && err.Error() != tt.errMsg {
					// Check if error message contains expected substring
					if len(err.Error()) < len(tt.errMsg) || err.Error()[:len(tt.errMsg)] != tt.errMsg {
						t.Errorf("Config.Validate() error = %q, want error containing %q", err.Error(), tt.errMsg)
					}
				}
			} else {
				if err != nil {
					t.Errorf("Config.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}

// TestParallelConfig_Validate tests ParallelConfig validation
func TestParallelConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  ParallelConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "disabled - always valid",
			config: ParallelConfig{
				Enabled: false,
			},
			wantErr: false,
		},
		{
			name: "negative workers",
			config: ParallelConfig{
				Enabled:              true,
				MaxWorkers:           -1,
				MinChunksForParallel: 4,
			},
			wantErr: true,
			errMsg:  "parallel max workers cannot be negative",
		},
		{
			name: "too many workers",
			config: ParallelConfig{
				Enabled:              true,
				MaxWorkers:           2000,
				MinChunksForParallel: 4,
			},
			wantErr: true,
			errMsg:  "parallel max workers must not exceed 1024",
		},
		{
			name: "zero min chunks",
			config: ParallelConfig{
				Enabled:              true,
				MaxWorkers:           4,
				MinChunksForParallel: 0,
			},
			wantErr: true,
			errMsg:  "parallel min chunks threshold must be at least 1",
		},
		{
			name: "valid config",
			config: ParallelConfig{
				Enabled:              true,
				MaxWorkers:           8,
				MinChunksForParallel: 4,
			},
			wantErr: false,
		},
		{
			name:    "default config",
			config:  DefaultParallelConfig(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParallelConfig.Validate() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ParallelConfig.Validate() unexpected error = %v", err)
				}
			}
		})
	}
}
