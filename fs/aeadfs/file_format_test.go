package aeadfs

import (
	"bytes"
	"testing"
)

func TestFileHeader_NewFileHeader(t *testing.T) {
	nonce := make([]byte, 12)

	header := NewFileHeader(CipherAES256GCM, nonce)

	if header.Magic != MagicBytes {
		t.Errorf("Magic: got %x, want %x", header.Magic, MagicBytes)
	}

	if header.Version != CurrentVersion {
		t.Errorf("Version: got %d, want %d", header.Version, CurrentVersion)
	}

	if header.Cipher != CipherAES256GCM {
		t.Errorf("Cipher: got %d, want %d", header.Cipher, CipherAES256GCM)
	}

	if header.NonceSize != 12 {
		t.Errorf("NonceSize: got %d, want 12", header.NonceSize)
	}
}

func TestFileHeader_Size(t *testing.T) {
	nonce := make([]byte, 12)

	header := NewFileHeader(CipherAES256GCM, nonce)

	// MinHeaderSize (6) + nonce size field (2) + nonce (12) = 20
	expectedSize := MinHeaderSize + 2 + 12
	if header.Size() != expectedSize {
		t.Errorf("Size: got %d, want %d", header.Size(), expectedSize)
	}
}

func TestFileHeader_WriteTo_ReadFrom(t *testing.T) {
	nonce := []byte("123456789012")

	header := NewFileHeader(CipherAES256GCM, nonce)

	// Write
	buf := new(bytes.Buffer)
	n, err := header.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	expectedSize := int64(header.Size())
	if n != expectedSize {
		t.Errorf("WriteTo wrote %d bytes, want %d", n, expectedSize)
	}

	// Read
	readHeader := &FileHeader{}
	n, err = readHeader.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify fields
	if readHeader.Magic != header.Magic {
		t.Errorf("Magic mismatch")
	}
	if readHeader.Version != header.Version {
		t.Errorf("Version mismatch")
	}
	if readHeader.Cipher != header.Cipher {
		t.Errorf("Cipher mismatch")
	}
	if !bytes.Equal(readHeader.Nonce, header.Nonce) {
		t.Errorf("Nonce mismatch")
	}
}

func TestFileHeader_Validate(t *testing.T) {
	tests := []struct {
		name      string
		header    *FileHeader
		expectErr bool
	}{
		{
			name: "Valid AES-GCM",
			header: &FileHeader{
				Magic:   MagicBytes,
				Version: CurrentVersion,
				Cipher:  CipherAES256GCM,
				Nonce:   make([]byte, 12),
			},
			expectErr: false,
		},
		{
			name: "Valid XChaCha20",
			header: &FileHeader{
				Magic:   MagicBytes,
				Version: CurrentVersion,
				Cipher:  CipherXChaCha20Poly1305,
				Nonce:   make([]byte, 12),
			},
			expectErr: false,
		},
		{
			name: "Invalid magic",
			header: &FileHeader{
				Magic:   0x12345678,
				Version: CurrentVersion,
				Cipher:  CipherAES256GCM,
				Nonce:   make([]byte, 12),
			},
			expectErr: true,
		},
		{
			name: "Future version",
			header: &FileHeader{
				Magic:   MagicBytes,
				Version: CurrentVersion + 1,
				Cipher:  CipherAES256GCM,
				Nonce:   make([]byte, 12),
			},
			expectErr: true,
		},
		{
			name: "Invalid cipher",
			header: &FileHeader{
				Magic:   MagicBytes,
				Version: CurrentVersion,
				Cipher:  99,
				Nonce:   make([]byte, 12),
			},
			expectErr: true,
		},
		{
			name: "Empty nonce",
			header: &FileHeader{
				Magic:   MagicBytes,
				Version: CurrentVersion,
				Cipher:  CipherAES256GCM,
				Nonce:   []byte{},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.header.Validate()
			if tt.expectErr && err == nil {
				t.Error("Expected error")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestFileHeader_ReadFrom_InvalidMagic(t *testing.T) {
	// Create invalid data with wrong magic bytes
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x01}
	buf := bytes.NewReader(data)

	header := &FileHeader{}
	_, err := header.ReadFrom(buf)

	if err != ErrInvalidHeader {
		t.Errorf("Expected ErrInvalidHeader, got %v", err)
	}
}

func TestFileHeader_ReadFrom_FutureVersion(t *testing.T) {
	// Create data with future version
	buf := new(bytes.Buffer)

	// Write magic (little-endian)
	buf.Write([]byte{0x52, 0x43, 0x4E, 0x45}) // "ENCR" in little-endian
	// Write version (future)
	buf.WriteByte(0xFF)

	header := &FileHeader{}
	_, err := header.ReadFrom(buf)

	if err != ErrUnsupportedVersion {
		t.Errorf("Expected ErrUnsupportedVersion, got %v", err)
	}
}
