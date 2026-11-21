package aeadfs

import (
	"bytes"
	"testing"
)

func TestNewRawKeyProvider(t *testing.T) {
	key := make([]byte, KeySize)
	for i := range key {
		key[i] = byte(i)
	}

	provider, err := NewRawKeyProvider(key)
	if err != nil {
		t.Fatalf("NewRawKeyProvider: %v", err)
	}

	got, err := provider.Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatalf("Key() = %x, want %x", got, key)
	}
}

func TestNewRawKeyProviderRejectsWrongLength(t *testing.T) {
	for _, size := range []int{0, 1, KeySize - 1, KeySize + 1, 64} {
		if _, err := NewRawKeyProvider(make([]byte, size)); err == nil {
			t.Errorf("NewRawKeyProvider(%d bytes) succeeded, want error", size)
		}
	}
}

func TestRawKeyProviderCopiesKey(t *testing.T) {
	key := bytes.Repeat([]byte{0xAB}, KeySize)
	provider, err := NewRawKeyProvider(key)
	if err != nil {
		t.Fatalf("NewRawKeyProvider: %v", err)
	}

	// Mutating the caller's slice must not change the provider's key.
	key[0] = 0x00

	got, err := provider.Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if got[0] != 0xAB {
		t.Fatalf("provider key changed after caller mutation: %x", got)
	}

	// Mutating the returned slice must not change the provider's key.
	got[1] = 0x00
	again, err := provider.Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if again[1] != 0xAB {
		t.Fatalf("provider key changed after returned-slice mutation: %x", again)
	}
}

func TestEnvKeyProvider(t *testing.T) {
	key := string(bytes.Repeat([]byte{0x7F}, KeySize))

	t.Setenv("CRYPTFS_TEST_KEY", key)
	provider := NewEnvKeyProvider("CRYPTFS_TEST_KEY")

	got, err := provider.Key()
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if string(got) != key {
		t.Fatalf("Key() = %q, want %q", got, key)
	}
}

func TestEnvKeyProviderErrors(t *testing.T) {
	t.Setenv("CRYPTFS_TEST_KEY", "")
	if _, err := NewEnvKeyProvider("CRYPTFS_TEST_KEY").Key(); err == nil {
		t.Error("missing environment variable should fail")
	}

	t.Setenv("CRYPTFS_TEST_KEY", "too-short")
	if _, err := NewEnvKeyProvider("CRYPTFS_TEST_KEY").Key(); err == nil {
		t.Error("short key should fail")
	}

	t.Setenv("CRYPTFS_TEST_KEY", string(bytes.Repeat([]byte{0x01}, KeySize+1)))
	if _, err := NewEnvKeyProvider("CRYPTFS_TEST_KEY").Key(); err == nil {
		t.Error("long key should fail")
	}
}
