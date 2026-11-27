package system

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	hstore "github.com/malivvan/tempest/store"
	bolt "go.etcd.io/bbolt"
)

func TestKDFPresets(t *testing.T) {
	if FastKDFTime != 1 || FastKDFMemory != 64*MiB || FastKDFThreads != 1 {
		t.Fatalf("fast preset = (%d, %d, %d)", FastKDFTime, FastKDFMemory, FastKDFThreads)
	}
	if DefaultKDFTime != 3 || DefaultKDFMemory != 64*MiB || DefaultKDFThreads != 4 {
		t.Fatalf("default preset = (%d, %d, %d)", DefaultKDFTime, DefaultKDFMemory, DefaultKDFThreads)
	}
	if StrongKDFTime != 4 || StrongKDFMemory != 256*MiB || StrongKDFThreads != 8 {
		t.Fatalf("strong preset = (%d, %d, %d)", StrongKDFTime, StrongKDFMemory, StrongKDFThreads)
	}

	if FastKDFTime >= DefaultKDFTime || DefaultKDFTime >= StrongKDFTime {
		t.Fatal("preset time values are not ordered")
	}
	if FastKDFMemory > DefaultKDFMemory || DefaultKDFMemory >= StrongKDFMemory {
		t.Fatal("preset memory values are not ordered")
	}
	if FastKDFThreads >= DefaultKDFThreads || DefaultKDFThreads >= StrongKDFThreads {
		t.Fatal("preset thread values are not ordered")
	}
}

func TestValidateKDFParams(t *testing.T) {
	tests := []struct {
		name    string
		time    uint32
		memory  uint32
		threads uint8
		flags   uint16
		wantErr bool
	}{
		{name: "default", time: DefaultKDFTime, memory: DefaultKDFMemory, threads: DefaultKDFThreads, flags: EncryptAll},
		{name: "zero time", memory: DefaultKDFMemory, threads: 1, wantErr: true},
		{name: "zero memory", time: 1, threads: 1, wantErr: true},
		{name: "zero threads", time: 1, memory: DefaultKDFMemory, wantErr: true},
		{name: "memory below 8*threads", time: 1, memory: 8, threads: 2, wantErr: true},
		{name: "unknown flag", time: 1, memory: 64, threads: 1, flags: uint16(1) << 15, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKDFParams(tt.time, tt.memory, tt.threads, tt.flags, ErrInvalidOptions)
			if tt.wantErr && !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("ValidateKDFParams() = %v, want ErrInvalidOptions", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateKDFParams() = %v, want nil", err)
			}
		})
	}
}

func TestNewSalt(t *testing.T) {
	first, err := NewSalt()
	if err != nil {
		t.Fatalf("newSalt: %v", err)
	}
	if len(first) != SaltSize {
		t.Fatalf("salt length = %d, want %d", len(first), SaltSize)
	}
	second, err := NewSalt()
	if err != nil {
		t.Fatalf("newSalt: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("newSalt returned the same salt twice")
	}
}

func TestDeriveSeed(t *testing.T) {
	salt := []byte("0123456789abcdef")
	first := DeriveSeed([]byte("password"), salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if len(first) != KeySize {
		t.Fatalf("seed length = %d, want %d", len(first), KeySize)
	}
	again := DeriveSeed([]byte("password"), salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if !bytes.Equal(first, again) {
		t.Fatal("deriveSeed is not deterministic")
	}
	if bytes.Equal(first, DeriveSeed([]byte("other"), salt, FastKDFTime, FastKDFMemory, FastKDFThreads)) {
		t.Fatal("different passwords produced the same seed")
	}
	if bytes.Equal(first, DeriveSeed([]byte("password"), []byte("fedcba9876543210"), FastKDFTime, FastKDFMemory, FastKDFThreads)) {
		t.Fatal("different salts produced the same seed")
	}
}

func TestHasFlag(t *testing.T) {
	flags := EncryptRecordKeys | EncryptFileContent
	if !HasFlag(flags, EncryptRecordKeys) || !HasFlag(flags, EncryptFileContent) || !HasFlag(flags, EncryptRecordKeys|EncryptFileContent) {
		t.Fatal("Has did not report set flags")
	}
	if HasFlag(flags, EncryptRecordValues) {
		t.Fatal("Has reported an unset flag")
	}
	if HasFlag(flags, EncryptRecordKeys|EncryptRecordValues) {
		t.Fatal("Has must require all requested bits")
	}
}

func TestMetadataEncodingRoundTrip(t *testing.T) {
	md := Metadata{
		Salt:    []byte("0123456789abcdef"),
		Time:    DefaultKDFTime,
		Flags:   EncryptRecordKeys | EncryptFileContent,
		Memory:  DefaultKDFMemory,
		Threads: DefaultKDFThreads,
		Canary:  []byte("canary-ciphertext"),
	}
	raw := md.Encode()
	if len(raw) != metadataFixedSize+len(md.Canary) {
		t.Fatalf("encoded size = %d, want %d", len(raw), metadataFixedSize+len(md.Canary))
	}
	got, err := DecodeMetadata(raw)
	if err != nil {
		t.Fatalf("decodeMetadata: %v", err)
	}
	if !bytes.Equal(got.Salt, md.Salt) || !bytes.Equal(got.Canary, md.Canary) ||
		got.Time != md.Time || got.Flags != md.Flags || got.Memory != md.Memory || got.Threads != md.Threads {
		t.Fatalf("round-trip = %+v, want %+v", got, md)
	}
}

func TestMetadataStorageRoundTrip(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "meta.db"), 0600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })

	if _, creating, err := ReadMetadata(bdb); err != nil || !creating {
		t.Fatalf("readMetadata empty = creating %v, err %v", creating, err)
	}

	md := Metadata{
		Salt:    []byte("0123456789abcdef"),
		Time:    FastKDFTime,
		Flags:   EncryptAll,
		Memory:  FastKDFMemory,
		Threads: FastKDFThreads,
		Canary:  []byte("canary"),
	}
	if err := WriteMetadata(bdb, md); err != nil {
		t.Fatalf("writeMetadata: %v", err)
	}
	got, creating, err := ReadMetadata(bdb)
	if err != nil {
		t.Fatalf("readMetadata: %v", err)
	}
	if creating {
		t.Fatal("readMetadata reported an existing store as new")
	}
	if !bytes.Equal(got.Salt, md.Salt) || got.Flags != md.Flags {
		t.Fatalf("readMetadata = %+v, want %+v", got, md)
	}
}

func TestReadMetadataRejectsMalformedPayloads(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "meta.db"), 0600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	t.Cleanup(func() { _ = bdb.Close() })

	md := Metadata{
		Salt:    []byte("0123456789abcdef"),
		Time:    FastKDFTime,
		Flags:   EncryptAll,
		Memory:  FastKDFMemory,
		Threads: FastKDFThreads,
		Canary:  []byte("canary"),
	}
	raw := md.Encode()

	for name, data := range map[string][]byte{
		"truncated": raw[:len(raw)-1],
		"trailing":  append(append([]byte(nil), raw...), 0x00),
	} {
		t.Run(name, func(t *testing.T) {
			if err := bdb.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte(hstore.SYSTEM))
				if err != nil {
					return err
				}
				return b.Put([]byte("Metadata"), data)
			}); err != nil {
				t.Fatalf("write Metadata: %v", err)
			}
			if _, _, err := ReadMetadata(bdb); !errors.Is(err, ErrCorruptStore) {
				t.Fatalf("ReadMetadata(%s) = %v, want ErrCorruptStore", name, err)
			}
		})
	}
}

func TestKDFParametersAreCapped(t *testing.T) {
	tests := []struct {
		name    string
		time    uint32
		memory  uint32
		threads uint8
	}{
		{"time", MaxKDFTime + 1, FastKDFMemory, 1},
		{"memory", 1, MaxKDFMemory + 1, 1},
		{"threads", 1, 64 * MiB, MaxKDFThreads + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKDFParams(tt.time, tt.memory, tt.threads, EncryptAll, ErrInvalidOptions)
			if !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("ValidateKDFParams() = %v, want ErrInvalidOptions", err)
			}
		})
	}
}
