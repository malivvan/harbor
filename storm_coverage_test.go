package storm

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/malivvan/storm/internal/crypto"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestCanaryHelpers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canary.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	md, keys, err := loadOrCreateState(bdb, testOptions("pw"))
	require.NoError(t, err)
	defer keys.wipe()

	sealed, err := sealCanary(keys.canary, md.Flags)
	require.NoError(t, err)
	require.NoError(t, verifyCanary(keys.canary, sealed, md.Flags))

	// Flags are authenticated: flipping one breaks verification.
	require.Error(t, verifyCanary(keys.canary, sealed, md.Flags^1))
	// Corrupt ciphertext and a wrong-size key fail cleanly.
	require.Error(t, verifyCanary(keys.canary, []byte("garbage"), md.Flags))
	require.Error(t, verifyCanary(make([]byte, crypto.KeySize), sealed, md.Flags))
	require.Error(t, verifyCanary([]byte("short"), sealed, md.Flags))

	_, err = sealCanary([]byte("short"), md.Flags)
	require.Error(t, err)

	// The state was persisted; reading it again is not a create.
	again, creating, err := readMetadata(bdb)
	require.NoError(t, err)
	require.False(t, creating)
	require.Equal(t, md.Flags, again.Flags)
}

func TestValidateAndWriteMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	valid := Metadata{
		Salt:    make([]byte, SaltSize),
		Canary:  []byte("canary"),
		Time:    FastKDFTime,
		Memory:  FastKDFMemory,
		Threads: FastKDFThreads,
		Flags:   EncryptAll,
	}

	invalid := []Metadata{
		{Canary: valid.Canary, Time: valid.Time, Memory: valid.Memory, Threads: valid.Threads},
		{Salt: valid.Salt, Time: valid.Time, Memory: valid.Memory, Threads: valid.Threads},
		{Salt: valid.Salt, Canary: valid.Canary},
		{Salt: valid.Salt, Canary: valid.Canary, Time: valid.Time, Memory: valid.Memory, Threads: valid.Threads, Flags: 1 << 15},
	}
	for i, md := range invalid {
		require.ErrorIs(t, validateMetadata(md), ErrCorruptStore, "case %d", i)
		require.ErrorIs(t, writeMetadata(bdb, md), ErrCorruptStore, "case %d", i)
	}

	require.NoError(t, writeMetadata(bdb, valid))
	read, creating, err := readMetadata(bdb)
	require.NoError(t, err)
	require.False(t, creating)
	require.Equal(t, valid.Flags, read.Flags)
}

func TestMetadataClosedStore(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "closed.db"), "pw")
	salt, _, _, _, flags, err := s.Metadata()
	require.NoError(t, err)
	require.Len(t, salt, SaltSize)
	require.Equal(t, EncryptAll, flags)

	require.NoError(t, s.Close())
	_, _, _, _, _, err = s.Metadata()
	require.ErrorIs(t, err, ErrClosed)
}

func TestKeysAndLenEncrypted(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "kv.db"), "pw")
	t.Cleanup(func() { _ = s.Close() })

	require.Equal(t, EncryptAll, s.EncryptFlags())

	for _, key := range []string{"b", "a", "c"} {
		require.NoError(t, s.Set(key, "value-"+key))
	}
	keys, err := s.Keys()
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, keys)

	n, err := s.Len()
	require.NoError(t, err)
	require.Equal(t, 3, n)

	require.NoError(t, s.Delete("b"))
	n, err = s.Len()
	require.NoError(t, err)
	require.Equal(t, 2, n)
	keys, err = s.Keys()
	require.NoError(t, err)
	require.Equal(t, []string{"a", "c"}, keys)

	// Deleting the value removes the key.
	require.NoError(t, s.Set("a", ""))
	has, err := s.Has("a")
	require.NoError(t, err)
	require.False(t, has)
}

func TestLoadOrCreateStateRejectsTampering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tamper.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)

	md, keys, err := loadOrCreateState(bdb, testOptions("pw"))
	require.NoError(t, err)
	keys.wipe()
	require.NoError(t, bdb.Close())

	tamper := func(mutate func(*Metadata)) error {
		db, err := bolt.Open(path, 0600, nil)
		require.NoError(t, err)
		defer db.Close()
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(metadataBucket)
			raw := b.Get(metadataRecordKey)
			parsed, err := decodeMetadata(raw)
			if err != nil {
				return err
			}
			mutate(&parsed)
			return b.Put(metadataRecordKey, parsed.encode())
		}))
		_, _, err = loadOrCreateState(db, testOptions("pw"))
		return err
	}

	require.ErrorIs(t, tamper(func(m *Metadata) { m.Flags ^= 1 }), ErrInvalidPassword)
	require.ErrorIs(t, tamper(func(m *Metadata) { m.Canary = []byte("garbage") }), ErrInvalidPassword)
	// A changed salt decodes as a different (zero-padded) salt, which the
	// canary reports as a wrong password.
	require.ErrorIs(t, tamper(func(m *Metadata) { m.Salt = []byte("short") }), ErrInvalidPassword)

	// Restoring the original metadata makes the store readable again.
	restore := func(m *Metadata) { *m = md }
	require.NoError(t, tamper(restore))
}

func TestOpenReopenIgnoresCreationOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")
	first := mustOpen(t, path, "pw")
	require.NoError(t, first.Set("k", "v"))
	require.NoError(t, first.Close())

	// A later Open uses the stored KDF/encryption settings even if the caller
	// passes different creation options.
	second, err := Open(context.Background(), path, 0600, &Options{
		Password:   "pw",
		KDFTime:    1,
		KDFMemory:  8 * MiB,
		KDFThreads: 1,
		Plaintext:  true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	value, err := second.Get("k")
	require.NoError(t, err)
	require.Equal(t, "v", value)
	require.Equal(t, EncryptAll, second.EncryptFlags())
}

func TestOpenNilReceiverHelpers(t *testing.T) {
	var s *Store
	require.ErrorIs(t, s.Set("k", "v"), ErrClosed)
	require.ErrorIs(t, s.Delete("k"), ErrClosed)
	_, err := s.Has("k")
	require.ErrorIs(t, err, ErrClosed)
	_, err = s.Keys()
	require.ErrorIs(t, err, ErrClosed)
	_, err = s.Len()
	require.ErrorIs(t, err, ErrClosed)
}

func TestReadMetadataMissingRecord(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "empty.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	_, creating, err := readMetadata(bdb)
	require.NoError(t, err)
	require.True(t, creating)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(metadataBucket)
		return err
	}))
	_, _, err = readMetadata(bdb)
	require.True(t, errors.Is(err, ErrCorruptStore))
}
