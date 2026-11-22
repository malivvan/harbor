package harbor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type corruptSubRecord struct {
	ID   int `db:"id"`
	Name string
}

func TestOpenFailsWhenSubstoreIsCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt-sub.db")
	s := mustOpen(t, path, "pw")
	require.NoError(t, s.Set("k", "v"))
	require.NoError(t, s.DB().Save(context.Background(), &corruptSubRecord{ID: 1, Name: "x"}))
	require.NoError(t, s.Close())

	raw, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, raw.Update(func(tx *bolt.Tx) error {
		sub := tx.Bucket([]byte("db"))
		require.NotNil(t, sub)
		info := sub.Bucket([]byte("__database"))
		require.NotNil(t, info)
		marker := info.Get([]byte("\x00__db_enc__"))
		require.NotNil(t, marker)
		corrupt := append([]byte(nil), marker...)
		corrupt[len(corrupt)-1] ^= 0xff
		return info.Put([]byte("\x00__db_enc__"), corrupt)
	}))
	require.NoError(t, raw.Close())

	_, err = Open(context.Background(), path, 0600, testOptions("pw"))
	require.Error(t, err)
}

func TestMetadataOnEmptyDatabase(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "empty.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	s := &Store{bs: bdb}
	_, _, _, _, _, err = s.Metadata()
	require.ErrorIs(t, err, ErrCorruptStore)
}

func TestOpenPathIsDirectory(t *testing.T) {
	_, err := Open(context.Background(), t.TempDir(), 0600, testOptions("pw"))
	require.Error(t, err)
}

func TestWriteMetadataClosedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, bdb.Close())

	md := Metadata{
		Salt:    make([]byte, SaltSize),
		Canary:  []byte("canary"),
		Time:    FastKDFTime,
		Memory:  FastKDFMemory,
		Threads: FastKDFThreads,
		Flags:   EncryptAll,
	}
	require.Error(t, writeMetadata(bdb, md))
}

func TestLoadOrCreateStateReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, bdb.Close())

	ro, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer ro.Close()

	// A new store needs to persist metadata, which a read-only handle cannot do.
	_, _, err = loadOrCreateState(ro, testOptions("pw"))
	require.Error(t, err)
}

func TestOpenRejectsCorruptCanaryAndFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canary.db")
	s := mustOpen(t, path, "pw")
	require.NoError(t, s.Close())

	// Flip a bit in the persisted canary.
	raw, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, raw.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		value := b.Get(metadataRecordKey)
		md, err := decodeMetadata(append([]byte(nil), value...))
		if err != nil {
			return err
		}
		md.Canary[0] ^= 0xff
		return b.Put(metadataRecordKey, md.encode())
	}))
	require.NoError(t, raw.Close())

	_, err = Open(context.Background(), path, 0600, testOptions("pw"))
	require.ErrorIs(t, err, ErrInvalidPassword)
}
