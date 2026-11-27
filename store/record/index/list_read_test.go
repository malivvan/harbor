package index_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/malivvan/tempest/store/record/index"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestNewListIndexReadSkipsIDsBucket verifies that read-only list indexes do
// not require or open the tempest__ids bucket: reads must work from the value
// bucket alone, and the IDs bucket is resolved only when a mutation needs it.
func TestNewListIndexReadSkipsIDsBucket(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tempest.db")

	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	defer db.Close()

	// Store a list-index entry without creating the tempest__ids bucket, as an
	// index written by an older version or a partially initialised index.
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		records, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		ib, err := records.CreateBucket([]byte("idx"))
		if err != nil {
			return err
		}
		return ib.Put(index.EncodeListKey([]byte("hello"), []byte("id1")), []byte("id1"))
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		records := tx.Bucket([]byte("records"))
		idx, err := index.NewListIndexRead(records, []byte("idx"))
		require.NoError(t, err)
		require.Nil(t, idx.IDs, "read-only construction must not open tempest__ids")

		ids, err := idx.All(ctx, []byte("hello"), nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("id1")}, ids)

		got, err := idx.Get(ctx, []byte("hello"))
		require.NoError(t, err)
		require.Equal(t, []byte("id1"), got)
		return nil
	}))

	// A read-only NewListIndex also defers the IDs bucket now.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		records := tx.Bucket([]byte("records"))
		idx, err := index.NewListIndex(records, []byte("idx"))
		require.NoError(t, err)
		require.Nil(t, idx.IDs)
		return nil
	}))

	// Mutating a read-only index lazily opens the bucket and fails on the
	// missing IDs bucket instead of panicking.
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		records := tx.Bucket([]byte("records"))
		idx, err := index.NewListIndexRead(records, []byte("idx"))
		require.NoError(t, err)
		require.ErrorIs(t, idx.RemoveID(ctx, []byte("id1")), index.ErrNotFound)
		return nil
	}))
}
