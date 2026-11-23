package series

import (
	"context"
	"testing"

	hstore "github.com/malivvan/harbor/store"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestEncStatePassthrough(t *testing.T) {
	plain := []byte("plain")

	var nilState *encState
	sealed, err := nilState.seal(plain)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err := nilState.open(plain)
	require.NoError(t, err)
	require.Equal(t, plain, opened)

	// A state without a keychain is also a passthrough.
	empty := &encState{}
	sealed, err = empty.seal(plain)
	require.NoError(t, err)
	require.Equal(t, plain, sealed)
	opened, err = empty.open(plain)
	require.NoError(t, err)
	require.Equal(t, plain, opened)
}

func TestStoreBytesAndEncStateHelpers(t *testing.T) {
	var nilStore *Store
	_, err := nilStore.sealBytes([]byte("x"))
	require.ErrorIs(t, err, ErrAuthFailed)
	_, err = nilStore.openBytes([]byte("x"))
	require.ErrorIs(t, err, ErrAuthFailed)
	require.Nil(t, nilStore.encState())

	store, _ := openTestStore(t, nil)
	require.Nil(t, store.encState())

	sealed, err := store.sealBytes([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte("x"), sealed)
	opened, err := store.openBytes(sealed)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), opened)
}

func TestDeleteBucketVariants(t *testing.T) {
	ctx := context.Background()

	// Rootless store.
	store, db := openTestStore(t, nil)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		require.NoError(t, store.deleteBucket(tx, []byte("missing")))
		if _, err := tx.CreateBucket([]byte("present")); err != nil {
			return err
		}
		require.NoError(t, store.deleteBucket(tx, []byte("present")))
		require.Nil(t, tx.Bucket([]byte("present")))
		return nil
	}))

	// Store with a root bucket.
	rootStore, rootDB := openTestStore(t, []byte(hstore.SERIES))
	require.NoError(t, rootDB.Update(func(tx *bolt.Tx) error {
		require.NoError(t, rootStore.deleteBucket(tx, []byte("missing")))
		root, err := tx.CreateBucketIfNotExists([]byte(hstore.SERIES))
		require.NoError(t, err)
		if _, err := root.CreateBucket([]byte("present")); err != nil {
			return err
		}
		require.NoError(t, rootStore.deleteBucket(tx, []byte("present")))
		require.Nil(t, root.Bucket([]byte("present")))
		return nil
	}))

	// Deleting a real series bucket works end to end.
	require.NoError(t, rootStore.Series("series").Insert(ctx, Point{Timestamp: 1, Value: 1}))
	require.NoError(t, rootDB.Update(func(tx *bolt.Tx) error {
		key, err := rootStore.bucketKey("series")
		if err != nil {
			return err
		}
		return rootStore.deleteBucket(tx, key)
	}))
}

func TestHasData(t *testing.T) {
	store, db := openTestStore(t, []byte(hstore.SERIES))
	has, err := store.hasData(db)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, store.Series("s").Insert(context.Background(), Point{Timestamp: 1, Value: 1}))
	has, err = store.hasData(db)
	require.NoError(t, err)
	require.True(t, has)

	// Rootless stores only inspect the metadata bucket.
	rootless, rootlessDB := openTestStore(t, nil)
	has, err = rootless.hasData(rootlessDB)
	require.NoError(t, err)
	require.False(t, has)
}
