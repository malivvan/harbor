package index

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func withIndexBuckets(t *testing.T, fn func(b *bolt.Bucket)) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "index.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		for _, k := range []string{"a", "b", "c"} {
			if err := b.Put([]byte(k), []byte("value-"+k)); err != nil {
				return err
			}
		}
		fn(b)
		return nil
	}))
}

func TestIDIndexOperations(t *testing.T) {
	ctx := context.Background()
	withIndexBuckets(t, func(b *bolt.Bucket) {
		_, err := NewIDIndex(nil, []byte("idx"))
		require.ErrorIs(t, err, ErrNilParam)

		idx, err := NewIDIndex(b, []byte("idx"))
		require.NoError(t, err)

		require.ErrorIs(t, idx.Add(ctx, nil, []byte("a")), ErrNilParam)
		require.ErrorIs(t, idx.Add(ctx, []byte("a"), nil), ErrNilParam)
		require.NoError(t, idx.Add(ctx, []byte("a"), []byte("a")))
		require.NoError(t, idx.Remove(ctx, []byte("a")))
		require.NoError(t, idx.RemoveID(ctx, []byte("a")))

		got, err := idx.Get(ctx, []byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("value-a"), got)

		got, err = idx.Get(ctx, []byte("missing"))
		require.NoError(t, err)
		require.Nil(t, got)

		all, err := idx.All(ctx, []byte("a"), nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("a")}, all)

		all, err = idx.All(ctx, []byte("missing"), nil)
		require.NoError(t, err)
		require.Nil(t, all)

		all, err = idx.All(ctx, []byte("a"), &Options{Limit: 0})
		require.NoError(t, err)
		require.Nil(t, all)

		all, err = idx.All(ctx, []byte("a"), &Options{Skip: 1})
		require.NoError(t, err)
		require.Nil(t, all)

		records, err := idx.AllRecords(ctx, nil)
		require.NoError(t, err)
		require.Empty(t, records)

		rng, err := idx.Range(ctx, []byte("a"), []byte("b"), nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, rng)

		rng, err = idx.Range(ctx, []byte("a"), []byte("b"), &Options{Reverse: true, Limit: 1})
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("b")}, rng)

		pre, err := idx.Prefix(ctx, []byte("a"), nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("a")}, pre)

		pre, err = idx.Prefix(ctx, []byte("a"), &Options{Limit: 0})
		require.NoError(t, err)
		require.Nil(t, pre)

		// Cancellation is observed by every method.
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, idx.Add(canceled, []byte("a"), []byte("a")), context.Canceled)
		require.ErrorIs(t, idx.Remove(canceled, []byte("a")), context.Canceled)
		require.ErrorIs(t, idx.RemoveID(canceled, []byte("a")), context.Canceled)
		_, err = idx.Get(canceled, []byte("a"))
		require.ErrorIs(t, err, context.Canceled)
		_, err = idx.All(canceled, []byte("a"), nil)
		require.ErrorIs(t, err, context.Canceled)
		_, err = idx.AllRecords(canceled, nil)
		require.ErrorIs(t, err, context.Canceled)
		_, err = idx.Range(canceled, nil, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
		_, err = idx.Prefix(canceled, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestListAndUniqueHasValueID(t *testing.T) {
	ctx := context.Background()
	withIndexBuckets(t, func(b *bolt.Bucket) {
		list, err := NewListIndex(b, []byte("__list"))
		require.NoError(t, err)
		require.NoError(t, list.Add(ctx, []byte("v"), []byte("a")))

		ok, err := list.HasValueID(ctx, []byte("v"), []byte("a"))
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = list.HasValueID(ctx, []byte("v"), []byte("b"))
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = list.HasValueID(ctx, nil, []byte("a"))
		require.NoError(t, err)
		require.False(t, ok)

		got, err := list.Get(ctx, []byte("v"))
		require.NoError(t, err)
		require.Equal(t, []byte("a"), got)

		got, err = list.Get(ctx, nil)
		require.NoError(t, err)
		require.Nil(t, got)

		unique, err := NewUniqueIndex(b, []byte("__unique"))
		require.NoError(t, err)
		require.NoError(t, unique.Add(ctx, []byte("v"), []byte("a")))
		ok, err = unique.HasValueID(ctx, []byte("v"), []byte("a"))
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = unique.HasValueID(ctx, []byte("v"), []byte("b"))
		require.NoError(t, err)
		require.False(t, ok)

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = list.HasValueID(canceled, []byte("v"), []byte("a"))
		require.ErrorIs(t, err, context.Canceled)
		_, err = unique.HasValueID(canceled, []byte("v"), []byte("a"))
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestListIndexReadAndResultCap(t *testing.T) {
	withIndexBuckets(t, func(b *bolt.Bucket) {
		_, err := NewListIndexRead(nil, []byte("x"))
		require.ErrorIs(t, err, ErrNilParam)
		_, err = NewListIndexRead(b, nil)
		require.ErrorIs(t, err, ErrNilParam)
		_, err = NewListIndexRead(b, []byte("missing"))
		require.ErrorIs(t, err, ErrNotFound)

		list, err := NewListIndex(b, []byte("__list2"))
		require.NoError(t, err)
		read, err := NewListIndexRead(b, []byte("__list2"))
		require.NoError(t, err)
		require.NotNil(t, list)
		require.Nil(t, read.IDs)

		require.Nil(t, resultSlice(0))
		require.Nil(t, resultSlice(-1))
		require.NotNil(t, resultSlice(2))

	})
}

func TestResultCapCommittedBucket(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "cap.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		for _, k := range []string{"a", "b", "c"} {
			if err := b.Put([]byte(k), nil); err != nil {
				return err
			}
		}
		return nil
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("b"))
		require.Zero(t, resultCap(nil, nil))
		require.Equal(t, 3, resultCap(b, nil))
		require.Equal(t, 1, resultCap(b, &Options{Limit: 1}))
		require.Equal(t, 3, resultCap(b, &Options{Limit: 10}))
		return nil
	}))
}

func TestIndexConstructorErrors(t *testing.T) {
	ctx := context.Background()
	withIndexBuckets(t, func(b *bolt.Bucket) {
		_, err := NewListIndex(nil, []byte("x"))
		require.ErrorIs(t, err, ErrNilParam)
		_, err = NewListIndex(b, nil)
		require.ErrorIs(t, err, ErrNilParam)

		list, err := NewListIndex(b, []byte("__new-list"))
		require.NoError(t, err)
		require.NotNil(t, list.IDs)
		require.ErrorIs(t, list.Add(ctx, nil, []byte("a")), ErrNilParam)
		require.ErrorIs(t, list.Add(ctx, []byte("a"), nil), ErrNilParam)
	})
}

func TestIndexNilContextAndLazyIDErrors(t *testing.T) {
	//lint:ignore SA1012 deliberately nil to exercise the guard
	require.ErrorIs(t, checkContext(nil), ErrNilContext)

	ctx := context.Background()
	empty := &ListIndex{}
	require.ErrorIs(t, empty.Add(ctx, []byte("v"), []byte("id")), ErrNilParam)
	require.ErrorIs(t, empty.Remove(ctx, []byte("v")), ErrNilParam)
	require.ErrorIs(t, empty.RemoveID(ctx, []byte("id")), ErrNilParam)
	_, err := empty.HasValueID(ctx, []byte("v"), []byte("id"))
	require.ErrorIs(t, err, ErrNilParam)
}

func TestListIndexReaddAndRemove(t *testing.T) {
	ctx := context.Background()
	withIndexBuckets(t, func(b *bolt.Bucket) {
		list, err := NewListIndex(b, []byte("__move"))
		require.NoError(t, err)

		require.NoError(t, list.Add(ctx, []byte("old"), []byte("id")))
		// Re-adding the same ID replaces its value.
		require.NoError(t, list.Add(ctx, []byte("new"), []byte("id")))
		ids, err := list.All(ctx, []byte("new"), nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("id")}, ids)
		ids, err = list.All(ctx, []byte("old"), nil)
		require.NoError(t, err)
		require.Empty(t, ids)

		// Two IDs under one value, then remove the value.
		require.NoError(t, list.Add(ctx, []byte("shared"), []byte("id1")))
		require.NoError(t, list.Add(ctx, []byte("shared"), []byte("id2")))
		require.NoError(t, list.Remove(ctx, []byte("shared")))
		ids, err = list.All(ctx, []byte("shared"), nil)
		require.NoError(t, err)
		require.Empty(t, ids)

		// Empty values are no-ops for Remove/Get, and the sentinel bucket is
		// skipped by prefix lookups.
		require.NoError(t, list.Remove(ctx, nil))
		got, err := list.Get(ctx, nil)
		require.NoError(t, err)
		require.Nil(t, got)
		got, err = list.Get(ctx, []byte("eidos"))
		require.NoError(t, err)
		require.Nil(t, got)
		require.NoError(t, list.Remove(ctx, []byte("eidos")))

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, list.Remove(canceled, []byte("x")), context.Canceled)
		require.ErrorIs(t, list.RemoveID(canceled, []byte("id")), context.Canceled)
		_, err = list.HasValueID(canceled, []byte("x"), []byte("id"))
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestUniqueIndexAllVariants(t *testing.T) {
	ctx := context.Background()
	withIndexBuckets(t, func(b *bolt.Bucket) {
		unique, err := NewUniqueIndex(b, []byte("__unique-all"))
		require.NoError(t, err)
		require.NoError(t, unique.Add(ctx, []byte("v"), []byte("id")))

		ids, err := unique.All(ctx, []byte("v"), nil)
		require.NoError(t, err)
		require.Equal(t, [][]byte{[]byte("id")}, ids)

		ids, err = unique.All(ctx, []byte("missing"), nil)
		require.NoError(t, err)
		require.Nil(t, ids)

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = unique.All(canceled, []byte("v"), nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}
