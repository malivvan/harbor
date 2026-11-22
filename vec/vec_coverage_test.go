package vec

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestValueJSONAndDeepCopy(t *testing.T) {
	var nilValue *Value
	require.Nil(t, nilValue.GetJSONValue())

	require.Nil(t, (&Value{Kind: KindString, BytesValue: []byte("x")}).GetJSONValue())
	v := &Value{Kind: KindJSON, BytesValue: []byte(`{"a":1}`)}
	require.Equal(t, []byte(`{"a":1}`), v.GetJSONValue())

	original := map[string]any{
		"n":   int64(1),
		"s":   "text",
		"m":   map[string]any{"inner": []any{1, 2}},
		"arr": []int{1, 2},
		"b":   []byte("bytes"),
	}
	copied := deepCopyValue(original).(map[string]any)
	require.Equal(t, original, copied)

	copied["s"] = "changed"
	copied["m"].(map[string]any)["inner"].([]any)[0] = 99
	copied["arr"].([]int)[0] = 99
	copied["b"].([]byte)[0] = 'X'
	require.Equal(t, "text", original["s"])
	require.Equal(t, 1, original["m"].(map[string]any)["inner"].([]any)[0])
	require.Equal(t, 1, original["arr"].([]int)[0])
	require.Equal(t, byte('b'), original["b"].([]byte)[0])

	require.Equal(t, 5, deepCopyValue(5))
	require.Nil(t, deepCopyValue(nil))
	require.Equal(t, []string{"a"}, deepCopyValue([]string{"a"}))
	require.Equal(t, []int64{1}, deepCopyValue([]int64{1}))
	require.Equal(t, []float32{1}, deepCopyValue([]float32{1}))
	require.Equal(t, []float64{1}, deepCopyValue([]float64{1}))
}

func TestMatchContainsTypes(t *testing.T) {
	require.True(t, matchContains([]any{int64(1), "two"}, "two"))
	require.False(t, matchContains([]any{int64(1)}, "missing"))
	require.True(t, matchContains([]string{"a", "b"}, "b"))
	require.False(t, matchContains([]string{"a"}, 1))
	require.True(t, matchContains([]int{1, 2}, 2))
	require.True(t, matchContains([]int64{1, 2}, int64(2)))
	require.True(t, matchContains([]float64{1.5, 2.5}, 2.5))
	require.True(t, matchContains("hello", "ell"))
	require.False(t, matchContains("hello", ""))
	require.False(t, matchContains(42, "x"))
}

func TestCloneRoot(t *testing.T) {
	require.Nil(t, cloneRoot(nil))

	root := [][]byte{[]byte("a"), []byte("b")}
	cloned := cloneRoot(root)
	require.Equal(t, root, cloned)
	cloned[0][0] = 'X'
	require.Equal(t, byte('a'), root[0][0])
}

func TestStoreFlushAndOwnership(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "vec.db"), 0600, nil)
	require.NoError(t, err)

	store, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)

	col, err := store.NewCollection("c", 4, Euclid, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]PointStruct{{ID: "p", Vector: []float32{1, 2, 3, 4}}}))
	require.NoError(t, store.Flush())

	loaded, err := store.LoadCollection("c")
	require.NoError(t, err)
	require.Len(t, loaded, 1)

	// A store that does not own the handle leaves it usable after Close.
	store.SetOwnsDB(false)
	store.trackCollection(nil)
	require.NoError(t, store.Close())
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error { return nil }))
	require.Error(t, store.Flush())
	require.NoError(t, store.Close())

	var nilStore *Store
	nilStore.SetOwnsDB(true) // no-op

	require.NoError(t, bdb.Close())
}

func TestRotationBucketHelpers(t *testing.T) {
	ctx := context.Background()

	for _, root := range [][]byte{nil, []byte("root")} {
		bdb, err := bolt.Open(filepath.Join(t.TempDir(), "rot.db"), 0600, nil)
		require.NoError(t, err)
		store, err := openStore(ctx, bdb, root, Config{})
		require.NoError(t, err)
		require.NoError(t, store.EnsureCollection("c"))

		require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
			r := &rotation{ctx: ctx, store: store, old: store.enc.Load()}
			require.True(t, r.bucketExists(tx, []byte("c")))
			require.False(t, r.bucketExists(tx, []byte("missing")))

			names, err := r.collectionNames(tx)
			require.NoError(t, err)
			require.Contains(t, names, "c")

			b, err := r.createBucket(tx, []byte("d"))
			require.NoError(t, err)
			require.NotNil(t, b)
			require.NoError(t, r.deleteBucket(tx, []byte("d")))
			require.NoError(t, r.deleteBucket(tx, []byte("missing")))
			return nil
		}))

		require.NoError(t, bdb.Close())
	}
}

func TestRotateErrors(t *testing.T) {
	ctx := context.Background()
	key := []byte("01234567890123456789012345678901")

	var nilStore *Store
	require.Error(t, nilStore.RotateAll(ctx, key))
	require.Error(t, nilStore.VerifyAll(ctx))

	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "plain.db"), 0600, nil)
	require.NoError(t, err)
	store, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	require.ErrorIs(t, store.RotateAll(ctx, key), ErrInvalidConfig)
	require.ErrorIs(t, store.VerifyAll(ctx), ErrInvalidConfig)
	require.NoError(t, bdb.Close())
}

func TestHNSWScanFiltersAndOrdering(t *testing.T) {
	points := flatTestPoints(60, 8)
	query := make([]float32, 8)
	for i := range query {
		query[i] = float32(i%3) / 3
	}

	for _, metric := range []Distance{Euclid, Dot} {
		idx := NewHNSWIndex(metric)
		require.NoError(t, idx.Upsert(points))

		all := idx.scan(query, nil)
		require.Len(t, all, len(points))
		for i := 1; i < len(all); i++ {
			if metric == Euclid {
				require.GreaterOrEqual(t, all[i].Score, all[i-1].Score)
			} else {
				require.LessOrEqual(t, all[i].Score, all[i-1].Score)
			}
		}

		filter := &Filter{Must: []Condition{{
			Key:   "bucket",
			Type:  MatchTypeExact,
			Match: MatchValue{Value: 2},
		}}}
		filtered := idx.scan(query, filter)
		require.NotEmpty(t, filtered)
		for _, point := range filtered {
			require.EqualValues(t, 2, point.Payload["bucket"])
		}
	}
}

func TestStoreMetaAndDropBranches(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "meta.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	store, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)

	metas, err := store.ListCollectionMetas()
	require.NoError(t, err)
	require.Empty(t, metas)

	_, err = store.LoadCollectionMeta("missing")
	require.ErrorIs(t, err, ErrMetadataNotFound)

	require.Error(t, store.EnsureCollection(""))
	require.NoError(t, store.DropCollection("missing"))

	meta := CollectionMeta{
		Name:              "c",
		VectorLen:         4,
		Metric:            Cosine,
		FormatVersion:     collectionFormatVersion,
		SchemaFingerprint: StoredPointSchemaFingerprint,
	}
	require.NoError(t, store.SaveCollectionMeta("c", meta))
	got, err := store.LoadCollectionMeta("c")
	require.NoError(t, err)
	require.Equal(t, "c", got.Name)

	metas, err = store.ListCollectionMetas()
	require.NoError(t, err)
	require.Len(t, metas, 1)

	require.NoError(t, store.DropCollection("c"))
}
