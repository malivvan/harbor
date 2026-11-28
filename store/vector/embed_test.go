package vector

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func openEmbedStore(t *testing.T, allowMixed bool) (*Store, *Collection) {
	t.Helper()
	bdb, err := bbolt.Open(filepath.Join(t.TempDir(), "embed.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	store, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	col, err := store.NewCollectionWithOptions(CollectionOptions{
		Name:             "docs",
		VectorLen:        2,
		Metric:           Cosine,
		AllowMixedModels: allowMixed,
	})
	require.NoError(t, err)
	return store, col
}

func TestSetModelPersistsAndValidates(t *testing.T) {
	store, col := openEmbedStore(t, false)
	require.NoError(t, col.SetModel(EmbeddingModel{ID: "m1", Dimensions: 2, Metric: Cosine}))

	meta, err := store.LoadCollectionMeta("docs")
	require.NoError(t, err)
	require.Equal(t, "m1", meta.Model.ID)
	require.Equal(t, 2, meta.Model.Dimensions)

	require.ErrorIs(t, col.SetModel(EmbeddingModel{ID: "m2", Dimensions: 3}), ErrInvalidConfig)
	require.ErrorIs(t, col.SetModel(EmbeddingModel{ID: "m2", Metric: Euclid}), ErrInvalidConfig)
	require.ErrorIs(t, col.SetModel(EmbeddingModel{}), ErrInvalidConfig)
}

func TestStalePointsAndReembed(t *testing.T) {
	store, col := openEmbedStore(t, false)
	require.NoError(t, col.SetModel(EmbeddingModel{ID: "m1", Dimensions: 2, Metric: Cosine}))
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}},
		{ID: "c", Vector: []float32{1, 1}},
	}))
	require.NoError(t, col.Flush())

	stale, err := col.StalePoints("m1", 0)
	require.NoError(t, err)
	require.Empty(t, stale)
	stale, err = col.StalePoints("m2", 0)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, stale)

	var progress []int
	report, err := col.Reembed(context.Background(), func(id string, old []float32) ([]float32, error) {
		return old, nil
	}, ReembedOptions{
		Model:     "m2",
		Revision:  "2026-01",
		BatchSize: 1,
		Progress: func(done, total int) {
			progress = append(progress, done)
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, report.Done)
	require.Equal(t, 3, report.Total)
	require.Equal(t, []int{1, 2, 3}, progress)

	stale, err = col.StalePoints("m2", 0)
	require.NoError(t, err)
	require.Empty(t, stale)
	meta, err := store.LoadCollectionMeta("docs")
	require.NoError(t, err)
	require.Equal(t, "m2", meta.Model.ID)
	require.Equal(t, "2026-01", meta.Model.Revision)

	point, err := col.Get("a")
	require.NoError(t, err)
	require.Equal(t, "m2", point.Payload[ModelPayloadKey])
}

func TestReembedResumesAfterFailure(t *testing.T) {
	_, col := openEmbedStore(t, false)
	require.NoError(t, col.SetModel(EmbeddingModel{ID: "m1", Dimensions: 2, Metric: Cosine}))
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}},
		{ID: "c", Vector: []float32{1, 1}},
		{ID: "d", Vector: []float32{2, 1}},
		{ID: "e", Vector: []float32{1, 2}},
	}))
	require.NoError(t, col.Flush())

	calls := 0
	_, err := col.Reembed(context.Background(), func(id string, old []float32) ([]float32, error) {
		calls++
		if calls == 3 {
			return nil, errors.New("boom")
		}
		return old, nil
	}, ReembedOptions{Model: "m2", BatchSize: 1})
	require.Error(t, err)

	stale, err := col.StalePoints("m2", 0)
	require.NoError(t, err)
	require.Len(t, stale, 3)

	report, err := col.Reembed(context.Background(), func(id string, old []float32) ([]float32, error) {
		return old, nil
	}, ReembedOptions{Model: "m2", BatchSize: 2})
	require.NoError(t, err)
	require.Equal(t, 3, report.Done)
	require.Equal(t, 3, report.Total)
	stale, err = col.StalePoints("m2", 0)
	require.NoError(t, err)
	require.Empty(t, stale)
}

func TestReembedRejectsWrongDimension(t *testing.T) {
	_, col := openEmbedStore(t, false)
	require.NoError(t, col.SetModel(EmbeddingModel{ID: "m1", Dimensions: 2, Metric: Cosine}))
	require.NoError(t, col.Upsert([]PointStruct{{ID: "a", Vector: []float32{1, 0}}}))
	require.NoError(t, col.Flush())

	_, err := col.Reembed(context.Background(), func(id string, old []float32) ([]float32, error) {
		return []float32{1}, nil
	}, ReembedOptions{Model: "m2"})
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestMixedModelsRejectedUnlessAllowed(t *testing.T) {
	store, col := openEmbedStore(t, false)
	require.NoError(t, col.SetModel(EmbeddingModel{ID: "m1", Dimensions: 2, Metric: Cosine}))
	require.NoError(t, col.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}, Payload: Payload{ModelPayloadKey: "m2"}},
	}))
	require.NoError(t, col.Flush())

	reopened, err := Open(context.Background(), store.db, Config{})
	require.NoError(t, err)
	mixed, err := reopened.Collection("docs")
	require.NoError(t, err)
	_, err = mixed.Search([]float32{1, 0}, nil, 10)
	require.ErrorIs(t, err, ErrMixedModels)

	allowedStore, allowedCol := openEmbedStore(t, true)
	require.NoError(t, allowedCol.SetModel(EmbeddingModel{ID: "m1", Dimensions: 2, Metric: Cosine}))
	require.NoError(t, allowedCol.Upsert([]PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}, Payload: Payload{ModelPayloadKey: "m2"}},
	}))
	require.NoError(t, allowedCol.Flush())
	reopenedAllowed, err := Open(context.Background(), allowedStore.db, Config{})
	require.NoError(t, err)
	allowed2, err := reopenedAllowed.Collection("docs")
	require.NoError(t, err)
	_, err = allowed2.Search([]float32{1, 0}, nil, 10)
	require.NoError(t, err)
}
