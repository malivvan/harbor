package tempest

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/vector"
)

func openHybridStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "hybrid.db"), 0o600, &Options{
		Password:   "hybrid-password",
		Plaintext:  true,
		KDFTime:    FastKDFTime,
		KDFMemory:  FastKDFMemory,
		KDFThreads: FastKDFThreads,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	col, err := store.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{
		{ID: "a", Vector: []float32{1, 0}, Payload: vector.Payload{"topic": "x"}},
		{ID: "b", Vector: []float32{0, 1}, Payload: vector.Payload{"topic": "y"}},
	}))

	idx, err := store.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "a", "apple banana"))
	require.NoError(t, idx.Upsert(ctx, "b", "banana cherry"))
	return store, ctx
}

func TestHybridTextArmOnly(t *testing.T) {
	store, ctx := openHybridStore(t)
	hits, cursor, err := store.HybridSearch(ctx, HybridRequest{
		Text:      "apple",
		TextIndex: "docs",
		TopK:      10,
	})
	require.NoError(t, err)
	require.Nil(t, cursor)
	require.Len(t, hits, 1)
	require.Equal(t, "a", hits[0].ID)
	require.Contains(t, hits[0].Why, "text")
}

func TestHybridVectorArmOnly(t *testing.T) {
	store, ctx := openHybridStore(t)
	hits, cursor, err := store.HybridSearch(ctx, HybridRequest{
		Vector:    []float32{1, 0},
		VectorCol: "docs",
		TopK:      10,
	})
	require.NoError(t, err)
	require.Nil(t, cursor)
	require.NotEmpty(t, hits)
	require.Equal(t, "a", hits[0].ID)
	require.Contains(t, hits[0].Why, "dense")
}

func TestHybridFusesBothArmsDeterministically(t *testing.T) {
	store, ctx := openHybridStore(t)
	req := HybridRequest{
		Text:      "banana",
		TextIndex: "docs",
		Vector:    []float32{1, 0},
		VectorCol: "docs",
		TopK:      10,
	}
	first, _, err := store.HybridSearch(ctx, req)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, "a", first[0].ID)
	require.Contains(t, first[0].Why, "text")
	require.Contains(t, first[0].Why, "dense")

	second, _, err := store.HybridSearch(ctx, req)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestHybridFilterAppliesToBothArms(t *testing.T) {
	store, ctx := openHybridStore(t)
	filter := &vector.Filter{Must: []vector.Condition{{
		Key:   "topic",
		Type:  vector.MatchTypeExact,
		Match: vector.MatchValue{Value: "y"},
	}}}
	hits, _, err := store.HybridSearch(ctx, HybridRequest{
		Text:      "banana",
		TextIndex: "docs",
		Vector:    []float32{1, 0},
		VectorCol: "docs",
		Filter:    filter,
		TopK:      10,
	})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "b", hits[0].ID)
}

func TestHybridCursorResumes(t *testing.T) {
	store, ctx := openHybridStore(t)
	req := HybridRequest{
		Text:      "banana",
		TextIndex: "docs",
		Vector:    []float32{1, 0},
		VectorCol: "docs",
		TopK:      1,
	}
	first, cursor, err := store.HybridSearch(ctx, req)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.NotNil(t, cursor)
	require.Equal(t, "a", first[0].ID)

	req.Cursor = cursor
	second, next, err := store.HybridSearch(ctx, req)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, "b", second[0].ID)
	require.Nil(t, next)
	require.Equal(t, 2, second[0].Rank)
}

func TestHybridRerank(t *testing.T) {
	store, ctx := openHybridStore(t)
	hits, _, err := store.HybridSearch(ctx, HybridRequest{
		Text:      "banana",
		TextIndex: "docs",
		Vector:    []float32{1, 0},
		VectorCol: "docs",
		TopK:      10,
		Rerank:    &HybridRerank{TextWeight: 0.5, VectorWeight: 0.5},
	})
	require.NoError(t, err)
	require.NotEmpty(t, hits)
	require.Contains(t, hits[0].Why, "rerank")
}

func TestHybridErrors(t *testing.T) {
	store, ctx := openHybridStore(t)
	_, _, err := store.HybridSearch(ctx, HybridRequest{})
	require.ErrorIs(t, err, ErrHybridQuery)

	_, _, err = store.HybridSearch(ctx, HybridRequest{Text: "x"})
	require.ErrorIs(t, err, ErrHybridConfig)

	_, _, err = store.HybridSearch(ctx, HybridRequest{Vector: []float32{1, 0}})
	require.ErrorIs(t, err, ErrHybridConfig)

	_, _, err = store.HybridSearch(ctx, HybridRequest{
		Text:      "x",
		TextIndex: "docs",
		Filter:    &vector.Filter{},
	})
	require.ErrorIs(t, err, ErrHybridFilter)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.HybridSearch(ctx, HybridRequest{Text: "x", TextIndex: "docs"})
	require.ErrorIs(t, err, ErrClosed)
}

func TestHybridRetrievalSink(t *testing.T) {
	store, ctx := openHybridStore(t)
	var events []RetrievalEvent
	store.SetRetrievalSink(func(event RetrievalEvent) {
		events = append(events, event)
	})

	_, _, err := store.HybridSearch(ctx, HybridRequest{
		Text:      "banana",
		TextIndex: "docs",
		Vector:    []float32{1, 0},
		VectorCol: "docs",
		TopK:      10,
	})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "banana", events[0].Query)
	require.NotEmpty(t, events[0].Hits)
	require.NotZero(t, events[0].Latency)
	require.Contains(t, events[0].Why, "text")

	store.SetRetrievalSink(nil)
	_, _, err = store.HybridSearch(ctx, HybridRequest{Text: "apple", TextIndex: "docs"})
	require.NoError(t, err)
	require.Len(t, events, 1)
}
