package search

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func openSearchExpiryStore(t *testing.T) (*Store, *Index) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "search-expiry.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	store, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	idx, err := store.Open("docs", Config{})
	require.NoError(t, err)
	return store, idx
}

func TestSearchExpiryHidesAndSweeps(t *testing.T) {
	store, idx := openSearchExpiryStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, idx.UpsertWithExpiry(ctx, "a", "apple", now.Add(-time.Minute)))
	require.NoError(t, idx.Upsert(ctx, "b", "apple banana"))

	hits, err := idx.Search(ctx, "apple", 10)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "b", hits[0].ID)

	at, ok, err := idx.Expiry(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, at.Equal(now.Add(-time.Minute)))

	docs, err := idx.Docs(ctx)
	require.NoError(t, err)
	require.Len(t, docs, 2)
	for _, doc := range docs {
		if doc.ID == "a" {
			require.NotZero(t, doc.ExpiresAt)
		}
	}

	removed, err := idx.SweepExpired(ctx, now)
	require.NoError(t, err)
	require.Equal(t, 1, removed)

	count, err := idx.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	hits, err = idx.Search(ctx, "apple", 10)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "b", hits[0].ID)

	_, _, err = idx.Expiry(ctx, "a")
	require.ErrorIs(t, err, ErrDocNotFound)
}

func TestSearchExpiryZeroClearsAndUpsertClears(t *testing.T) {
	store, idx := openSearchExpiryStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, idx.UpsertWithExpiry(ctx, "a", "apple", now.Add(-time.Minute)))
	require.Empty(t, mustHits(t, idx, ctx))

	require.NoError(t, idx.SetExpiry(ctx, "a", time.Time{}))
	_, ok, err := idx.Expiry(ctx, "a")
	require.NoError(t, err)
	require.False(t, ok)
	require.Len(t, mustHits(t, idx, ctx), 1)

	require.NoError(t, idx.SetExpiry(ctx, "a", now.Add(time.Hour)))
	require.NoError(t, idx.Upsert(ctx, "a", "apple again"))
	_, ok, err = idx.Expiry(ctx, "a")
	require.NoError(t, err)
	require.False(t, ok)

	require.ErrorIs(t, idx.SetExpiry(ctx, "missing", now), ErrDocNotFound)
}

func TestSearchExpiryUpgradesFormatAndReopens(t *testing.T) {
	store, idx := openSearchExpiryStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, idx.UpsertWithExpiry(ctx, "a", "apple", now.Add(time.Hour)))

	var version uint64
	require.NoError(t, store.view(func(tx *bolt.Tx) error {
		b, err := store.indexBucket(tx, "docs")
		if err != nil {
			return err
		}
		raw := b.Get([]byte(versionKey))
		for i := len(raw) - 1; i >= 0; i-- {
			version = version<<8 | uint64(raw[i])
		}
		return nil
	}))
	require.Equal(t, FormatVersionExpiry, version)

	reopened, err := Open(ctx, store.db, Config{})
	require.NoError(t, err)
	reopened.SetNow(func() time.Time { return now.Add(2 * time.Hour) })
	idx2, err := reopened.Open("docs", Config{})
	require.NoError(t, err)
	hits, err := idx2.Search(ctx, "apple", 10)
	require.NoError(t, err)
	require.Empty(t, hits)
}

func TestSearchExpiryRestoreIndexRoundTrip(t *testing.T) {
	store, idx := openSearchExpiryStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, idx.UpsertWithExpiry(ctx, "a", "apple", now.Add(-time.Minute)))
	docs, err := idx.Docs(ctx)
	require.NoError(t, err)
	terms, err := idx.Terms(ctx)
	require.NoError(t, err)
	meta, err := idx.Meta(ctx)
	require.NoError(t, err)

	require.NoError(t, store.RestoreIndex(ctx, "copy", Config{}, meta, docs, terms))
	copyIdx, err := store.Open("copy", Config{})
	require.NoError(t, err)
	at, ok, err := copyIdx.Expiry(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, at.Equal(now.Add(-time.Minute)))
	hits, err := copyIdx.Search(ctx, "apple", 10)
	require.NoError(t, err)
	require.Empty(t, hits)
}

func TestSearchExpiryGeneralityCache(t *testing.T) {
	store, idx := openSearchExpiryStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })

	require.NoError(t, idx.UpsertWithExpiry(ctx, "cache-key", "value", now.Add(time.Minute)))
	require.Len(t, mustHitsFor(t, idx, ctx, "value"), 1)

	store.SetNow(func() time.Time { return now.Add(2 * time.Minute) })
	require.Empty(t, mustHitsFor(t, idx, ctx, "value"))
}

func mustHits(t *testing.T, idx *Index, ctx context.Context) []Hit {
	t.Helper()
	hits, err := idx.Search(ctx, "apple", 10)
	require.NoError(t, err)
	return hits
}

func mustHitsFor(t *testing.T, idx *Index, ctx context.Context, query string) []Hit {
	t.Helper()
	hits, err := idx.Search(ctx, query, 10)
	require.NoError(t, err)
	return hits
}
