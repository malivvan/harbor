package record

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type softDoc struct {
	ID       string `db:"id"`
	Category string `db:"index"`
	Value    string
}

func openTombstoneStore(t *testing.T, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "tombstone.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	store, err := Open(context.Background(), bdb, cfg)
	require.NoError(t, err)
	return store, bdb
}

func TestSoftDeleteHidesFromEveryReadPath(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &softDoc{ID: "a", Category: "x", Value: "v"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.DeleteStructWith(ctx, doc, DeleteOptions{Soft: true, At: now}))

	err := store.One(ctx, "ID", "a", &softDoc{})
	require.ErrorIs(t, err, ErrNotFound)

	var all []softDoc
	require.NoError(t, store.All(ctx, &all))
	require.Empty(t, all)

	var byIndex []softDoc
	err = store.Find(ctx, "Category", "x", &byIndex)
	require.ErrorIs(t, err, ErrNotFound)

	count, err := store.Count(ctx, &softDoc{})
	require.NoError(t, err)
	require.Zero(t, count)

	var many []softDoc
	missing, err := store.GetMany(ctx, []any{"a"}, &many)
	require.NoError(t, err)
	require.Equal(t, []any{"a"}, missing)

	iter, err := store.Select().Iter(ctx, &softDoc{})
	require.NoError(t, err)
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())

	// Audit escape hatch.
	var seen softDoc
	require.NoError(t, store.One(WithDeleted(ctx), "ID", "a", &seen))
	require.Equal(t, "v", seen.Value)

	refs, err := store.Deleted(ctx, &softDoc{}, 0)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.True(t, refs[0].At.Equal(now))
}

func TestSoftDeleteRestore(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &softDoc{ID: "a", Category: "x", Value: "v"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.DeleteStructWith(ctx, doc, DeleteOptions{Soft: true, At: now}))
	require.ErrorIs(t, store.One(ctx, "ID", "a", &softDoc{}), ErrNotFound)

	require.NoError(t, store.Restore(ctx, doc))
	var got softDoc
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	require.Equal(t, softDoc{ID: "a", Category: "x", Value: "v"}, got)

	refs, err := store.Deleted(ctx, &softDoc{}, 0)
	require.NoError(t, err)
	require.Empty(t, refs)
}

func TestSoftDeleteSaveAndUpdateResurrect(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &softDoc{ID: "a", Category: "x", Value: "v"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.DeleteStructWith(ctx, doc, DeleteOptions{Soft: true, At: now}))
	require.ErrorIs(t, store.One(ctx, "ID", "a", &softDoc{}), ErrNotFound)

	// Update reads the tombstoned record and clears the tombstone on save.
	require.NoError(t, store.Update(ctx, &softDoc{ID: "a", Value: "updated"}))
	var got softDoc
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	require.Equal(t, "updated", got.Value)

	// Save has the same effect.
	require.NoError(t, store.DeleteStructWith(ctx, &got, DeleteOptions{Soft: true, At: now}))
	require.NoError(t, store.Save(ctx, &softDoc{ID: "a", Category: "y", Value: "saved"}))
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	require.Equal(t, "saved", got.Value)
}

func TestSoftDeleteReIndexDoesNotResurrect(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &softDoc{ID: "a", Category: "x", Value: "v"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.DeleteStructWith(ctx, doc, DeleteOptions{Soft: true, At: now}))
	require.NoError(t, store.ReIndex(ctx, &softDoc{}))
	require.ErrorIs(t, store.One(ctx, "ID", "a", &softDoc{}), ErrNotFound)
}

func TestPurgeDeleted(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	for _, id := range []string{"old", "new"} {
		doc := &softDoc{ID: id, Category: "x"}
		require.NoError(t, store.Save(ctx, doc))
	}
	oldDoc := &softDoc{ID: "old"}
	newDoc := &softDoc{ID: "new"}
	require.NoError(t, store.DeleteStructWith(ctx, oldDoc, DeleteOptions{Soft: true, At: now.Add(-2 * time.Hour)}))
	require.NoError(t, store.DeleteStructWith(ctx, newDoc, DeleteOptions{Soft: true, At: now}))

	refs, err := store.Deleted(ctx, &softDoc{}, 0)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.True(t, refs[0].At.Before(refs[1].At))

	n, err := store.PurgeDeleted(ctx, &softDoc{}, now.Add(-time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	refs, err = store.Deleted(ctx, &softDoc{}, 0)
	require.NoError(t, err)
	require.Len(t, refs, 1)

	var live softDoc
	require.NoError(t, store.One(WithDeleted(ctx), "ID", "new", &live))
	require.ErrorIs(t, store.One(WithDeleted(ctx), "ID", "old", &softDoc{}), ErrNotFound)
}

func TestSoftDeleteCapabilitySurvivesReopenWithoutOption(t *testing.T) {
	store, bdb := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &softDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.DeleteStructWith(ctx, doc, DeleteOptions{Soft: true, At: now}))

	reopened, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	require.ErrorIs(t, reopened.One(ctx, "ID", "a", &softDoc{}), ErrNotFound)
	var seen softDoc
	require.NoError(t, reopened.One(WithDeleted(ctx), "ID", "a", &seen))
}

func TestTombstoneDisabledByDefault(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{})
	ctx := context.Background()
	doc := &softDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.ErrorIs(t, store.DeleteStructWith(ctx, doc, DeleteOptions{Soft: true}), ErrTombstoneDisabled)
	_, err := store.Deleted(ctx, &softDoc{}, 0)
	require.ErrorIs(t, err, ErrTombstoneDisabled)
	_, err = store.PurgeDeleted(ctx, &softDoc{}, time.Time{})
	require.ErrorIs(t, err, ErrTombstoneDisabled)
}

func TestSoftDeleteHardDeleteStillRemovesBytes(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	doc := &softDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.DeleteStructWith(ctx, doc, DeleteOptions{}))
	require.ErrorIs(t, store.One(WithDeleted(ctx), "ID", "a", &softDoc{}), ErrNotFound)
	refs, err := store.Deleted(ctx, &softDoc{}, 0)
	require.NoError(t, err)
	require.Empty(t, refs)
}

func TestTombstoneGeneralityTodoList(t *testing.T) {
	type todo struct {
		ID   string `db:"id"`
		Text string
	}
	store, _ := openTombstoneStore(t, Config{Tombstone: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	item := &todo{ID: "1", Text: "buy milk"}
	require.NoError(t, store.Save(ctx, item))
	require.NoError(t, store.DeleteStructWith(ctx, item, DeleteOptions{Soft: true, At: now}))

	var all []todo
	require.NoError(t, store.All(ctx, &all))
	require.Empty(t, all)

	require.NoError(t, store.Restore(ctx, item))
	require.NoError(t, store.All(ctx, &all))
	require.Len(t, all, 1)
}
