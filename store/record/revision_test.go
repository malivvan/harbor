package record

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type revisionDoc struct {
	ID   string `db:"id"`
	Name string
	N    int
}

func openRevisionStore(t *testing.T) (*Store, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "revision.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	store, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	return store, bdb
}

func TestRevisionLifecycle(t *testing.T) {
	store, _ := openRevisionStore(t)
	ctx := context.Background()

	doc := &revisionDoc{ID: "a", Name: "one", N: 5}
	require.NoError(t, store.Save(ctx, doc))

	rev, err := store.Revision(ctx, doc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), rev)

	doc.Name = "two"
	require.NoError(t, store.Update(ctx, doc))
	rev, err = store.Revision(ctx, doc)
	require.NoError(t, err)
	require.Equal(t, uint64(2), rev)

	// A conditional update merges like Update: N is not named, so it survives.
	next, err := store.UpdateIf(ctx, &revisionDoc{ID: "a", Name: "three"}, Precondition{Revision: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(3), next)

	var got revisionDoc
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	require.Equal(t, "three", got.Name)
	require.Equal(t, 5, got.N)

	// A stale precondition is a conflict and leaves the value untouched.
	_, err = store.UpdateIf(ctx, &revisionDoc{ID: "a", Name: "stale"}, Precondition{Revision: 2})
	require.ErrorIs(t, err, ErrConflict)
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	require.Equal(t, "three", got.Name)

	// Zero means "must not exist".
	_, err = store.UpdateIf(ctx, &revisionDoc{ID: "a", Name: "exists"}, Precondition{})
	require.ErrorIs(t, err, ErrConflict)

	next, err = store.UpdateIf(ctx, &revisionDoc{ID: "b", Name: "new"}, Precondition{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), next)
}

func TestDeleteIfRevision(t *testing.T) {
	store, _ := openRevisionStore(t)
	ctx := context.Background()

	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))

	err := store.DeleteIf(ctx, doc, Precondition{Revision: 99})
	require.ErrorIs(t, err, ErrConflict)
	require.NoError(t, store.One(ctx, "ID", "a", doc))

	require.NoError(t, store.DeleteIf(ctx, doc, Precondition{Revision: 1}))
	_, err = store.Revision(ctx, doc)
	require.ErrorIs(t, err, ErrNotFound)

	// Deleting a missing record is not a conflict; it is not there.
	require.ErrorIs(t, store.DeleteIf(ctx, doc, Precondition{}), ErrNotFound)
}

func TestRevisionZeroPreconditionDeletes(t *testing.T) {
	store, _ := openRevisionStore(t)
	ctx := context.Background()
	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.DeleteIf(ctx, doc, Precondition{}))
	require.ErrorIs(t, store.One(ctx, "ID", "a", &revisionDoc{}), ErrNotFound)
}

func TestRevisionPersistsAcrossReopen(t *testing.T) {
	store, bdb := openRevisionStore(t)
	ctx := context.Background()
	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))
	doc.Name = "two"
	require.NoError(t, store.Update(ctx, doc))

	reopened, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	rev, err := reopened.Revision(ctx, doc)
	require.NoError(t, err)
	require.Equal(t, uint64(2), rev)
}

func TestRevisionCASConcurrent(t *testing.T) {
	store, _ := openRevisionStore(t)
	ctx := context.Background()
	require.NoError(t, store.Save(ctx, &revisionDoc{ID: "a", Name: "start"}))

	const writers = 8
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		conflicts int
	)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.UpdateIf(ctx, &revisionDoc{ID: "a", Name: "writer"}, Precondition{Revision: 1})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrConflict):
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	require.Equal(t, 1, succeeded)
	require.Equal(t, writers-1, conflicts)

	var got revisionDoc
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	require.Equal(t, "writer", got.Name)
	rev, err := store.Revision(ctx, &got)
	require.NoError(t, err)
	require.Equal(t, uint64(2), rev)
}

func TestRevisionEncryptedMetadata(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "rev-enc.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	secret := bytes.Repeat([]byte{7}, 32)
	ctx := context.Background()
	store, err := Open(ctx, bdb, Config{Secret: secret, EncryptValues: true, EncryptMetadata: true})
	require.NoError(t, err)

	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))
	rev, err := store.Revision(ctx, doc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), rev)

	next, err := store.UpdateIf(ctx, &revisionDoc{ID: "a", Name: "two"}, Precondition{Revision: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(2), next)

	reopened, err := Open(ctx, bdb, Config{Secret: secret, EncryptValues: true, EncryptMetadata: true})
	require.NoError(t, err)
	rev, err = reopened.Revision(ctx, doc)
	require.NoError(t, err)
	require.Equal(t, uint64(2), rev)
}

func TestReIndexDoesNotBumpRevision(t *testing.T) {
	store, _ := openRevisionStore(t)
	ctx := context.Background()
	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.ReIndex(ctx, &revisionDoc{}))
	rev, err := store.Revision(ctx, doc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), rev)
}
