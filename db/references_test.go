package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/db/q"
	bolt "go.etcd.io/bbolt"
)

type refDoc struct {
	ID    string `db:"id"`
	Title string `db:"index"`
}

type refChunk struct {
	ID    string `db:"id"`
	DocID string `db:"index,ref=refDoc"`
	Body  string
}

type refChunkCascade struct {
	ID    string `db:"id"`
	DocID string `db:"index,ref=refDoc,ondelete=cascade"`
	Body  string
}

type refGrand struct {
	ID      string `db:"id"`
	ChunkID string `db:"index,ref=refChunkCascade,ondelete=cascade"`
}

func newRefStore(t *testing.T) *Store {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "refs.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	st, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })
	return st
}

func TestReferenceRequiresTarget(t *testing.T) {
	ctx := context.Background()
	st := newRefStore(t)

	// Before the target bucket exists the failure is a missing target.
	err := st.Save(ctx, &refChunk{ID: "c1", DocID: "missing", Body: "x"})
	require.ErrorIs(t, err, ErrReferenceTargetMissing)

	var got refChunk
	require.ErrorIs(t, st.One(ctx, "ID", "c1", &got), ErrNotFound, "a rejected save must roll back")

	// Once the bucket exists, a missing record is a dangling reference.
	require.NoError(t, st.Save(ctx, &refDoc{ID: "d1", Title: "doc"}))
	err = st.Save(ctx, &refChunk{ID: "c1", DocID: "missing", Body: "x"})
	require.ErrorIs(t, err, ErrDanglingReference)
	require.ErrorIs(t, st.One(ctx, "ID", "c1", &got), ErrNotFound, "a dangling save must not create the record")

	require.NoError(t, st.Save(ctx, &refChunk{ID: "c1", DocID: "d1", Body: "x"}))

	// The reference field is indexed automatically.
	var chunks []refChunk
	require.NoError(t, st.Find(ctx, "DocID", "d1", &chunks))
	require.Len(t, chunks, 1)
}

func TestReferenceZeroValueSkipsValidation(t *testing.T) {
	ctx := context.Background()
	st := newRefStore(t)
	require.NoError(t, st.Save(ctx, &refChunk{ID: "c1", Body: "no parent yet"}))
}

func TestReferenceRestrictBlocksDelete(t *testing.T) {
	ctx := context.Background()
	st := newRefStore(t)
	require.NoError(t, st.Save(ctx, &refDoc{ID: "d1", Title: "doc"}))
	require.NoError(t, st.Save(ctx, &refChunk{ID: "c1", DocID: "d1"}))

	err := st.DeleteStruct(ctx, &refDoc{ID: "d1"})
	require.ErrorIs(t, err, ErrReferenceViolation)

	// The target is still there.
	var doc refDoc
	require.NoError(t, st.One(ctx, "ID", "d1", &doc))

	// Removing the referencing record unblocks it.
	require.NoError(t, st.DeleteStruct(ctx, &refChunk{ID: "c1"}))
	require.NoError(t, st.DeleteStruct(ctx, &refDoc{ID: "d1"}))
}

func TestReferenceCascadeDelete(t *testing.T) {
	ctx := context.Background()
	st := newRefStore(t)
	require.NoError(t, st.Save(ctx, &refDoc{ID: "d1", Title: "doc"}))
	require.NoError(t, st.Save(ctx, &refChunkCascade{ID: "c1", DocID: "d1"}))
	require.NoError(t, st.Save(ctx, &refChunkCascade{ID: "c2", DocID: "d1"}))
	require.NoError(t, st.Save(ctx, &refChunk{ID: "other", DocID: "d1"}))

	// But the restrict chunk still blocks the delete.
	require.ErrorIs(t, st.DeleteStruct(ctx, &refDoc{ID: "d1"}), ErrReferenceViolation)
	require.NoError(t, st.DeleteStruct(ctx, &refChunk{ID: "other"}))

	require.NoError(t, st.DeleteStruct(ctx, &refDoc{ID: "d1"}))

	var chunks []refChunkCascade
	require.NoError(t, st.All(ctx, &chunks))
	require.Empty(t, chunks)

	var doc refDoc
	require.ErrorIs(t, st.One(ctx, "ID", "d1", &doc), ErrNotFound)
}

func TestReferenceCascadeIsRecursive(t *testing.T) {
	ctx := context.Background()
	st := newRefStore(t)
	require.NoError(t, st.Save(ctx, &refDoc{ID: "d1", Title: "doc"}))
	require.NoError(t, st.Save(ctx, &refChunkCascade{ID: "c1", DocID: "d1"}))
	require.NoError(t, st.Save(ctx, &refGrand{ID: "g1", ChunkID: "c1"}))

	require.NoError(t, st.DeleteStruct(ctx, &refDoc{ID: "d1"}))

	var grands []refGrand
	require.NoError(t, st.All(ctx, &grands))
	require.Empty(t, grands)
	var chunks []refChunkCascade
	require.NoError(t, st.All(ctx, &chunks))
	require.Empty(t, chunks)
}

func TestReferenceBulkDeleteEnforces(t *testing.T) {
	ctx := context.Background()
	st := newRefStore(t)
	require.NoError(t, st.Save(ctx, &refDoc{ID: "d1", Title: "doc"}))
	require.NoError(t, st.Save(ctx, &refChunk{ID: "c1", DocID: "d1"}))

	err := st.Select(q.Eq("ID", "d1")).Delete(ctx, &refDoc{})
	require.ErrorIs(t, err, ErrReferenceViolation)

	var doc refDoc
	require.NoError(t, st.One(ctx, "ID", "d1", &doc), "the blocked delete must roll back")
}

func TestReferenceDropEnforces(t *testing.T) {
	ctx := context.Background()

	t.Run("restrict", func(t *testing.T) {
		st := newRefStore(t)
		require.NoError(t, st.Save(ctx, &refDoc{ID: "d1", Title: "doc"}))
		require.NoError(t, st.Save(ctx, &refChunk{ID: "c1", DocID: "d1"}))

		require.ErrorIs(t, st.Drop(ctx, &refDoc{}), ErrReferenceViolation)
		var doc refDoc
		require.NoError(t, st.One(ctx, "ID", "d1", &doc))
	})

	t.Run("cascade", func(t *testing.T) {
		st := newRefStore(t)
		require.NoError(t, st.Save(ctx, &refDoc{ID: "d1", Title: "doc"}))
		require.NoError(t, st.Save(ctx, &refChunkCascade{ID: "c1", DocID: "d1"}))
		require.NoError(t, st.Save(ctx, &refGrand{ID: "g1", ChunkID: "c1"}))

		require.NoError(t, st.Drop(ctx, &refDoc{}))
		var chunks []refChunkCascade
		require.NoError(t, st.All(ctx, &chunks))
		require.Empty(t, chunks)
		var grands []refGrand
		require.NoError(t, st.All(ctx, &grands))
		require.Empty(t, grands)
	})
}

func TestValidateSchema(t *testing.T) {
	ctx := context.Background()
	st := newRefStore(t)

	err := st.ValidateSchema(ctx, &refChunk{})
	require.ErrorIs(t, err, ErrReferenceTargetMissing)

	require.NoError(t, st.Save(ctx, &refDoc{ID: "d1"}))
	require.NoError(t, st.ValidateSchema(ctx, &refChunk{}))

	// Each referenced bucket must exist; create the intermediate buckets.
	require.NoError(t, st.Save(ctx, &refChunkCascade{ID: "c1", DocID: "d1"}))
	require.NoError(t, st.ValidateSchema(ctx, &refChunk{}, &refChunkCascade{}, &refGrand{}))

	// A bucket that does not exist has no reference fields, so its schema
	// validates trivially.
	require.NoError(t, st.ValidateSchema(ctx, &refDoc{}))
}

func TestReferenceTagRejectsBadOnDelete(t *testing.T) {
	type badRef struct {
		ID    string `db:"id"`
		DocID string `db:"index,ref=refDoc,ondelete=explode"`
	}
	ctx := context.Background()
	st := newRefStore(t)
	err := st.Save(ctx, &badRef{ID: "x", DocID: "d1"})
	require.True(t, errors.Is(err, ErrUnknownTag), "err = %v", err)
}
