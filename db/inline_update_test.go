package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type inlineBase struct {
	A string `db:"index"`
	B string `db:"index"`
}

type inlineRec struct {
	ID   int        `db:"id"`
	Base inlineBase `db:"inline"`
}

type inlinePtrRec struct {
	ID   int         `db:"id"`
	Base *inlineBase `db:"inline"`
}

// Update must merge inline embedded fields individually instead of replacing
// the whole embedded struct and zeroing the fields the caller omitted.
func TestUpdateInlineStructMergesSubfields(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &inlineRec{ID: 1, Base: inlineBase{A: "aa", B: "bb"}}))
	require.NoError(t, db.Update(ctx, &inlineRec{ID: 1, Base: inlineBase{A: "aa2"}}))

	var got inlineRec
	require.NoError(t, db.One(ctx, "ID", 1, &got))
	require.Equal(t, "aa2", got.Base.A)
	require.Equal(t, "bb", got.Base.B, "unsupplied inline sibling was zeroed")

	// The index for the untouched sibling must still find the record.
	var byB []inlineRec
	require.NoError(t, db.Find(ctx, "B", "bb", &byB))
	require.Len(t, byB, 1)

	// The updated field must be indexed under its new value only.
	var byA []inlineRec
	require.NoError(t, db.Find(ctx, "A", "aa2", &byA))
	require.Len(t, byA, 1)
}

// The same merge applies when the inline field is a pointer.
func TestUpdateInlinePointerMergesSubfields(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &inlinePtrRec{ID: 1, Base: &inlineBase{A: "aa", B: "bb"}}))
	require.NoError(t, db.Update(ctx, &inlinePtrRec{ID: 1, Base: &inlineBase{A: "aa2"}}))

	var got inlinePtrRec
	require.NoError(t, db.One(ctx, "ID", 1, &got))
	require.NotNil(t, got.Base)
	require.Equal(t, "aa2", got.Base.A)
	require.Equal(t, "bb", got.Base.B, "unsupplied inline pointer sibling was zeroed")
}
