package record

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type listKeyRec struct {
	ID  int    `db:"id"`
	Tag string `db:"index"`
}

// Values that prefix one another, and values containing the legacy "__"
// separator, must stay distinct for exact lookups.
func TestListIndexExactMatchWithSeparatorValues(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &listKeyRec{ID: 1, Tag: "a"}))
	require.NoError(t, db.Save(ctx, &listKeyRec{ID: 2, Tag: "a__b"}))

	var found []listKeyRec
	require.NoError(t, db.Find(ctx, "Tag", "a", &found))
	require.Len(t, found, 1)
	require.Equal(t, 1, found[0].ID)

	var one listKeyRec
	require.NoError(t, db.One(ctx, "Tag", "a", &one))
	require.Equal(t, 1, one.ID)

	require.NoError(t, db.Find(ctx, "Tag", "a__b", &found))
	require.Len(t, found, 1)
	require.Equal(t, 2, found[0].ID)
}

// Updating a record from one colliding value to another must move the index
// entry, not leave it under the old value.
func TestListIndexUpdateBetweenCollidingValues(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &listKeyRec{ID: 1, Tag: "a"}))
	require.NoError(t, db.Save(ctx, &listKeyRec{ID: 2, Tag: "a__b"}))

	require.NoError(t, db.Update(ctx, &listKeyRec{ID: 2, Tag: "a"}))

	var underA []listKeyRec
	require.NoError(t, db.Find(ctx, "Tag", "a", &underA))
	ids := make([]int, 0, len(underA))
	for _, r := range underA {
		ids = append(ids, r.ID)
	}
	require.ElementsMatch(t, []int{1, 2}, ids)

	var underOld []listKeyRec
	err := db.Find(ctx, "Tag", "a__b", &underOld)
	require.True(t, err == nil || errors.Is(err, ErrNotFound), "unexpected error: %v", err)
	require.Empty(t, underOld, "stale index entry left under the old value")
}

// List-index ordering and ranges must follow value order, not the raw
// concatenated key order ("_" sorts after printable bytes).
func TestListIndexOrderingAndRange(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	for i, v := range []string{"a", "a!", "a0", "b"} {
		require.NoError(t, db.Save(ctx, &listKeyRec{ID: i + 1, Tag: v}))
	}

	// AllRecords must be value ordered: "a" < "a!" < "a0" < "b".
	var all []listKeyRec
	require.NoError(t, db.AllByIndex(ctx, "Tag", &all))
	tags := make([]string, 0, len(all))
	for _, r := range all {
		tags = append(tags, r.Tag)
	}
	require.Equal(t, []string{"a", "a!", "a0", "b"}, tags)

	// An equal min and max must find the value.
	var ranged []listKeyRec
	require.NoError(t, db.Range(ctx, "Tag", "a", "a", &ranged))
	require.Len(t, ranged, 1)
	require.Equal(t, "a", ranged[0].Tag)

	var upToA0 []listKeyRec
	require.NoError(t, db.Range(ctx, "Tag", "a", "a0", &upToA0))
	require.Len(t, upToA0, 3)
}
