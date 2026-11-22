package db

import (
	"context"
	"testing"

	"github.com/malivvan/harbor/db/q"
	"github.com/stretchr/testify/require"
)

type orderedRecord struct {
	ID   int     `db:"id"`
	Age  int     `db:"index"`
	Dist float64 `db:"index"`
}

type stringIDRecord struct {
	ID  string `db:"id"`
	Tag string `db:"index"`
}

func TestIndexOrderingForNegativeIntegersAndFloats(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	for i, r := range []orderedRecord{
		{ID: 1, Age: -5, Dist: 2},
		{ID: 2, Age: 1, Dist: 10},
		{ID: 3, Age: 10, Dist: 3},
	} {
		r.ID = i + 1
		require.NoError(t, db.Save(ctx, &r))
	}

	var ages []orderedRecord
	require.NoError(t, db.Range(ctx, "Age", -10, 0, &ages))
	require.Len(t, ages, 1)
	require.Equal(t, -5, ages[0].Age)

	var ordered []orderedRecord
	require.NoError(t, db.AllByIndex(ctx, "Age", &ordered))
	require.Len(t, ordered, 3)
	require.Equal(t, []int{-5, 1, 10}, []int{ordered[0].Age, ordered[1].Age, ordered[2].Age})

	var dists []orderedRecord
	require.NoError(t, db.Range(ctx, "Dist", 1.0, 4.0, &dists))
	require.Len(t, dists, 2)
}

func TestListIndexKeysWithUnderscoresAreUnambiguous(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	for _, id := range []string{"plain1", "plain2", "a__b", "a__c"} {
		require.NoError(t, db.Save(ctx, &stringIDRecord{ID: id, Tag: "x"}))
	}

	var out []stringIDRecord
	require.NoError(t, db.Range(ctx, "Tag", "x", "x", &out))
	require.Len(t, out, 4)

	var one stringIDRecord
	require.NoError(t, db.One(ctx, "Tag", "x", &one))
	require.Equal(t, "x", one.Tag)
}

func TestUpdateFieldRejectsPrimaryKey(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	r := &orderedRecord{ID: 1, Age: 1}
	require.NoError(t, db.Save(ctx, r))
	require.ErrorIs(t, db.UpdateField(ctx, r, "ID", 2), ErrImmutableID)

	var got orderedRecord
	require.NoError(t, db.One(ctx, "ID", 1, &got))
	require.Equal(t, 1, got.ID)
}

func TestRawWithMatcherRequiresSample(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &stringIDRecord{ID: "1", Tag: "x"}))
	_, err := db.Select(q.Eq("Tag", "x")).Raw(ctx, nil)
	require.ErrorIs(t, err, ErrSampleRequired)
}
