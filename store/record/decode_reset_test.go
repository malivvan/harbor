package record

import (
	"context"
	"testing"

	"github.com/malivvan/eidos-db/store/record/q"
	"github.com/stretchr/testify/require"
)

// decodeResetRec exercises fields that a codec omits when zero.
type decodeResetRec struct {
	ID  int    `db:"id"`
	Ord int    `db:"index"`
	Tag string `db:"index"`
	Opt string `json:"opt,omitempty"`
}

// ReIndex must not leak values from one record into the next record decoded
// into the same destination.
func TestReIndexResetsDecodeDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &decodeResetRec{ID: 1, Opt: "AAA"}))
	require.NoError(t, db.Save(ctx, &decodeResetRec{ID: 2}))
	require.NoError(t, db.ReIndex(ctx, &decodeResetRec{}))

	var r1, r2 decodeResetRec
	require.NoError(t, db.One(ctx, "ID", 1, &r1))
	require.NoError(t, db.One(ctx, "ID", 2, &r2))
	require.Equal(t, "AAA", r1.Opt)
	require.Empty(t, r2.Opt, "ReIndex leaked a value into the next record")
}

// OrderBy reuses a scratch destination; omitted fields must not survive
// between records.
func TestOrderByResetsDecodeDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &decodeResetRec{ID: 1, Ord: 1, Tag: "skip", Opt: "AAA"}))
	require.NoError(t, db.Save(ctx, &decodeResetRec{ID: 2, Ord: 2, Tag: "keep"}))

	var out []decodeResetRec
	require.NoError(t, db.Select(q.True()).OrderBy("Ord").Find(ctx, &out))
	require.Len(t, out, 2)
	require.Equal(t, 2, out[1].ID)
	require.Empty(t, out[1].Opt, "OrderBy leaked a value from the previous record")
}

// Filtered scans decode rejected records into a reused slot before a match is
// found; the match must not inherit their values.
func TestFilteredScanResetsDecodeDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &decodeResetRec{ID: 1, Ord: 1, Tag: "skip", Opt: "AAA"}))
	require.NoError(t, db.Save(ctx, &decodeResetRec{ID: 2, Ord: 2, Tag: "keep"}))

	var out []decodeResetRec
	require.NoError(t, db.Select(q.Eq("Tag", "keep")).Find(ctx, &out))
	require.Len(t, out, 1)
	require.Empty(t, out[0].Opt, "filtered scan leaked a value from a rejected record")
}
