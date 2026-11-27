package record

import (
	"context"
	"testing"

	"github.com/malivvan/tempest/store/record/q"
	"github.com/stretchr/testify/require"
)

// An ordered query with a matcher must return only matching records. This is
// the regression for a port of tempest's graph: selecting FromNodeID = "a" with
// OrderBy("ID") returned the non-matching record, while the same query without
// OrderBy returned the right one.
type orderedFilterRec struct {
	ID   string `db:"id"`
	From string `db:"index"`
	To   string `db:"index"`
}

func TestOrderedQueryKeepsTheMatcher(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Save(ctx, &orderedFilterRec{ID: "e1", From: "a", To: "b"}))
	require.NoError(t, db.Save(ctx, &orderedFilterRec{ID: "e2", From: "c", To: "a"}))

	var unordered []*orderedFilterRec
	require.NoError(t, db.Select(q.Eq("From", "a")).Find(ctx, &unordered))
	require.Len(t, unordered, 1)
	require.Equal(t, "e1", unordered[0].ID)

	var ordered []*orderedFilterRec
	require.NoError(t, db.Select(q.Eq("From", "a")).OrderBy("ID").Find(ctx, &ordered))
	require.Len(t, ordered, 1)
	require.Equal(t, "e1", ordered[0].ID)

	var both []*orderedFilterRec
	require.NoError(t, db.Select(q.Or(q.Eq("From", "a"), q.Eq("To", "a"))).OrderBy("ID").Find(ctx, &both))
	require.Len(t, both, 2)
	require.Equal(t, "e1", both[0].ID)
	require.Equal(t, "e2", both[1].ID)
}
