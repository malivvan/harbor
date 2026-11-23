package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQueryBuildersDoNotMutateBase pins the copy-on-write builder contract:
// deriving a limited, skipped, reversed or ordered query must leave the base
// query usable unchanged.
func TestQueryBuildersDoNotMutateBase(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	base := store.Select()

	limited := base.Limit(3)
	skipped := base.Skip(18)
	reversed := base.Reverse()
	ordered := base.OrderBy("Value").Reverse()

	var all []Score
	require.NoError(t, base.Find(ctx, &all))
	require.Len(t, all, 20)
	require.Equal(t, 0, all[0].Value, "the base query must still walk forward")

	var few []Score
	require.NoError(t, limited.Find(ctx, &few))
	require.Len(t, few, 3)
	require.Equal(t, 0, few[0].Value)

	var tail []Score
	require.NoError(t, skipped.Find(ctx, &tail))
	require.Len(t, tail, 2)
	require.Equal(t, 18, tail[0].Value)

	var descending []Score
	require.NoError(t, reversed.Find(ctx, &descending))
	require.Len(t, descending, 20)
	require.Equal(t, 19, descending[0].Value)

	var orderedDesc []Score
	require.NoError(t, ordered.Find(ctx, &orderedDesc))
	require.Equal(t, 19, orderedDesc[0].Value)
	require.Equal(t, 0, orderedDesc[len(orderedDesc)-1].Value)

	// The base must be unaffected by every derived query.
	all = nil
	require.NoError(t, base.Find(ctx, &all))
	require.Len(t, all, 20)
	require.Equal(t, 0, all[0].Value)
	require.Equal(t, 19, all[len(all)-1].Value)
}

// TestQueryBucketDoesNotMutateBase checks that selecting another bucket on a
// derived query does not redirect the base.
func TestQueryBucketDoesNotMutateBase(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	base := store.Select()
	elsewhere := base.Bucket("NoSuchBucket")

	var all []Score
	require.NoError(t, base.Find(ctx, &all))
	require.Len(t, all, 20)

	var missing []Score
	require.ErrorIs(t, elsewhere.Find(ctx, &missing), ErrNotFound)

	// The base still resolves its bucket from the sample record.
	all = nil
	require.NoError(t, base.Find(ctx, &all))
	require.Len(t, all, 20)
}

// TestQueryFirstDoesNotLimitLaterUses is the regression test for First
// overwriting the query's limit in place: a query used for First must still be
// able to Find the complete result set afterwards.
func TestQueryFirstDoesNotLimitLaterUses(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	query := store.Select()

	var one Score
	require.NoError(t, query.First(ctx, &one))
	require.Equal(t, 0, one.Value)

	var all []Score
	require.NoError(t, query.Find(ctx, &all))
	require.Len(t, all, 20)
}

// TestQueryBuilderResultsAreIndependent checks that two derived queries do not
// observe each other's options.
func TestQueryBuilderResultsAreIndependent(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	base := store.Select()
	first := base.Limit(2)
	second := base.Skip(18)

	// Deriving another variant from first must not change second.
	firstPage := first.Skip(1)

	var few []Score
	require.NoError(t, firstPage.Find(ctx, &few))
	require.Len(t, few, 2)
	require.Equal(t, 1, few[0].Value)
	require.Equal(t, 2, few[1].Value)

	var tail []Score
	require.NoError(t, second.Find(ctx, &tail))
	require.Len(t, tail, 2)
	require.Equal(t, 18, tail[0].Value)
}
