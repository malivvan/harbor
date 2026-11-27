package record

import (
	"context"
	"errors"
	"testing"

	"github.com/malivvan/tempest/store/record/q"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// collectIter drains an iterator, decoding every record.
func collectIter(t *testing.T, it *Iterator) []int {
	t.Helper()
	var values []int
	for it.Next() {
		var s Score
		require.NoError(t, it.Decode(&s))
		values = append(values, s.Value)
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return values
}

func TestIterStreamsMatchingRecords(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	it, err := store.Select(q.Gte("Value", 15)).Iter(ctx, &Score{})
	require.NoError(t, err)

	var values []int
	for it.Next() {
		var s Score
		require.NoError(t, it.Decode(&s))
		require.NotEmpty(t, it.Key())
		require.NotEmpty(t, it.Value())
		values = append(values, s.Value)
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Equal(t, []int{15, 16, 17, 18, 19}, values)
}

func TestIterSkipLimitReverse(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	it, err := store.Select().Skip(2).Limit(3).Reverse().Iter(ctx, &Score{})
	require.NoError(t, err)
	require.Equal(t, []int{17, 16, 15}, collectIter(t, it))
}

func TestIterLimitZero(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	it, err := store.Select().Limit(0).Iter(ctx, &Score{})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()

	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

func TestIterRejectsOrdering(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := store.Select(q.Gte("Value", 1)).OrderBy("Value").Iter(ctx, &Score{})
	require.ErrorIs(t, err, ErrStreamOrdered)
}

func TestIterRequiresSampleForMatchers(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := store.Select(q.Eq("Value", 1)).Iter(ctx, nil)
	require.ErrorIs(t, err, ErrSampleRequired)
}

func TestIterBucketWithoutSample(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	it, err := store.Select().Bucket("Score").Iter(ctx, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	require.True(t, it.Next())
}

func TestIterCancellation(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	it, err := store.Select().Iter(ctx, &Score{})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()

	require.True(t, it.Next())
	cancel()
	require.False(t, it.Next())
	require.ErrorIs(t, it.Err(), context.Canceled)

	// A canceled context is also rejected at the entry point.
	_, err = store.Select().Iter(ctx, &Score{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestIterDecodeRejectsWrongType(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	it, err := store.Select().Iter(ctx, &Score{})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()

	// Decode before Next has no current record.
	require.ErrorIs(t, it.Decode(&Score{}), ErrNotFound)

	require.True(t, it.Next())

	var other struct{ Name string }
	require.ErrorIs(t, it.Decode(&other), ErrIncompatibleValue)
	require.ErrorIs(t, it.Decode(nil), ErrStructPtrNeeded)

	var s Score
	require.NoError(t, it.Decode(&s))
}

func TestIterCloseIsIdempotent(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	it, err := store.Select().Iter(ctx, &Score{})
	require.NoError(t, err)
	require.True(t, it.Next())
	require.NoError(t, it.Close())
	require.NoError(t, it.Close())
	require.False(t, it.Next())
	require.Nil(t, it.Key())
	require.Nil(t, it.Value())

	// A closed iterator must not hold the read transaction: another iteration
	// still works.
	it2, err := store.Select().Iter(ctx, &Score{})
	require.NoError(t, err)
	require.True(t, it2.Next())
	require.NoError(t, it2.Close())
}

func TestIterUsesAttachedTransactionWithoutClosingIt(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()
	ctx := context.Background()

	err := store.bolt.View(func(tx *bolt.Tx) error {
		attached, err := store.Attach(tx)
		require.NoError(t, err)

		it, err := attached.Select(q.Gte("Value", 18)).Iter(ctx, &Score{})
		require.NoError(t, err)
		defer func() { require.NoError(t, it.Close()) }()

		var values []int
		for it.Next() {
			var s Score
			require.NoError(t, it.Decode(&s))
			values = append(values, s.Value)
		}
		require.NoError(t, it.Err())
		require.Equal(t, []int{18, 19}, values)

		// The iterator must leave the caller's transaction open and usable.
		require.NoError(t, it.Close())
		count, err := attached.Count(ctx, &Score{})
		require.NoError(t, err)
		require.Equal(t, 20, count)
		return nil
	})
	require.NoError(t, err)
}

func TestIterCancellationDuringDecodeIsReported(t *testing.T) {
	store, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	it, err := store.Select().Iter(ctx, &Score{})
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()

	// Cancel after the iterator was created; the first Next must observe it
	// even though the cursor was never advanced.
	cancel()
	require.False(t, it.Next())
	require.True(t, errors.Is(it.Err(), context.Canceled))
}
