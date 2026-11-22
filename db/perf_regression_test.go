package db

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/malivvan/storm/db/q"
	"github.com/stretchr/testify/require"
)

// countingCodec delegates to JSONCodec and counts decode calls. It is used to
// prove that paths which do not need record values skip unmarshalling.
type countingCodec struct {
	unmarshals atomic.Int64
}

func (c *countingCodec) Marshal(v interface{}) ([]byte, error) {
	return JSONCodec.Marshal(v)
}

func (c *countingCodec) Unmarshal(b []byte, v interface{}) error {
	c.unmarshals.Add(1)
	return JSONCodec.Unmarshal(b, v)
}

func (c *countingCodec) Name() string { return "counting-json" }

func TestCountAndDeleteDoNotDecodeRecords(t *testing.T) {
	ctx := context.Background()
	cc := &countingCodec{}
	db, cleanup := createDB(t, testCodec(cc))
	defer cleanup()

	for i := 0; i < 10; i++ {
		u := User{Name: "John"}
		require.NoError(t, db.Save(ctx, &u))
	}

	// Seeding already decoded nothing but marshalled; reset the counter.
	cc.unmarshals.Store(0)

	n, err := db.Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Zero(t, cc.unmarshals.Load(), "Count must not decode records")

	err = db.Select().Delete(ctx, &User{})
	require.NoError(t, err)
	require.Zero(t, cc.unmarshals.Load(), "Delete without matchers must not decode records")

	n, err = db.Count(ctx, &User{})
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestCountWithMatcherStillDecodes(t *testing.T) {
	ctx := context.Background()
	cc := &countingCodec{}
	db, cleanup := createDB(t, testCodec(cc))
	defer cleanup()

	for i := 0; i < 5; i++ {
		u := User{Name: "John", Group: "Staff"}
		require.NoError(t, db.Save(ctx, &u))
	}
	cc.unmarshals.Store(0)

	n, err := db.Select().Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Zero(t, cc.unmarshals.Load(), "an empty matcher list needs no decode")

	n, err = db.Select().OrderBy("Name").Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.NotZero(t, cc.unmarshals.Load(), "ordering forces decoding")
}

func TestUnindexedScanPresizedResults(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 0; i < 25; i++ {
		u := User{Name: "John", Group: "Staff"}
		require.NoError(t, db.Save(ctx, &u))
	}

	// All: the sink is pre-sized from the bucket key count, then truncated.
	var all []User
	require.NoError(t, db.All(ctx, &all))
	require.Len(t, all, 25)

	// Limit smaller than the bucket: pre-size must be bounded by the limit.
	var limited []User
	require.NoError(t, db.Select().Limit(3).Find(ctx, &limited))
	require.Len(t, limited, 3)

	// Skip: pre-sized slots before the first published record are reused.
	var skipped []User
	require.NoError(t, db.Select().Skip(23).Find(ctx, &skipped))
	require.Len(t, skipped, 2)

	// Pointer slices decode into the pre-sized slots as well.
	var ptrs []*User
	require.NoError(t, db.All(ctx, &ptrs))
	require.Len(t, ptrs, 25)
	for _, p := range ptrs {
		require.NotNil(t, p)
	}

	// No match: an over-sized slice must be truncated to empty, not published.
	var none []User
	err := db.Select(q.Eq("Group", "NoSuchGroup")).Find(ctx, &none)
	require.ErrorIs(t, err, ErrNotFound)
	require.Empty(t, none)

	// Reverse reads the same number of records.
	var reversed []User
	require.NoError(t, db.All(ctx, &reversed, Reverse()))
	require.Len(t, reversed, 25)
}
