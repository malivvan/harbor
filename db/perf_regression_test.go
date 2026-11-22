package db

import (
	"context"
	"sync/atomic"
	"testing"

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
