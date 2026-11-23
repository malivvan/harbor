package db

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompareValueKinds(t *testing.T) {
	s := &sorter{node: &node{codec: JSONCodec}}
	less := func(a, b any) int { return s.compareValue(reflect.ValueOf(a), reflect.ValueOf(b)) }

	// Signed, unsigned and float.
	require.Equal(t, -1, less(int64(1), int64(2)))
	require.Equal(t, 0, less(int64(2), int64(2)))
	require.Equal(t, 1, less(int64(3), int64(2)))
	require.Equal(t, -1, less(uint64(1), uint64(2)))
	require.Equal(t, 1, less(uint64(3), uint64(2)))
	require.Equal(t, -1, less(1.5, 2.5))
	require.Equal(t, 1, less(3.5, 2.5))

	// Strings and booleans.
	require.Equal(t, -1, less("a", "b"))
	require.Equal(t, 1, less("c", "b"))
	require.Equal(t, 0, less("a", "a"))
	require.Equal(t, -1, less(false, true))
	require.Equal(t, 1, less(true, false))
	require.Equal(t, 0, less(true, true))

	// time.Time uses Equal/Before instead of the JSON fallback.
	now := time.Now()
	require.Equal(t, 0, less(now, now))
	require.Equal(t, -1, less(now.Add(-time.Second), now))
	require.Equal(t, 1, less(now.Add(time.Second), now))

	// Invalid values sort first (or last when compared against a valid one).
	var invalid reflect.Value
	require.Equal(t, 1, s.compareValue(reflect.ValueOf(1), invalid))
	require.Equal(t, -1, s.compareValue(invalid, reflect.ValueOf(1)))
	require.Equal(t, -1, s.compareValue(invalid, invalid))

	// The default branch encodes through the codec's key encoder.
	require.Equal(t, 0, less(map[string]int{"a": 1}, map[string]int{"a": 1}))
	require.NotZero(t, less(map[string]int{"a": 2}, map[string]int{"a": 1}))
	require.NoError(t, s.compareErr)
}

func TestSinkHelpers(t *testing.T) {
	ctx := context.Background()

	// listSink bucket name, reset, add and flush.
	var list []User
	ls, err := newListSink(nil, &list)
	require.NoError(t, err)
	require.Equal(t, "User", ls.bucketName())
	require.True(t, ls.needsValue())
	ls.reset()
	require.True(t, ls.slice().IsValid())
	require.Zero(t, ls.slice().Len())

	v := reflect.ValueOf(&User{ID: 1, Name: "n"})
	ls.add(ctx, item{value: &v})
	require.NoError(t, ls.flush(ctx))
	require.Len(t, list, 1)

	// firstSink.
	dest := User{}
	fs, err := newFirstSink(nil, &dest)
	require.NoError(t, err)
	require.Equal(t, "User", fs.bucketName())
	require.True(t, fs.needsValue())
	require.ErrorIs(t, fs.flush(ctx), ErrNotFound)
	require.NoError(t, fs.add(ctx, item{value: &v}))
	require.NoError(t, fs.flush(ctx))
	require.Equal(t, 1, dest.ID)

	// countSink.
	cs, err := newCountSink(nil, &User{})
	require.NoError(t, err)
	require.Equal(t, "User", cs.bucketName())
	require.False(t, cs.needsValue())
	require.NoError(t, cs.add(ctx, item{}))
	require.Equal(t, 1, cs.counter)
	require.NoError(t, cs.flush(ctx))

	// deleteSink.
	ds, err := newDeleteSink(nil, &User{})
	require.NoError(t, err)
	require.Equal(t, "User", ds.bucketName())
	require.False(t, ds.needsValue())
	// A bulk delete that matched nothing is a no-op, not an error: a cleanup
	// sweep whose predicate finds nothing is a normal outcome.
	require.NoError(t, ds.flush(ctx))
	require.NoError(t, ds.add(ctx, item{k: []byte("k")}))
	require.Len(t, ds.pending, 1)

	// rawSink and matchedRawSink.
	raw, sink, err := newRawSink(nil)
	require.NoError(t, err)
	require.Empty(t, raw.bucketName())
	require.False(t, raw.needsValue())
	require.NoError(t, raw.add(ctx, item{k: []byte("k"), v: []byte("v")}))
	require.Equal(t, [][]byte{[]byte("v")}, raw.results)
	require.NoError(t, raw.flush(ctx))
	require.False(t, sink.needsValue())

	var collected []byte
	raw.execFn = func(k, v []byte) error {
		collected = append(collected, v...)
		return nil
	}
	require.NoError(t, raw.add(ctx, item{k: []byte("k"), v: []byte("w")}))
	require.Equal(t, []byte("w"), collected)

	_, matched, err := newRawSink(&User{})
	require.NoError(t, err)
	require.Equal(t, "User", matched.bucketName())
	require.True(t, matched.needsValue())
	mrs := matched.(*matchedRawSink)
	require.True(t, mrs.elem().IsValid())

	// eachSink.
	es, err := newEachSink(&User{})
	require.NoError(t, err)
	require.Equal(t, "User", es.bucketName())
	require.True(t, es.needsValue())
	var seen any
	es.execFn = func(v any) error { seen = v; return nil }
	require.NoError(t, es.add(ctx, item{value: &v}))
	require.IsType(t, &User{}, seen)
	require.NoError(t, es.flush(ctx))
}

func TestIsZeroBranches(t *testing.T) {
	require.True(t, isZero(nil))
	var invalid reflect.Value
	require.True(t, isZero(&invalid))

	zero := reflect.ValueOf(0)
	require.True(t, isZero(&zero))
	nonZero := reflect.ValueOf(1)
	require.False(t, isZero(&nonZero))
}

func TestPrefixQueryVariants(t *testing.T) {
	ctx := context.Background()
	db, cleanup := createDB(t)
	defer cleanup()

	for _, name := range []string{"alpha", "alpine", "beta", "alps"} {
		u := User{Name: name, Group: "Staff"}
		require.NoError(t, db.Save(ctx, &u))
	}

	// Indexed prefix.
	var users []User
	require.NoError(t, db.Prefix(ctx, "Name", "alp", &users))
	require.Len(t, users, 3)

	users = nil
	require.NoError(t, db.Prefix(ctx, "Name", "alp", &users, Limit(1)))
	require.Len(t, users, 1)

	users = nil
	require.NoError(t, db.Prefix(ctx, "Name", "alp", &users, Reverse()))
	require.Len(t, users, 3)

	// No match on an indexed prefix.
	users = nil
	require.ErrorIs(t, db.Prefix(ctx, "Name", "zzz", &users), ErrNotFound)

	// Unindexed prefix falls back to a scan.
	users = nil
	require.NoError(t, db.Prefix(ctx, "Group", "Sta", &users))
	require.Len(t, users, 4)
}
