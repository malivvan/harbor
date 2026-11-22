package internal

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func withKeys(t *testing.T, keys []string, fn func(*bolt.Bucket)) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "cursor.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("b"))
		if err != nil {
			return err
		}
		for _, k := range keys {
			if err := b.Put([]byte(k), []byte("v:"+k)); err != nil {
				return err
			}
		}
		return nil
	}))

	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		fn(tx.Bucket([]byte("b")))
		return nil
	}))
}

func collect(t *testing.T, first func() ([]byte, []byte), next func() ([]byte, []byte), cont func([]byte) bool) []string {
	t.Helper()
	var out []string
	for k, _ := first(); cont(k); k, _ = next() {
		out = append(out, string(k))
	}
	return out
}

func TestCursorForwardAndReverse(t *testing.T) {
	withKeys(t, []string{"a", "b", "c"}, func(b *bolt.Bucket) {
		fwd := Cursor{C: b.Cursor()}
		require.Equal(t, []string{"a", "b", "c"}, collect(t, fwd.First, fwd.Next, func(k []byte) bool { return k != nil }))

		rev := Cursor{C: b.Cursor(), Reverse: true}
		require.Equal(t, []string{"c", "b", "a"}, collect(t, rev.First, rev.Next, func(k []byte) bool { return k != nil }))
	})
}

func TestRangeCursorForward(t *testing.T) {
	withKeys(t, []string{"a", "b", "c", "d"}, func(b *bolt.Bucket) {
		cmp := bytes.Compare
		c := RangeCursor{C: b.Cursor(), Min: []byte("b"), Max: []byte("c"), CompareFn: cmp}
		require.Equal(t, []string{"b", "c"}, collect(t, c.First, c.Next, c.Continue))

		// An empty Max means "no upper bound".
		c = RangeCursor{C: b.Cursor(), Min: []byte("c"), CompareFn: cmp}
		require.Equal(t, []string{"c", "d"}, collect(t, c.First, c.Next, c.Continue))

		// No keys above Min.
		c = RangeCursor{C: b.Cursor(), Min: []byte("z"), Max: []byte("zz"), CompareFn: cmp}
		require.Empty(t, collect(t, c.First, c.Next, c.Continue))
	})
}

func TestRangeCursorReverse(t *testing.T) {
	withKeys(t, []string{"a", "b", "c", "d"}, func(b *bolt.Bucket) {
		cmp := bytes.Compare

		// Empty Max: start at the last key.
		c := RangeCursor{C: b.Cursor(), Reverse: true, CompareFn: cmp}
		require.Equal(t, []string{"d", "c", "b", "a"}, collect(t, c.First, c.Next, c.Continue))

		// Exact upper bound is included.
		c = RangeCursor{C: b.Cursor(), Reverse: true, Max: []byte("c"), CompareFn: cmp}
		require.Equal(t, []string{"c", "b", "a"}, collect(t, c.First, c.Next, c.Continue))

		// Seek lands past the upper bound: step back one key.
		c = RangeCursor{C: b.Cursor(), Reverse: true, Max: []byte("bb"), CompareFn: cmp}
		require.Equal(t, []string{"b", "a"}, collect(t, c.First, c.Next, c.Continue))

		// Max sorts above every key: fall back to the last key.
		c = RangeCursor{C: b.Cursor(), Reverse: true, Max: []byte("z"), CompareFn: cmp}
		require.Equal(t, []string{"d", "c", "b", "a"}, collect(t, c.First, c.Next, c.Continue))

		// Both bounds.
		c = RangeCursor{C: b.Cursor(), Reverse: true, Min: []byte("b"), Max: []byte("d"), CompareFn: cmp}
		require.Equal(t, []string{"d", "c", "b"}, collect(t, c.First, c.Next, c.Continue))
	})
}

func TestPrefixCursor(t *testing.T) {
	withKeys(t, []string{"other", "pre1", "pre2", "pre3", "zzz"}, func(b *bolt.Bucket) {
		c := PrefixCursor{C: b.Cursor(), Prefix: []byte("pre")}
		require.Equal(t, []string{"pre1", "pre2", "pre3"}, collect(t, c.First, c.Next, c.Continue))

		rev := PrefixCursor{C: b.Cursor(), Reverse: true, Prefix: []byte("pre")}
		require.Equal(t, []string{"pre3", "pre2", "pre1"}, collect(t, rev.First, rev.Next, rev.Continue))

		// A prefix that ends inside a matching run stops at the right key.
		rev = PrefixCursor{C: b.Cursor(), Reverse: true, Prefix: []byte("pre1")}
		require.Equal(t, []string{"pre1"}, collect(t, rev.First, rev.Next, rev.Continue))

		none := PrefixCursor{C: b.Cursor(), Prefix: []byte("nope")}
		k, v := none.First()
		require.Nil(t, k)
		require.Nil(t, v)

		require.False(t, none.Continue(nil))
	})
}

func TestAppID(t *testing.T) {
	require.Equal(t, "com.github.malivvan.harbor", AppID)
}
