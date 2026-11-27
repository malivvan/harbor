package cluster

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

// TestNodeFacades exercises the replicated write views end-to-end against a
// single-node cluster, so every facade method (including the previously
// uncovered Remove/Rename/Update/Drop/Delete variants) is driven through a real
// raft apply.
func TestNodeFacades(t *testing.T) {
	ctx := context.Background()
	node, err := Open(ctx, testConfig("node-1", freeAddr(t), true, t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Close() })

	leader := waitLeader(t, node)
	require.Equal(t, raft.Leader, leader.State())

	t.Run("node introspection", func(t *testing.T) {
		require.NotNil(t, node.Store())
		addr, id := node.Leader()
		require.NotEmpty(t, addr)
		require.Equal(t, node.cfg.NodeID, id)
		require.Equal(t, raft.Leader, node.State())
		require.NotEmpty(t, node.Stats()["state"])
		require.NoError(t, node.Barrier(5*time.Second))
		require.NoError(t, node.Snapshot())
		require.Greater(t, node.AppliedIndex(), uint64(0))
	})

	t.Run("join self is a no-op", func(t *testing.T) {
		addr, id := node.Leader()
		require.NoError(t, node.Join(id, addr))
	})

	t.Run("leave non-member keeps leader", func(t *testing.T) {
		_ = node.Leave("node-ghost")
		require.Equal(t, raft.Leader, node.State())
	})

	t.Run("record", func(t *testing.T) {
		Register(&testUser{})
		Register(nil)        // nil is ignored, not a panic
		Register(testUser{}) // value form also registers
		sub := node.Record().From("tenant", "acme")

		require.NoError(t, sub.Save(ctx, &testUser{ID: "u1", Name: "alice"}))
		require.NoError(t, sub.Update(ctx, &testUser{ID: "u1", Name: "alicia"}))
		require.NoError(t, sub.UpdateField(ctx, &testUser{ID: "u1"}, "Name", "alex"))
		require.NoError(t, sub.Set(ctx, "settings", "theme", "dark"))
		require.NoError(t, sub.Set(ctx, "settings", 7, "seven"))
		require.NoError(t, sub.SetBytes(ctx, "blobs", "raw", []byte{1, 2, 3}))
		require.NoError(t, sub.Delete(ctx, "settings", "theme"))

		// A non-pointer record write is rejected before touching raft.
		require.Error(t, sub.Save(ctx, testUser{ID: "bad"}))
		require.NoError(t, node.Barrier(5*time.Second))

		var got testUser
		require.NoError(t, node.Store().Record().From("tenant", "acme").One(ctx, "ID", "u1", &got))
		require.Equal(t, "alex", got.Name)

		require.NoError(t, sub.Drop(ctx, &testUser{}))
	})

	t.Run("series", func(t *testing.T) {
		s := node.Series()
		require.NoError(t, s.Insert(ctx, "cpu", series.Point{Timestamp: 1, Value: 1}))
		require.NoError(t, s.InsertMany(ctx, "cpu", []series.Point{{Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}}))
		require.NoError(t, s.SetLabels(ctx, "cpu", map[string]string{"host": "a"}))
		require.NoError(t, s.SetRetention(ctx, "cpu", series.Retention{MaxPoints: 100}))
		removed, err := s.DeleteRange(ctx, "cpu", 2, 2)
		require.NoError(t, err)
		require.Equal(t, 1, removed)
		require.NoError(t, node.Barrier(5*time.Second))

		count, err := node.Store().Series().Series("cpu").Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		require.NoError(t, s.Drop(ctx, "cpu"))
		require.NoError(t, s.Insert(ctx, "tmp", series.Point{Timestamp: 1, Value: 1}))
		require.NoError(t, s.DeleteSeries(ctx, "tmp"))
	})

	t.Run("vector", func(t *testing.T) {
		v := node.Vector()
		require.NoError(t, v.CreateCollection("colors", 2, vector.Dot, false))
		require.NoError(t, v.CreateCollectionWithOptions(ctx, vector.CollectionOptions{
			Name: "shapes", VectorLen: 2, Metric: vector.Dot,
		}))
		require.NoError(t, v.EnsureCollection(ctx, "docs"))
		require.NoError(t, v.Upsert("colors", []vector.PointStruct{
			{ID: "red", Vector: []float32{1, 0}},
			{ID: "blue", Vector: []float32{0, 1}},
		}))
		require.NoError(t, v.Flush(ctx))
		require.NoError(t, v.Delete("colors", []string{"blue"}))
		require.NoError(t, v.Flush(ctx))
		require.NoError(t, node.Barrier(5*time.Second))

		points, err := node.Store().Vector().LoadCollection("colors")
		require.NoError(t, err)
		require.Contains(t, points, "red")
		require.NotContains(t, points, "blue")

		require.NoError(t, v.DropCollection("shapes"))
	})

	t.Run("file", func(t *testing.T) {
		f := node.Fstree().Open("data")
		require.NoError(t, f.Mkdir(ctx, "/dir", 0o755))
		require.NoError(t, f.WriteFile(ctx, "/dir/a.txt", []byte("hello"), 0o644))

		// Remove an existing entry and verify it is gone.
		require.NoError(t, f.WriteFile(ctx, "/dir/b.txt", []byte("bye"), 0o644))
		require.NoError(t, f.Remove(ctx, "/dir/b.txt"))
		// Removing a missing entry reports an error through raft.
		require.Error(t, f.Remove(ctx, "/dir/missing.txt"))

		require.NoError(t, f.Rename(ctx, "/dir/a.txt", "/dir/c.txt"))
		require.NoError(t, node.Barrier(5*time.Second))

		fsys, err := node.Store().Fstree().Open("data")
		require.NoError(t, err)
		data, err := fsys.ReadFile("/dir/c.txt")
		require.NoError(t, err)
		require.Equal(t, "hello", string(data))
		_, err = fsys.Stat("/dir/b.txt")
		require.True(t, errors.Is(err, os.ErrNotExist), "removed file still present")

		// Buffered handle: Truncate regrows/shrinks and Close replicates.
		h, err := f.Create(ctx, "/dir/h.bin", 0o644)
		require.NoError(t, err)
		_, err = h.Write([]byte("abcdef"))
		require.NoError(t, err)
		require.NoError(t, h.Truncate(3))
		require.Equal(t, int64(3), h.Size())
		require.NoError(t, h.Truncate(5)) // grow with zero bytes
		require.Equal(t, int64(5), h.Size())
		require.Error(t, h.Truncate(-1))
		require.NoError(t, h.Close())
		require.NoError(t, h.Close()) // idempotent

		require.NoError(t, f.RemoveAll(ctx, "/dir"))
		require.NoError(t, node.DeleteFileSystem(ctx, "data"))
	})
}

// TestFacadeNilReceivers locks in that every replicated view returns ErrClosed
// (or nil for lookups) on a nil receiver instead of panicking.
func TestFacadeNilReceivers(t *testing.T) {
	ctx := context.Background()

	var f *Fstree
	require.Nil(t, f.Open("x"))
	for _, err := range []error{
		f.Mkdir(ctx, "/d", 0o755),
		f.Remove(ctx, "/x"),
		f.RemoveAll(ctx, "/x"),
		f.Rename(ctx, "/a", "/b"),
		f.WriteFile(ctx, "/x", nil, 0o644),
		f.MkdirAll(ctx, "/d", 0o755),
		f.Truncate(ctx, "/x", 0),
		f.Chmod(ctx, "/x", 0o600),
		f.Chtimes(ctx, "/x", time.Unix(0, 0), time.Unix(0, 0)),
		f.Chown(ctx, "/x", 0, 0),
		f.Symlink(ctx, "/a", "/b"),
	} {
		require.ErrorIs(t, err, ErrClosed)
	}
	_, err := f.Create(ctx, "/x", 0o644)
	require.ErrorIs(t, err, ErrClosed)
	_, err = f.OpenWriter(ctx, "/x", 0o644)
	require.ErrorIs(t, err, ErrClosed)

	var node *Node
	require.ErrorIs(t, node.DeleteFileSystem(ctx, "data"), ErrClosed)

	var r *Record
	require.Nil(t, r.From("a"))
	require.ErrorIs(t, r.Save(ctx, &testUser{}), ErrClosed)
	require.ErrorIs(t, r.Update(ctx, &testUser{}), ErrClosed)
	require.ErrorIs(t, r.UpdateField(ctx, &testUser{}, "Name", "x"), ErrClosed)
	require.ErrorIs(t, r.DeleteStruct(ctx, &testUser{}), ErrClosed)
	require.ErrorIs(t, r.Drop(ctx, &testUser{}), ErrClosed)
	require.ErrorIs(t, r.Set(ctx, "b", "k", "v"), ErrClosed)
	require.ErrorIs(t, r.SetBytes(ctx, "b", "k", nil), ErrClosed)
	require.ErrorIs(t, r.Delete(ctx, "b", "k"), ErrClosed)

	var v *Vector
	require.ErrorIs(t, v.CreateCollection("c", 2, vector.Dot, false), ErrClosed)
	require.ErrorIs(t, v.CreateCollectionWithOptions(ctx, vector.CollectionOptions{Name: "c", VectorLen: 2}), ErrClosed)
	require.ErrorIs(t, v.EnsureCollection(ctx, "c"), ErrClosed)
	require.ErrorIs(t, v.Flush(ctx), ErrClosed)
	require.ErrorIs(t, v.Upsert("c", nil), ErrClosed)
	require.ErrorIs(t, v.Delete("c", nil), ErrClosed)
	require.ErrorIs(t, v.DropCollection("c"), ErrClosed)
}

// TestFacadeCancelledContext locks in that a cancelled context short-circuits
// every facade before any raft round trip.
func TestFacadeCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var f *Fstree
	for _, err := range []error{
		f.Mkdir(ctx, "/d", 0o755),
		f.Remove(ctx, "/x"),
		f.RemoveAll(ctx, "/x"),
		f.Rename(ctx, "/a", "/b"),
		f.WriteFile(ctx, "/x", nil, 0o644),
		f.MkdirAll(ctx, "/d", 0o755),
		f.Truncate(ctx, "/x", 0),
		f.Chmod(ctx, "/x", 0o600),
		f.Chtimes(ctx, "/x", time.Unix(0, 0), time.Unix(0, 0)),
		f.Chown(ctx, "/x", 0, 0),
		f.Symlink(ctx, "/a", "/b"),
	} {
		require.ErrorIs(t, err, context.Canceled)
	}

	var r *Record
	require.ErrorIs(t, r.Save(ctx, &testUser{}), context.Canceled)
	require.ErrorIs(t, r.Set(ctx, "b", "k", "v"), context.Canceled)
	require.ErrorIs(t, r.SetBytes(ctx, "b", "k", nil), context.Canceled)
	require.ErrorIs(t, r.Delete(ctx, "b", "k"), context.Canceled)

	var v *Vector
	require.ErrorIs(t, v.CreateCollectionWithOptions(ctx, vector.CollectionOptions{}), context.Canceled)
	require.ErrorIs(t, v.EnsureCollection(ctx, "c"), context.Canceled)
	require.ErrorIs(t, v.Flush(ctx), context.Canceled)

	var s *Series
	for _, err := range []error{
		s.Insert(ctx, "cpu", series.Point{}),
		s.InsertMany(ctx, "cpu", nil),
		s.SetLabels(ctx, "cpu", nil),
		s.SetRetention(ctx, "cpu", series.Retention{}),
		s.DeleteSeries(ctx, "cpu"),
	} {
		require.ErrorIs(t, err, context.Canceled)
	}
	if _, err := s.DeleteRange(ctx, "cpu", 0, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteRange: %v", err)
	}
}
