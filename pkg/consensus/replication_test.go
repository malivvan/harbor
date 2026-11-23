package consensus

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/vector"
)

// TestReplicateDBKeyValue covers the raw key/value writes: Set encodes the
// value once on the leader, SetBytes replicates caller-encoded bytes, and
// Delete removes the entry. Key encoding is type-stable: an int key and a
// string key reach the same slot on every replica.
func TestReplicateDBKeyValue(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.DB().Set(ctx, "settings", "theme", "dark"))
	require.NoError(t, leader.DB().Set(ctx, "settings", 42, "answer"))
	require.NoError(t, leader.DB().SetBytes(ctx, "blobs", "raw", []byte{1, 2, 3}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		var theme string
		require.NoError(t, n.Store().Record().Get(ctx, "settings", "theme", &theme), "read on %s", n.cfg.NodeID)
		require.Equal(t, "dark", theme)

		var answer string
		require.NoError(t, n.Store().Record().Get(ctx, "settings", 42, &answer))
		require.Equal(t, "answer", answer)

		raw, err := n.Store().Record().GetBytes(ctx, "blobs", "raw")
		require.NoError(t, err)
		require.Equal(t, []byte{1, 2, 3}, raw)
	}

	require.NoError(t, leader.DB().Delete(ctx, "settings", "theme"))
	require.NoError(t, leader.DB().Delete(ctx, "blobs", "raw"))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		var theme string
		require.Error(t, n.Store().Record().Get(ctx, "settings", "theme", &theme))
		exists, err := n.Store().Record().KeyExists(ctx, "blobs", "raw")
		require.NoError(t, err)
		require.False(t, exists)
	}
}

// TestReplicateFSMetadata covers MkdirAll, Truncate, Chmod, Chtimes, Chown and
// Symlink. Directory times are pinned so every replica records the same
// metadata even though the replicas create the directories independently.
func TestReplicateFSMetadata(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	fsys := leader.FS().Open("data")
	require.NoError(t, fsys.MkdirAll(ctx, "/a/b/c", 0o750))
	require.NoError(t, fsys.WriteFile(ctx, "/a/b/file.txt", []byte("0123456789"), 0o644))
	require.NoError(t, fsys.Truncate(ctx, "/a/b/file.txt", 4))
	require.NoError(t, fsys.Chmod(ctx, "/a/b/file.txt", 0o600))
	mtime := time.Unix(2000, 0)
	require.NoError(t, fsys.Chtimes(ctx, "/a/b/file.txt", time.Unix(1000, 0), mtime))
	require.NoError(t, fsys.Chown(ctx, "/a/b/file.txt", 7, 8))
	require.NoError(t, fsys.Symlink(ctx, "/a/b/file.txt", "/a/link"))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		local, err := n.Store().File().Open("data")
		require.NoError(t, err)

		data, err := local.ReadFile("/a/b/file.txt")
		require.NoError(t, err)
		require.Equal(t, "0123", string(data), "truncate on %s", n.cfg.NodeID)

		info, err := local.Stat("/a/b/file.txt")
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		require.WithinDuration(t, mtime, info.ModTime(), time.Second)

		target, err := local.Readlink("/a/link")
		require.NoError(t, err)
		require.Equal(t, "/a/b/file.txt", target)

		dirInfo, err := local.Stat("/a/b/c")
		require.NoError(t, err)
		require.True(t, dirInfo.IsDir())
		require.Equal(t, os.FileMode(0o750), dirInfo.Mode().Perm())
	}
}

// TestReplicateFSStreamingWrite covers the buffered replicated file handle:
// nothing is visible until Close, and Close replicates the complete content.
// OpenWriter appends to the leader's current content.
func TestReplicateFSStreamingWrite(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	fsys := leader.FS().Open("data")

	file, err := fsys.Create(ctx, "/stream.bin", 0o644)
	require.NoError(t, err)
	_, err = file.Write([]byte("hello "))
	require.NoError(t, err)
	_, err = file.WriteAt([]byte("WORLD"), 6)
	require.NoError(t, err)
	require.Equal(t, int64(11), file.Size())

	// Buffered writes must not reach the log before Close.
	waitApplied(t, leader, nodes...)
	for _, n := range nodes {
		local, err := n.Store().File().Open("data")
		require.NoError(t, err)
		_, err = local.Stat("/stream.bin")
		require.True(t, os.IsNotExist(err), "buffered file visible on %s before Close", n.cfg.NodeID)
	}

	require.NoError(t, file.Close())
	waitApplied(t, leader, nodes...)
	for _, n := range nodes {
		local, err := n.Store().File().Open("data")
		require.NoError(t, err)
		data, err := local.ReadFile("/stream.bin")
		require.NoError(t, err)
		require.Equal(t, "hello WORLD", string(data))
	}

	app, err := fsys.OpenWriter(ctx, "/stream.bin", 0o644)
	require.NoError(t, err)
	_, err = app.Write([]byte("!"))
	require.NoError(t, err)
	require.NoError(t, app.Close())
	waitApplied(t, leader, nodes...)
	for _, n := range nodes {
		local, err := n.Store().File().Open("data")
		require.NoError(t, err)
		data, err := local.ReadFile("/stream.bin")
		require.NoError(t, err)
		require.Equal(t, "hello WORLD!", string(data))
	}
}

// TestReplicateVecFlushAndOptions covers the configuration and flush commands.
// Because vector writes are buffered, the replicated Flush is the log point
// that makes a point durable on every replica.
func TestReplicateVecFlushAndOptions(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.Vec().EnsureCollection(ctx, "docs"))
	require.NoError(t, leader.Vec().CreateCollectionWithOptions(ctx, vector.CollectionOptions{
		Name:           "colors",
		VectorLen:      2,
		Metric:         vector.Dot,
		FlushThreshold: 1000,
	}))
	require.NoError(t, leader.Vec().Upsert("colors", []vector.PointStruct{
		{ID: "red", Vector: []float32{1, 0}},
	}))
	require.NoError(t, leader.Vec().Flush(ctx))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		st := n.Store().Vector()

		names, err := st.ListCollections()
		require.NoError(t, err)
		require.Contains(t, names, "docs", "EnsureCollection on %s", n.cfg.NodeID)
		require.Contains(t, names, "colors")

		metas, err := st.ListCollectionMetas()
		require.NoError(t, err)
		var found bool
		for _, meta := range metas {
			if meta.Name == "colors" {
				found = true
				require.Equal(t, 2, meta.VectorLen)
				require.Equal(t, vector.Dot, meta.Metric)
			}
		}
		require.True(t, found, "collection metadata missing on %s", n.cfg.NodeID)

		points, err := st.LoadCollection("colors")
		require.NoError(t, err)
		require.Contains(t, points, "red", "flushed point missing on %s", n.cfg.NodeID)
		require.Equal(t, []float32{1, 0}, points["red"].Vector)
	}
}

// TestReplicateTSMetadataAndDeletion covers metadata and range deletion on top
// of the insert path: labels, retention and a deleted range must match on every
// replica.
func TestReplicateTSMetadataAndDeletion(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.TS().InsertMany(ctx, "cpu", []series.Point{
		{Timestamp: 1, Value: 1},
		{Timestamp: 2, Value: 2},
		{Timestamp: 3, Value: 3},
	}))
	require.NoError(t, leader.TS().SetLabels(ctx, "cpu", map[string]string{"host": "a"}))
	require.NoError(t, leader.TS().SetRetention(ctx, "cpu", series.Retention{MaxPoints: 10}))
	removed, err := leader.TS().DeleteRange(ctx, "cpu", 2, 2)
	require.NoError(t, err)
	require.Equal(t, 1, removed)
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		ser := n.Store().Series().Series("cpu")
		count, err := ser.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, count, "count on %s", n.cfg.NodeID)

		_, err = ser.Get(ctx, 2)
		require.Error(t, err)

		infos, err := n.Store().Series().ListSeries(ctx)
		require.NoError(t, err)
		var info *series.SeriesInfo
		for i := range infos {
			if infos[i].Name == "cpu" {
				info = &infos[i]
			}
		}
		require.NotNil(t, info, "series metadata missing on %s", n.cfg.NodeID)
		require.Equal(t, "a", info.Labels["host"])
		require.Equal(t, 10, info.Retention.MaxPoints)
		require.Equal(t, int64(2), info.Count)
	}
}
