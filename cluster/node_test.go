package cluster

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

// testUser is a db record with a string primary key and an indexed field.
type testUser struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

func (testUser) TempestBucketName() string { return "users" }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func testStoreOpts() *tempest.Options {
	return &tempest.Options{
		Password:   "test-password",
		Plaintext:  true,
		KDFTime:    tempest.FastKDFTime,
		KDFMemory:  tempest.FastKDFMemory,
		KDFThreads: tempest.FastKDFThreads,
	}
}

func testConfig(id raft.ServerID, addr string, bootstrap bool, t *testing.T) Config {
	dir := t.TempDir()
	return Config{
		NodeID:       id,
		BindAddr:     addr,
		Dir:          dir,
		Bootstrap:    bootstrap,
		StorePath:    filepath.Join(dir, "tempest.db"),
		StoreMode:    0o600,
		StoreOpts:    testStoreOpts(),
		ApplyTimeout: 10 * time.Second,
	}
}

// waitLeader polls until exactly one node reports itself the leader.
func waitLeader(t *testing.T, nodes ...*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var leader *Node
		for _, n := range nodes {
			if n.State() == raft.Leader {
				leader = n
				break
			}
		}
		if leader != nil {
			// Give the leader a moment to apply any in-flight configuration.
			return leader
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

// waitApplied blocks until every node has applied at least the leader's
// current index, so follower reads are safe.
func waitApplied(t *testing.T, leader *Node, nodes ...*Node) {
	t.Helper()
	require.NoError(t, leader.Barrier(10*time.Second))
	target := leader.AppliedIndex()
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			if n.AppliedIndex() < target {
				return false
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond)
}

// startCluster forms an n-node cluster. node 0 bootstraps; the rest join it.
func startCluster(t *testing.T, n int) []*Node {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = freeAddr(t)
	}
	nodes := make([]*Node, n)
	for i := range nodes {
		id := raft.ServerID("node-" + string(rune('1'+i)))
		n, err := Open(context.Background(), testConfig(id, addrs[i], i == 0, t))
		require.NoError(t, err)
		nodes[i] = n
	}
	leader := waitLeader(t, nodes[0])
	for i := 1; i < n; i++ {
		require.NoError(t, leader.Join(raft.ServerID("node-"+string(rune('1'+i))), raft.ServerAddress(addrs[i])))
	}
	_ = waitLeader(t, nodes...)
	return nodes
}

func closeAll(nodes ...*Node) {
	for _, n := range nodes {
		if n != nil {
			_ = n.Close()
		}
	}
}

func TestClusterElectsLeader(t *testing.T) {
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NotNil(t, leader)
	_, leaderID := leader.Leader()
	require.NotEmpty(t, leaderID)
}

func TestReplicateDB(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.Record().Save(ctx, &testUser{ID: "u1", Name: "alice"}))
	require.NoError(t, leader.Record().Save(ctx, &testUser{ID: "u2", Name: "bob"}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		var got testUser
		require.NoError(t, n.Store().Record().One(ctx, "ID", "u1", &got), "read on %s", n.cfg.NodeID)
		require.Equal(t, "alice", got.Name)
	}

	// UpdateField and delete also replicate.
	require.NoError(t, leader.Record().UpdateField(ctx, &testUser{ID: "u1"}, "Name", "alicia"))
	require.NoError(t, leader.Record().DeleteStruct(ctx, &testUser{ID: "u2"}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		var got testUser
		require.NoError(t, n.Store().Record().One(ctx, "ID", "u1", &got))
		require.Equal(t, "alicia", got.Name)
		require.Error(t, n.Store().Record().One(ctx, "ID", "u2", &got))
	}
}

func TestReplicateTS(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.Series().Insert(ctx, "cpu", series.Point{Timestamp: 1, Value: 1.5}))
	require.NoError(t, leader.Series().InsertMany(ctx, "cpu", []series.Point{
		{Timestamp: 2, Value: 2.5},
		{Timestamp: 3, Value: 3.5},
	}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		p, err := n.Store().Series().Series("cpu").Get(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, 2.5, p.Value)

		count, err := n.Store().Series().Series("cpu").Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	}
}

func TestReplicateVec(t *testing.T) {
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.Vector().CreateCollection("colors", 2, vector.Dot, false))
	require.NoError(t, leader.Vector().Upsert("colors", []vector.PointStruct{
		{ID: "red", Vector: []float32{1, 0}},
		{ID: "blue", Vector: []float32{0, 1}},
	}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		points, err := n.Store().Vector().LoadCollection("colors")
		require.NoError(t, err)
		require.Len(t, points, 2)
		require.Contains(t, points, "red")
		require.Contains(t, points, "blue")
	}
}

func TestReplicateFS(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	fs := leader.File().Open("data")
	require.NoError(t, fs.Mkdir(ctx, "/docs", 0o755))
	require.NoError(t, fs.WriteFile(ctx, "/docs/readme.txt", []byte("hello tempest"), 0o644))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		fsys, err := n.Store().File().Open("data")
		require.NoError(t, err)
		data, err := fsys.ReadFile("/docs/readme.txt")
		require.NoError(t, err)
		require.Equal(t, "hello tempest", string(data))
	}
}

func TestSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	for i := 1; i <= 5; i++ {
		require.NoError(t, leader.Record().Save(ctx, &testUser{ID: "u" + string(rune('0'+i)), Name: "n"}))
	}
	waitApplied(t, leader, nodes...)

	// Force a snapshot, then start a fresh node that must catch up from it.
	require.NoError(t, leader.Snapshot())
	require.Eventually(t, func() bool {
		stats := leader.Stats()
		return stats["last_snapshot_index"] != "0"
	}, 10*time.Second, 100*time.Millisecond)

	addr4 := freeAddr(t)
	n4, err := Open(context.Background(), testConfig("node-4", addr4, false, t))
	require.NoError(t, err)
	defer n4.Close()

	require.NoError(t, leader.Join("node-4", raft.ServerAddress(addr4)))
	// Wait until the new node has caught up via snapshot + log.
	require.Eventually(t, func() bool {
		return n4.AppliedIndex() >= leader.AppliedIndex() && n4.AppliedIndex() > 0
	}, 15*time.Second, 100*time.Millisecond)

	var got testUser
	require.NoError(t, n4.Store().Record().One(ctx, "ID", "u3", &got))
	require.Equal(t, "n", got.Name)
}

// TestReplicatedRetentionIsDeterministic pins the leader's clock into the
// replicated command, so every replica prunes with the same reference time
// even when their local clocks disagree.
func TestReplicatedRetentionIsDeterministic(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)

	// Each node runs a wildly different local clock. Only the leader's clock is
	// pinned into the command, so the outcome must not depend on the others.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	leader.SetNow(func() int64 { return base })
	for i, n := range nodes {
		if n == leader {
			continue
		}
		skew := int64(i) * int64(365*24*time.Hour)
		n.SetNow(func() int64 { return base + skew })
	}

	// MaxAge of one hour with points one minute apart: the leader's clock keeps
	// all of them, and every replica must agree.
	require.NoError(t, leader.Series().SetRetention(ctx, "cpu", series.Retention{MaxAge: time.Hour}))
	for i := int64(0); i < 5; i++ {
		require.NoError(t, leader.Series().Insert(ctx, "cpu", series.Point{
			Timestamp: base - i*int64(time.Minute),
			Value:     float64(i),
		}))
	}
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		count, err := n.Store().Series().Series("cpu").Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, count, "node %s pruned differently", n.cfg.NodeID)
	}
}

// TestReplicatedRetentionPrunesIdentically checks the pruning path itself: with
// the leader's clock far ahead, every replica must drop exactly the same points.
func TestReplicatedRetentionPrunesIdentically(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	leader.SetNow(func() int64 { return base })
	for i, n := range nodes {
		if n == leader {
			continue
		}
		// A clock far in the past would prune everything if it were consulted.
		_ = i
		n.SetNow(func() int64 { return base - int64(10*365*24*time.Hour) })
	}

	require.NoError(t, leader.Series().SetRetention(ctx, "cpu", series.Retention{MaxAge: time.Hour}))
	require.NoError(t, leader.Series().InsertMany(ctx, "cpu", []series.Point{
		{Timestamp: base - int64(30*time.Minute), Value: 1}, // kept
		{Timestamp: base - int64(2*time.Hour), Value: 2},    // pruned by MaxAge
		{Timestamp: base - int64(3*time.Hour), Value: 3},    // pruned by MaxAge
	}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		pts, err := n.Store().Series().Series("cpu").Range(ctx, base-int64(24*time.Hour), base)
		require.NoError(t, err)
		require.Len(t, pts, 1, "node %s kept the wrong points", n.cfg.NodeID)
		require.Equal(t, 1.0, pts[0].Value)
	}
}

// TestApplyErrorsAreStructured checks that a corrupt, unknown, or malformed log
// entry is reported as an ApplyError instead of panicking the process, and that
// the node keeps serving writes afterwards.
func TestApplyErrorsAreStructured(t *testing.T) {
	node, err := Open(context.Background(), testConfig("node-1", freeAddr(t), true, t))
	require.NoError(t, err)
	defer closeAll(node)

	// A single-node cluster elects itself; wait for that before writing.
	_ = waitLeader(t, node)

	f := &fsm{node: node}

	t.Run("undecodable command", func(t *testing.T) {
		resp := f.Apply(&raft.Log{Index: 7, Data: []byte("not json")})
		var ae *ApplyError
		require.ErrorAs(t, resp.(error), &ae)
		require.Equal(t, uint64(7), ae.Index)
		require.Empty(t, ae.Op)
	})

	t.Run("unknown op", func(t *testing.T) {
		data, err := encodeCommand("nope.do_thing", struct{}{})
		require.NoError(t, err)
		resp := f.Apply(&raft.Log{Index: 8, Data: data})
		var ae *ApplyError
		require.ErrorAs(t, resp.(error), &ae)
		require.Equal(t, uint64(8), ae.Index)
		require.Equal(t, "nope.do_thing", ae.Op)
	})

	t.Run("malformed argument", func(t *testing.T) {
		data, err := json.Marshal(Command{Op: opSeriesInsert, Arg: json.RawMessage(`{"series":42}`)})
		require.NoError(t, err)
		resp := f.Apply(&raft.Log{Index: 9, Data: data})
		var ae *ApplyError
		require.ErrorAs(t, resp.(error), &ae)
		require.Equal(t, uint64(9), ae.Index)
		require.Equal(t, opSeriesInsert, ae.Op)
	})

	t.Run("node stays usable", func(t *testing.T) {
		require.NoError(t, node.Series().Insert(context.Background(), "cpu", series.Point{Timestamp: 1, Value: 1}))
		count, err := node.Store().Series().Series("cpu").Count(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
}
