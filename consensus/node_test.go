package consensus

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor"
	"github.com/malivvan/harbor/ts"
	"github.com/malivvan/harbor/vec"
)

// testUser is a db record with a string primary key and an indexed field.
type testUser struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

func (testUser) HarborBucketName() string { return "users" }

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

func testStoreOpts() *harbor.Options {
	return &harbor.Options{
		Password:   "test-password",
		Plaintext:  true,
		KDFTime:    harbor.FastKDFTime,
		KDFMemory:  harbor.FastKDFMemory,
		KDFThreads: harbor.FastKDFThreads,
	}
}

func testConfig(id raft.ServerID, addr string, bootstrap bool, t *testing.T) Config {
	dir := t.TempDir()
	return Config{
		NodeID:       id,
		BindAddr:     addr,
		Dir:          dir,
		Bootstrap:    bootstrap,
		StorePath:    filepath.Join(dir, "harbor.db"),
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
	require.NoError(t, leader.DB().Save(ctx, &testUser{ID: "u1", Name: "alice"}))
	require.NoError(t, leader.DB().Save(ctx, &testUser{ID: "u2", Name: "bob"}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		var got testUser
		require.NoError(t, n.Store().DB().One(ctx, "ID", "u1", &got), "read on %s", n.cfg.NodeID)
		require.Equal(t, "alice", got.Name)
	}

	// UpdateField and delete also replicate.
	require.NoError(t, leader.DB().UpdateField(ctx, &testUser{ID: "u1"}, "Name", "alicia"))
	require.NoError(t, leader.DB().DeleteStruct(ctx, &testUser{ID: "u2"}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		var got testUser
		require.NoError(t, n.Store().DB().One(ctx, "ID", "u1", &got))
		require.Equal(t, "alicia", got.Name)
		require.Error(t, n.Store().DB().One(ctx, "ID", "u2", &got))
	}
}

func TestReplicateTS(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.TS().Insert(ctx, "cpu", ts.Point{Timestamp: 1, Value: 1.5}))
	require.NoError(t, leader.TS().InsertMany(ctx, "cpu", []ts.Point{
		{Timestamp: 2, Value: 2.5},
		{Timestamp: 3, Value: 3.5},
	}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		p, err := n.Store().TS().Series("cpu").Get(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, 2.5, p.Value)

		count, err := n.Store().TS().Series("cpu").Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	}
}

func TestReplicateVec(t *testing.T) {
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	require.NoError(t, leader.Vec().CreateCollection("colors", 2, vec.Dot, false))
	require.NoError(t, leader.Vec().Upsert("colors", []vec.PointStruct{
		{ID: "red", Vector: []float32{1, 0}},
		{ID: "blue", Vector: []float32{0, 1}},
	}))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		points, err := n.Store().Vec().LoadCollection("colors")
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
	fs := leader.FS().Open("data")
	require.NoError(t, fs.Mkdir(ctx, "/docs", 0o755))
	require.NoError(t, fs.WriteFile(ctx, "/docs/readme.txt", []byte("hello harbor"), 0o644))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		fsys, err := n.Store().FS().Open("data")
		require.NoError(t, err)
		data, err := fsys.ReadFile("/docs/readme.txt")
		require.NoError(t, err)
		require.Equal(t, "hello harbor", string(data))
	}
}

func TestSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 3)
	defer closeAll(nodes...)

	leader := waitLeader(t, nodes...)
	for i := 1; i <= 5; i++ {
		require.NoError(t, leader.DB().Save(ctx, &testUser{ID: "u" + string(rune('0'+i)), Name: "n"}))
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
	require.NoError(t, n4.Store().DB().One(ctx, "ID", "u3", &got))
	require.Equal(t, "n", got.Name)
}
