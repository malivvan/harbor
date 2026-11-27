package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/vector"
)

// followerOf returns a node that is not the leader.
func followerOf(t *testing.T, leader *Node, nodes []*Node) *Node {
	t.Helper()
	for _, n := range nodes {
		if n != leader {
			return n
		}
	}
	t.Fatal("no follower in the cluster")
	return nil
}

func TestClusterSearchReplication(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)
	leader := waitLeader(t, nodes...)

	require.NoError(t, leader.Search().Create("docs", search.Config{}))
	require.NoError(t, leader.Search().Upsert("docs", "d1", "alpha beta"))
	require.NoError(t, leader.Search().Upsert("docs", "d2", "beta gamma"))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		idx, err := n.Store().Search().Open("docs", search.Config{})
		require.NoError(t, err, "open on %s", n.cfg.NodeID)
		hits, err := idx.Search(ctx, "beta", 0)
		require.NoError(t, err)
		require.Len(t, hits, 2, "search on %s", n.cfg.NodeID)
	}

	require.NoError(t, leader.Search().Delete("docs", "d1"))
	waitApplied(t, leader, nodes...)
	idx, err := followerOf(t, leader, nodes).Store().Search().Open("docs", search.Config{})
	require.NoError(t, err)
	hits, err := idx.Search(ctx, "alpha", 0)
	require.NoError(t, err)
	require.Empty(t, hits)

	require.NoError(t, leader.Search().Drop("docs"))
	waitApplied(t, leader, nodes...)
	names, err := followerOf(t, leader, nodes).Store().Search().Indexes(ctx)
	require.NoError(t, err)
	require.Empty(t, names)
}

func TestClusterGraphsReplication(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)
	leader := waitLeader(t, nodes...)

	require.NoError(t, leader.Graphs().Create("social"))
	require.NoError(t, leader.Graphs().UpsertNode("social", &graphs.Node{ID: "a", Type: "person", Content: "alice"}))
	require.NoError(t, leader.Graphs().UpsertNode("social", &graphs.Node{ID: "b", Type: "person", Content: "bob"}))
	require.NoError(t, leader.Graphs().UpsertEdge("social", &graphs.Edge{ID: "e1", From: "a", To: "b", Type: "knows"}))
	waitApplied(t, leader, nodes...)

	follower := followerOf(t, leader, nodes)
	g, err := follower.Store().Graphs().Open("social")
	require.NoError(t, err)
	gotNodes, err := g.GetAllNodes(ctx, nil)
	require.NoError(t, err)
	require.Len(t, gotNodes, 2)
	gotEdges, err := g.GetAllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, gotEdges, 1)
	require.Equal(t, "knows", gotEdges[0].Type)

	require.NoError(t, leader.Graphs().DeleteEdge("social", "e1"))
	require.NoError(t, leader.Graphs().DeleteNode("social", "b"))
	waitApplied(t, leader, nodes...)
	gotEdges, err = g.GetAllEdges(ctx)
	require.NoError(t, err)
	require.Empty(t, gotEdges)
	gotNodes, err = g.GetAllNodes(ctx, nil)
	require.NoError(t, err)
	require.Len(t, gotNodes, 1)

	require.NoError(t, leader.Graphs().Drop("social"))
	waitApplied(t, leader, nodes...)
	names, err := follower.Store().Graphs().Names(ctx)
	require.NoError(t, err)
	require.Empty(t, names)
}

func TestClusterObjectsReplication(t *testing.T) {
	ctx := context.Background()
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)
	leader := waitLeader(t, nodes...)

	require.NoError(t, leader.Objects().Put("tenant-a", "a/b.txt", []byte("hello")))
	waitApplied(t, leader, nodes...)

	follower := followerOf(t, leader, nodes)
	data, err := follower.Store().Objects().ReadAll(ctx, "tenant-a", "a/b.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	require.NoError(t, leader.Objects().Move("tenant-a", "a/b.txt", "a/c.txt"))
	waitApplied(t, leader, nodes...)
	_, ok, err := follower.Store().Objects().Stat(ctx, "tenant-a", "a/b.txt")
	require.NoError(t, err)
	require.False(t, ok)
	data, err = follower.Store().Objects().ReadAll(ctx, "tenant-a", "a/c.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	require.NoError(t, leader.Objects().Delete("tenant-a", "a/c.txt"))
	waitApplied(t, leader, nodes...)
	_, ok, err = follower.Store().Objects().Stat(ctx, "tenant-a", "a/c.txt")
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, leader.Objects().Put("tenant-b", "k", []byte("v")))
	require.NoError(t, leader.Objects().DropBucket("tenant-b"))
	waitApplied(t, leader, nodes...)
	buckets, err := follower.Store().Objects().Buckets(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-a"}, buckets)

	require.NoError(t, leader.Objects().DropBucket("tenant-a"))
	waitApplied(t, leader, nodes...)
	buckets, err = follower.Store().Objects().Buckets(ctx)
	require.NoError(t, err)
	require.Empty(t, buckets)
}

func TestClusterFacadeNotLeader(t *testing.T) {
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)
	leader := waitLeader(t, nodes...)
	follower := followerOf(t, leader, nodes)

	require.ErrorIs(t, follower.Search().Create("docs", search.Config{}), ErrNotLeader)
	require.ErrorIs(t, follower.Search().Upsert("docs", "d1", "text"), ErrNotLeader)
	require.ErrorIs(t, follower.Search().Delete("docs", "d1"), ErrNotLeader)
	require.ErrorIs(t, follower.Search().Drop("docs"), ErrNotLeader)

	require.ErrorIs(t, follower.Graphs().Create("g"), ErrNotLeader)
	require.ErrorIs(t, follower.Graphs().Drop("g"), ErrNotLeader)
	require.ErrorIs(t, follower.Graphs().UpsertNode("g", &graphs.Node{ID: "a"}), ErrNotLeader)
	require.ErrorIs(t, follower.Graphs().DeleteNode("g", "a"), ErrNotLeader)
	require.ErrorIs(t, follower.Graphs().UpsertEdge("g", &graphs.Edge{ID: "e"}), ErrNotLeader)
	require.ErrorIs(t, follower.Graphs().DeleteEdge("g", "e"), ErrNotLeader)

	require.ErrorIs(t, follower.Objects().Put("b", "k", nil), ErrNotLeader)
	require.ErrorIs(t, follower.Objects().Delete("b", "k"), ErrNotLeader)
	require.ErrorIs(t, follower.Objects().Move("b", "k", "j"), ErrNotLeader)
	require.ErrorIs(t, follower.Objects().DropBucket("b"), ErrNotLeader)
}

// TestClusterSearchCustomTokenizerRejected proves a tokenizer that cannot be
// serialized is refused instead of being replaced by the default on replicas.
func TestClusterSearchCustomTokenizerRejected(t *testing.T) {
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)
	leader := waitLeader(t, nodes...)

	require.ErrorIs(t, leader.Search().Create("docs", search.Config{Tokenizer: clusterCustomTokenizer{}}), ErrInvalidConfig)
}

type clusterCustomTokenizer struct{}

func (clusterCustomTokenizer) Tokenize(text string) []string { return []string{text} }

// TestClusterVectorIndexRefresh proves a replicated write refreshes the live
// in-memory index on every node, without reopening the collection.
func TestClusterVectorIndexRefresh(t *testing.T) {
	nodes := startCluster(t, 2)
	defer closeAll(nodes...)
	leader := waitLeader(t, nodes...)

	require.NoError(t, leader.Vector().CreateCollection("docs", 2, vector.Cosine, false))
	require.NoError(t, leader.Vector().Upsert("docs", []vector.PointStruct{{ID: "p1", Vector: []float32{1, 0}}}))
	require.NoError(t, leader.Vector().Flush(context.Background()))
	waitApplied(t, leader, nodes...)

	for _, n := range nodes {
		col, err := n.Store().Vector().Collection("docs")
		require.NoError(t, err, "collection on %s", n.cfg.NodeID)
		hits, err := col.Search([]float32{1, 0}, nil, 10)
		require.NoError(t, err)
		require.Len(t, hits, 1, "live index on %s", n.cfg.NodeID)
		require.Equal(t, "p1", hits[0].ID)
	}

	require.NoError(t, leader.Vector().Delete("docs", []string{"p1"}))
	waitApplied(t, leader, nodes...)
	for _, n := range nodes {
		col, err := n.Store().Vector().Collection("docs")
		require.NoError(t, err)
		hits, err := col.Search([]float32{1, 0}, nil, 10)
		require.NoError(t, err)
		require.Empty(t, hits, "live index on %s after delete", n.cfg.NodeID)
	}
}
