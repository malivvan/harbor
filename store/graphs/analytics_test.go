package graphs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/graphs"
)

func starGraph(t *testing.T) *graphs.Graph {
	t.Helper()
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("g")
	require.NoError(t, err)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: id, Type: "node"}))
	}
	for i, id := range []string{"b", "c", "d"} {
		require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "e" + id, From: "a", To: id, Type: "link", Weight: float64(i + 1)}))
	}
	return g
}

func TestPersonalizedPageRank(t *testing.T) {
	g := starGraph(t)
	ctx := context.Background()
	result, err := g.PersonalizedPageRank(ctx, []string{"a"}, graphs.PPROptions{})
	require.NoError(t, err)
	sum := 0.0
	for _, score := range result.Scores {
		sum += score
	}
	require.InDelta(t, 1.0, sum, 1e-9)
	require.Greater(t, result.Score("a"), result.Score("b"))

	again, err := g.PersonalizedPageRank(ctx, []string{"a"}, graphs.PPROptions{})
	require.NoError(t, err)
	require.Equal(t, result.Scores, again.Scores)
}

func TestCommunities(t *testing.T) {
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("g")
	require.NoError(t, err)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: id}))
	}
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "ab", From: "a", To: "b"}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "cd", From: "c", To: "d"}))

	result, err := g.Communities(ctx, graphs.CommunityOptions{})
	require.NoError(t, err)
	require.Len(t, result.Communities, 2)
	require.NotEqual(t, result.Labels["a"], result.Labels["c"])
	require.Equal(t, result.Labels["a"], result.Labels["b"])
	require.Equal(t, result.Labels["c"], result.Labels["d"])
}

func TestDegreeCentrality(t *testing.T) {
	g := starGraph(t)
	ctx := context.Background()
	centrality, err := g.DegreeCentrality(ctx)
	require.NoError(t, err)
	require.InDelta(t, 1.0, centrality["a"], 1e-9)
	require.InDelta(t, 1.0/3.0, centrality["b"], 1e-9)
}

func TestWeightedShortestPath(t *testing.T) {
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("g")
	require.NoError(t, err)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: id}))
	}
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "ab", From: "a", To: "b", Weight: 1}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "bc", From: "b", To: "c", Weight: 1}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "ac", From: "a", To: "c", Weight: 10}))

	path, err := g.WeightedShortestPath(ctx, "a", "c", "")
	require.NoError(t, err)
	require.Equal(t, 2.0, path.Distance)
	require.Len(t, path.Nodes, 3)
	require.Equal(t, "a", path.Nodes[0].ID)
	require.Equal(t, "c", path.Nodes[2].ID)

	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "neg", From: "a", To: "c", Weight: -1}))
	_, err = g.WeightedShortestPath(ctx, "a", "c", "")
	require.Error(t, err)
}

func TestAnalyticsMissingNode(t *testing.T) {
	g := starGraph(t)
	_, err := g.WeightedShortestPath(context.Background(), "a", "missing", "")
	require.ErrorIs(t, err, graphs.ErrNodeNotFound)
}
