package graphs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/graphs"
)

func TestGraphsDrop(t *testing.T) {
	ctx := context.Background()
	s, _ := openDirect(t, graphs.Config{})

	g, err := s.Open("social")
	require.NoError(t, err)
	seedGraph(t, g)

	other, err := s.Open("other")
	require.NoError(t, err)
	require.NoError(t, other.UpsertNode(ctx, &graphs.Node{ID: "x", Type: "thing"}))

	require.NoError(t, s.Drop(ctx, "social"))

	names, err := s.Names(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"other"}, names)

	// Dropping a missing graph is a no-op.
	require.NoError(t, s.Drop(ctx, "social"))

	// Opening the dropped name again yields an empty graph.
	fresh, err := s.Open("social")
	require.NoError(t, err)
	nodes, err := fresh.GetAllNodes(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, nodes)
}
