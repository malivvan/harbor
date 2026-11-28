package graphs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/graphs"
)

func schemaViolation(err error) (graphs.SchemaViolation, bool) {
	var violation graphs.SchemaViolation
	return violation, errors.As(err, &violation)
}

func TestSchemaRequiredUniqueAndEnum(t *testing.T) {
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("people")
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, g.SetSchema(ctx, graphs.Schema{
		NodeTypes: map[string]graphs.NodeTypeSpec{
			"person": {
				Required:   []string{"name", "kind"},
				Unique:     []string{"name"},
				Properties: map[string]graphs.PropertySpec{"kind": {Type: "string", Enum: []string{"human", "agent"}}},
			},
		},
	}))

	_, ok := schemaViolation(g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person"}))
	require.True(t, ok)

	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{
		ID: "a", Type: "person",
		Properties: map[string]any{"name": "alice", "kind": "human"},
	}))

	_, ok = schemaViolation(g.UpsertNode(ctx, &graphs.Node{
		ID: "b", Type: "person",
		Properties: map[string]any{"name": "alice", "kind": "human"},
	}))
	require.True(t, ok)

	_, ok = schemaViolation(g.UpsertNode(ctx, &graphs.Node{
		ID: "b", Type: "person",
		Properties: map[string]any{"name": "bob", "kind": "robot"},
	}))
	require.True(t, ok)

	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{
		ID: "b", Type: "person",
		Properties: map[string]any{"name": "bob", "kind": "agent"},
	}))

	_, ok = schemaViolation(g.UpsertNode(ctx, &graphs.Node{ID: "c", Type: "unknown"}))
	require.True(t, ok)
}

func TestSchemaEdgeEndpointsAndCardinality(t *testing.T) {
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("org")
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, g.SetSchema(ctx, graphs.Schema{
		NodeTypes: map[string]graphs.NodeTypeSpec{
			"person":  {},
			"company": {},
		},
		EdgeTypes: map[string]graphs.EdgeTypeSpec{
			"works_for": {From: []string{"person"}, To: []string{"company"}, MaxPerPair: 1},
		},
	}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "p", Type: "person"}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "c", Type: "company"}))

	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "e1", From: "p", To: "c", Type: "works_for"}))
	_, ok := schemaViolation(g.UpsertEdge(ctx, &graphs.Edge{ID: "e2", From: "p", To: "c", Type: "works_for"}))
	require.True(t, ok)

	_, ok = schemaViolation(g.UpsertEdge(ctx, &graphs.Edge{ID: "e3", From: "c", To: "p", Type: "works_for"}))
	require.True(t, ok)

	_, ok = schemaViolation(g.UpsertEdge(ctx, &graphs.Edge{ID: "e4", From: "p", To: "c", Type: "unknown"}))
	require.True(t, ok)
}

func TestSchemaValidateReportsExistingViolations(t *testing.T) {
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("legacy")
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person"}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "b", Type: "person", Properties: map[string]any{"name": "bob"}}))
	require.NoError(t, g.SetSchema(ctx, graphs.Schema{
		NodeTypes: map[string]graphs.NodeTypeSpec{"person": {Required: []string{"name"}}},
	}))

	report, err := g.Validate(ctx, true)
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Len(t, report.Violations, 1)
	require.Equal(t, "a", report.Violations[0].ID)
	require.Equal(t, "name", report.Violations[0].Field)

	// Validation never mutates the graph.
	node, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "person", node.Type)
}

func TestSchemaSurvivesReopenAndCanBeRead(t *testing.T) {
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("g")
	require.NoError(t, err)
	ctx := context.Background()

	want := graphs.Schema{
		NodeTypes: map[string]graphs.NodeTypeSpec{"person": {Required: []string{"name"}}},
	}
	require.NoError(t, g.SetSchema(ctx, want))

	again, err := store.Open("g")
	require.NoError(t, err)
	got, err := again.Schema(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, want, *got)

	err = again.UpsertNode(ctx, &graphs.Node{ID: "x", Type: "person"})
	require.Error(t, err)
}

func TestSchemaDisabledIsPermissive(t *testing.T) {
	store, _ := openDirect(t, graphs.Config{})
	g, err := store.Open("g")
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "anything"}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "e", From: "a", To: "a", Type: "self"}))
	schema, err := g.Schema(ctx)
	require.NoError(t, err)
	require.Nil(t, schema)
}

func TestSchemaRotatesAndVerifies(t *testing.T) {
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = 3
	}
	store, _ := openDirect(t, graphs.Config{Secret: secret, EncryptValues: true})
	g, err := store.Open("g")
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, g.SetSchema(ctx, graphs.Schema{
		NodeTypes: map[string]graphs.NodeTypeSpec{"person": {Required: []string{"name"}}},
	}))
	require.NoError(t, store.VerifyAll(ctx))

	newKey := make([]byte, 32)
	for i := range newKey {
		newKey[i] = 4
	}
	require.NoError(t, store.RotateAll(ctx, newKey))
	require.NoError(t, store.VerifyAll(ctx))

	again, err := store.Open("g")
	require.NoError(t, err)
	schema, err := again.Schema(ctx)
	require.NoError(t, err)
	require.NotNil(t, schema)
}
