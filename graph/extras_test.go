package graph

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestDiffReportsAddedChangedAndRemoved(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	for _, n := range []*Node{node("a", "person"), node("b", "city"), node("gone", "person")} {
		if err := g.UpsertNode(ctx, n); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	if err := g.UpsertEdge(ctx, edge("ab", "a", "b", "lives_in")); err != nil {
		t.Fatalf("edge: %v", err)
	}
	from := c.Now()
	c.Advance(time.Hour)
	if err := g.UpsertNode(ctx, &Node{ID: "a", Type: "person", Content: "changed"}); err != nil {
		t.Fatalf("change: %v", err)
	}
	if err := g.DeleteNode(ctx, "gone"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := g.DeleteEdge(ctx, "ab"); err != nil {
		t.Fatalf("delete edge: %v", err)
	}
	if err := g.UpsertNode(ctx, node("new", "city")); err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := g.UpsertEdge(ctx, edge("new-edge", "a", "new", "visits")); err != nil {
		t.Fatalf("new edge: %v", err)
	}
	to := c.Now().Add(time.Minute)

	diff, err := g.Diff(ctx, from.Add(time.Minute), to)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	kinds := map[string]ChangeKind{}
	for _, change := range diff.Nodes {
		kinds[change.ID] = change.Kind
	}
	if kinds["a"] != ChangeChanged || kinds["gone"] != ChangeRemoved || kinds["new"] != ChangeAdded {
		t.Fatalf("node diff = %+v", diff.Nodes)
	}
	edgeKinds := map[string]ChangeKind{}
	for _, change := range diff.Edges {
		edgeKinds[change.ID] = change.Kind
	}
	if edgeKinds["ab"] != ChangeRemoved || edgeKinds["new-edge"] != ChangeAdded {
		t.Fatalf("edge diff = %+v", diff.Edges)
	}
}

func TestVocabulary(t *testing.T) {
	g, _ := newTestGraph(t)
	ctx := context.Background()
	for _, n := range []*Node{
		{ID: "a", Type: "person", Content: "Ada", Properties: map[string]any{"name": "Ada Lovelace"}},
		{ID: "b", Type: "city", Content: "London"},
		{ID: "c", Type: "person", Content: "Charles"},
	} {
		if err := g.UpsertNode(ctx, n); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	if err := g.UpsertEdge(ctx, edge("e1", "a", "b", "lives_in")); err != nil {
		t.Fatalf("edge: %v", err)
	}
	if err := g.UpsertEdge(ctx, edge("e2", "c", "b", "lives_in")); err != nil {
		t.Fatalf("edge: %v", err)
	}

	labels, err := g.NodeLabels(ctx, nil)
	if err != nil {
		t.Fatalf("labels: %v", err)
	}
	if len(labels) != 3 {
		t.Fatalf("labels = %+v", labels)
	}
	for _, label := range labels {
		if label.ID == "a" && label.Name != "Ada Lovelace" {
			t.Fatalf("label name = %q", label.Name)
		}
	}

	shapes, err := g.EdgeShapes(ctx)
	if err != nil {
		t.Fatalf("shapes: %v", err)
	}
	if len(shapes) != 1 || shapes[0].Type != "lives_in" || shapes[0].Count != 2 {
		t.Fatalf("shapes = %+v", shapes)
	}
	if len(shapes[0].FromTypes) != 1 || shapes[0].FromTypes[0] != "person" {
		t.Fatalf("shape from types = %+v", shapes[0].FromTypes)
	}

	counts, err := g.PropertyCounts(ctx, "name")
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts["Ada Lovelace"] != 1 {
		t.Fatalf("property counts = %+v", counts)
	}

	records, err := g.RecordsWithProperties(ctx, "name", "Ada Lovelace")
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 1 || records[0].ID != "a" || records[0].Kind != "node" {
		t.Fatalf("records = %+v", records)
	}
}

func TestDocumentRoundTrip(t *testing.T) {
	g, _ := newTestGraph(t)
	ctx := context.Background()
	for _, n := range []*Node{node("a", "person"), node("b", "city")} {
		if err := g.UpsertNode(ctx, n); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	if err := g.UpsertEdge(ctx, edge("e", "a", "b", "lives_in")); err != nil {
		t.Fatalf("edge: %v", err)
	}
	var buf bytes.Buffer
	if err := g.ExportJSON(ctx, &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	other := New(g.DB(), WithPrefix("copy"))
	result, err := other.ImportJSON(ctx, &buf)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if result.SuccessCount != 3 || result.FailedCount != 0 {
		t.Fatalf("import = %+v", result)
	}
	stats, err := other.Statistics(ctx)
	if err != nil {
		t.Fatalf("statistics: %v", err)
	}
	if stats.Nodes != 2 || stats.Edges != 1 {
		t.Fatalf("copy statistics = %+v", stats)
	}
}
