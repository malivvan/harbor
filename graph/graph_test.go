package graph

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/malivvan/harbor"
)

// newTestGraph opens a graph on a fresh harbor file with a controllable clock.
func newTestGraph(t *testing.T) (*Graph, *clock) {
	t.Helper()
	c := &clock{at: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	store, err := harbor.Open(context.Background(), filepath.Join(t.TempDir(), "graph.db"), 0o600,
		&harbor.Options{Password: "test", Plaintext: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return New(store.DB(), WithClock(c.Now)), c
}

// clock is a clock the tests move by hand.
type clock struct{ at time.Time }

func (c *clock) Now() time.Time          { return c.at }
func (c *clock) Advance(d time.Duration) { c.at = c.at.Add(d) }
func (c *clock) Set(at time.Time)        { c.at = at }

func node(id, typ string) *Node {
	return &Node{ID: id, Type: typ, Content: "content of " + id}
}

func edge(id, from, to, typ string) *Edge {
	return &Edge{ID: id, From: from, To: to, Type: typ, Weight: 1}
}

func TestNodeUpsertAndNoOp(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	if err := g.UpsertNode(ctx, node("a", "person")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := g.GetNode(ctx, "a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Type != "person" || got.Content != "content of a" {
		t.Fatalf("node = %+v", got)
	}
	// An unchanged write creates no version.
	c.Advance(time.Minute)
	if err := g.UpsertNode(ctx, node("a", "person")); err != nil {
		t.Fatalf("no-op upsert: %v", err)
	}
	history, err := g.NodeHistory(ctx, "a")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("an unchanged write created %d versions", len(history))
	}
}

func TestUpdateCreatesAVersionAndAsOfReads(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	if err := g.UpsertNode(ctx, &Node{ID: "a", Type: "city", Content: "first"}); err != nil {
		t.Fatalf("first: %v", err)
	}
	firstAt := c.Now()
	c.Advance(time.Hour)
	if err := g.UpsertNode(ctx, &Node{ID: "a", Type: "city", Content: "second"}); err != nil {
		t.Fatalf("second: %v", err)
	}
	secondAt := c.Now()

	history, err := g.NodeHistory(ctx, "a")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("want one closed version, got %d", len(history))
	}
	if history[0].Status() != "superseded" || !history[0].ValidTo.Equal(secondAt) {
		t.Fatalf("closed version = %+v", history[0])
	}

	// Before the update, the old content; after it, the new one.
	before := AsOf(ctx, firstAt.Add(time.Minute))
	old, err := g.GetNode(before, "a")
	if err != nil {
		t.Fatalf("as-of get: %v", err)
	}
	if old.Content != "first" {
		t.Fatalf("as-of content = %q, want first", old.Content)
	}
	after := AsOf(ctx, secondAt.Add(time.Minute))
	current, err := g.GetNode(after, "a")
	if err != nil {
		t.Fatalf("as-of get after: %v", err)
	}
	if current.Content != "second" {
		t.Fatalf("as-of content after = %q, want second", current.Content)
	}
	// As of the first write the node existed once, not twice.
	nodes, err := g.GetAllNodes(before, nil)
	if err != nil {
		t.Fatalf("as-of list: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("as-of list = %d nodes, want 1", len(nodes))
	}
}

func TestDeleteNodeCascadesAndRetractsEdges(t *testing.T) {
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
	if err := g.DeleteNode(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := g.GetNode(ctx, "a"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("deleted node reads %v", err)
	}
	edges, err := g.GetEdges(ctx, "b", "in")
	if err != nil {
		t.Fatalf("edges: %v", err)
	}
	if len(edges) != 0 {
		t.Fatalf("the cascade left %d live edges", len(edges))
	}
	edgeHistory, err := g.EdgeHistory(ctx, "e")
	if err != nil {
		t.Fatalf("edge history: %v", err)
	}
	if len(edgeHistory) != 1 || edgeHistory[0].Status() != "retracted" {
		t.Fatalf("edge history = %+v", edgeHistory)
	}
}

func TestBatchAndTraversal(t *testing.T) {
	g, _ := newTestGraph(t)
	ctx := context.Background()
	batch := &BatchOperation{
		NodeUpserts: []*Node{node("a", "step"), node("b", "step"), node("c", "step")},
		EdgeUpserts: []*Edge{
			edge("ab", "a", "b", "next"),
			edge("bc", "b", "c", "next"),
		},
	}
	result, err := g.ExecuteBatch(ctx, batch)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if result.SuccessCount != 5 || result.FailedCount != 0 {
		t.Fatalf("batch result = %+v", result)
	}
	// A batch with a bad edge is partial: the node parts still land.
	result, err = g.UpsertEdgesBatch(ctx, []*Edge{edge("bad", "a", "missing", "next")})
	if err != nil {
		t.Fatalf("bad batch: %v", err)
	}
	if result.SuccessCount != 0 || result.FailedCount != 1 || !errors.Is(result.Errors[0], ErrNodeNotFound) {
		t.Fatalf("bad batch = %+v", result)
	}

	neighbors, err := g.Neighbors(ctx, "a", TraversalOptions{MaxDepth: 2, Direction: "out"})
	if err != nil {
		t.Fatalf("neighbors: %v", err)
	}
	if len(neighbors) != 2 {
		t.Fatalf("neighbors = %+v", neighbors)
	}
	path, err := g.ShortestPath(ctx, "a", "c")
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	if path.Distance != 2 || len(path.Nodes) != 3 {
		t.Fatalf("path = %+v", path)
	}
	if connected, err := g.Connected(ctx, "a", "c", 2); err != nil || !connected {
		t.Fatalf("connected = %v %v", connected, err)
	}
	if connected, err := g.Connected(ctx, "a", "c", 1); err != nil || connected {
		t.Fatalf("connected within one hop = %v %v", connected, err)
	}
}

func TestCountsAndStatistics(t *testing.T) {
	g, _ := newTestGraph(t)
	ctx := context.Background()
	for _, n := range []*Node{node("a", "person"), node("b", "person"), node("c", "city")} {
		if err := g.UpsertNode(ctx, n); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	if err := g.UpsertEdge(ctx, edge("e", "a", "c", "lives_in")); err != nil {
		t.Fatalf("edge: %v", err)
	}
	counts, err := g.NodeTypeCounts(ctx)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts["person"] != 2 || counts["city"] != 1 {
		t.Fatalf("node type counts = %+v", counts)
	}
	stats, err := g.Statistics(ctx)
	if err != nil {
		t.Fatalf("statistics: %v", err)
	}
	if stats.Nodes != 3 || stats.Edges != 1 || stats.EdgeTypes["lives_in"] != 1 {
		t.Fatalf("statistics = %+v", stats)
	}
	filtered, err := g.GetAllNodes(ctx, &Filter{NodeTypes: []string{"person"}, Limit: 1})
	if err != nil {
		t.Fatalf("filtered: %v", err)
	}
	if len(filtered) != 1 || filtered[0].Type != "person" {
		t.Fatalf("filtered = %+v", filtered)
	}
}

func TestPurgeAndRetraction(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	if err := g.UpsertNode(ctx, node("a", "person")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	c.Advance(time.Hour)
	if err := g.DeleteNode(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	deletedAt := c.Now()

	dry, err := g.Purge(ctx, deletedAt.Add(time.Minute), true)
	if err != nil {
		t.Fatalf("dry purge: %v", err)
	}
	if dry.Nodes != 1 || !dry.DryRun {
		t.Fatalf("dry purge = %+v", dry)
	}
	if history, _ := g.NodeHistory(ctx, "a"); len(history) != 1 {
		t.Fatalf("a dry purge removed the history")
	}
	report, err := g.Purge(ctx, deletedAt.Add(time.Minute), false)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if report.Nodes != 1 {
		t.Fatalf("purge = %+v", report)
	}
	if history, _ := g.NodeHistory(ctx, "a"); len(history) != 0 {
		t.Fatalf("the purge left %d versions", len(history))
	}
	if _, err := g.Purge(ctx, time.Time{}, true); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a purge without a cutoff = %v", err)
	}
}

func TestRetractAt(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	if err := g.UpsertNode(ctx, node("a", "person")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	c.Advance(time.Hour)
	at := c.Now()
	if err := g.RetractNodeAt(ctx, "a", at); err != nil {
		t.Fatalf("retract: %v", err)
	}
	history, err := g.NodeHistory(ctx, "a")
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 || history[0].Status() != "retracted" || !history[0].RetractedAt.Equal(at) {
		t.Fatalf("history = %+v", history)
	}
	// Before the retraction it was still there.
	before := AsOf(ctx, at.Add(-time.Minute))
	if _, err := g.GetNode(before, "a"); err != nil {
		t.Fatalf("as-of before retraction: %v", err)
	}
}

func TestWriteUnderAsOfIsRefused(t *testing.T) {
	g, _ := newTestGraph(t)
	ctx := AsOf(context.Background(), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := g.UpsertNode(ctx, node("a", "person")); !errors.Is(err, ErrAsOfWrite) {
		t.Fatalf("as-of write = %v, want ErrAsOfWrite", err)
	}
}

func TestPrefixesShareOneStoreWithoutMixing(t *testing.T) {
	g, _ := newTestGraph(t)
	ctx := context.Background()
	one := New(g.DB(), WithPrefix("one"))
	two := New(g.DB(), WithPrefix("two"))
	if err := one.UpsertNode(ctx, node("a", "person")); err != nil {
		t.Fatalf("one: %v", err)
	}
	if _, err := two.GetNode(ctx, "a"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("the second graph saw the first graph's node: %v", err)
	}
	if _, err := one.GetNode(ctx, "a"); err != nil {
		t.Fatalf("one lost its node: %v", err)
	}
}

func TestEndEdgeClosesWithoutRetracting(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := g.UpsertNode(ctx, node(id, "person")); err != nil {
			t.Fatalf("upsert node %s: %v", id, err)
		}
	}
	if err := g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")); err != nil {
		t.Fatalf("upsert edge: %v", err)
	}
	c.Advance(time.Hour)
	at := c.Now()
	ctx = WithInvalidation(ctx, Invalidation{Reason: "superseded", SupersededBy: "e2", Producer: "test"})
	if err := g.EndEdge(ctx, "e1", at); err != nil {
		t.Fatalf("end edge: %v", err)
	}
	// The live edge is gone...
	if edges, err := g.GetEdges(ctx, "a", "out"); err != nil || len(edges) != 0 {
		t.Fatalf("live edges after EndEdge = %d, err %v", len(edges), err)
	}
	// ...and it was not retracted: its version says it was superseded, at the
	// instant the caller named.
	history, err := g.EdgeHistory(ctx, "e1")
	if err != nil || len(history) != 1 {
		t.Fatalf("edge history = %+v, err %v", history, err)
	}
	if history[0].Status() != "superseded" || !history[0].RetractedAt.IsZero() {
		t.Fatalf("ended version = %+v (status %s)", history[0], history[0].Status())
	}
	if !history[0].ValidTo.Equal(at) || history[0].Reason != "superseded" || history[0].SupersededBy != "e2" {
		t.Fatalf("ended version = %+v", history[0])
	}
	// As of before the closure it is still in force.
	before := AsOf(ctx, at.Add(-time.Minute))
	if edges, err := g.GetEdges(before, "a", "out"); err != nil || len(edges) != 1 || edges[0].ID != "e1" {
		t.Fatalf("as-of edges before EndEdge = %d, err %v", len(edges), err)
	}
	if err := g.EndEdge(ctx, "missing", at); !errors.Is(err, ErrEdgeNotFound) {
		t.Fatalf("EndEdge(unknown id) = %v, want ErrEdgeNotFound", err)
	}
}

func TestAllVersionsAndCallerCreatedAt(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	created := c.Now().Add(-24 * time.Hour)
	first := node("a", "person")
	first.CreatedAt = created
	if err := g.UpsertNode(ctx, first); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if !first.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %s, want the caller's %s", first.CreatedAt, created)
	}
	if err := g.UpsertNode(ctx, node("b", "person")); err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	c.Advance(time.Hour)
	changed := node("a", "person")
	changed.Content = "changed"
	if err := g.UpsertNode(WithInvalidation(ctx, Invalidation{Reason: "superseded"}), changed); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	if err := g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")); err != nil {
		t.Fatalf("upsert edge: %v", err)
	}
	c.Advance(time.Hour)
	if err := g.DeleteEdge(ctx, "e1"); err != nil {
		t.Fatalf("delete edge: %v", err)
	}

	// The whole history, oldest first, with each closure's reason.
	nodes, err := g.AllNodeVersions(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("AllNodeVersions = %d versions, err %v", len(nodes), err)
	}
	if nodes[0].ID != "a" || nodes[0].Content != "content of a" || nodes[0].Status() != "superseded" ||
		nodes[0].Reason != "superseded" {
		t.Fatalf("node version = %+v (status %s)", nodes[0], nodes[0].Status())
	}
	edges, err := g.AllEdgeVersions(ctx)
	if err != nil || len(edges) != 1 {
		t.Fatalf("AllEdgeVersions = %d versions, err %v", len(edges), err)
	}
	if edges[0].ID != "e1" || edges[0].From != "a" || edges[0].To != "b" || edges[0].Status() != "retracted" {
		t.Fatalf("edge version = %+v (status %s)", edges[0], edges[0].Status())
	}

	// The rewrite kept the instant the record first existed.
	got, err := g.GetNode(ctx, "a")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if !got.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt after rewrite = %s, want %s", got.CreatedAt, created)
	}
}

func TestRetractNodeAtCascadesToEdges(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := g.UpsertNode(ctx, node(id, "person")); err != nil {
			t.Fatalf("upsert node %s: %v", id, err)
		}
	}
	if err := g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")); err != nil {
		t.Fatalf("upsert edge: %v", err)
	}
	c.Advance(time.Hour)
	at := c.Now()
	if err := g.RetractNodeAt(ctx, "a", at); err != nil {
		t.Fatalf("retract node: %v", err)
	}
	// The edge went with the node, so the far end no longer reads it either.
	if edges, err := g.GetEdges(ctx, "b", "both"); err != nil || len(edges) != 0 {
		t.Fatalf("edges at the far end = %d, err %v", len(edges), err)
	}
	history, err := g.EdgeHistory(ctx, "e1")
	if err != nil || len(history) != 1 || history[0].Status() != "retracted" {
		t.Fatalf("edge history = %+v, err %v", history, err)
	}
	before := AsOf(ctx, at.Add(-time.Minute))
	if edges, err := g.GetEdges(before, "b", "both"); err != nil || len(edges) != 1 {
		t.Fatalf("as-of edges before the retraction = %d, err %v", len(edges), err)
	}
}
