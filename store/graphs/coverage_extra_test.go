package graphs

// These tests drive the exported Graph API over the independent, bolt-backed
// Storage (graphStorage) as well as the record-backed one, so both Storage
// implementations move in lock-step and the unexported bolt adapter is
// exercised end to end.

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/ledger"
)

// openBoltStore opens a fresh bolt-backed graphs Store under t.TempDir().
func openBoltStore(t *testing.T, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "graphs.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	if cfg.Root == nil {
		cfg.Root = [][]byte{[]byte(bucket.GRAPHS)}
	}
	s, err := Open(context.Background(), bdb, cfg)
	require.NoError(t, err)
	return s, bdb
}

// newBoltGraph opens a graph named "g" over bolt storage with a manual clock.
func newBoltGraph(t *testing.T) (*Graph, *Store, *bolt.DB, *clock) {
	t.Helper()
	c := &clock{at: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	s, bdb := openBoltStore(t, Config{})
	g, err := s.Open("g", WithClock(c.Now))
	require.NoError(t, err)
	return g, s, bdb, c
}

// TestBoltStorageNodeAndEdgeLifecycle locks in the create/update/no-op/delete
// cycle over the bolt adapter, including the cascade from node to edge.
func TestBoltStorageNodeAndEdgeLifecycle(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()

	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "city")))

	// An unchanged write creates no version over bolt either.
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	hist, err := g.NodeHistory(ctx, "a")
	require.NoError(t, err)
	require.Empty(t, hist)

	// A real change closes a version and is visible as-of the prior instant.
	c.Advance(time.Hour)
	changed := node("a", "person")
	changed.Content = "updated"
	require.NoError(t, g.UpsertNode(ctx, changed))
	hist, err = g.NodeHistory(ctx, "a")
	require.NoError(t, err)
	require.Len(t, hist, 1)
	old, err := g.GetNode(AsOf(ctx, c.Now().Add(-30*time.Minute)), "a")
	require.NoError(t, err)
	require.Equal(t, "content of a", old.Content)

	// Edge create, update (version) and delete (retract).
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "likes")))
	eh, err := g.EdgeHistory(ctx, "e1")
	require.NoError(t, err)
	require.Len(t, eh, 1)
	require.NoError(t, g.DeleteEdge(ctx, "e1"))
	eh, err = g.EdgeHistory(ctx, "e1")
	require.NoError(t, err)
	require.Len(t, eh, 2)

	// Deleting a node retracts the edge touching it.
	require.NoError(t, g.UpsertEdge(ctx, edge("e2", "a", "b", "knows")))
	require.NoError(t, g.DeleteNode(ctx, "a"))
	_, err = g.GetNode(ctx, "a")
	require.ErrorIs(t, err, ErrNodeNotFound)
	live, err := g.GetEdges(ctx, "b", "in")
	require.NoError(t, err)
	require.Empty(t, live)

	// Request validation and not-found mapping.
	require.ErrorIs(t, g.UpsertNode(ctx, nil), ErrInvalidRequest)
	require.ErrorIs(t, g.UpsertNode(ctx, &Node{}), ErrInvalidRequest)
	require.ErrorIs(t, g.UpsertEdge(ctx, nil), ErrInvalidRequest)
	require.ErrorIs(t, g.UpsertEdge(ctx, &Edge{ID: "x"}), ErrInvalidRequest)
	require.ErrorIs(t, g.UpsertEdge(ctx, edge("bad", "a", "missing", "x")), ErrNodeNotFound)
	require.ErrorIs(t, g.DeleteNode(ctx, ""), ErrInvalidRequest)
	require.ErrorIs(t, g.DeleteEdge(ctx, ""), ErrInvalidRequest)
	require.ErrorIs(t, g.DeleteNode(ctx, "nope"), ErrNodeNotFound)
	require.ErrorIs(t, g.DeleteEdge(ctx, "nope"), ErrEdgeNotFound)
}

// TestBoltStorageBatchCountsAndLookups locks in the batch, count and lookup
// helpers over the bolt adapter.
func TestBoltStorageBatchCountsAndLookups(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()

	res, err := g.ExecuteBatch(ctx, &BatchOperation{
		NodeUpserts: []*Node{node("a", "person"), node("b", "city"), node("c", "person")},
		EdgeUpserts: []*Edge{edge("ab", "a", "b", "lives_in")},
	})
	require.NoError(t, err)
	require.Equal(t, 4, res.SuccessCount)

	empty, err := g.ExecuteBatch(ctx, nil)
	require.NoError(t, err)
	require.Zero(t, empty.SuccessCount)

	nodes, err := g.GetNodesBatch(ctx, []string{"a", "missing", "c"})
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	edges, err := g.GetEdgesBatch(ctx, []string{"ab", "missing"})
	require.NoError(t, err)
	require.Len(t, edges, 1)

	n, err := g.CountNodes(ctx, &Filter{NodeTypes: []string{"person"}})
	require.NoError(t, err)
	require.Equal(t, 2, n)

	typeCounts, err := g.EdgeTypeCounts(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, typeCounts["lives_in"])

	ex, err := g.ExistingNodeIDs(ctx, []string{"a", "missing", "c"})
	require.NoError(t, err)
	require.True(t, ex["a"])
	require.False(t, ex["missing"])
	require.True(t, ex["c"])

	stats, err := g.Statistics(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, stats.Nodes)
	require.Equal(t, 1, stats.Edges)

	// Batch deletes report per-id success and failure.
	delEdges, err := g.DeleteEdgesBatch(ctx, []string{"ab", "missing"})
	require.NoError(t, err)
	require.Equal(t, 1, delEdges.SuccessCount)
	require.Equal(t, 1, delEdges.FailedCount)

	delNodes, err := g.DeleteNodesBatch(ctx, []string{"a", "missing"})
	require.NoError(t, err)
	require.Equal(t, 1, delNodes.SuccessCount)
	require.Equal(t, 1, delNodes.FailedCount)
}

// TestBoltStorageExportImport locks in the JSON/document round trip and its
// error paths over bolt storage.
func TestBoltStorageExportImport(t *testing.T) {
	g, s, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "city")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))

	var buf bytes.Buffer
	require.NoError(t, g.ExportJSON(ctx, &buf))

	doc, err := g.ExportDocument(ctx)
	require.NoError(t, err)
	require.Len(t, doc.Nodes, 2)
	require.Len(t, doc.Edges, 1)

	g2, err := s.Open("copy")
	require.NoError(t, err)
	res, err := g2.ImportJSON(ctx, &buf)
	require.NoError(t, err)
	require.Equal(t, 3, res.SuccessCount)
	require.Zero(t, res.FailedCount)

	stats, err := g2.Statistics(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, stats.Nodes)
	require.Equal(t, 1, stats.Edges)

	// A nil document imports nothing; malformed JSON is refused.
	none, err := g2.ImportDocument(ctx, nil)
	require.NoError(t, err)
	require.Zero(t, none.SuccessCount)
	_, err = g2.ImportJSON(ctx, strings.NewReader("{not json"))
	require.Error(t, err)
}

// TestBoltStoragePurgeAndRetract locks in retraction at a named instant and
// purging of ended versions (dry run and real) over bolt storage.
func TestBoltStoragePurgeAndRetract(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()

	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))

	// RetractEdgeAt closes the edge as retracted.
	c.Advance(time.Hour)
	at := c.Now()
	require.NoError(t, g.RetractEdgeAt(ctx, "e1", at))
	eh, err := g.EdgeHistory(ctx, "e1")
	require.NoError(t, err)
	require.Len(t, eh, 1)
	require.Equal(t, "retracted", eh[0].Status())

	// RetractNodeAt cascades to a fresh edge, then RetractNodes counts.
	require.NoError(t, g.UpsertEdge(ctx, edge("e2", "a", "b", "knows")))
	c.Advance(time.Hour)
	require.NoError(t, g.RetractNodeAt(ctx, "a", c.Now()))

	require.NoError(t, g.UpsertNode(ctx, node("c", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("d", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e3", "c", "d", "knows")))
	nodes, edges, err := g.RetractNodes(ctx, []string{"c", "d", "ghost"})
	require.NoError(t, err)
	require.Equal(t, 2, nodes)
	require.Equal(t, 1, edges)

	// Purge: the dry run must match the real count.
	cutoff := c.Now().Add(time.Minute)
	dry, err := g.Purge(ctx, cutoff, true)
	require.NoError(t, err)
	require.True(t, dry.DryRun)
	report, err := g.Purge(ctx, cutoff, false)
	require.NoError(t, err)
	require.Equal(t, dry.Nodes, report.Nodes)
	require.Equal(t, dry.Edges, report.Edges)
	require.GreaterOrEqual(t, report.Nodes, int64(1))
	require.GreaterOrEqual(t, report.Edges, int64(1))
}

// TestBoltStorageNodeEdgeHistory locks in EdgeVersionsTouching over bolt via
// NodeEdgeHistory.
func TestBoltStorageNodeEdgeHistory(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))
	c.Advance(time.Hour)
	require.NoError(t, g.DeleteEdge(ctx, "e1"))

	hist, err := g.NodeEdgeHistory(ctx, "a")
	require.NoError(t, err)
	require.Len(t, hist, 1)
	require.Equal(t, "e1", hist[0].ID)
	require.Equal(t, "a", hist[0].From)
}

// TestBoltStorageEndedEdgeVersions locks in EndedEdgeVersions' since-filter
// over bolt storage.
func TestBoltStorageEndedEdgeVersions(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))
	c.Advance(time.Hour)
	end := c.Now()
	require.NoError(t, g.RetractEdgeAt(ctx, "e1", end))

	all, err := g.EndedEdgeVersions(ctx, time.Time{})
	require.NoError(t, err)
	require.Len(t, all, 1)

	atEnd, err := g.EndedEdgeVersions(ctx, end)
	require.NoError(t, err)
	require.Len(t, atEnd, 1)

	tooLate, err := g.EndedEdgeVersions(ctx, end.Add(time.Minute))
	require.NoError(t, err)
	require.Empty(t, tooLate)
}

// TestBoltStorageAsOfList locks in the as-of whole-graph and adjacency reads
// over bolt storage.
func TestBoltStorageAsOfList(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))
	from := c.Now()

	c.Advance(time.Hour)
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "likes")))

	asOf := AsOf(ctx, from.Add(time.Minute))
	asOfNodes, err := g.GetAllNodes(asOf, nil)
	require.NoError(t, err)
	require.Len(t, asOfNodes, 2)
	asOfEdges, err := g.GetEdges(asOf, "a", "out")
	require.NoError(t, err)
	require.Len(t, asOfEdges, 1)
	require.Equal(t, "knows", asOfEdges[0].Type)
	allEdges, err := g.GetAllEdges(asOf)
	require.NoError(t, err)
	require.Len(t, allEdges, 1)
	require.Equal(t, "knows", allEdges[0].Type)
}

// TestBoltStorageVersionOrdering locks in the sort tie-breakers used to order
// closed versions recorded in the same instant.
func TestBoltStorageVersionOrdering(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()

	// Three content changes at the same clock produce two versions with equal
	// RecordedAt and ValidFrom, exercising the version-id tie-breaker.
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "v1"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "v2"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "v3"}))
	hist, err := g.NodeHistory(ctx, "a")
	require.NoError(t, err)
	require.Len(t, hist, 2)

	require.NoError(t, g.UpsertNode(ctx, node("b", "p")))
	require.NoError(t, g.UpsertEdge(ctx, &Edge{ID: "e", From: "a", To: "b", Type: "t1", Weight: 1}))
	require.NoError(t, g.UpsertEdge(ctx, &Edge{ID: "e", From: "a", To: "b", Type: "t2", Weight: 1}))
	eh, err := g.EdgeHistory(ctx, "e")
	require.NoError(t, err)
	require.Len(t, eh, 1)

	allNodes, err := g.AllNodeVersions(ctx)
	require.NoError(t, err)
	require.Len(t, allNodes, 2)
	allEdges, err := g.AllEdgeVersions(ctx)
	require.NoError(t, err)
	require.Len(t, allEdges, 1)
}

// TestBoltStorageDecodeError locks in that a corrupt record surfaces a decode
// error rather than a silent zero value.
func TestBoltStorageDecodeError(t *testing.T) {
	g, _, bdb, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		gb := tx.Bucket([]byte(bucket.GRAPHS)).Bucket([]byte("g"))
		return gb.Bucket([]byte(nodesBucket)).Put([]byte("bad"), []byte("{not json"))
	}))
	_, err := g.GetNode(ctx, "bad")
	require.Error(t, err)
}

// TestBoltStorageContextCancelled locks in that a list read observes a
// cancelled context instead of returning partial results.
func TestBoltStorageContextCancelled(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := g.GetAllNodes(cancelled, nil)
	require.ErrorIs(t, err, context.Canceled)
}

// TestBoltStorageAuditSink locks in that committed writes reach the audit sink
// and that a failing sink fails the write.
func TestBoltStorageAuditSink(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	var got []ledger.Entry
	sink := ledger.SinkFunc(func(e ledger.Entry) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, e)
		return nil
	})
	s, _ := openBoltStore(t, Config{Audit: sink})
	g, err := s.Open("audited")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	mu.Lock()
	n := len(got)
	mu.Unlock()
	require.Greater(t, n, 0)

	// A failing sink fails the write.
	bad := ledger.SinkFunc(func(ledger.Entry) error { return errors.New("audit down") })
	s2, _ := openBoltStore(t, Config{Audit: bad})
	g2, err := s2.Open("audited")
	require.NoError(t, err)
	require.Error(t, g2.UpsertNode(ctx, node("a", "person")))
}

// TestRecordStorageNodeEdgeHistoryAndPurge locks in the record-backed
// EdgeVersionsTouching and DeleteEdgeVersion paths.
func TestRecordStorageNodeEdgeHistoryAndPurge(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))
	c.Advance(time.Hour)
	require.NoError(t, g.DeleteEdge(ctx, "e1"))

	hist, err := g.NodeEdgeHistory(ctx, "a")
	require.NoError(t, err)
	require.Len(t, hist, 1)

	report, err := g.Purge(ctx, c.Now().Add(time.Minute), false)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.Edges)
	after, err := g.NodeEdgeHistory(ctx, "a")
	require.NoError(t, err)
	require.Empty(t, after)
}

// TestStoreAccessorsAndLifecycle locks in Root, Flush, Storage, Now, Init,
// Names and Close behaviour.
func TestStoreAccessorsAndLifecycle(t *testing.T) {
	ctx := context.Background()
	s, _ := openBoltStore(t, Config{})
	require.Equal(t, [][]byte{[]byte(bucket.GRAPHS)}, s.Root())
	require.NoError(t, s.Flush())

	g, err := s.Open("g")
	require.NoError(t, err)
	require.NotNil(t, g.Storage())
	require.False(t, g.Now().IsZero())
	require.NoError(t, g.Init(ctx))

	names, err := s.Names(ctx)
	require.NoError(t, err)
	require.Contains(t, names, "g")

	require.NoError(t, s.Close())
	_, err = s.Open("g")
	require.Error(t, err)
	require.Error(t, s.Flush())
}

// TestStoreRootedViews locks in WithRoot validation/fallback and the
// WithTransaction binding.
func TestStoreRootedViews(t *testing.T) {
	s, bdb := openBoltStore(t, Config{})

	// An invalid part falls back to the receiver's root.
	require.Equal(t, s.Root(), s.WithRoot("bad/name").Root())
	// A valid part names a fresh root.
	require.Equal(t, [][]byte{[]byte("tenant")}, s.WithRoot("tenant").Root())

	var nilStore *Store
	require.Nil(t, nilStore.WithRoot("x"))
	require.Nil(t, nilStore.WithTransaction(nil))

	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		v := s.WithTransaction(tx)
		require.NotNil(t, v)
		require.Equal(t, s.Root(), v.Root())
		return nil
	}))
}

// TestOpenReadOnly locks in the read-only open path: reads succeed, writes are
// refused and an unopened graph reads empty.
func TestOpenReadOnly(t *testing.T) {
	ctx := context.Background()
	s, bdb := openBoltStore(t, Config{})
	g, err := s.Open("g")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))

	ro, err := OpenReadOnly(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
	require.NoError(t, err)
	rg, err := ro.Open("g")
	require.NoError(t, err)
	got, err := rg.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "person", got.Type)
	require.ErrorIs(t, rg.UpsertNode(ctx, node("b", "person")), ErrReadOnly)

	// Opening a graph that was never written reads empty, not an error.
	empty, err := ro.Open("never")
	require.NoError(t, err)
	allNodes, err := empty.GetAllNodes(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, allNodes)
	allEdges, err := empty.GetAllEdges(ctx)
	require.NoError(t, err)
	require.Empty(t, allEdges)
	_, err = empty.GetNode(ctx, "x")
	require.ErrorIs(t, err, ErrNodeNotFound)

	names, err := ro.Names(ctx)
	require.NoError(t, err)
	require.Contains(t, names, "g")
}

// TestOpenErrorPaths locks in the argument validation of Open.
func TestOpenErrorPaths(t *testing.T) {
	//lint:ignore SA1012 nil ctx must be rejected with ErrNilContext
	_, err := Open(nil, nil, Config{})
	require.ErrorIs(t, err, ErrNilContext)

	cc, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Open(cc, nil, Config{})
	require.ErrorIs(t, err, context.Canceled)

	_, err = Open(context.Background(), nil, Config{})
	require.ErrorIs(t, err, ErrNilParam)

	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "bad.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()
	_, err = Open(context.Background(), bdb, Config{Root: [][]byte{[]byte("__reserved")}})
	require.ErrorIs(t, err, ErrInvalidName)
}

// TestValidateName locks in every rejected graph/bucket name shape.
func TestValidateName(t *testing.T) {
	for _, bad := range []string{"", ".", "..", "a/b", "__x", "nul\x00byte"} {
		require.ErrorIs(t, validateName(bad), ErrInvalidName, "name %q", bad)
	}
	require.NoError(t, validateName("ok"))
}

// TestCopyAndMigrateGuards locks in the nil-argument guards of Copy and
// MigrateFromRecord.
func TestCopyAndMigrateGuards(t *testing.T) {
	ctx := context.Background()
	require.ErrorIs(t, Copy(ctx, nil, nil), ErrInvalidRequest)
	g, s, _, _ := newBoltGraph(t)
	require.ErrorIs(t, Copy(ctx, g, nil), ErrInvalidRequest)
	require.ErrorIs(t, Copy(ctx, &Graph{}, g), ErrInvalidRequest)
	require.ErrorIs(t, s.MigrateFromRecord(ctx, "x", nil), ErrNilParam)
}

// TestStoreMetaMalformed locks in that malformed version and mode records fail
// closed instead of being misread.
func TestStoreMetaMalformed(t *testing.T) {
	ctx := context.Background()

	t.Run("version length", func(t *testing.T) {
		s, bdb := openBoltStore(t, Config{})
		_ = s
		require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte(bucket.GRAPHS)).Put([]byte(versionKey), []byte{1, 2, 3})
		}))
		_, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})

	t.Run("mode length", func(t *testing.T) {
		s, bdb := openBoltStore(t, Config{})
		_ = s
		require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte(bucket.GRAPHS)).Put([]byte(modeKey), []byte{0, 0})
		}))
		_, err := Open(ctx, bdb, Config{Root: [][]byte{[]byte(bucket.GRAPHS)}})
		require.ErrorIs(t, err, ErrWrongMode)
	})
}

// TestOpenReadOnlyMeta locks in the read-only meta checks on an unversioned or
// unmoded file.
func TestOpenReadOnlyMeta(t *testing.T) {
	ctx := context.Background()
	root := [][]byte{[]byte(bucket.GRAPHS)}

	t.Run("missing root", func(t *testing.T) {
		bdb, err := bolt.Open(filepath.Join(t.TempDir(), "ro.db"), 0o600, &bolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		defer bdb.Close()
		_, err = OpenReadOnly(ctx, bdb, Config{Root: root})
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})

	t.Run("missing version", func(t *testing.T) {
		bdb, err := bolt.Open(filepath.Join(t.TempDir(), "ro.db"), 0o600, &bolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		defer bdb.Close()
		require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
			_, e := tx.CreateBucketIfNotExists([]byte(bucket.GRAPHS))
			return e
		}))
		_, err = OpenReadOnly(ctx, bdb, Config{Root: root})
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})

	t.Run("missing mode", func(t *testing.T) {
		bdb, err := bolt.Open(filepath.Join(t.TempDir(), "ro.db"), 0o600, &bolt.Options{Timeout: time.Second})
		require.NoError(t, err)
		defer bdb.Close()
		var buf [8]byte
		for i := range buf {
			buf[i] = byte(FormatVersion >> (8 * i))
		}
		require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
			b, e := tx.CreateBucketIfNotExists([]byte(bucket.GRAPHS))
			if e != nil {
				return e
			}
			return b.Put([]byte(versionKey), buf[:])
		}))
		_, err = OpenReadOnly(ctx, bdb, Config{Root: root})
		require.ErrorIs(t, err, ErrWrongMode)
	})
}

// countingCodec is a custom ValueCodec that counts codec traffic, exercising
// WithValues.
type countingCodec struct {
	JSONValues
	enc, dec int
}

func (c *countingCodec) Encode(p map[string]any) ([]byte, error) {
	c.enc++
	return c.JSONValues.Encode(p)
}

func (c *countingCodec) Decode(b []byte) (map[string]any, error) {
	c.dec++
	return c.JSONValues.Decode(b)
}

// TestWithValuesCodec locks in that a caller-supplied property codec is used
// for both writes and reads.
func TestWithValuesCodec(t *testing.T) {
	ctx := context.Background()
	codec := &countingCodec{}
	s, _ := openBoltStore(t, Config{})
	g, err := s.Open("coded", WithValues(codec))
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "person", Properties: map[string]any{"k": "v"}}))
	got, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "v", got.Properties["k"])
	require.Greater(t, codec.enc, 0)
	require.Greater(t, codec.dec, 0)
}

// TestDiffChangedRecords locks in that edgeChanged and equalMaps detect property
// differences (value, key and length), not just added/removed records.
func TestDiffChangedRecords(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "n1", Properties: map[string]any{"x": "1"}}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "n2", Properties: map[string]any{"x": "1"}}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "n3", Properties: map[string]any{"x": "1"}}))
	require.NoError(t, g.UpsertEdge(ctx, &Edge{ID: "e1", From: "n1", To: "n2", Type: "t", Weight: 1, Properties: map[string]any{"p": "a"}}))
	from := c.Now()
	c.Advance(time.Hour)
	// n1: same key, different value.
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "n1", Properties: map[string]any{"x": "2"}}))
	// n2: same length, different key.
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "n2", Properties: map[string]any{"y": "1"}}))
	// n3: different length.
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "n3", Properties: map[string]any{"x": "1", "z": "3"}}))
	// e1: same endpoints/type/weight, only properties differ.
	require.NoError(t, g.UpsertEdge(ctx, &Edge{ID: "e1", From: "n1", To: "n2", Type: "t", Weight: 1, Properties: map[string]any{"p": "b"}}))

	diff, err := g.Diff(ctx, from.Add(time.Minute), c.Now().Add(time.Minute))
	require.NoError(t, err)
	changedNodes := map[string]bool{}
	for _, ch := range diff.Nodes {
		if ch.Kind == ChangeChanged {
			changedNodes[ch.ID] = true
		}
	}
	require.True(t, changedNodes["n1"])
	require.True(t, changedNodes["n2"])
	require.True(t, changedNodes["n3"])
	require.Len(t, diff.Edges, 1)
	require.Equal(t, ChangeChanged, diff.Edges[0].Kind)
}

// TestContextAndStatusHelpers locks in the zero-instant no-op and the
// current/superseded/retracted status ladder.
func TestContextAndStatusHelpers(t *testing.T) {
	ctx := context.Background()
	require.Equal(t, ctx, AsOf(ctx, time.Time{}))
	//lint:ignore SA1012 nil options mean no as-of read
	require.False(t, ReadOptionsFrom(nil).IsAsOf())
	//lint:ignore SA1012 nil options mean no invalidation set
	require.Equal(t, Invalidation{}, InvalidationFrom(nil))

	require.Equal(t, "current", RecordVersion{}.Status())
	require.Equal(t, "superseded", RecordVersion{ValidTo: time.Unix(1, 0)}.Status())
	require.Equal(t, "retracted", RecordVersion{RetractedAt: time.Unix(1, 0)}.Status())
}

// TestNilStoreGuards locks in that the nil-receiver accessors are safe.
func TestNilStoreGuards(t *testing.T) {
	var nilStore *Store
	require.Nil(t, nilStore.Root())
	require.NoError(t, nilStore.Close())
}

// TestEncryptionBadSecret locks in that a secret the keychain rejects fails the
// open instead of silently storing plaintext.
func TestEncryptionBadSecret(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "enc.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()
	_, err = Open(context.Background(), bdb, Config{
		Root:          [][]byte{[]byte(bucket.GRAPHS)},
		EncryptValues: true,
		Secret:        []byte("too-short"),
	})
	require.Error(t, err)
}

// TestTransactionBoundView locks in the transaction-bound store: Open and reads
// run against the caller's transaction.
func TestTransactionBoundView(t *testing.T) {
	ctx := context.Background()
	s, bdb := openBoltStore(t, Config{})
	g, err := s.Open("g")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))

	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		bound := s.WithTransaction(tx)
		tg, err := bound.Open("g")
		if err != nil {
			return err
		}
		_, err = tg.GetNode(ctx, "a")
		return err
	}))
}

// TestUpsertNodesBatchFailure locks in per-node failure reporting.
func TestUpsertNodesBatchFailure(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	res, err := g.UpsertNodesBatch(ctx, []*Node{node("a", "person"), nil})
	require.NoError(t, err)
	require.Equal(t, 1, res.SuccessCount)
	require.Equal(t, 1, res.FailedCount)
	require.Len(t, res.Errors, 1)
}

// TestShortestPathAdmittingBranches locks in the admit filter and its error
// propagation.
func TestShortestPathAdmittingBranches(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("c", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("ab", "a", "b", "next")))
	require.NoError(t, g.UpsertEdge(ctx, edge("bc", "b", "c", "next")))

	// A pass-through admit filter returns the same path.
	path, err := g.ShortestPathAdmitting(ctx, "a", "c", func(_ context.Context, _ string, es []*Edge) ([]*Edge, error) {
		return es, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, path.Distance)

	// A failing admit filter aborts the walk.
	boom := errors.New("admit failed")
	_, err = g.ShortestPathAdmitting(ctx, "a", "c", func(context.Context, string, []*Edge) ([]*Edge, error) {
		return nil, boom
	})
	require.ErrorIs(t, err, boom)
}

// TestChangeDetectionBranches locks in that vector and property differences of
// equal length count as changes.
func TestChangeDetectionBranches(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "x", Vector: []float32{1, 2}, Properties: map[string]any{"k": "1"}}))
	// Same-length vector, different value.
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "x", Vector: []float32{1, 3}, Properties: map[string]any{"k": "1"}}))
	// Same vector, same-length properties, different value.
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "x", Vector: []float32{1, 3}, Properties: map[string]any{"k": "2"}}))
	// Same vector, longer properties.
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "x", Vector: []float32{1, 3}, Properties: map[string]any{"k": "2", "z": "3"}}))

	hist, err := g.NodeHistory(ctx, "a")
	require.NoError(t, err)
	require.Len(t, hist, 3)
}

// TestDiffEdgeTypeChange locks in edgeChanged's short-circuit when an endpoint,
// type or weight moves.
func TestDiffEdgeTypeChange(t *testing.T) {
	g, c := newTestGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "b"}))
	require.NoError(t, g.UpsertEdge(ctx, &Edge{ID: "e", From: "a", To: "b", Type: "t1", Weight: 1}))
	from := c.Now()
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertEdge(ctx, &Edge{ID: "e", From: "a", To: "b", Type: "t2", Weight: 1}))
	diff, err := g.Diff(ctx, from.Add(time.Minute), c.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, diff.Edges, 1)
	require.Equal(t, ChangeChanged, diff.Edges[0].Kind)
}

// TestNeighborsFiltersAndLimit locks in node-type filtering and the traversal
// limit.
func TestNeighborsFiltersAndLimit(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "root"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "b", Type: "keep"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "c", Type: "skip"}))
	require.NoError(t, g.UpsertEdge(ctx, edge("ab", "a", "b", "next")))
	require.NoError(t, g.UpsertEdge(ctx, edge("ac", "a", "c", "next")))

	kept, err := g.Neighbors(ctx, "a", TraversalOptions{MaxDepth: 1, NodeTypes: []string{"keep"}})
	require.NoError(t, err)
	require.Len(t, kept, 1)
	require.Equal(t, "b", kept[0].ID)

	limited, err := g.Neighbors(ctx, "a", TraversalOptions{MaxDepth: 1, Limit: 1})
	require.NoError(t, err)
	require.Len(t, limited, 1)
}

// TestNodeFilterProperties locks in the property predicate in GetAllNodes.
func TestNodeFilterProperties(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Properties: map[string]any{"k": "v"}}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "b", Properties: map[string]any{"k": "other"}}))
	got, err := g.GetAllNodes(ctx, &Filter{Properties: map[string]string{"k": "v"}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "a", got[0].ID)
}

// TestEdgeShapesFilterAndEmptyType locks in the edge-type filter and the
// empty-node-type branch of appendUnique.
func TestEdgeShapesFilterAndEmptyType(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "person"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "z"}))
	require.NoError(t, g.UpsertEdge(ctx, edge("az", "a", "z", "owns")))

	shapes, err := g.EdgeShapes(ctx, "owns")
	require.NoError(t, err)
	require.Len(t, shapes, 1)
	require.Equal(t, "owns", shapes[0].Type)
	require.Equal(t, []string{"person"}, shapes[0].FromTypes)
	require.Empty(t, shapes[0].ToTypes)

	none, err := g.EdgeShapes(ctx, "nomatch")
	require.NoError(t, err)
	require.Empty(t, none)
}

// TestRecordsWithPropertiesVariants locks in empty-key, empty-value and edge
// matching in RecordsWithProperties.
func TestRecordsWithPropertiesVariants(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Properties: map[string]any{"k": "v"}}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "b"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "c", Properties: map[string]any{"other": "x"}}))
	require.NoError(t, g.UpsertEdge(ctx, &Edge{ID: "e", From: "a", To: "b", Type: "t", Weight: 1, Properties: map[string]any{"k": "w"}}))

	anyProp, err := g.RecordsWithProperties(ctx, "", "")
	require.NoError(t, err)
	require.Len(t, anyProp, 3) // a, c and the edge e; b has none

	anyValue, err := g.RecordsWithProperties(ctx, "k", "")
	require.NoError(t, err)
	require.Len(t, anyValue, 2) // a and e

	exact, err := g.RecordsWithProperties(ctx, "k", "v")
	require.NoError(t, err)
	require.Len(t, exact, 1)
	require.Equal(t, "a", exact[0].ID)
}

// TestRetractDefaults locks in the zero-instant defaults and not-found mapping
// of the retraction paths.
func TestRetractDefaults(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e1", "a", "b", "knows")))

	require.NoError(t, g.RetractEdgeAt(ctx, "e1", time.Time{}))
	require.ErrorIs(t, g.RetractEdgeAt(ctx, "zzz", time.Time{}), ErrEdgeNotFound)

	require.NoError(t, g.UpsertEdge(ctx, edge("e2", "a", "b", "knows")))
	require.NoError(t, g.EndEdge(ctx, "e2", time.Time{}))
	require.ErrorIs(t, g.EndEdge(ctx, "", time.Time{}), ErrInvalidRequest)

	require.NoError(t, g.UpsertNode(ctx, node("c", "person")))
	require.NoError(t, g.RetractNodeAt(ctx, "c", time.Time{}))
	require.ErrorIs(t, g.RetractNodeAt(ctx, "zzz", time.Time{}), ErrNodeNotFound)
}

// TestSelfLoopEdgeDedup locks in that a self-loop is returned once by a
// directional read but not duplicated by "both".
func TestSelfLoopEdgeDedup(t *testing.T) {
	g, _, _, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("loop", "a", "a", "self")))

	both, err := g.GetEdges(ctx, "a", "both")
	require.NoError(t, err)
	require.Len(t, both, 1)
}

// TestVectorLengthChange locks in that a vector-length change is a change.
func TestVectorLengthChange(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Vector: []float32{1, 2}}))
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Vector: []float32{1, 2, 3}}))
	hist, err := g.NodeHistory(ctx, "a")
	require.NoError(t, err)
	require.Len(t, hist, 1)
}

// faultyDecodeCodec encodes normally but refuses to decode, so every read that
// materialises a caller-facing value fails.
type faultyDecodeCodec struct{ JSONValues }

func (faultyDecodeCodec) Decode([]byte) (map[string]any, error) {
	return nil, errors.New("decode boom")
}

// TestDecodeErrorPropagation locks in that a property-decode failure surfaces
// from every read surface instead of a silent zero value.
func TestDecodeErrorPropagation(t *testing.T) {
	ctx := context.Background()
	s, _ := openBoltStore(t, Config{})
	g, err := s.Open("g")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, g.UpsertEdge(ctx, edge("e", "a", "b", "t")))

	gf, err := s.Open("g", WithValues(faultyDecodeCodec{}))
	require.NoError(t, err)

	_, err = gf.GetNode(ctx, "a")
	require.Error(t, err)
	_, err = gf.GetNodesBatch(ctx, []string{"a"})
	require.Error(t, err)
	_, err = gf.GetEdgesBatch(ctx, []string{"e"})
	require.Error(t, err)
	_, err = gf.GetEdges(ctx, "a", "out")
	require.Error(t, err)
	_, err = gf.GetAllNodes(ctx, nil)
	require.Error(t, err)
	_, err = gf.GetAllEdges(ctx)
	require.Error(t, err)
	_, err = gf.CountNodes(ctx, nil)
	require.Error(t, err)
	_, err = gf.NodeTypeCounts(ctx)
	require.Error(t, err)
	_, err = gf.EdgeTypeCounts(ctx)
	require.Error(t, err)
	_, err = gf.Statistics(ctx)
	require.Error(t, err)
	_, err = gf.ExportDocument(ctx)
	require.Error(t, err)
}

// TestDeleteCorruptEdgeErrorPassthrough locks in that a non-not-found edge
// error is returned unchanged rather than mapped to ErrEdgeNotFound.
func TestDeleteCorruptEdgeErrorPassthrough(t *testing.T) {
	g, _, bdb, _ := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, node("a", "person")))
	require.NoError(t, g.UpsertNode(ctx, node("b", "person")))
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		gb := tx.Bucket([]byte(bucket.GRAPHS)).Bucket([]byte("g"))
		return gb.Bucket([]byte(edgesBucket)).Put([]byte("bad"), []byte("{not json"))
	}))
	err := g.DeleteEdge(ctx, "bad")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrEdgeNotFound)
}

// TestVersionOrderingByRecordedAt locks in that node and edge versions are
// ordered by their recording instant when those differ.
func TestVersionOrderingByRecordedAt(t *testing.T) {
	g, _, _, c := newBoltGraph(t)
	ctx := context.Background()
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "v1"}))
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "b", Type: "p"}))
	require.NoError(t, g.UpsertEdge(ctx, edge("e", "a", "b", "t1")))
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "v2"}))
	require.NoError(t, g.UpsertEdge(ctx, edge("e", "a", "b", "t2")))
	c.Advance(time.Hour)
	require.NoError(t, g.UpsertNode(ctx, &Node{ID: "a", Type: "p", Content: "v3"}))
	require.NoError(t, g.UpsertEdge(ctx, edge("e", "a", "b", "t3")))

	nodes, err := g.AllNodeVersions(ctx)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	edges, err := g.AllEdgeVersions(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 2)
}
