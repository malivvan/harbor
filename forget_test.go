package tempest

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/record"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/vector"
)

type forgetDoc struct {
	ID   string `db:"id"`
	Text string
}

func openForgetStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "forget.db"), 0o600, &Options{
		Password:   "forget-password",
		Plaintext:  true,
		KDFTime:    FastKDFTime,
		KDFMemory:  FastKDFMemory,
		KDFThreads: FastKDFThreads,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, ctx
}

func TestForgetRemovesEveryLinkedStore(t *testing.T) {
	store, ctx := openForgetStore(t)
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "record", Type: &forgetDoc{}}))
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "vector", Name: "docs"}))
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "search", Name: "docs"}))
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "graphs", Name: "g"}))
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "object", Bucket: "media"}))
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "fstree", Name: "fs"}))

	require.NoError(t, store.Record().Save(ctx, &forgetDoc{ID: "a", Text: "alice"}))
	require.NoError(t, store.Record().Save(ctx, &forgetDoc{ID: "b", Text: "bob"}))

	col, err := store.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{0, 1}},
	}))

	idx, err := store.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "a", "alice text"))
	require.NoError(t, idx.Upsert(ctx, "b", "bob text"))

	g, err := store.Graphs().Open("g")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person"}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "b", Type: "person"}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "edge", From: "a", To: "b", Type: "knows"}))

	_, err = store.Objects().Put(ctx, "media", "a", bytes.NewReader([]byte("alice")), 5, "text/plain")
	require.NoError(t, err)
	_, err = store.Objects().Put(ctx, "media", "b", bytes.NewReader([]byte("bob")), 3, "text/plain")
	require.NoError(t, err)

	fsys, err := store.Fstree().Open("fs")
	require.NoError(t, err)
	for _, name := range []string{"/a", "/b"} {
		file, err := fsys.Create(name)
		require.NoError(t, err)
		_, err = file.Write([]byte("x"))
		require.NoError(t, err)
		require.NoError(t, file.Close())
	}

	report, err := store.Forget(ctx, "a", ForgetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, report.Deleted)
	require.Empty(t, report.Missing)

	require.ErrorIs(t, store.Record().One(ctx, "ID", "a", &forgetDoc{}), record.ErrNotFound)
	require.NoError(t, store.Record().One(ctx, "ID", "b", &forgetDoc{}))

	hits, err := col.Search([]float32{1, 0}, nil, 10)
	require.NoError(t, err)
	for _, hit := range hits {
		require.NotEqual(t, "a", hit.ID)
	}
	searchHits, err := idx.Search(ctx, "alice", 10)
	require.NoError(t, err)
	require.Empty(t, searchHits)

	_, err = g.GetNode(ctx, "a")
	require.ErrorIs(t, err, graphs.ErrNodeNotFound)
	nodeHistory, err := g.NodeHistory(ctx, "a")
	require.NoError(t, err)
	require.Empty(t, nodeHistory)
	edgeHistory, err := g.EdgeHistory(ctx, "edge")
	require.NoError(t, err)
	require.Empty(t, edgeHistory)
	edges, err := g.GetAllEdges(ctx)
	require.NoError(t, err)
	for _, edge := range edges {
		require.NotEqual(t, "edge", edge.ID)
	}

	_, _, err = store.Objects().Get(ctx, "media", "a")
	require.Error(t, err)
	_, _, err = store.Objects().Get(ctx, "media", "b")
	require.NoError(t, err)

	fsys, err = store.Fstree().Open("fs")
	require.NoError(t, err)
	_, err = fsys.Stat("/a")
	require.Error(t, err)
	_, err = fsys.Stat("/b")
	require.NoError(t, err)
}

func TestOrphansReportsRowsWithoutRecord(t *testing.T) {
	store, ctx := openForgetStore(t)
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "record", Type: &forgetDoc{}}))
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "vector", Name: "docs"}))
	require.NoError(t, store.RegisterLink(ctx, LinkSpec{Store: "search", Name: "docs"}))

	require.NoError(t, store.Record().Save(ctx, &forgetDoc{ID: "a"}))
	col, err := store.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "orphan", Vector: []float32{0, 1}},
	}))
	idx, err := store.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "a", "a text"))
	require.NoError(t, idx.Upsert(ctx, "orphan", "orphan text"))

	report, err := store.Orphans(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, report.Orphans)
	for _, orphan := range report.Orphans {
		require.Equal(t, "orphan", orphan.ID)
	}

	report, err = store.Orphans(ctx, OrphansOptions{Remove: true})
	require.NoError(t, err)
	require.Positive(t, report.Removed)
	report, err = store.Orphans(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Orphans)
}

func TestForgetRollsBackOnError(t *testing.T) {
	store, ctx := openForgetStore(t)
	require.NoError(t, store.Record().Save(ctx, &forgetDoc{ID: "a"}))
	report, err := store.Forget(ctx, "a", ForgetOptions{Links: []LinkSpec{
		{Store: "record", Type: &forgetDoc{}},
		{Store: "bogus", Name: "x"},
	}})
	require.Error(t, err)
	require.Nil(t, report)
	require.NoError(t, store.Record().One(ctx, "ID", "a", &forgetDoc{}))
}
