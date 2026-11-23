package harbor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/graph"
)

func TestGraphStoreAuditAndBackup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "graph.db")
	opts := testOptions("graph-password")
	opts.Audit = AuditGraphs
	store, err := Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	defer mustClose(t, store)

	g, err := store.Graphs().Open("social")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graph.Node{ID: "a", Type: "person"}))
	require.NoError(t, g.UpsertNode(ctx, &graph.Node{ID: "b", Type: "person"}))
	require.NoError(t, g.UpsertEdge(ctx, &graph.Edge{ID: "e", From: "a", To: "b"}))

	res, err := store.VerifyAudit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, res.Count, uint64(3))

	backupPath := filepath.Join(t.TempDir(), "copy.db")
	info, err := store.BackupFile(ctx, backupPath, BackupOptions{Manifest: true})
	require.NoError(t, err)
	verified, err := VerifyBackup(ctx, backupPath, "graph-password", nil)
	require.NoError(t, err)
	require.Equal(t, info.SHA256, verified.SHA256)

	restored := mustOpen(t, backupPath, "graph-password")
	defer mustClose(t, restored)
	rg, err := restored.Graphs().Open("social")
	require.NoError(t, err)
	got, err := rg.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "person", got.Type)
}
