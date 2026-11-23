package harbor

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectStoreAuditAndBackup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "object.db")
	opts := testOptions("object-password")
	opts.Audit = AuditObject
	store, err := Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	defer mustClose(t, store)

	_, err = store.Objects().Put(ctx, "bucket", "a.txt", strings.NewReader("alpha"), 5, "")
	require.NoError(t, err)
	res, err := store.VerifyAudit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, res.Count, uint64(1))

	backupPath := filepath.Join(t.TempDir(), "copy.db")
	info, err := store.BackupFile(ctx, backupPath, BackupOptions{Manifest: true})
	require.NoError(t, err)
	verified, err := VerifyBackup(ctx, backupPath, "object-password", nil)
	require.NoError(t, err)
	require.Equal(t, info.SHA256, verified.SHA256)

	restored := mustOpen(t, backupPath, "object-password")
	defer mustClose(t, restored)
	body, err := restored.Objects().ReadAll(ctx, "bucket", "a.txt")
	require.NoError(t, err)
	require.Equal(t, "alpha", string(body))
}
