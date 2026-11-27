package tempest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

// seedBackup writes one record, fstree, series, vector and full-text document.
func seedBackup(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, s.Record().Save(ctx, &txRecord{ID: "b1", Name: "backup"}))
	fsys, err := s.File().Open("data")
	require.NoError(t, err)
	f, err := fsys.Create("/note.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("backed up"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, s.Series().Series("cpu").Insert(ctx, series.Point{Timestamp: 1_000, Value: 3.5}))
	col, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{{ID: "p1", Vector: []float32{1, 0}}}))
	require.NoError(t, col.Flush())
	idx, err := s.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "d1", "backup text"))
}

func TestBackupFileVerifyAndRestore(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "tempest.db")
	s := mustOpen(t, src, "backup-password")
	defer mustClose(t, s)
	seedBackup(t, s)

	path := filepath.Join(t.TempDir(), "copy.db")
	info, err := s.BackupFile(ctx, path, BackupOptions{Manifest: true})
	require.NoError(t, err)
	require.Equal(t, path, info.Path)
	require.Positive(t, info.Size)
	require.Len(t, info.SHA256, 64)
	require.Equal(t, s.Flags(), info.Flags)

	verified, err := VerifyBackup(ctx, path, "backup-password", nil)
	require.NoError(t, err)
	require.Equal(t, info.Size, verified.Size)
	require.Equal(t, info.SHA256, verified.SHA256)
	require.Equal(t, info.Flags, verified.Flags)

	// The manifest must describe the same bytes.
	manifest, found, err := readBackupManifest(path)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, info.SHA256, manifest.SHA256)
	require.Equal(t, "1", manifest.Formats[bucket.FSTREE])
	require.Equal(t, "1", manifest.Formats[bucket.SEARCH])

	// The backup opens and holds every store's data.
	restored := mustOpen(t, path, "backup-password")
	defer mustClose(t, restored)

	var rec txRecord
	require.NoError(t, restored.Record().One(ctx, "ID", "b1", &rec))
	require.Equal(t, "backup", rec.Name)

	fsys, err := restored.File().Open("data")
	require.NoError(t, err)
	body, err := fsys.ReadFile("/note.txt")
	require.NoError(t, err)
	require.Equal(t, "backed up", string(body))

	points, err := restored.Series().Series("cpu").Range(ctx, 0, 10_000)
	require.NoError(t, err)
	require.Len(t, points, 1)

	col, err := restored.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	hits, err := col.Search([]float32{1, 0}, nil, 1)
	require.NoError(t, err)
	require.Len(t, hits, 1)

	idx, err := restored.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	fhits, err := idx.Search(ctx, "backup", 10)
	require.NoError(t, err)
	require.Len(t, fhits, 1)
}

func TestVerifyBackupRejectsWrongPassword(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tempest.db"), "right-password")
	defer mustClose(t, s)
	seedBackup(t, s)

	path := filepath.Join(t.TempDir(), "copy.db")
	_, err := s.BackupFile(ctx, path, BackupOptions{})
	require.NoError(t, err)

	_, err = VerifyBackup(ctx, path, "wrong-password", nil)
	require.ErrorIs(t, err, ErrInvalidPassword)
}

func TestVerifyBackupRejectsCorruptFile(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tempest.db"), "backup-password")
	defer mustClose(t, s)
	seedBackup(t, s)

	path := filepath.Join(t.TempDir(), "copy.db")
	_, err := s.BackupFile(ctx, path, BackupOptions{})
	require.NoError(t, err)

	// Truncating the fstree must be detected by the checksum and the open path.
	require.NoError(t, os.Truncate(path, 4096))
	_, err = VerifyBackup(ctx, path, "backup-password", nil)
	require.Error(t, err)
}

func TestVerifyBackupRejectsManifestMismatch(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tempest.db"), "backup-password")
	defer mustClose(t, s)
	seedBackup(t, s)

	path := filepath.Join(t.TempDir(), "copy.db")
	_, err := s.BackupFile(ctx, path, BackupOptions{Manifest: true})
	require.NoError(t, err)

	// Corrupt the sidecar checksum.
	raw, err := os.ReadFile(manifestPath(path))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"sha256": "`)
	corrupt := strings.Replace(string(raw), `"sha256": "`, `"sha256": "0`, 1)
	require.NoError(t, os.WriteFile(manifestPath(path), []byte(corrupt), 0o600))

	_, err = VerifyBackup(ctx, path, "backup-password", nil)
	require.ErrorIs(t, err, ErrBackupManifest)
}

func TestBackupFileOverwritePolicy(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tempest.db"), "backup-password")
	defer mustClose(t, s)

	path := filepath.Join(t.TempDir(), "copy.db")
	_, err := s.BackupFile(ctx, path, BackupOptions{})
	require.NoError(t, err)

	if _, err := s.BackupFile(ctx, path, BackupOptions{}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second BackupFile err = %v, want os.ErrExist", err)
	}
	if _, err := s.BackupFile(ctx, path, BackupOptions{Overwrite: true}); err != nil {
		t.Fatalf("overwriting BackupFile: %v", err)
	}
	// BackupTo keeps its overwrite behavior.
	if err := s.BackupTo(ctx, path); err != nil {
		t.Fatalf("BackupTo: %v", err)
	}
}

func TestBackupFileCancellationLeavesNoFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := mustOpen(t, filepath.Join(t.TempDir(), "tempest.db"), "backup-password")
	defer mustClose(t, s)

	path := filepath.Join(t.TempDir(), "copy.db")
	_, err := s.BackupFile(ctx, path, BackupOptions{})
	require.ErrorIs(t, err, context.Canceled)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr))

	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), ".tempest-backup-", "temporary fstree left behind")
	}
}

// TestBackupUnderConcurrentWrites takes a backup while another goroutine keeps
// writing. The backup must verify, and the source must still serve reads.
func TestBackupUnderConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, filepath.Join(t.TempDir(), "tempest.db"), "backup-password")
	defer mustClose(t, s)
	seedBackup(t, s)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			_ = s.Record().Save(context.Background(), &txRecord{ID: "hot-" + time.Now().Format("150405.000000000")})
		}
	}()

	path := filepath.Join(t.TempDir(), "copy.db")
	info, err := s.BackupFile(ctx, path, BackupOptions{Manifest: true})
	require.NoError(t, err)
	<-done

	verified, err := VerifyBackup(ctx, path, "backup-password", nil)
	require.NoError(t, err)
	require.Equal(t, info.SHA256, verified.SHA256)
}

func TestObjectStoreTraceAndBackup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "object.db")
	opts := testOptions("object-password")
	opts.Trace = TraceObject
	store, err := Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	defer mustClose(t, store)

	_, err = store.Objects().Put(ctx, "bucket", "a.txt", strings.NewReader("alpha"), 5, "")
	require.NoError(t, err)
	res, err := store.VerifyTrace()
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

func TestGraphsStoreTraceAndBackup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "graphs.db")
	opts := testOptions("graph-password")
	opts.Trace = TraceGraphs
	store, err := Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	defer mustClose(t, store)

	g, err := store.Graphs().Open("social")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person"}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "b", Type: "person"}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "e", From: "a", To: "b"}))

	res, err := store.VerifyTrace()
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

func TestRestoreBackupRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "tempest.db")
	s := mustOpen(t, src, "restore-password")
	seedBackup(t, s)
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	_, err := s.BackupFile(ctx, backupPath, BackupOptions{Manifest: true})
	require.NoError(t, err)
	mustClose(t, s)

	dst := filepath.Join(t.TempDir(), "restored.db")
	info, err := RestoreBackup(ctx, backupPath, dst, "restore-password", nil, RestoreOptions{})
	require.NoError(t, err)
	require.Equal(t, dst, info.Path)
	require.Positive(t, info.Size)
	require.Len(t, info.SHA256, 64)

	restored := mustOpen(t, dst, "restore-password")
	defer mustClose(t, restored)

	var rec txRecord
	require.NoError(t, restored.Record().One(ctx, "ID", "b1", &rec))
	require.Equal(t, "backup", rec.Name)

	hits, err := restored.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	got, err := hits.Search(ctx, "backup", 0)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "d1", got[0].ID)
}

func TestRestoreBackupWrongPassword(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "tempest.db")
	s := mustOpen(t, src, "right-password")
	seedBackup(t, s)
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	_, err := s.BackupFile(ctx, backupPath, BackupOptions{})
	require.NoError(t, err)
	mustClose(t, s)

	_, err = RestoreBackup(ctx, backupPath, filepath.Join(t.TempDir(), "dst.db"), "wrong-password", nil, RestoreOptions{})
	require.ErrorIs(t, err, ErrInvalidPassword)
}

func TestRestoreBackupCorruptSource(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "tempest.db")
	s := mustOpen(t, src, "corrupt-password")
	seedBackup(t, s)
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	_, err := s.BackupFile(ctx, backupPath, BackupOptions{Manifest: true})
	require.NoError(t, err)
	mustClose(t, s)

	raw, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	raw[len(raw)/2] ^= 0xff
	require.NoError(t, os.WriteFile(backupPath, raw, 0o600))

	dst := filepath.Join(t.TempDir(), "dst.db")
	_, err = RestoreBackup(ctx, backupPath, dst, "corrupt-password", nil, RestoreOptions{})
	require.ErrorIs(t, err, ErrBackupManifest)
	_, statErr := os.Stat(dst)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRestoreBackupOverwrite(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "tempest.db")
	s := mustOpen(t, src, "overwrite-password")
	seedBackup(t, s)
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	_, err := s.BackupFile(ctx, backupPath, BackupOptions{})
	require.NoError(t, err)
	mustClose(t, s)

	dst := filepath.Join(t.TempDir(), "dst.db")
	require.NoError(t, os.WriteFile(dst, []byte("existing"), 0o600))

	_, err = RestoreBackup(ctx, backupPath, dst, "overwrite-password", nil, RestoreOptions{})
	require.ErrorIs(t, err, os.ErrExist)

	info, err := RestoreBackup(ctx, backupPath, dst, "overwrite-password", nil, RestoreOptions{Overwrite: true})
	require.NoError(t, err)
	require.Equal(t, dst, info.Path)

	restored := mustOpen(t, dst, "overwrite-password")
	mustClose(t, restored)
}

func TestRestoreBackupCanceledContext(t *testing.T) {
	ctx := context.Background()
	src := filepath.Join(t.TempDir(), "tempest.db")
	s := mustOpen(t, src, "cancel-password")
	seedBackup(t, s)
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	_, err := s.BackupFile(ctx, backupPath, BackupOptions{})
	require.NoError(t, err)
	mustClose(t, s)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	dst := filepath.Join(t.TempDir(), "dst.db")
	_, err = RestoreBackup(canceled, backupPath, dst, "cancel-password", nil, RestoreOptions{})
	require.ErrorIs(t, err, context.Canceled)
	_, statErr := os.Stat(dst)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
