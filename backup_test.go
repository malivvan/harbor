package harbor

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/db/q"
	"github.com/malivvan/harbor/ts"
	"github.com/malivvan/harbor/vec"
)

// seedBackupStore writes one value into every sub-store. The vector point is
// left in the collection buffer on purpose: Backup must flush it first.
func seedBackupStore(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, s.DB().Save(ctx, &harborRecord{Name: "alice"}))

	cpu := s.TS().Series("cpu")
	require.NotNil(t, cpu)
	require.NoError(t, cpu.Insert(ctx, ts.Point{Timestamp: 10, Value: 0.5}))

	vfs, err := s.FS().Open("files")
	require.NoError(t, err)
	f, err := vfs.OpenFile("/hello.txt", os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte("hi"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	col, err := s.Vec().NewCollection("docs", 3, vec.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vec.PointStruct{{
		ID:      "p1",
		Vector:  []float32{1, 0, 0},
		Payload: vec.Payload{"role": "admin"},
	}}))
	// No Flush: Backup is responsible for draining the buffer.
}

func verifyBackupStore(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()

	var rec harborRecord
	require.NoError(t, s.DB().Select(q.Eq("Name", "alice")).First(ctx, &rec))
	require.Equal(t, "alice", rec.Name)

	points, err := s.TS().Series("cpu").Range(ctx, 0, 100)
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Equal(t, int64(10), points[0].Timestamp)
	require.InDelta(t, 0.5, points[0].Value, 1e-9)

	vfs, err := s.FS().Open("files")
	require.NoError(t, err)
	data, err := vfs.ReadFile("/hello.txt")
	require.NoError(t, err)
	require.Equal(t, "hi", string(data))

	loaded, err := s.Vec().LoadCollection("docs")
	require.NoError(t, err)
	point, ok := loaded["p1"]
	require.True(t, ok, "the buffered vector point must be in the backup")
	require.Equal(t, []float32{1, 0, 0}, point.Vector)
	require.Equal(t, "admin", point.Payload["role"])
}

func TestBackupRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const password = "backup password"

	store := mustOpen(t, filepath.Join(dir, "store.db"), password)
	seedBackupStore(t, store)

	var buf bytes.Buffer
	require.NoError(t, store.Backup(context.Background(), &buf))
	require.NotEmpty(t, buf.Bytes())
	require.NoError(t, store.Close())

	backupPath := filepath.Join(dir, "backup.db")
	require.NoError(t, os.WriteFile(backupPath, buf.Bytes(), 0o600))

	restored := mustOpen(t, backupPath, password)
	defer func() { require.NoError(t, restored.Close()) }()
	verifyBackupStore(t, restored)
}

func TestBackupToIsAtomicAndReplaces(t *testing.T) {
	dir := t.TempDir()
	backupDir := t.TempDir()
	path := filepath.Join(backupDir, "backup.db")

	store := mustOpen(t, filepath.Join(dir, "store.db"), "pw")
	defer func() { require.NoError(t, store.Close()) }()
	seedBackupStore(t, store)

	require.NoError(t, store.BackupTo(context.Background(), path))
	entries, err := os.ReadDir(backupDir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temporary files may be left behind")
	require.Equal(t, "backup.db", entries[0].Name())

	// A second backup replaces the first file.
	require.NoError(t, store.DB().Save(context.Background(), &harborRecord{Name: "bob"}))
	require.NoError(t, store.BackupTo(context.Background(), path))

	restored := mustOpen(t, path, "pw")
	defer func() { require.NoError(t, restored.Close()) }()

	ctx := context.Background()
	var rec harborRecord
	require.NoError(t, restored.DB().Select(q.Eq("Name", "alice")).First(ctx, &rec))
	require.NoError(t, restored.DB().Select(q.Eq("Name", "bob")).First(ctx, &rec))
}

func TestBackupClosedAndInvalidArguments(t *testing.T) {
	var nilStore *Store
	var buf bytes.Buffer
	require.ErrorIs(t, nilStore.Backup(context.Background(), &buf), ErrClosed)
	require.ErrorIs(t, nilStore.BackupTo(context.Background(), filepath.Join(t.TempDir(), "x.db")), ErrClosed)

	store := mustOpen(t, filepath.Join(t.TempDir(), "store.db"), "pw")
	require.ErrorIs(t, store.Backup(context.Background(), nil), ErrInvalidOptions)
	require.ErrorIs(t, store.BackupTo(context.Background(), ""), ErrInvalidOptions)
	require.NoError(t, store.Close())
	require.ErrorIs(t, store.Backup(context.Background(), &buf), ErrClosed)
}

// cancelAfterFirstWrite cancels the context after the first chunk is written.
type cancelAfterFirstWrite struct {
	cancel context.CancelFunc
	buf    bytes.Buffer
	writes int
}

func (w *cancelAfterFirstWrite) Write(p []byte) (int, error) {
	n, err := w.buf.Write(p)
	if w.writes == 0 {
		w.cancel()
	}
	w.writes++
	return n, err
}

func TestBackupCancellation(t *testing.T) {
	store := mustOpen(t, filepath.Join(t.TempDir(), "store.db"), "pw")
	defer func() { require.NoError(t, store.Close()) }()

	// Seed enough rows that the snapshot needs several writes.
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		require.NoError(t, store.DB().Save(ctx, &harborRecord{Name: "record"}))
	}

	// A pre-canceled context fails before writing anything.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, store.Backup(canceled, &bytes.Buffer{}), context.Canceled)

	// Cancellation mid-snapshot aborts and leaves no destination file.
	backupDir := t.TempDir()
	path := filepath.Join(backupDir, "canceled.db")
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	w := &cancelAfterFirstWrite{cancel: ccancel}
	err := store.Backup(cctx, w)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, store.BackupTo(cctx, path), context.Canceled)
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "a canceled BackupTo must not create the destination")
	entries, err := os.ReadDir(backupDir)
	require.NoError(t, err)
	require.Empty(t, entries, "a canceled BackupTo must not leave temporary files")
}

// TestBackupStaysEncrypted proves a backup is the encrypted on-disk form: the
// payload is not plaintext and the same password is required to open it.
func TestBackupStaysEncrypted(t *testing.T) {
	dir := t.TempDir()
	const secret = "very secret payload"
	const password = "correct horse"

	store := mustOpen(t, filepath.Join(dir, "store.db"), password)
	require.NoError(t, store.DB().Save(context.Background(), &harborRecord{Name: secret}))

	var buf bytes.Buffer
	require.NoError(t, store.Backup(context.Background(), &buf))
	require.NoError(t, store.Close())

	require.NotContains(t, buf.String(), secret, "an encrypted backup must not contain plaintext")

	path := filepath.Join(dir, "backup.db")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))

	_, err := Open(context.Background(), path, 0o600, testOptions("wrong password"))
	require.ErrorIs(t, err, ErrInvalidPassword)

	restored := mustOpen(t, path, password)
	defer func() { require.NoError(t, restored.Close()) }()
	var rec harborRecord
	require.NoError(t, restored.DB().Select(q.Eq("Name", secret)).First(context.Background(), &rec))
}

func TestBackupIsConsistentDuringWrites(t *testing.T) {
	store := mustOpen(t, filepath.Join(t.TempDir(), "store.db"), "pw")
	defer func() { require.NoError(t, store.Close()) }()
	ctx := context.Background()

	// A record that every backup must contain, written before the writer starts.
	require.NoError(t, store.DB().Save(ctx, &harborRecord{Name: "anchor"}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_ = store.DB().Save(ctx, harborRecord{Name: "concurrent"})
		}
	}()

	for i := 0; i < 20; i++ {
		var buf bytes.Buffer
		require.NoError(t, store.Backup(ctx, &buf))

		path := filepath.Join(t.TempDir(), "snapshot.db")
		require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
		restored := mustOpen(t, path, "pw")

		var rec harborRecord
		require.NoError(t, restored.DB().Select(q.Eq("Name", "anchor")).First(ctx, &rec))
		require.NoError(t, restored.Close())
	}
	<-done
}

// TestBackupOverwritesExistingOnSuccess guards the atomic replace path: a
// successful backup replaces a file that is not even a valid database.
func TestBackupOverwritesExistingOnSuccess(t *testing.T) {
	store := mustOpen(t, filepath.Join(t.TempDir(), "store.db"), "pw")
	defer func() { require.NoError(t, store.Close()) }()
	seedBackupStore(t, store)

	path := filepath.Join(t.TempDir(), "backup.db")
	require.NoError(t, os.WriteFile(path, []byte("not a database"), 0o600))
	require.NoError(t, store.BackupTo(context.Background(), path))

	restored := mustOpen(t, path, "pw")
	defer func() { require.NoError(t, restored.Close()) }()
	verifyBackupStore(t, restored)
}
