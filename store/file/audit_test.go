package file

import (
	"context"
	"crypto/ed25519"
	"path/filepath"
	"testing"
	"time"

	"github.com/malivvan/harbor/pkg/audit"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func auditSeed() []byte {
	return []byte("000102030405060708090a0b0c0d0e0f")
}

func openAuditedStore(t *testing.T) (*Store, *bolt.DB, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := audit.DeriveKey(auditSeed(), "m/1'")
	require.NoError(t, err)

	db, err := bolt.Open(filepath.Join(t.TempDir(), "fs-audit.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rec := audit.NewRecorder(db, map[string]ed25519.PrivateKey{audit.StoreFS: priv})
	store, err := Open(context.Background(), db, Config{Root: [][]byte{[]byte("fs")}, Audit: rec})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, db, pub
}

func TestAuditRecordsFSWrites(t *testing.T) {
	store, db, pub := openAuditedStore(t)
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	require.NoError(t, fsys.Mkdir("/dir", 0o755))
	require.NoError(t, writeFile(t, fsys, "/dir/a.txt", "hello"))
	require.NoError(t, fsys.Chmod("/dir/a.txt", 0o600))
	require.NoError(t, fsys.Chtimes("/dir/a.txt", time.Now(), time.Now()))
	require.NoError(t, fsys.Chown("/dir/a.txt", 1000, 1000))
	require.NoError(t, fsys.Symlink("/dir/a.txt", "/dir/link"))
	require.NoError(t, fsys.Rename("/dir/a.txt", "/dir/b.txt"))
	require.NoError(t, fsys.Remove("/dir/b.txt"))
	require.NoError(t, fsys.RemoveAll("/dir"))
	require.NoError(t, store.DeleteFS("fs"))

	res, err := audit.Verify(db, map[string]ed25519.PublicKey{audit.StoreFS: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(11), res.Count)
}

func TestAuditDisabledRecordsNothing(t *testing.T) {
	store, db := openEncStore(t, []byte("fs"), Config{})
	fsys, err := store.Open("fs")
	require.NoError(t, err)
	require.NoError(t, fsys.Mkdir("/dir", 0o755))

	_, err = audit.Verify(db, map[string]ed25519.PublicKey{audit.StoreFS: nil})
	require.ErrorIs(t, err, audit.ErrNoLog)
}

// writeFile creates a file and writes content through the absfs file API, which
// is the path the audit trail instruments.
func writeFile(t *testing.T, fsys *FS, name, content string) error {
	t.Helper()
	f, err := fsys.Create(name)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(content)); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Rule: Copy records one audit event under the destination name. Content and
// inode writes are part of the copy and must not be invisible.
func TestAuditRecordsCopy(t *testing.T) {
	store, db, pub := openAuditedStore(t)
	fsys, err := store.Open("fs")
	require.NoError(t, err)

	require.NoError(t, writeFile(t, fsys, "/src.txt", "hello"))
	require.NoError(t, fsys.Copy("/src.txt", "/dst.txt"))

	res, err := audit.Verify(db, map[string]ed25519.PublicKey{audit.StoreFS: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(3), res.Count, "write, close and copy must be recorded")

	body, err := fsys.ReadFile("/dst.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(body))
}
