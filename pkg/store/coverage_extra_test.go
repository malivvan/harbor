package store

// Tests for the parts of the package that the happy-path tests do not reach:
// the per-store accessors (manager and process-wide), the configured key-file
// mode, and the error paths of the password-file helpers.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Rule: every accessor returns nil before Open and a non-nil store after it, so a
// caller can always ask for a store without checking whether Open has run.
func TestManagerAccessorsBeforeAndAfterOpen(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(Config{Dir: dir})
	t.Cleanup(func() { _ = m.Close() })

	if m.Store() != nil {
		t.Fatal("Store is non-nil before Open")
	}
	// Compare each accessor directly: a nil *record.Store held in an `any` is not
	// itself nil, so a table of `any` values cannot be used for this check.
	if m.Record() != nil || m.File() != nil || m.Series() != nil || m.Vector() != nil {
		t.Fatal("a store accessor is non-nil before Open")
	}
	if m.Search() != nil || m.Graphs() != nil || m.Objects() != nil {
		t.Fatal("a store accessor is non-nil before Open")
	}

	if got := m.Config(); got.Dir != dir || got.DBFile != DefaultDBFile || got.KeyFile != DefaultKeyFile || got.Mode != DefaultMode {
		t.Fatalf("Config = %+v, want the defaulted config", got)
	}
	if m.Dir() != dir {
		t.Fatalf("Dir = %q, want %q", m.Dir(), dir)
	}

	if _, err := m.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if m.Record() == nil || m.File() == nil || m.Series() == nil || m.Vector() == nil {
		t.Fatal("a store accessor is nil after Open")
	}
	if m.Search() == nil || m.Graphs() == nil || m.Objects() == nil {
		t.Fatal("a store accessor is nil after Open")
	}

	// The same accessors accept path segments and return a rooted view.
	if m.Record("tenant", "records") == nil || m.File("tenant") == nil || m.Series("tenant") == nil {
		t.Fatal("a rooted accessor is nil")
	}
	if m.Vector("tenant") == nil || m.Search("tenant") == nil || m.Graphs("tenant") == nil || m.Objects("tenant") == nil {
		t.Fatal("a rooted accessor is nil")
	}
}

// Rule: the process-wide accessors expose every store, rooted or not, exactly
// like the manager they delegate to.
func TestProcessWideAccessorsCoverEveryStore(t *testing.T) {
	if _, err := Open(context.Background(), Config{Dir: t.TempDir()}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = Close() })

	if Record() == nil || File() == nil || Series() == nil || Vector() == nil {
		t.Fatal("a process-wide accessor returned nil")
	}
	if Search() == nil || Graphs() == nil || Objects() == nil {
		t.Fatal("a process-wide accessor returned nil")
	}
	if Record("tenant") == nil || File("tenant") == nil || Series("tenant") == nil {
		t.Fatal("a rooted process-wide accessor returned nil")
	}
	if Vector("tenant") == nil || Search("tenant") == nil || Graphs("tenant") == nil || Objects("tenant") == nil {
		t.Fatal("a rooted process-wide accessor returned nil")
	}
}

// Rule: Config.Mode applies to the password file, which is written through a
// temporary file, so the mode has to be set before the name is published.
func TestKeyFileUsesConfiguredMode(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(Config{Dir: dir, Mode: 0o640})
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, DefaultKeyFile))
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Fatalf("key file mode = %o, want 640", got)
	}
}

// Rule: a key path that exists but cannot be read as a file is a hard error, not
// a signal to create a new key.
func TestKeyPathThatIsADirectoryFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, DefaultKeyFile), 0o750); err != nil {
		t.Fatalf("mkdir key path: %v", err)
	}
	m := NewManager(Config{Dir: dir})
	if _, err := m.Open(context.Background()); err == nil {
		t.Fatal("Open succeeded with a directory as the key file")
	}
}

// Rule: an uncreatable directory fails the open before any key is generated.
func TestOpenWithUncreatableDirectoryFails(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write parent file: %v", err)
	}
	// Dir is a child of a regular file, so MkdirAll cannot create it.
	m := NewManager(Config{Dir: filepath.Join(parent, "store")})
	if _, err := m.Open(context.Background()); err == nil {
		t.Fatal("Open succeeded below a regular file")
	}
}

// Rule: a failed temporary key write leaves nothing behind for the next Open to
// mistake for a key.
func TestWriteKeyTempFailureLeavesNoTempFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) })

	if _, err := writeKeyTemp(dir, 0o600, strings.Repeat("a", KeySize*2)); err == nil {
		t.Fatal("writeKeyTemp succeeded in a read-only directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("read-only directory holds %d entries, want 0", len(entries))
	}
}

// Rule: the exclusive-create fallback creates and fills the key on a path where
// nothing exists yet.
func TestCreateKeyExclusiveCreatesNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultKeyFile)
	want := strings.Repeat("c", KeySize*2)

	got, err := createKeyExclusive(path, 0o600, want)
	if err != nil {
		t.Fatalf("createKeyExclusive: %v", err)
	}
	if got != want {
		t.Fatalf("createKeyExclusive = %q, want the generated key", got)
	}
	if got := strings.TrimSpace(readKey(t, path)); got != want {
		t.Fatal("key file does not hold the key that was written")
	}
}

// Rule: a loser of the exclusive-create race waits for the winner to finish
// writing instead of failing on the transiently empty file.
func TestReadWinningKeyWaitsForContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultKeyFile)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("seed empty key file: %v", err)
	}
	want := strings.Repeat("d", KeySize*2)
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = os.WriteFile(path, []byte(want), 0o600)
	}()

	got, err := readWinningKey(path)
	if err != nil {
		t.Fatalf("readWinningKey: %v", err)
	}
	if got != want {
		t.Fatalf("readWinningKey = %q, want the key the winner wrote", got)
	}
}

// Rule: when the winner never writes, the loser gives up with the validation
// error instead of blocking forever.
func TestReadWinningKeyGivesUpOnEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultKeyFile)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("seed empty key file: %v", err)
	}

	start := time.Now()
	_, err := readWinningKey(path)
	if err == nil {
		t.Fatal("readWinningKey accepted an empty key file")
	}
	if !strings.Contains(err.Error(), "64 hex characters") {
		t.Fatalf("error = %v, want the key-length validation error", err)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("readWinningKey gave up after %v, want it to retry first", elapsed)
	}
}

// Rule: a read error that is not "missing" fails immediately; only a missing or
// still-empty file is retried.
func TestReadWinningKeyFailsOnUnreadablePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultKeyFile)
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatalf("mkdir key path: %v", err)
	}

	start := time.Now()
	if _, err := readWinningKey(path); err == nil {
		t.Fatal("readWinningKey read a directory as a key")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("readWinningKey retried a hard read error for %v", elapsed)
	}
}

// Rule: the directory sync is best-effort, so a missing directory is not a
// problem and never panics.
func TestSyncDirIgnoresMissingDirectory(t *testing.T) {
	syncDir(filepath.Join(t.TempDir(), "missing"))
	syncDir(t.TempDir())
}
