package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/malivvan/eidos-db"
	bolt "go.etcd.io/bbolt"
)

func TestOpenCreatesOneStoreAndKey(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(Config{Dir: dir})
	t.Cleanup(func() { _ = m.Close() })

	st, err := m.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if st.Record() == nil || st.File() == nil || st.Vector() == nil {
		t.Fatal("Open returned a store with a nil sub-store")
	}
	if _, err := os.Stat(filepath.Join(dir, DefaultDBFile)); err != nil {
		t.Fatalf("database file missing: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, DefaultKeyFile))
	if err != nil {
		t.Fatalf("key file missing: %v", err)
	}
	if info.Mode().Perm() != DefaultMode {
		t.Fatalf("key file mode = %o, want %o", info.Mode().Perm(), DefaultMode)
	}
	if got := len(strings.TrimSpace(readKey(t, filepath.Join(dir, DefaultKeyFile)))); got != KeySize*2 {
		t.Fatalf("key file has %d characters, want %d", got, KeySize*2)
	}

	again, err := m.Open(context.Background())
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	if again != st {
		t.Fatal("Open did not return the manager's existing store")
	}
}

func TestWrongKeyIsRejected(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(Config{Dir: dir})
	if _, err := m.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Replace the password with a different (valid-length) one.
	wrong := strings.Repeat("0", KeySize*2)
	if err := os.WriteFile(filepath.Join(dir, DefaultKeyFile), []byte(wrong), 0o600); err != nil {
		t.Fatalf("overwrite key: %v", err)
	}
	if _, err := m.Open(context.Background()); err == nil {
		t.Fatal("Open succeeded with a different password")
	}
}

// TestOpenFollowsDirChange is the isolation guarantee a test harness relies on:
// pointing the manager at another directory yields a store for THAT directory.
func TestOpenFollowsDirChange(t *testing.T) {
	first := t.TempDir()
	m := NewManager(Config{Dir: first})
	t.Cleanup(func() { _ = m.Close() })

	st1, err := m.Open(context.Background())
	if err != nil {
		t.Fatalf("Open first: %v", err)
	}
	if _, err := os.Stat(filepath.Join(first, DefaultDBFile)); err != nil {
		t.Fatalf("first database missing: %v", err)
	}

	second := t.TempDir()
	m.SetConfig(Config{Dir: second})

	st2, err := m.Open(context.Background())
	if err != nil {
		t.Fatalf("Open second: %v", err)
	}
	if st2 == st1 {
		t.Fatal("Open returned the store for the OLD directory")
	}
	if _, err := os.Stat(filepath.Join(second, DefaultDBFile)); err != nil {
		t.Fatalf("second database missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(second, DefaultKeyFile)); err != nil {
		t.Fatalf("second key file missing: %v", err)
	}

	// The handle is stable while the directory is unchanged.
	st3, err := m.Open(context.Background())
	if err != nil {
		t.Fatalf("Open third: %v", err)
	}
	if st3 != st2 {
		t.Fatal("Open reopened an unchanged directory")
	}
}

// TestExplicitPasswordSkipsKeyFile: a caller that supplies a password must not get
// a key file written, because there is no secret to persist.
func TestExplicitPasswordSkipsKeyFile(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(Config{Dir: dir, Password: "correct horse battery staple"})
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, DefaultKeyFile)); !os.IsNotExist(err) {
		t.Fatalf("key file was written despite an explicit password (err=%v)", err)
	}

	// The same explicit password reopens the same data.
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	m2 := NewManager(Config{Dir: dir, Password: "correct horse battery staple"})
	t.Cleanup(func() { _ = m2.Close() })
	if _, err := m2.Open(context.Background()); err != nil {
		t.Fatalf("reopen with the same password: %v", err)
	}
}

func TestManagersAreIndependent(t *testing.T) {
	a := NewManager(Config{Dir: t.TempDir()})
	b := NewManager(Config{Dir: t.TempDir()})
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	sa, err := a.Open(context.Background())
	if err != nil {
		t.Fatalf("manager a: %v", err)
	}
	sb, err := b.Open(context.Background())
	if err != nil {
		t.Fatalf("manager b: %v", err)
	}
	if sa == sb {
		t.Fatal("two managers on different directories share one store")
	}
	if a.Dir() == b.Dir() {
		t.Fatal("managers report the same directory")
	}
}

// TestConcurrentFirstOpenAgreesOnOneKey: the O_EXCL create must not let two racing
// callers produce different passwords for the same directory.
func TestConcurrentFirstOpenAgreesOnOneKey(t *testing.T) {
	dir := t.TempDir()

	const n = 8
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ok  int
		err error
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			// This test opens the same directory eight times: every winner
			// holds the file lock through Argon2id, so it must not use the
			// short production lock timeout.
			opts := &eidos.Options{Options: bolt.Options{Timeout: time.Minute}}
			m := NewManager(Config{Dir: dir, Options: opts})
			if _, oerr := m.Open(context.Background()); oerr != nil {
				mu.Lock()
				err = oerr
				mu.Unlock()
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
			_ = m.Close()
		}()
	}
	wg.Wait()

	if err != nil {
		t.Fatalf("a concurrent Open failed: %v", err)
	}
	if ok == 0 {
		t.Fatal("no concurrent Open succeeded")
	}
	// Whatever the interleaving, exactly one key file exists and it parses.
	key := strings.TrimSpace(readKey(t, filepath.Join(dir, DefaultKeyFile)))
	if len(key) != KeySize*2 {
		t.Fatalf("key file has %d characters, want %d", len(key), KeySize*2)
	}
}

func TestConfigDefaults(t *testing.T) {
	got := Config{Dir: "/tmp/x"}.withDefaults()
	if got.DBFile != DefaultDBFile || got.KeyFile != DefaultKeyFile || got.Mode != DefaultMode {
		t.Fatalf("withDefaults = %+v", got)
	}
	custom := Config{Dir: "/tmp/x", DBFile: "a.db", KeyFile: "a.key", Mode: 0o640}.withDefaults()
	if custom.DBFile != "a.db" || custom.KeyFile != "a.key" || custom.Mode != 0o640 {
		t.Fatalf("withDefaults overwrote explicit values: %+v", custom)
	}
}

func TestOpenRequiresDir(t *testing.T) {
	if _, err := NewManager(Config{}).Open(context.Background()); err == nil {
		t.Fatal("Open with no Dir: want an error")
	}
}

func TestCustomFileNames(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(Config{Dir: dir, DBFile: "custom.db", KeyFile: "custom.key"})
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, name := range []string{"custom.db", "custom.key"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s missing: %v", name, err)
		}
	}
}

// TestProcessWideDefault exercises the package-level manager.
func TestProcessWideDefault(t *testing.T) {
	t.Cleanup(func() { _ = Close() })

	dir := t.TempDir()
	st, err := Open(context.Background(), Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if Default() == nil {
		t.Fatal("Default returned nil after Open")
	}
	if Store() != st {
		t.Fatal("Store did not return the process-wide store")
	}
	if Record() == nil || File() == nil || Vector() == nil {
		t.Fatal("a process-wide accessor returned nil")
	}

	// Closing forgets the manager, so a later Open starts clean.
	if err := Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if Default() != nil {
		t.Fatal("Default is still set after Close")
	}
}

// Rule: the password file is published atomically, so a racing reader never sees
// an empty key file, and no temporary file survives the publish.
func TestKeyFileIsPublishedAtomically(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(Config{Dir: dir})
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	got := make(map[string]bool, len(entries))
	for _, e := range entries {
		got[e.Name()] = true
	}
	want := []string{DefaultDBFile, DefaultKeyFile}
	if len(got) != len(want) {
		t.Fatalf("directory holds %v, want only %v (left-over temp key file?)", got, want)
	}
	for _, name := range want {
		if !got[name] {
			t.Fatalf("directory holds %v, want %s", got, name)
		}
	}
}

// Rule: when the exclusive-create fallback loses the race, the loser adopts the
// winner's key instead of opening the same directory with a different password.
func TestKeyExclusiveCreateAdoptsWinnerKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultKeyFile)
	winner := strings.Repeat("b", KeySize*2)
	if err := os.WriteFile(path, []byte(winner), DefaultMode); err != nil {
		t.Fatalf("seed key file: %v", err)
	}

	got, err := createKeyExclusive(path, DefaultMode, strings.Repeat("a", KeySize*2))
	if err != nil {
		t.Fatalf("createKeyExclusive: %v", err)
	}
	if got != winner {
		t.Fatalf("fallback key = %.8s…, want the winner's key", got)
	}
	if on := strings.TrimSpace(readKey(t, path)); on != winner {
		t.Fatal("fallback overwrote the winner's key file")
	}
}

func TestValidateKeyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultKeyFile)
	if _, err := validateKeyFile(path, []byte(strings.Repeat("a", KeySize*2))); err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	if _, err := validateKeyFile(path, []byte("too-short")); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := validateKeyFile(path, []byte(strings.Repeat("z", KeySize*2))); err == nil {
		t.Fatal("non-hex key accepted")
	}
}

func readKey(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	return string(b)
}

// Rule: the manager and the process-wide singleton expose all four stores, so a
// caller never has to reach through Store() for one of them.
func TestAllFourStoresAreExposed(t *testing.T) {
	m := NewManager(Config{Dir: t.TempDir(), Password: "test-password"})
	t.Cleanup(func() { _ = m.Close() })
	if _, err := m.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if m.Record() == nil || m.File() == nil || m.Series() == nil || m.Vector() == nil {
		t.Fatalf("manager accessors: DB=%v FS=%v TS=%v Vec=%v",
			m.Record() != nil, m.File() != nil, m.Series() != nil, m.Vector() != nil)
	}
	if m.Series() != m.Store().Series() {
		t.Fatal("Manager.TS does not return the store's series store")
	}

	// The process-wide singleton follows the same shape.
	if _, err := Open(context.Background(), Config{Dir: t.TempDir(), Password: "test-password"}); err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = Close() })
	if Series() == nil || Record() == nil || File() == nil || Vector() == nil {
		t.Fatal("process-wide accessors returned a nil store")
	}
}
