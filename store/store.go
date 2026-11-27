// Package store manages the process-wide tempest store.
//
// A tempest deployment normally has exactly one database file and one password for
// the whole process. This package owns that convention: it resolves a directory,
// reads or creates the password file, opens the database, and hands out the same
// handle to every caller. Applications that want a different lifecycle can use a
// Manager directly; Open keeps a single default manager for the common case.
//
// # Password file
//
// When Config.Password is empty the password is a 32-byte random value, hex
// encoded (64 characters) and stored at <Dir>/<KeyFile> with mode 0600. The file
// is published atomically (a synced temporary file hard-linked into place, with
// an O_EXCL create as the fallback where links are unsupported), so two
// processes racing to initialise the same directory agree on one key instead of
// clobbering each other, and a racing reader never sees a partial key. The value
// is never logged.
//
// A password is a secret: reading it from the file is what makes the database
// reopenable across restarts, and losing the file loses the data. Back it up with
// the database.
//
// # Reopening when the directory changes
//
// Manager.Open compares the configured directory with the one it last opened. If
// they differ the previous store is closed and the new directory's database is
// opened instead. In a server the directory is resolved once, so this never fires;
// it exists so that test harnesses can point each test at a fresh directory and
// get a genuinely fresh database rather than a stale handle.
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/store/fstree"
	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/object"
	"github.com/malivvan/tempest/store/record"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

const (
	// DefaultDBFile is the database file name used when Config.DBFile is empty.
	DefaultDBFile = "tempest.db"
	// DefaultKeyFile is the password file name used when Config.KeyFile is empty.
	DefaultKeyFile = "tempest.key"
	// DefaultMode is the permission used for both files.
	DefaultMode os.FileMode = 0o600
	// KeySize is the password length in bytes (hex-encoded to 64 characters).
	KeySize = 32
)

// Config describes the store Manager.Open opens.
type Config struct {
	// Dir is the directory holding the database and password files. Required.
	Dir string
	// DBFile is the database file name. Empty means DefaultDBFile.
	DBFile string
	// KeyFile is the password file name. Empty means DefaultKeyFile.
	KeyFile string
	// Mode is the permission for both files. Zero means DefaultMode.
	Mode os.FileMode
	// Password is an explicit password. When empty it is read from, or created
	// at, Dir/KeyFile.
	Password string
	// Options are extra tempest options (KDF cost, encryption flags, metrics).
	// The Password field is always taken from Config, not from here.
	Options *tempest.Options
}

// withDefaults returns a copy of cfg with the defaulted fields filled in.
func (c Config) withDefaults() Config {
	if c.DBFile == "" {
		c.DBFile = DefaultDBFile
	}
	if c.KeyFile == "" {
		c.KeyFile = DefaultKeyFile
	}
	if c.Mode == 0 {
		c.Mode = DefaultMode
	}
	return c
}

// keyPath is the resolved password file path.
func (c Config) keyPath() string { return filepath.Join(c.Dir, c.KeyFile) }

// dbPath is the resolved database file path.
func (c Config) dbPath() string { return filepath.Join(c.Dir, c.DBFile) }

// Manager owns one tempest store for one directory at a time.
//
// A Manager is safe for concurrent use. Its zero value is not usable; build one
// with NewManager.
type Manager struct {
	mu   sync.Mutex
	cfg  Config
	st   *tempest.Store
	root string // directory the open store belongs to
}

// NewManager returns a Manager for cfg. It does not open anything; call Open.
func NewManager(cfg Config) *Manager {
	return &Manager{cfg: cfg.withDefaults()}
}

// Config returns the manager's current configuration.
func (m *Manager) Config() Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg
}

// SetConfig replaces the configuration. The next Open call honours it, closing
// the current store first if the directory changed.
func (m *Manager) SetConfig(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg.withDefaults()
}

// Dir returns the directory the manager is configured for.
func (m *Manager) Dir() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.Dir
}

// Open returns the manager's store, opening (and creating the password file for)
// the configured directory on first call.
//
// If the configured directory differs from the one the current store belongs to,
// that store is closed first and the new directory's database is opened.
func (m *Manager) Open(ctx context.Context) (*tempest.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.openLocked(ctx)
}

func (m *Manager) openLocked(ctx context.Context) (*tempest.Store, error) {
	if m.cfg.Dir == "" {
		return nil, errors.New("store: Config.Dir is required")
	}

	if m.st != nil {
		if m.root == m.cfg.Dir {
			return m.st, nil
		}
		prev, prevRoot := m.st, m.root
		m.st, m.root = nil, ""
		if err := prev.Close(); err != nil {
			return nil, fmt.Errorf("store: close store for %s: %w", prevRoot, err)
		}
	}

	if err := os.MkdirAll(m.cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("store: create dir %s: %w", m.cfg.Dir, err)
	}

	password := m.cfg.Password
	if password == "" {
		var err error
		password, err = loadOrCreatePassword(m.cfg.keyPath(), m.cfg.Mode)
		if err != nil {
			return nil, err
		}
	}

	opts := tempest.Options{}
	if m.cfg.Options != nil {
		opts = *m.cfg.Options
	}
	opts.Password = password

	st, err := tempest.Open(ctx, m.cfg.dbPath(), m.cfg.Mode, &opts)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", m.cfg.dbPath(), err)
	}
	m.st, m.root = st, m.cfg.Dir
	return st, nil
}

// Store returns the open store, or nil before Open.
func (m *Manager) Store() *tempest.Store {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st
}

// Record returns the record store, or nil before Open. Extra path segments
// select a rooted view of the store.
func (m *Manager) Record(path ...string) *record.Store {
	if st := m.Store(); st != nil {
		return st.Record(path...)
	}
	return nil
}

// Fstree returns the in-database filesystem store, or nil before Open. Extra path
// segments select a rooted view.
func (m *Manager) Fstree(path ...string) *fstree.Store {
	if st := m.Store(); st != nil {
		return st.Fstree(path...)
	}
	return nil
}

// Series returns the time-series store, or nil before Open. Extra path segments
// select a rooted view.
func (m *Manager) Series(path ...string) *series.Store {
	if st := m.Store(); st != nil {
		return st.Series(path...)
	}
	return nil
}

// Vector returns the vector store, or nil before Open. Extra path segments
// select a rooted view.
func (m *Manager) Vector(path ...string) *vector.Store {
	if st := m.Store(); st != nil {
		return st.Vector(path...)
	}
	return nil
}

// Search returns the full-text store, or nil before Open. Extra path segments
// select a rooted view.
func (m *Manager) Search(path ...string) *search.Store {
	if st := m.Store(); st != nil {
		return st.Search(path...)
	}
	return nil
}

// Graphs returns the property-graph store, or nil before Open. Extra path
// segments select a rooted view.
func (m *Manager) Graphs(path ...string) *graphs.Store {
	if st := m.Store(); st != nil {
		return st.Graphs(path...)
	}
	return nil
}

// Objects returns the flat object store, or nil before Open. Extra path
// segments select a rooted view.
func (m *Manager) Objects(path ...string) *object.Store {
	if st := m.Store(); st != nil {
		return st.Objects(path...)
	}
	return nil
}

// Close closes the open store. A later Open reopens it.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.st == nil {
		return nil
	}
	err := m.st.Close()
	m.st, m.root = nil, ""
	return err
}

// ── process-wide default ─────────────────────────────────────────────────────

var (
	defMu sync.Mutex
	def   *Manager
)

// Open opens the process-wide store described by cfg.
//
// Repeated calls reuse the default manager. A call whose cfg.Dir differs from the
// currently open directory closes that store and opens the new one, which is what
// lets a test harness isolate each test with its own directory.
func Open(ctx context.Context, cfg Config) (*tempest.Store, error) {
	defMu.Lock()
	if def == nil {
		def = NewManager(cfg)
	} else {
		def.SetConfig(cfg)
	}
	m := def
	defMu.Unlock()
	return m.Open(ctx)
}

// Default returns the process-wide manager, or nil before Open.
func Default() *Manager {
	defMu.Lock()
	defer defMu.Unlock()
	return def
}

// Store returns the process-wide store, or nil before Open.
func Store() *tempest.Store {
	if m := Default(); m != nil {
		return m.Store()
	}
	return nil
}

// Record returns the process-wide record store, or nil before Open. Extra path
// segments select a rooted view.
func Record(path ...string) *record.Store {
	if m := Default(); m != nil {
		return m.Record(path...)
	}
	return nil
}

// Fstree returns the process-wide filesystem store, or nil before Open. Extra
// path segments select a rooted view.
func Fstree(path ...string) *fstree.Store {
	if m := Default(); m != nil {
		return m.Fstree(path...)
	}
	return nil
}

// Series returns the process-wide time-series store, or nil before Open. Extra
// path segments select a rooted view.
func Series(path ...string) *series.Store {
	if m := Default(); m != nil {
		return m.Series(path...)
	}
	return nil
}

// Vector returns the process-wide vector store, or nil before Open. Extra path
// segments select a rooted view.
func Vector(path ...string) *vector.Store {
	if m := Default(); m != nil {
		return m.Vector(path...)
	}
	return nil
}

// Search returns the process-wide full-text store, or nil before Open. Extra
// path segments select a rooted view.
func Search(path ...string) *search.Store {
	if m := Default(); m != nil {
		return m.Search(path...)
	}
	return nil
}

// Graphs returns the process-wide property-graph store, or nil before Open.
// Extra path segments select a rooted view.
func Graphs(path ...string) *graphs.Store {
	if m := Default(); m != nil {
		return m.Graphs(path...)
	}
	return nil
}

// Objects returns the process-wide object store, or nil before Open. Extra path
// segments select a rooted view.
func Objects(path ...string) *object.Store {
	if m := Default(); m != nil {
		return m.Objects(path...)
	}
	return nil
}

// Close closes the process-wide store and forgets it, so the next Open starts
// from the configuration it is given.
func Close() error {
	defMu.Lock()
	m := def
	def = nil
	defMu.Unlock()
	if m == nil {
		return nil
	}
	return m.Close()
}

// ── password file ────────────────────────────────────────────────────────────

// loadOrCreatePassword reads the hex password at path, or creates it with KeySize
// random bytes if it does not exist.
//
// The file is published atomically: the key is written and synced to a
// temporary file in the same directory, which is then hard-linked into place.
// A racing caller therefore sees either no file or a complete one, never the
// empty file that an O_EXCL create exposes between creation and write. Linking
// fails with ErrExist when another caller published first, which is the same
// "exactly one winner" guarantee O_EXCL gave.
func loadOrCreatePassword(path string, mode os.FileMode) (string, error) {
	if data, err := os.ReadFile(path); err == nil {
		return validateKeyFile(path, data)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("store: read key %s: %w", path, err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("store: create key dir: %w", err)
	}
	buf := make([]byte, KeySize)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("store: generate key: %w", err)
	}
	encoded := hex.EncodeToString(buf)

	tmp, err := writeKeyTemp(dir, mode, encoded)
	if err != nil {
		return "", err
	}
	// The temporary name is gone after a successful link; this only removes it
	// on a lost race or a failure.
	defer os.Remove(tmp)

	if err := os.Link(tmp, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			// Lost the race: use the winner's key, so both open the same database.
			return readWinningKey(path)
		}
		// File systems without hard links (FAT, some network mounts) keep the
		// exclusive-create path, which publishes the name before the content.
		return createKeyExclusive(path, mode, encoded)
	}
	syncDir(dir)
	return encoded, nil
}

// writeKeyTemp writes encoded to a new temporary file in dir and returns its
// path. The file is synced before its name is published, so a crash cannot
// leave an empty key file behind.
func writeKeyTemp(dir string, mode os.FileMode, encoded string) (string, error) {
	f, err := os.CreateTemp(dir, ".tempest-key-*")
	if err != nil {
		return "", fmt.Errorf("store: create key temp file: %w", err)
	}
	tmp := f.Name()
	fail := func(err error) (string, error) {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Chmod(mode); err != nil {
		return fail(fmt.Errorf("store: set key mode %s: %w", tmp, err))
	}
	if _, err := f.WriteString(encoded); err != nil {
		return fail(fmt.Errorf("store: write key %s: %w", tmp, err))
	}
	if err := f.Sync(); err != nil {
		return fail(fmt.Errorf("store: sync key %s: %w", tmp, err))
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("store: close key %s: %w", tmp, err)
	}
	return tmp, nil
}

// createKeyExclusive creates the password file with O_EXCL and writes the key
// into it. It is the fallback for file systems that do not support hard links;
// a concurrent reader can briefly observe the empty file it publishes first,
// which readWinningKey tolerates.
func createKeyExclusive(path string, mode os.FileMode, encoded string) (string, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return readWinningKey(path)
		}
		return "", fmt.Errorf("store: create key %s: %w", path, err)
	}
	if _, err := f.WriteString(encoded); err != nil {
		f.Close()
		return "", fmt.Errorf("store: write key %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", fmt.Errorf("store: sync key %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("store: close key %s: %w", path, err)
	}
	return encoded, nil
}

// readWinningKey reads the key file another caller published. It retries for a
// short while because createKeyExclusive publishes the name before the content;
// the hard-link path returns a complete file immediately.
func readWinningKey(path string) (string, error) {
	const (
		budget = 2 * time.Second
		pause  = 10 * time.Millisecond
	)
	deadline := time.Now().Add(budget)
	for {
		data, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("store: read key %s: %w", path, err)
		}
		if err == nil {
			key, verr := validateKeyFile(path, data)
			if verr == nil {
				return key, nil
			}
			if time.Now().After(deadline) {
				return "", verr
			}
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("store: key file %s was not readable after creation", path)
		}
		time.Sleep(pause)
	}
}

// syncDir fsyncs dir so a newly published key file survives a crash. It is
// best-effort: some platforms and file systems do not support directory sync.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

// validateKeyFile checks that path holds a hex password of the expected length.
// The error deliberately names the file but never the value.
func validateKeyFile(path string, data []byte) (string, error) {
	key := strings.TrimSpace(string(data))
	if len(key) != KeySize*2 {
		return "", fmt.Errorf("store: key file %s must contain %d hex characters, found %d", path, KeySize*2, len(key))
	}
	if _, err := hex.DecodeString(key); err != nil {
		return "", fmt.Errorf("store: key file %s is not valid hex", path)
	}
	return key, nil
}
