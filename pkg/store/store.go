// Package store manages the process-wide Harbor database.
//
// A Harbor deployment normally has exactly one database file and one password for
// the whole process. This package owns that convention: it resolves a directory,
// reads or creates the password file, opens the database, and hands out the same
// handle to every caller. Applications that want a different lifecycle can use a
// Manager directly; Open keeps a single default manager for the common case.
//
// # Password file
//
// When Config.Password is empty the password is a 32-byte random value, hex
// encoded (64 characters) and stored at <Dir>/<KeyFile> with mode 0600. The file
// is created with O_EXCL, so two processes racing to initialise the same
// directory agree on one key instead of clobbering each other. The value is never
// logged.
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

	"github.com/malivvan/harbor"
	"github.com/malivvan/harbor/db"
	"github.com/malivvan/harbor/fs"
	"github.com/malivvan/harbor/vec"
)

const (
	// DefaultDBFile is the database file name used when Config.DBFile is empty.
	DefaultDBFile = "harbor.db"
	// DefaultKeyFile is the password file name used when Config.KeyFile is empty.
	DefaultKeyFile = "harbor.key"
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
	// Options are extra Harbor options (KDF cost, encryption flags, metrics).
	// The Password field is always taken from Config, not from here.
	Options *harbor.Options
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

// Manager owns one Harbor store for one directory at a time.
//
// A Manager is safe for concurrent use. Its zero value is not usable; build one
// with NewManager.
type Manager struct {
	mu   sync.Mutex
	cfg  Config
	st   *harbor.Store
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
func (m *Manager) Open(ctx context.Context) (*harbor.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.openLocked(ctx)
}

func (m *Manager) openLocked(ctx context.Context) (*harbor.Store, error) {
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

	opts := harbor.Options{}
	if m.cfg.Options != nil {
		opts = *m.cfg.Options
	}
	opts.Password = password

	st, err := harbor.Open(ctx, m.cfg.dbPath(), m.cfg.Mode, &opts)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", m.cfg.dbPath(), err)
	}
	m.st, m.root = st, m.cfg.Dir
	return st, nil
}

// Store returns the open store, or nil before Open.
func (m *Manager) Store() *harbor.Store {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st
}

// DB returns the record store, or nil before Open.
func (m *Manager) DB() *db.Store {
	if st := m.Store(); st != nil {
		return st.DB()
	}
	return nil
}

// FS returns the in-database filesystem store, or nil before Open.
func (m *Manager) FS() *fs.Store {
	if st := m.Store(); st != nil {
		return st.FS()
	}
	return nil
}

// Vec returns the vector store, or nil before Open.
func (m *Manager) Vec() *vec.Store {
	if st := m.Store(); st != nil {
		return st.Vec()
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
func Open(ctx context.Context, cfg Config) (*harbor.Store, error) {
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
func Store() *harbor.Store {
	if m := Default(); m != nil {
		return m.Store()
	}
	return nil
}

// DB returns the process-wide record store, or nil before Open.
func DB() *db.Store {
	if m := Default(); m != nil {
		return m.DB()
	}
	return nil
}

// FS returns the process-wide filesystem store, or nil before Open.
func FS() *fs.Store {
	if m := Default(); m != nil {
		return m.FS()
	}
	return nil
}

// Vec returns the process-wide vector store, or nil before Open.
func Vec() *vec.Store {
	if m := Default(); m != nil {
		return m.Vec()
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
func loadOrCreatePassword(path string, mode os.FileMode) (string, error) {
	if data, err := os.ReadFile(path); err == nil {
		return validateKeyFile(path, data)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("store: read key %s: %w", path, err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("store: create key dir: %w", err)
	}
	buf := make([]byte, KeySize)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("store: generate key: %w", err)
	}
	encoded := hex.EncodeToString(buf)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Lost the race with another process: use the winner's key, so both
			// processes open the same database.
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				return "", fmt.Errorf("store: read key after race: %w", rerr)
			}
			return validateKeyFile(path, data)
		}
		return "", fmt.Errorf("store: create key %s: %w", path, err)
	}
	if _, err := f.WriteString(encoded); err != nil {
		f.Close()
		return "", fmt.Errorf("store: write key %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("store: close key %s: %w", path, err)
	}
	return encoded, nil
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
