package tempest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/malivvan/tempest/internal/bucket"
	"github.com/malivvan/tempest/store/fstree"
	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/object"
	"github.com/malivvan/tempest/store/record"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/system"
	"github.com/malivvan/tempest/store/vector"
	bolt "go.etcd.io/bbolt"
)

// ErrBackupManifest reports a backup whose bytes or metadata do not match the
// manifest written next to it.
var ErrBackupManifest = errors.New("tempest: backup manifest does not match the fstree")

// ErrInvalidBackup reports a fstree that is not a readable tempest database.
var ErrInvalidBackup = errors.New("tempest: invalid backup")

// BackupInfo describes one backup fstree.
type BackupInfo struct {
	// Path is the backup fstree.
	Path string `json:"path"`
	// Size is the fstree length in bytes.
	Size int64 `json:"size"`
	// SHA256 is the lowercase hex SHA-256 of the whole fstree.
	SHA256 string `json:"sha256"`
	// CreatedAt is when the backup was written.
	CreatedAt time.Time `json:"created_at"`
	// Flags is the encryption selection of the source store.
	Flags uint16 `json:"flags"`
	// Version is the db store format version.
	Version string `json:"version"`
}

// BackupOptions tunes BackupFile.
type BackupOptions struct {
	// Manifest writes a JSON sidecar next to the backup describing its size,
	// checksum, flags and format versions.
	Manifest bool
	// Overwrite replaces an existing fstree at the destination. When false, an
	// existing destination fails with os.ErrExist.
	Overwrite bool
}

// backupManifest is the sidecar record written next to a backup.
type backupManifest struct {
	BackupInfo
	Formats map[string]string `json:"formats"`
}

// manifestPath returns the sidecar path for a backup.
func manifestPath(path string) string { return path + ".manifest.json" }

// BackupFile writes a consistent snapshot of the whole database to path and
// returns its size and SHA-256.
//
// The bytes go to a temporary fstree in the same directory, are synced and
// closed, and only then replace path, so a failed or canceled backup never
// truncates an existing fstree. With BackupOptions.Manifest, a JSON sidecar
// records the checksum, creation time, encryption flags and format versions.
//
// The backup is the on-disk representation of an encrypted store, so it is
// encrypted with the same password and flags. Verify it with VerifyBackup
// before restoring.
func (s *Store) BackupFile(ctx context.Context, path string, opts BackupOptions) (BackupInfo, error) {
	var info BackupInfo
	if path == "" {
		return info, fmt.Errorf("tempest: backup: %w: path is required", ErrInvalidOptions)
	}
	if !opts.Overwrite {
		if _, err := os.Stat(path); err == nil {
			return info, fmt.Errorf("tempest: backup: %w: %s already exists", os.ErrExist, path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return info, fmt.Errorf("tempest: backup: %w", err)
		}
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tempest-backup-*")
	if err != nil {
		return info, fmt.Errorf("tempest: backup to %q: %w", path, err)
	}
	tmpPath := tmp.Name()
	discard := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	hasher := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(tmp, hasher)}
	if err := s.Backup(ctx, counter); err != nil {
		discard()
		return info, err
	}
	if err := tmp.Sync(); err != nil {
		discard()
		return info, fmt.Errorf("tempest: backup to %q: sync: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return info, fmt.Errorf("tempest: backup to %q: close: %w", path, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return info, fmt.Errorf("tempest: backup to %q: %w", path, err)
	}

	info = BackupInfo{
		Path:      path,
		Size:      counter.n,
		SHA256:    hex.EncodeToString(hasher.Sum(nil)),
		CreatedAt: time.Now().UTC(),
		Flags:     s.Flags(),
		Version:   record.Version,
	}
	if opts.Manifest {
		if err := writeBackupManifest(path, info); err != nil {
			return info, err
		}
	}
	return info, nil
}

// writeBackupManifest writes the sidecar atomically.
func writeBackupManifest(path string, info BackupInfo) error {
	m := backupManifest{
		BackupInfo: info,
		Formats: map[string]string{
			bucket.RECORD: record.Version,
			bucket.FSTREE: fmt.Sprint(fstree.FormatVersion),
			bucket.SERIES: fmt.Sprint(series.FormatVersion),
			bucket.VECTOR: fmt.Sprint(vector.CollectionFormatVersion),
			bucket.SEARCH: fmt.Sprint(search.FormatVersion),
			bucket.GRAPHS: fmt.Sprint(graphs.FormatVersion),
			bucket.OBJECT: fmt.Sprint(object.FormatVersion),
		},
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("tempest: encode backup manifest: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tempest-manifest-*")
	if err != nil {
		return fmt.Errorf("tempest: write manifest: %w", err)
	}
	tmpPath := tmp.Name()
	remove := func() { _ = os.Remove(tmpPath) }
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		remove()
		return fmt.Errorf("tempest: write manifest: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		remove()
		return fmt.Errorf("tempest: write manifest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		remove()
		return fmt.Errorf("tempest: write manifest: %w", err)
	}
	if err := os.Rename(tmpPath, manifestPath(path)); err != nil {
		remove()
		return fmt.Errorf("tempest: write manifest: %w", err)
	}
	return nil
}

// readBackupManifest reads the sidecar, if present.
func readBackupManifest(path string) (backupManifest, bool, error) {
	raw, err := os.ReadFile(manifestPath(path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return backupManifest{}, false, nil
		}
		return backupManifest{}, false, fmt.Errorf("tempest: read backup manifest: %w", err)
	}
	var m backupManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return backupManifest{}, false, fmt.Errorf("tempest: decode backup manifest: %w", err)
	}
	return m, true, nil
}

// VerifyBackup opens a backup read-only and validates it end to end: the
// metadata canary proves the password and the persisted flags, and every
// sub-store marker and format version is checked. The sidecar manifest, when
// present, must match the fstree's size and checksum.
//
// It never writes to the backup. opts may supply the record codec and bolt
// options; the stored Argon2id parameters and encryption flags always win.
func VerifyBackup(ctx context.Context, path, password string, opts *Options) (BackupInfo, error) {
	var info BackupInfo
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return info, err
	}
	if path == "" {
		return info, fmt.Errorf("tempest: verify backup: %w: path is required", ErrInvalidOptions)
	}
	if password == "" {
		return info, fmt.Errorf("tempest: verify backup: %w: password is required", ErrInvalidPassword)
	}

	fi, err := os.Stat(path)
	if err != nil {
		return info, fmt.Errorf("tempest: verify backup: %w", err)
	}
	sum, err := hashBackup(ctx, path)
	if err != nil {
		return info, err
	}
	info = BackupInfo{
		Path:      path,
		Size:      fi.Size(),
		SHA256:    sum,
		CreatedAt: fi.ModTime().UTC(),
		Version:   record.Version,
	}

	manifest, hasManifest, err := readBackupManifest(path)
	if err != nil {
		return info, err
	}
	if hasManifest {
		if manifest.Size != info.Size || manifest.SHA256 != info.SHA256 {
			return info, fmt.Errorf("%w: size or checksum differs", ErrBackupManifest)
		}
		if manifest.Version != "" && manifest.Version != record.Version {
			return info, fmt.Errorf("%w: manifest version %q, this build supports %q", ErrBackupManifest, manifest.Version, record.Version)
		}
		info.CreatedAt = manifest.CreatedAt
		info.Flags = manifest.Flags
	}

	bopts := opts.boltOptions()
	bopts.ReadOnly = true
	if bopts.Timeout <= 0 {
		bopts.Timeout = time.Second
	}
	var codec record.Codec
	if opts != nil {
		codec = opts.Codec
	}
	bdb, err := bolt.Open(path, 0o600, &bopts)
	if err != nil {
		return info, fmt.Errorf("%w: %w", ErrInvalidBackup, err)
	}
	defer func() { _ = bdb.Close() }()

	md, creating, err := system.ReadMetadata(bdb)
	if err != nil {
		return info, fmt.Errorf("%w: %w", ErrInvalidBackup, err)
	}
	if creating {
		return info, fmt.Errorf("%w: %s holds no tempest metadata", ErrInvalidBackup, path)
	}

	keys, err := system.Derive(password, md.Salt, md.Time, md.Memory, md.Threads)
	if err != nil {
		return info, err
	}
	defer keys.Wipe()
	if err := system.VerifyCanary(keys.Canary, md.Canary, md.Flags); err != nil {
		return info, fmt.Errorf("%w: %w", ErrInvalidPassword, err)
	}
	if hasManifest && manifest.Flags != md.Flags {
		return info, fmt.Errorf("%w: manifest flags %#x, store flags %#x", ErrBackupManifest, manifest.Flags, md.Flags)
	}
	info.Flags = md.Flags

	// Validate every sub-store through its normal open path. A complete backup
	// needs no writes, so a read-only handle succeeds; an incomplete one fails
	// instead of being "repaired" during verification.
	if _, err := record.Open(ctx, bdb, record.Config{
		Root:            namedRoot(bucket.RECORD),
		Codec:           codec,
		Secret:          keys.Record,
		EncryptKeys:     system.HasFlag(md.Flags, EncryptRecordKeys),
		EncryptValues:   system.HasFlag(md.Flags, EncryptRecordValues),
		EncryptMetadata: system.HasFlag(md.Flags, EncryptRecordMetadata),
	}); err != nil {
		return info, fmt.Errorf("tempest: verify backup: record: %w", err)
	}

	fsStore, err := fstree.Open(ctx, bdb, fstree.Config{
		Root:            namedRoot(bucket.FSTREE),
		Secret:          keys.FSTree,
		EncryptMetadata: system.HasFlag(md.Flags, EncryptFSTreeMetadata),
		EncryptContent:  system.HasFlag(md.Flags, EncryptFSTreeContent),
	})
	if err != nil {
		return info, fmt.Errorf("tempest: verify backup: fstree: %w", err)
	}
	names, err := fsStore.FileSystems()
	if err != nil {
		return info, fmt.Errorf("tempest: verify backup: fstree: %w", err)
	}
	for _, name := range names {
		if _, err := fsStore.OpenReadOnly(name); err != nil {
			return info, fmt.Errorf("tempest: verify backup: fstree %q: %w", name, err)
		}
	}

	if _, err := series.Open(ctx, bdb, series.Config{
		Root:               namedRoot(bucket.SERIES),
		Secret:             keys.Series,
		EncryptValues:      system.HasFlag(md.Flags, EncryptSeriesValues),
		EncryptSeriesNames: system.HasFlag(md.Flags, EncryptSeriesNames),
	}); err != nil {
		return info, fmt.Errorf("tempest: verify backup: series: %w", err)
	}

	vecStore, err := vector.Open(ctx, bdb, vector.Config{
		Root:                   namedRoot(bucket.VECTOR),
		Secret:                 keys.Vector,
		EncryptPoints:          system.HasFlag(md.Flags, EncryptVectorPoints),
		EncryptPointIDs:        system.HasFlag(md.Flags, EncryptVectorPointIDs),
		EncryptCollectionNames: system.HasFlag(md.Flags, EncryptVectorCollectionNames),
		SharedDB:               true,
	})
	if err != nil {
		return info, fmt.Errorf("tempest: verify backup: vector: %w", err)
	}
	collections, err := vecStore.ListCollections()
	if err != nil {
		return info, fmt.Errorf("tempest: verify backup: vector: %w", err)
	}
	for _, name := range collections {
		if _, err := vecStore.LoadCollectionMeta(name); err != nil && !errors.Is(err, vector.ErrMetadataNotFound) {
			return info, fmt.Errorf("tempest: verify backup: vector %q: %w", name, err)
		}
	}

	ftsStore, err := search.OpenReadOnly(ctx, bdb, search.Config{
		Root:          namedRoot(bucket.SEARCH),
		Secret:        keys.Search,
		EncryptValues: system.HasFlag(md.Flags, EncryptRecordValues),
	})
	if err != nil {
		return info, fmt.Errorf("tempest: verify backup: search: %w", err)
	}
	indexes, err := ftsStore.Indexes(ctx)
	if err != nil {
		return info, fmt.Errorf("tempest: verify backup: search: %w", err)
	}
	for _, name := range indexes {
		if _, err := ftsStore.Open(name, search.Config{}); err != nil {
			return info, fmt.Errorf("tempest: verify backup: search %q: %w", name, err)
		}
	}

	if bucketExists(bdb, bucket.GRAPHS) {
		graphStore, err := graphs.OpenReadOnly(ctx, bdb, graphs.Config{
			Root:          namedRoot(bucket.GRAPHS),
			Secret:        keys.Graphs,
			EncryptValues: system.HasFlag(md.Flags, EncryptGraphValues) || system.HasFlag(md.Flags, EncryptRecordValues),
		})
		if err != nil {
			return info, fmt.Errorf("tempest: verify backup: graphs: %w", err)
		}
		graphNames, err := graphStore.Names(ctx)
		if err != nil {
			return info, fmt.Errorf("tempest: verify backup: graphs: %w", err)
		}
		for _, name := range graphNames {
			if _, err := graphStore.Open(name); err != nil {
				return info, fmt.Errorf("tempest: verify backup: graph %q: %w", name, err)
			}
		}
	}

	if bucketExists(bdb, bucket.OBJECT) {
		if _, err := object.OpenReadOnly(ctx, bdb, object.Config{
			Root:          namedRoot(bucket.OBJECT),
			Secret:        keys.Object,
			EncryptValues: system.HasFlag(md.Flags, EncryptObjectValues) || system.HasFlag(md.Flags, EncryptRecordValues),
		}); err != nil {
			return info, fmt.Errorf("tempest: verify backup: object: %w", err)
		}
	}

	return info, nil
}

// hashBackup streams the fstree through SHA-256, checking the context between
// reads.
func hashBackup(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("tempest: verify backup: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			if _, werr := h.Write(buf[:n]); werr != nil {
				return "", fmt.Errorf("tempest: verify backup: %w", werr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return "", fmt.Errorf("tempest: verify backup: %w", rerr)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// bucketExists reports whether a top-level bucket exists in bdb.
func bucketExists(bdb *bolt.DB, name string) bool {
	found := false
	_ = bdb.View(func(tx *bolt.Tx) error {
		found = tx.Bucket([]byte(name)) != nil
		return nil
	})
	return found
}

// RestoreOptions tunes RestoreBackup.
type RestoreOptions struct {
	// Overwrite replaces an existing file at the destination. When false, an
	// existing destination fails with os.ErrExist.
	Overwrite bool
}

// RestoreBackup verifies src (a backup written by BackupFile or BackupTo) with
// VerifyBackup and atomically restores it to dst: the bytes go to a temporary
// file in dst's directory, are synced and closed, and only then replace dst by
// rename. A failed or canceled restore therefore never truncates an existing
// destination.
//
// dst must not be the path of a store that is currently open: bbolt holds the
// file lock, and a backup is only restorable while nothing else owns dst.
// Restoring between encryption modes is not possible through a backup — that is
// Export/Import's job, because the backup is the encrypted on-disk image.
func RestoreBackup(ctx context.Context, src, dst, password string, opts *Options, rest RestoreOptions) (BackupInfo, error) {
	var info BackupInfo
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return info, err
	}
	if src == "" {
		return info, fmt.Errorf("tempest: restore backup: %w: src is required", ErrInvalidOptions)
	}
	if dst == "" {
		return info, fmt.Errorf("tempest: restore backup: %w: dst is required", ErrInvalidOptions)
	}
	if password == "" {
		return info, fmt.Errorf("tempest: restore backup: %w: password is required", ErrInvalidPassword)
	}

	// Prove the password, flags, every sub-store marker and the manifest
	// checksum before dst is touched.
	verified, err := VerifyBackup(ctx, src, password, opts)
	if err != nil {
		return info, fmt.Errorf("tempest: restore backup: %w", err)
	}

	if !rest.Overwrite {
		if _, err := os.Stat(dst); err == nil {
			return info, fmt.Errorf("tempest: restore backup: %w: %s already exists", os.ErrExist, dst)
		} else if !errors.Is(err, os.ErrNotExist) {
			return info, fmt.Errorf("tempest: restore backup: %w", err)
		}
	}

	in, err := os.Open(src)
	if err != nil {
		return info, fmt.Errorf("tempest: restore backup: %w", err)
	}
	defer func() { _ = in.Close() }()

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tempest-restore-*")
	if err != nil {
		return info, fmt.Errorf("tempest: restore to %q: %w", dst, err)
	}
	tmpPath := tmp.Name()
	discard := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	hasher := sha256.New()
	counter := &countingWriter{w: io.MultiWriter(tmp, hasher)}
	if _, err := io.Copy(counter, ctxReader{ctx: ctx, r: in}); err != nil {
		discard()
		return info, fmt.Errorf("tempest: restore to %q: %w", dst, err)
	}
	sum := hex.EncodeToString(hasher.Sum(nil))
	if counter.n != verified.Size || sum != verified.SHA256 {
		discard()
		return info, fmt.Errorf("tempest: restore to %q: %w: source changed during the restore", dst, ErrInvalidBackup)
	}
	if err := tmp.Sync(); err != nil {
		discard()
		return info, fmt.Errorf("tempest: restore to %q: sync: %w", dst, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return info, fmt.Errorf("tempest: restore to %q: close: %w", dst, err)
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		_ = os.Remove(tmpPath)
		return info, fmt.Errorf("tempest: restore to %q: %w", dst, err)
	}

	return BackupInfo{
		Path:      dst,
		Size:      counter.n,
		SHA256:    sum,
		CreatedAt: time.Now().UTC(),
		Flags:     verified.Flags,
		Version:   record.Version,
	}, nil
}

// ctxReader aborts a copy when the context is cancelled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (cr ctxReader) Read(p []byte) (int, error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	return cr.r.Read(p)
}
