# BoltFS - A Complete Filesystem Implementation for BoltDB

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/storm/fs.svg)](https://pkg.go.dev/github.com/malivvan/storm/fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/malivvan/storm/fs)](https://goreportcard.com/report/github.com/malivvan/storm/fs)
[![CI](https://github.com/malivvan/storm/fs/actions/workflows/ci.yml/badge.svg)](https://github.com/malivvan/storm/fs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

BoltFS provides a full featured filesystem on top of a boltdb database. This
package implements most of the filesystem functions from the `os` standard
library package, even including support for symbolic links.

## Features

- Compatible with the abstract filesystem interface `absfs.SymlinkFileSystem`
- Support for hard and soft linking
- Walk method like `filepath.Walk`
- **Thread-safe in-memory inode cache** for improved performance
- **Snapshot support** using BoltDB's MVCC for point-in-time filesystem views
- **External content storage** for storing file content outside BoltDB (filesystem, S3, etc.)
- Extensive tests with >90% coverage

### Inode Cache

The inode cache provides significant performance improvements by caching frequently accessed inodes in memory:

```go
bdb, _ := bolt.Open("myfs.db", 0644, nil)
defer bdb.Close()

store, _ := fs.Open(ctx, bdb, nil)
filesystem, _ := store.Open("")
defer filesystem.Close()

// Get cache statistics
stats := filesystem.CacheStats()
fmt.Printf("Hit rate: %.2f%%\n", stats.HitRate())

// Adjust cache size (default: 1000)
filesystem.SetCacheSize(5000)

// Flush cache if needed
filesystem.FlushCache()
```

### Store-managed file systems

A `Store` wraps an existing bbolt database and a root bucket, then manages
multiple named file systems beneath that root:

```go
bdb, _ := bolt.Open("myfs.db", 0644, nil)
defer bdb.Close()

store, _ := fs.Open(ctx, bdb, []byte("tenant"))
defer store.Close()

docs, _ := store.Open("docs")
media, _ := store.Open("media")

names, _ := store.FileSystems()
_ = store.DeleteFS("media")
```

The `Store` borrows `bdb`; neither `Store.Close` nor a file system's `Close`
closes the native database. Use `Store.Open` / `Store.OpenWithContentFS` for
file systems managed by a Store.

### Snapshots

Create read-only point-in-time views of the filesystem:

```go
// Create a snapshot
snapshot, _ := fs.CreateSnapshot("backup-2024")
defer snapshot.Release()

// Read files from snapshot
data, _ := snapshot.ReadFile("/config.json")

// Walk snapshot filesystem
snapshot.Walk("/", func(path string, info os.FileInfo, err error) error {
    fmt.Println(path)
    return nil
})

// Restore files from snapshot
snapshot.CopyToFS("/config.json", "/config-restored.json")

// Use snapshot manager for multiple snapshots
sm := fs.NewSnapshotManager()
sm.Create("daily-backup")
snap, _ := sm.Get("daily-backup")
```

### External Content Storage

Store file content in any `absfs.FileSystem` implementation while keeping metadata in BoltDB:

```go
import (
    "github.com/malivvan/storm/fs"
    "github.com/absfs/memfs"  // or any absfs implementation
)

// Use default BoltDB storage.
bdb, _ := bolt.Open("myfs.db", 0644, nil)
defer bdb.Close()
store, _ := fs.Open(ctx, bdb, nil)
filesystem, _ := store.Open("")
defer filesystem.Close()

// Use memfs for file content (in-memory storage).
contentFS, _ := memfs.NewFS()
filesystem, _ = store.OpenWithContentFS("", contentFS)
defer filesystem.Close()

// Or set content filesystem on an existing file system.
contentFS, _ = memfs.NewFS()
filesystem.SetContentFS(contentFS)
```

**Custom Storage Backends:**

Any `absfs.FileSystem` implementation can be used:

```go
// Example: osfs for local filesystem storage
type OSFileSystem struct {
    basePath string
}

func (fs *OSFileSystem) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
    // Implement absfs.FileSystem interface
    // Store files in basePath + name
}
// ... implement other absfs.FileSystem methods

// Use custom filesystem
osfs := &OSFileSystem{basePath: "/data/content"}
filesystem, _ := store.OpenWithContentFS("", osfs)
```

**Benefits:**
- **Idiomatic design**: Uses standard `absfs.FileSystem` interface
- **Composable**: Chain/layer multiple absfs implementations
- **Rich ecosystem**: Use any existing absfs implementation (memfs, osfs, s3fs, etc.)
- **Backward compatible**: Defaults to BoltDB storage
- **Automatic cleanup**: Files deleted from external storage when removed

## Optional encryption

Encryption is opt-in and leaves the plaintext bbolt format untouched when it is
not configured. Open a Store with `OpenWithConfig`:

```go
key := loadKey() // exactly fs.KeySize (32) bytes
store, err := fs.OpenWithConfig(ctx, bdb, nil, &fs.Config{
    Key:             key,
    EncryptMetadata: true, // seal inode blobs: names, modes, sizes, times
    EncryptContent:  true, // seal file content and symlink paths
})
filesystem, err := store.Open("vault")
```

- `EncryptMetadata` seals gob-encoded inodes; inode numbers and structural
  bucket keys stay clear.
- `EncryptContent` seals file content and symlink paths.
- `ChunkSize` (bytes) stores content as independently sealed chunks, so random
  reads, writes and truncation touch only the affected chunks.
- A per-filesystem `FENC` marker records the mode, and a sealed canary verifies
  the key at open time (wrong key returns `fs.ErrAuthFailed`).
- Opening an encrypted filesystem without a key, changing the mode, or asking
  for encryption on a filesystem that already holds plaintext data returns
  `fs.ErrWrongMode`.
- `RawKeyProvider`, `EnvKeyProvider` and `MultiKeyProvider` supply keys.
- `ReEncryptFS`, `RotateAll` and `VerifyAll` rotate keys and verify data.

External content filesystems configured with `SetContentFS` remain plaintext;
encryption applies to content stored in the bbolt data/chunk buckets.

## Coming soon

- Improved test coverage
- FastWalk high performance walker (non sorted, os.FileMode only)

## License

MIT license. See LICENSE file for more information.

