# fs — a file system inside BoltDB

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/harbor/fs.svg)](https://pkg.go.dev/github.com/malivvan/harbor/fs)
[![CI](https://github.com/malivvan/harbor/actions/workflows/ci.yml/badge.svg)](https://github.com/malivvan/harbor/actions/workflows/ci.yml)

`fs` implements a complete file system on top of bbolt. It supports
directories, regular files, symbolic links, POSIX-like metadata,
`filepath.Walk`-style and concurrent walking, read-only snapshots, an inode
cache, external content back ends, and optional chunked encryption.

A `Store` manages any number of named file systems under one root bucket:

```go
// Through the root package.
vfs, err := store.FS().Open("documents")

// Or directly, with a caller-owned bbolt handle.
fsStore, err := fs.Open(ctx, bdb, fs.Config{
	Root: [][]byte{[]byte("fs")},
})
defer fsStore.Close() // does not close bdb

vfs, err := fsStore.Open("documents")
defer vfs.Close() // does not close bdb
```

`Store.Close` and `FS.Close` release wrappers only. The caller owns the bbolt
handle and must keep it open while the file systems are in use.

`fs.Config` exposes `Validate()` and `Normalize()`. They check the same
invariants as `fs.Open`: `ChunkSize` must be non-negative, and any encryption
mode requires a 32-byte `Secret`.

A store can list and remove the file systems it manages:

```go
names, err := fsStore.FileSystems()
err = fsStore.DeleteFS("documents")
root := fsStore.Root() // copy of the root bucket path
```

## File systems and files

An empty name (`fsStore.Open("")`) uses the store's root bucket itself.

```go
if err := vfs.MkdirAll("/reports/2026", 0o755); err != nil {
	return err
}

f, err := vfs.Create("/reports/2026/q1.txt") // create or truncate
if err != nil {
	return err
}
if _, err := f.Write([]byte("hello")); err != nil {
	return err
}
if err := f.Close(); err != nil {
	return err
}

data, err := vfs.ReadFile("/reports/2026/q1.txt")

info, err := vfs.Stat("/reports/2026/q1.txt")
entries, err := vfs.ReadDir("/reports")
```

`OpenFile` accepts the `fs.O_*` flags (`O_RDONLY`, `O_WRONLY`, `O_RDWR`,
`O_APPEND`, `O_CREATE`, `O_EXCL`, `O_SYNC`, `O_TRUNC`):

```go
f, err := vfs.OpenFile("/reports/2026/q1.txt", fs.O_CREATE|fs.O_WRONLY, 0o644)
```

The returned `absfs.File` supports `Read`, `Write`, `ReadAt`, `WriteAt`, `Seek`,
`Stat`, `Truncate`, `Sync`, `ReadDir`, `Readdir`, `Readdirnames`, `Name` and
`Close`. `Sync` is a no-op because `Write` commits before returning.

Other operations:

```go
err = vfs.Rename("/reports/2026/q1.txt", "/reports/2026/q1-final.txt")
err = vfs.Copy("/reports/2026/q1-final.txt", "/reports/q1-copy.txt")
err = vfs.Truncate("/reports/q1-copy.txt", 0)
err = vfs.Remove("/reports/q1-copy.txt")
err = vfs.RemoveAll("/reports")

err = vfs.Chmod("/reports/2026/q1-final.txt", 0o600)
err = vfs.Chtimes("/reports/2026/q1-final.txt", atime, mtime)
err = vfs.Chown("/reports/2026/q1-final.txt", uid, gid)

mode := vfs.Umask()
vfs.SetUmask(0o022)
```

`Copy` fails when the destination exists. `CopyWithOptions` and `CopyContext`
add overwrite, metadata and symlink control, progress and cancellation:

```go
ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()

err := vfs.CopyContext(ctx, "/reports", "/archive/reports", fs.CopyOptions{
	Overwrite:    true,  // replace a file or an empty directory
	SkipSymlinks: true,  // omit links instead of recreating them
	SkipMetadata: true,  // default permissions, no owner, fresh times
	Progress: func(copied int64) {
		log.Printf("copied %d content bytes", copied)
	},
})
```

The copy is built under a new inode and published only after it succeeds: a
cancelled or failed copy removes its partial output and leaves an existing
destination untouched. Overwriting a non-empty directory returns
`ENOTEMPTY` (clear it first), and replacing a file with a directory (or the
reverse) fails like `Rename`. `Progress` is called with cumulative content
bytes while the file system is locked, so it must not call back into the same
`FS`.

### Links

```go
err := vfs.Symlink("/reports/2026/q1-final.txt", "/latest.txt")

target, err := vfs.Readlink("/latest.txt")
info, err := vfs.Lstat("/latest.txt") // does not follow the link
info, err = vfs.Stat("/latest.txt")   // follows the link
```

There is no public hard-link API; inode-level linking is an implementation
detail used by copy and rename. Renaming and removing entries update the parent
directory's inode and the affected indexes.

### Walking

`fs.Walk` has the shape of `filepath.Walk` and accepts any `absfs.Filer`,
including an `FS` or a `Snapshot`:

```go
err := fs.Walk(vfs, "/", func(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if info.IsDir() && info.Name() == ".trash" {
		return filepath.SkipDir
	}
	fmt.Println(path)
	return nil
})
```

`fs.FastWalk` traverses directories concurrently and passes an `io/fs.DirEntry`
to avoid unnecessary `Stat` calls. The callback may run from several
goroutines, so it must be safe for concurrent use. Returning `filepath.SkipDir`
skips a directory; returning `fs.ErrTraverseLink` makes a symlink be traversed
as a directory (callers are responsible for cycle detection).

```go
err := fs.FastWalk(vfs, "/", func(path string, d iofs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	fmt.Println(path)
	return nil
})
```

Use `fs.FastWalkWithConfig` to control worker count and symlink following:

```go
err := fs.FastWalkWithConfig(vfs, "/", &fs.FastWalkConfig{
	NumWorkers:     8,
	FollowSymlinks: false,
}, fn)
```

## Application files and sessions

Named file systems are a natural fit for application state that is already
file-shaped: session JSON, documents, generated reports.

```go
vfs, err := store.FS().Open("app")
err = vfs.MkdirAll("/sessions", 0o755)
err = writeFile(vfs, "/sessions/alice.json", sessionJSON)
```

The files share the encrypted database, are covered by backups, exports and fs
snapshots, and take part in cross-store transactions through `tx.FS()`. Keep
session semantics (expiry, invalidation, which fields are secret) in the
application: a session file is only as protected as the store password.

## Snapshots

```go
snap, err := vfs.CreateSnapshot("before-import")
if err != nil {
	return err
}
defer snap.Release()

data, err := snap.ReadFile("/reports/2026/q1-final.txt")
entries, err := snap.ReadDir("/reports")
info, err := snap.Stat("/reports/2026/q1-final.txt")

err = snap.CopyToFS("/reports/2026/q1-final.txt", "/restored.txt")
```

Snapshots are read-only. Their metadata and content are materialized into
memory when created, and the bbolt read transaction is released immediately.
This avoids pinning the database mmap or blocking writers, at the cost of
memory proportional to the file system size. Snapshots are not durable.

Write operations on a snapshot return `os.ErrPermission`, and `OpenFile`
permits only read-only access.

A `SnapshotManager` groups snapshots by name:

```go
sm := vfs.NewSnapshotManager()

snap, err := sm.Create("daily")
snap, ok := sm.Get("daily")
names := sm.List()
err = sm.Delete("daily")
err = sm.ReleaseAll()
```

## Inode cache

Each file system keeps a thread-safe LRU cache of inodes (1000 entries by
default). Reads consult the cache; mutations invalidate affected entries.

```go
stats := vfs.CacheStats()
fmt.Printf("hit rate: %.2f%%\n", stats.HitRate())

vfs.SetCacheSize(5000) // 0 or negative disables the cache
vfs.FlushCache()
```

## External content

Content can live in another backend while metadata stays in the database. The
backend implements the exported `ContentFS` interface, which uses only standard
library types and stores whole objects:

```go
type ContentFS interface {
	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte, perm os.FileMode) error
	Remove(name string) error
}
```

`ReadFile` must return an error satisfying `os.IsNotExist` for a missing
object; the FS treats that as an empty file. Neither `ReadFile` nor
`WriteFile` is partial: the FS never seeks or appends. This maps directly onto
an object store (`GET`/`PUT`/`DELETE`) and needs no module-internal types.

An in-module `*fs.FS` can be adapted with `AsContentFS`:

```go
content, err := fsStore.Open("content") // a plain fs.FS in another bucket
if err != nil {
	return err
}

vfs, err := fsStore.OpenWithContentFS("documents", content.AsContentFS())
if err != nil {
	return err
}
```

The same method works on snapshots and on an S3-backed `*fs.FS`. `fs/s3fs`
itself is an `absfs.Filer`; implement `ContentFS` directly over your object
store, or mount S3 through this package, when content should go to S3.
`EncryptContent` seals content before it reaches the backend, so the backend
never holds plaintext in that mode. External content is stored as one sealed
blob per file; chunking applies only to the bbolt `data`/`chunks` buckets.

## Encryption

Encryption is opt-in and per file system. A plaintext file system keeps the
existing on-disk format.

```go
fsStore, err := fs.Open(ctx, bdb, fs.Config{
	Root:            [][]byte{[]byte("fs")},
	Secret:          key, // exactly 32 bytes
	EncryptMetadata: true,
	EncryptContent:  true,
	ChunkSize:       64 * 1024,
})

vfs, err := fsStore.Open("vault")
```

| Field | Effect |
|-------|--------|
| `EncryptMetadata` | Seals inode blobs, hiding names, modes, sizes and times. |
| `EncryptContent` | Seals file content and symlink targets. |
| `ChunkSize` | Stores content as independently sealed chunks of this many plaintext bytes. A positive value implies content encryption. |

`fs.DefaultChunkSize` is 64 KiB. Chunking matters for large files: without it,
the whole file is one sealed blob and every write re-seals all content. With
chunking, a write or truncation touches only the affected chunks.

A per-file-system `FENC` marker records the mode, and a sealed canary verifies
the key at open time:

- opening an encrypted file system without a key returns `fs.ErrWrongMode`,
- a wrong key returns `fs.ErrAuthFailed`,
- changing the mode returns `fs.ErrWrongMode`,
- asking for encryption on a file system that already holds plaintext data
  returns `fs.ErrWrongMode`.

`Store.RotateAll` re-encrypts every file system and the marker under a new key;
`Store.VerifyAll` checks that everything decrypts. `Store.ReEncryptFS` is a
partial rotation for one file system: finish with `RotateAll` before closing
the store. Rotation requires an already encrypted store; there is no in-place
plaintext migration.

See [Encryption](../README.md#encryption) and [AGENTS.md](../AGENTS.md#encryption-internals).

## On-disk layout and limits

Each file system bucket contains `state`, `inodes`, `data`, `symlinks`, and
optional `chunks`/`dirents` buckets. Directories are encoded as a single inode
value containing their children, so creating, removing or renaming an entry is
O(children) for that directory. Very wide directories are therefore expensive
to mutate, though reads and lookups remain indexed.

Snapshots are memory-resident, not durable. Use `CopyToFS` to materialize the
files you need.

## Errors

| Sentinel | Meaning |
|----------|---------|
| `fs.ErrNilParam` | Nil required argument. |
| `fs.ErrNilContext` | Nil context. |
| `fs.ErrIncompatibleVersion` | Unsupported on-disk format. |
| `fs.ErrWrongMode` | Encryption mode does not match the marker. |
| `fs.ErrAuthFailed` | Ciphertext failed authentication. |
| `fs.ErrInvalidKey` | Missing or malformed key. |
| `fs.ErrInvalidConfig` | Inconsistent encryption config. |
| `fs.ErrTraverseLink` | `FastWalk` sentinel: traverse this symlink as a directory. |

Standard file errors (`os.ErrNotExist`, `os.ErrPermission`, `os.ErrExist`) are
returned through `*os.PathError`/`*os.LinkError` where applicable.

## Performance

- Use `Config.ChunkSize` for large files and random writes.
- Reuse one `FS` value; each has its own inode cache.
- Increase the cache for read-heavy, many-inode workloads, or disable it to
  bound memory.
- `FastWalk` is significantly faster than `Walk` on large trees because it
  traverses concurrently and avoids `Stat` when only type information is
  needed.
- A single `Write` merges its inode and content changes into one transaction.

## License

MIT — see [../LICENSE](../LICENSE). Start the whole-module tour at
[../README.md](../README.md).
