# AGENTS.md

This file is for contributors, reviewers and coding agents working on Harbor.
User-facing documentation lives in [README.md](README.md); per-package guides
live next to their code (`store/record/README.md`, `store/file/README.md`, `store/series/README.md`,
`store/vector/README.md`, `store/file/s3fs/README.md`). Keep it that way: do not recreate a
separate `docs/` tree.

## Repository map

| Package | Responsibility |
|---------|----------------|
| `github.com/malivvan/harbor` | Opens one bbolt file, derives per-store keys from a password, and exposes `Record()`, `File()`, `Series()`, `Vector()`, `Search()`, `Flags()` and `Meta()`. Each accessor takes optional bucket-path segments. |
| `.../store/record` | Struct-to-bucket mapping, indexes, matchers, managed transactions, key/value access, codecs. |
| `.../graph` | A reusable bitemporal property graph over `record`: nodes, directed edges, adjacency and traversal, counts, history/as-of/diff/purge, JSON interchange. |
| `.../store/file` | Named file systems implementing the `absfs` interfaces, with symlinks, snapshots, inode cache and chunked content. |
| `.../store/series` | Named time series with ordered scans, aggregation, labels and retention. |
| `.../store/vector` | Vector collections with Flat/HNSW indexes, payload filters and quantization. |
| `.../store/search` | Full-text indexes: BM25 ranking, word and CJK tokenizers, posting lists and corpus statistics. |
| `.../store/file/s3fs` | An `absfs.Filer` backed by S3-compatible object storage. |
| `.../store/vector/hnsw` | The vendored HNSW graph implementation used by `vector`. |
| `.../store/vector/hnsw/heap` | The heap used on the HNSW search hot path. |
| `.../store/vector/doc` | Document-oriented facade over `vector`: id, content, embedding and string metadata; the collection dimension is taken from the first upsert. |
| `.../pkg/metrics` | Dependency-free observer interface and nil-safe metric helpers used by every store; a nil observer means no instrumentation. |
| `.../pkg/audit` | Tamper-evident, append-only audit trail: signed, hash-linked events for every store write. |
| `.../pkg/consensus` | Raft-backed cluster replication of a whole store. |
| `.../pkg/store` | Process-wide database manager: resolves a directory, reads or creates the password file (`O_EXCL`, mode 0600, never logged), and opens one handle. |
| `.../pkg/blob` | Flat, contained object store over a named file system: `..` keys are rejected and `Put` installs through a temporary sibling rename. |
| `.../pkg/snapshot` | Content-addressed incremental snapshots, restore, retention and scheduling over an `ObjectStore`; manifests are hashed and verified before restore. |
| `.../pkg/snapshot/blobstore` | Adapter from `snapshot.ObjectStore` to `blob`. |
| `.../pkg/id` | ULID generation: 26-character Crockford base32, time-sortable. |
| `.../pkg/sign/canonical` | Deterministic JSON encoding for signatures and content addressing: sorted keys, stable numbers, no whitespace. |
| `.../pkg/fusion` | Reciprocal rank fusion for combining full-text and vector result lists. |
| `.../internal/...` | Shared bbolt helpers, AEAD primitives, Argon2id/BLAKE2b, SLIP-0010/0021 and CPU detection. Not public API. |

The stores are independent. The root package is a convenience composition,
not a required layer: each store can be opened directly with a caller-owned
`*bolt.DB` and its own 32-byte key.

## Architecture

### One file, five key spaces

The root package creates one bbolt database with this top-level shape:

```text
harbor.Metadata            unencrypted configuration record
db/                        record store root bucket (persisted name)
fs/                        file store root bucket (persisted name)
ts/                        series store root bucket (persisted name)
vec/                       vector store root bucket (persisted name)
fts/                       search store root bucket (persisted name)
```

The Go package names (`record`, `file`, `series`, `vector`, `search`) differ
from these persisted bucket names on purpose: the bucket names are part of the
on-disk format and do not change. The no-argument accessors use them; passing
explicit path segments selects a different root.

`harbor.Metadata/Metadata` contains the Argon2id salt and cost parameters, the
encryption flags, and a sealed canary. It is the only record that must be read
before any key exists. See [Encryption internals](#encryption-internals) for the
key pipeline.

Each sub-store is opened under its own root bucket, so names cannot collide
with application buckets. A store used directly can choose any root path, or
the database root for backward compatibility with Storm-style layouts.

### record layout

Inside the configured root:

```text
User/                          one bucket per mapped struct
  <primary key>                encoded record value
  __metadata/
    codec                      codec name
    <field>                    auto-increment counters
  __harbor_index_<Field>/      list or unique index bucket
__database/
  version                      "1.0.0"
  \x00__db_enc__                optional encryption marker (DENC)
```

- Records are keyed by their encoded primary key and stored with the codec's
  `Marshal` output.
- `__metadata` is created per mapped bucket and records the codec name. Opening
  a bucket with a different codec returns `ErrDifferentCodec`.
- Index buckets use the reserved `__harbor_index_` prefix. List indexes map
  value → primary keys; unique indexes enforce uniqueness.
- `__database` holds the on-disk version and, when encryption is enabled, the
  `DENC` marker with its canary. Unknown versions return
  `ErrIncompatibleVersion`.
- Bucket names and the codec name remain readable even when record keys and
  values are encrypted.

`Attach` binds the same operations to a caller-owned transaction. It resolves
the store's root bucket in that transaction and never commits or rolls back.

### file layout

Each named file system is a bucket containing:

```text
state/        format version and the optional FENC encryption marker
inodes/       encoded inode records (directory children, modes, sizes, times)
data/         file content (whole blobs, or symlink-free content when not chunked)
symlinks/     symlink targets
dirents/      optional per-entry directory keys
chunks/       optional independently sealed content chunks
```

- The root inode is inode 1. Directory inodes contain their children, so
  creating, removing or renaming an entry rewrites that directory's inode.
- File content lives in `data` unless chunked, in which case sealed chunks live
  in `chunks`. `Config.ChunkSize` selects chunking and implies content
  encryption.
- `state` records format version 1 and the `FENC` marker when the file system
  is encrypted. A mismatched marker or version fails the open instead of
  guessing.
- Each `FS` maintains an inode cache (default 1000 entries, LRU eviction) for
  reads. The cache is not part of the persisted format.
- Snapshots materialize inodes and content into memory and release the bbolt
  read transaction immediately, so a snapshot never pins the database mmap.

### series layout

Inside the configured root:

```text
__ts_meta__/                  series metadata, keyed by series name
<series>/                     one bucket per series (name may be AES-SIV encrypted)
  <timestamp key>             point value
```

- Plain point keys are 8-byte big-endian `uint64(timestamp) ^ (1<<63)`, which
  sorts int64 timestamps chronologically. Values are the 8-byte IEEE-754 bits
  of the `float64`, or a sealed value when `EncryptValues` is enabled.
- With `BlockDuration`, points are grouped into blocks keyed by the block start
  timestamp. Each block carries a small header and 16-byte `(timestamp, value)`
  records, and the whole block is sealed once. Reads of a single point locate
  and open its block.
- `__ts_meta__` stores labels, retention policy and a live point count. The
  count lets `Count` and satisfied retention policies avoid decoding data.
- `EncryptSeriesNames` derives the physical bucket and metadata names with
  AES-SIV. The logical name is recovered when listing.
- A `TENC` marker and a version record live in `__ts_meta__`.

### vector layout

Inside the configured root:

```text
__collections_meta__/         collection metadata records
<collection>/                 one bucket per collection (name may be encrypted)
  <point ID>                  encoded StoredPoint
```

- Point values use the hand-written, positional binary format documented in
  `vec/binary.go`. Payload keys are sorted so the encoding is deterministic.
- Each point carries a store-managed `Version`; caller-provided versions are
  not trusted.
- `CollectionMeta` persists the vector dimension, metric, index type, HNSW
  parameters, quantization mode, format version and schema fingerprints. A
  reordered or incompatible binary codec is rejected on load instead of
  silently reinterpreting old blobs.
- Search indexes (Flat or HNSW) live in memory. Opening a collection reloads
  its points from disk into the index.
- `Upsert` updates the in-memory index immediately and appends to a buffer.
  When the buffer reaches `DefaultFlushThreshold` (64 points), the batch is
  written in one transaction; `Flush` forces the same path.
- A `VENC` marker and canary live in the metadata bucket.

### search layout

Inside the configured root:

```text
<index>/
  __version__                8-byte format revision (1)
  __mode__                   plaintext encryption-mode bits
  __meta__                   document count, total length, K1/B, tokenizer name
  docs/<id>                  document length and term frequencies
  terms/<term>               posting list (term keys may be deterministically encrypted)
```

- Values (documents, posting lists and metadata) are sealed when value
  encryption is enabled; the root store follows `EncryptDBValues`.
- Term keys are plaintext by default. `Config.EncryptTokens` encrypts them with
  AES-SIV, which hides the vocabulary but leaks term equality and result sizes.
- The tokenizer and BM25 parameters are persisted and checked on reopen; a
  mismatch is `ErrInvalidConfig`.
- `Upsert` removes the previous postings before adding the new ones and adjusts
  the document count and total length in one transaction.

### Transactions and concurrency

bbolt provides one writer and many readers, and Harbor follows that model:

- A `record.Store` is safe for concurrent use. CRUD operations use short
  transactions; `ReadTransaction` and `WriteTransaction` provide explicit
  callback boundaries.
- `Attach` lets a caller own the bbolt transaction and mix Harbor operations
  with raw bucket writes. Harbor never opens a nested managed transaction for
  an attached handle.
- `record.TransactionManager`, `file.Walk`, `series.Store`, and `vector.Collection` all use
  the same bbolt handle and therefore share the single-writer constraint.
- `Store.WriteTransaction`/`ReadTransaction` bind every sub-store to one
  caller-owned bbolt transaction. `file`, `series` and `vector` carry an optional `tx`
  field: when set, their `view`/`update` helpers execute against it instead of
  opening a managed transaction, and `WithTransaction` builds the bound view
  without tracking it in the parent. Rotation is rejected on a bound store
  (`ErrInTransaction`). Audit events cannot be recorded inside the transaction
  (the recorder opens its own write transaction), so bound views receive a
  collecting sink and the root replays it after commit; `record.AttachWithAudit`
  provides the same for the record store. `vector` stages writes in its bound
  view and calls `AdoptTransaction` after commit to install the updated
  in-memory indexes, so no point is re-read. Write transactions are serialized
  by `Store.txMu`; nesting is unsupported.
- The root `Store` guards close against in-flight access with a read/write
  mutex. After `Close`, the sub-store accessors return `nil`.
- Vector writes are buffered; `Store.Close` flushes the vector store before
  closing the shared handle. When embedding `vector` with a shared handle, call
  `Flush` instead of `Close`, or call `SetOwnsDB(false)`.
- Contexts are checked at operation boundaries and loop checkpoints.
  Cancellation never interrupts a bbolt lock wait, a syscall, codec code, or
  an unreturned callback, and it never rewrites a committed write or a
  published read result.

### Buffering and fsync

Every bbolt commit is an fsync. Harbor batches where it can:

- `record.WriteTransaction` groups entity operations into one write transaction.
- `series.Series.InsertMany` writes a batch in one transaction.
- `vector.Collection.Upsert` buffers points and flushes in batches.
- `file` merges the inode and content writes of a single `Write` into one
  transaction, and chunking avoids rewriting unaffected content.

### Compatibility and versioning

- The module requires Go 1.27.4 or later.
- The first release has exactly one on-disk version, number 1, and no v2 or
  dual-version compatibility path. The exported constants are `record.Version`
  (`"1.0.0"`), `file.FormatVersion`, `series.FormatVersion`,
  `vector.CollectionFormatVersion` and `audit.FormatVersion`; all are 1.
- `record.Version` is stored in `__database`. Opening a different version returns
  `ErrIncompatibleVersion`; file, series and vector likewise check their persisted
  revision on open and fail closed.
- Key derivation labels, canary domains and SIV name prefixes are also `/v1`,
  so keys and formats advance together. A future format change bumps the
  relevant constant and ships a compatibility path; unknown versions are never
  reinterpreted.
- Encryption markers are versioned (`DENC`, `FENC`, `TENC`, `VENC`), and the
  sealed canary binds the mode flags.
- Vector schema fingerprints detect a reordered or otherwise incompatible
  point codec.
- There is no plaintext ↔ encrypted in-place migration; use `Export`/`Import`
  or rotate through sub-stores opened with caller-managed keys.

## internal/absfs

`internal/absfs` is the vendored abstraction layer behind `file`, `store/file/s3fs`,
`file.Snapshot`, and `file.Walk`/`FastWalk`. It is internal: other modules cannot
import it. The external-content hook is not part of it: `file.ContentFS` is an
exported whole-object interface using standard library types only. The old
per-package absfs Markdown documents were consolidated into this section and
the user-facing path/thread notes in
[README.md](README.md#the-absfs-interface-and-paths); keep both in sync when
changing the interfaces.

### Interfaces

`absfs` defines four file-system interfaces and a small file hierarchy.

```go
type Filer interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Mkdir(name string, perm os.FileMode) error
	Remove(name string) error
	Rename(oldpath, newpath string) error
	Stat(name string) (os.FileInfo, error)
	Chmod(name string, mode os.FileMode) error
	Chtimes(name string, atime, mtime time.Time) error
	Chown(name string, uid, gid int) error

	// io/fs compatibility
	ReadDir(name string) ([]fs.DirEntry, error)
	ReadFile(name string) ([]byte, error)
	Sub(dir string) (fs.FS, error)
}

type FileSystem interface {
	Filer
	Chdir(dir string) error
	Getwd() (dir string, err error)
	TempDir() string
	Open(name string) (File, error)
	Create(name string) (File, error)
	MkdirAll(name string, perm os.FileMode) error
	RemoveAll(path string) error
	Truncate(name string, size int64) error
}

type SymLinker interface {
	Lstat(name string) (os.FileInfo, error)
	Lchown(name string, uid, gid int) error
	Readlink(name string) (string, error)
	Symlink(oldname, newname string) error
}

type SymlinkFileSystem interface {
	FileSystem
	SymLinker
}
```

`ReadOnlyFiler` and `WriteOnlyFiler` are the minimal one-file interfaces used
by wrappers. `Separator` is always `/` and `ListSeparator` is always `:`,
regardless of the host OS.

### File hierarchy

- `UnSeekable`: `Read`, `Write`, `Close`, `Stat`, `Readdir`, `Sync`, `Name`.
- `Seekable`: `UnSeekable` plus `Seek`.
- `File`: `Seekable` plus `ReadAt`, `WriteAt`, `WriteString`, `Truncate`,
  `Readdirnames`, `ReadDir`, and the other convenience methods.
- `ExtendSeekable(Seekable) File` adds the higher-level methods by composing
  `Seek`, `Read`/`Write`, and `Readdir`; use it when a backend can only provide
  the minimal seekable file.

### ExtendFiler and optional interfaces

`ExtendFiler(Filer) FileSystem` wraps a minimal `Filer`, keeps a per-instance
current working directory, and supplies:

- `TempDir()` → `/tmp` for virtual file systems.
- `Chdir`/`Getwd` backed by the wrapper's `cwd`.
- `Open` (`O_RDONLY`), `Create` (`O_RDWR|O_CREATE|O_TRUNC`, `0666`).
- `MkdirAll`, `RemoveAll`, `Truncate`.

Relative paths are converted to absolute paths with the wrapper's `cwd` before
they reach the `Filer`, unless the `Filer` implements `dirnavigator` and
handles relative paths itself. `Filer` implementations therefore only need to
accept absolute paths.

A `Filer` can override any default by implementing one of the optional
interfaces: `opener`, `creator`, `mkaller`, `remover`, `dirReader`,
`fileReader`, `subber`, `dirnavigator`, `temper`, `truncater`, or
`symlinker`. `ExtendFiler` uses type assertions once per call; a missing
interface gets the default implementation. `FilerToFS` builds a read-only
`fs.FS` view rooted at a directory.

### Implementer contracts

- **OpenFile**: support `O_RDONLY`, `O_WRONLY`, `O_RDWR`, `O_CREATE`,
  `O_APPEND`, `O_TRUNC`, `O_EXCL`, `O_SYNC`. Return `os.ErrExist` for
  `O_CREATE|O_EXCL` on an existing file, `os.ErrNotExist` when the file is
  missing and `O_CREATE` is unset, and apply `perm` when creating.
- **Mkdir**: create only the final component; return `os.ErrNotExist` when the
  parent is missing, `os.ErrExist` when the entry exists, and fail if the entry
  is a file.
- **Remove**: remove a file or an empty directory; return an error for a
  non-empty directory and `os.ErrNotExist` for a missing path. Recursive
  removal belongs to `RemoveAll`.
- **Rename**: rename or move files and directories within the file system;
  return `os.ErrNotExist` for a missing source or parent, and document the
  platform-specific replace behavior.
- **Stat**: return an `os.FileInfo` with name, size, type/mode bits, mod time
  and `IsDir`; follow symlinks; return `os.ErrNotExist` when absent.
- **Chmod**: change permission bits; return `os.ErrNotExist` when absent.
  Permission support is advisory for virtual/cloud backends.
- **Chtimes**: update access and modification times; return `os.ErrNotExist`
  when absent. A backend may support only modification time.
- **Chown**: change uid/gid; `-1` leaves that value unchanged; return
  `os.ErrNotExist` when absent. Virtual backends may no-op or return an error.
- **ReadDir**: return entries sorted by filename; return `os.ErrNotExist` when
  absent and an error when the path is not a directory. Entries implement
  `Name`, `IsDir`, `Type` and `Info`.
- **ReadFile**: read the entire file; return `os.ErrNotExist` when absent and
  an error for a directory.
- **Sub**: return a read-only `fs.FS` rooted at an existing directory;
  relative names use `fs.ValidPath`; return `os.ErrNotExist` or a
  not-a-directory error when appropriate.

Use standard `os` errors (`ErrNotExist`, `ErrExist`, `ErrPermission`,
`ErrInvalid`, `ErrNotSupported`) and wrap backend-specific failures with
`%w`. Be consistent: `Stat`, `Remove`, `Rename`, `Chmod`, `Chtimes` and
`Chown` return `os.ErrNotExist`; `OpenFile` distinguishes missing files from
`O_CREATE`; `Mkdir` distinguishes existing directories from missing parents.

### Path rules

- Every absfs path is Unix-style and slash-separated, on every platform.
- Windows drives use `/c/`, `/d/`, etc.; UNC paths use
  `//server/share/path`. Upstream `osfs` converts these to native paths.
- A path starting with `/` (or `\`) is virtual-absolute and portable.
- Use `path.Join`/`path.Clean` for virtual paths. Use `filepath` only when
  bridging to a native OS API, because `filepath.Join` may introduce `\`.
- `Filer` methods receive absolute, ideally cleaned paths. Still validate and
  clean defensively before touching storage.
- `Sub` and `FilerToFS` must enforce `fs.ValidPath` and must not allow `..` or
  absolute names to escape the subtree.
- Upstream `osfs` provides the conversion helpers `ToNative`/`FromNative`,
  drive helpers (`GetDrive`, `SplitDrive`, `SetDrive`, `JoinDrive`), UNC
  helpers (`IsUNC`, `SplitUNC`, `JoinUNC`) and validation helpers
  (`ValidatePath`, `IsReservedName`). They are external osfs utilities, not
  part of this module.
- The historical `WindowsDriveMapper` build-tag pattern is obsolete: upstream
  `osfs` now handles `/c/...` internally. Do not reintroduce that wrapper in
  this module; use the `/c/...` convention directly.

Platform summary:

| Operation | Unix | Windows |
|-----------|------|---------|
| `Separator` / `ListSeparator` | `/` / `:` | `/` / `:` |
| `Getwd()` | `/home/user` | `/c/Users/user` |
| `TempDir()` | `/tmp` | `/c/Users/user/AppData/Local/Temp` |
| `Open("/foo")` | `/foo` | current drive, `C:\foo` |
| `Open("/c/foo")` | `/c/foo` | `C:\foo` |

### Security

- Reject empty paths, NUL bytes and traversal that escapes the configured root.
  Callers may pass a root-relative path; a `Filer` must not let `..` climb above
  its logical root.
- Permissions are advisory: enforcement depends on the backend. Do not rely on
  `Chmod` for isolation.
- The abstraction layer does not encrypt data and does not audit operations.
  Encryption belongs to the Harbor stores (`record`, `file`, `series`, `vector`); the
  `internal/absfs` layer must remain a transparent pass-through.
- Apply resource limits where untrusted paths or sizes are accepted; do not log
  file contents, credentials or keys.
- Permission constants are security-relevant. `OS_ALL_RWX` must be `0777`; a
  historical bug produced `0776` and is covered by tests. Keep such constants
  covered.
- Only the latest release is supported. Security fixes are released as new
  tags; report issues privately to the maintainers with the affected path,
  reproduction steps and impact. Do not open a public issue first.

### Thread safety

The generic `ExtendFiler` wrapper is **not** goroutine-safe: it mutates a
per-instance `cwd`. Safe patterns:

1. One wrapper per goroutine.
2. Use absolute paths only on a shared wrapper.
3. Guard `Chdir` plus relative-path operations with a mutex.

The concrete `*file.FS` serializes its own cwd and inode mutations, but callers
should still prefer absolute paths for shared instances. Individual `File`
values should be used by a single goroutine unless the implementation
explicitly documents otherwise.

### Composition and testing

- absfs is designed for layering: base/root constraints, copy-on-write
  overlays, read-only wrappers and cache-on-read wrappers all implement
  `Filer`/`FileSystem` and delegate to the layer below.
- Upstream implementations (`osfs`, `memfs`, `basefs`, `rofs`, `cowfs`,
  `corfs`, `s3fs`, `sftpfs`) are external projects and are not dependencies of
  this module. In this repository the concrete implementations are `*file.FS`,
  `*file.Snapshot`, and the `fs/s3fs` `Filer` adapter.
- Test implementations through the interfaces: basic file/directory
  operations, error conditions (`ErrNotExist`, `ErrExist`, `ErrPermission`),
  `ExtendFiler` defaults, `Sub`/`io/fs` views, and concurrent access. Use
  `testing/fstest` for `io/fs` conformance where applicable.
- Keep compile-time assertions for the interfaces a type claims to implement
  (for example `var _ absfs.FileSystem = (*Snapshot)(nil)`).
- Performance guidance: batch directory reads, cache stable stat results with
  invalidation, and pool large temporary buffers; keep the wrapper pass-through
  paths allocation-light.

### User-facing practices

- Accept `absfs.FileSystem`/`Filer` interfaces in APIs rather than concrete
  types.
- Always close files; check errors; use `MkdirAll` before creating nested
  files; call `Sync` for data that must be durable.
- For an atomic write, write a temporary sibling, `Sync`, close it, then
  `Rename` it into place.
- Copy files with `io.Copy` between `Open` and `Create`; walk trees with
  `file.Walk`/`FastWalk` for `Filer` values, or `fs.WalkDir` for read-only
  `io/fs` views.
- `ReadDir`, `ReadFile` and `Sub` follow `io/fs`; `Sub` is read-only.

## Encryption internals

The user-facing flag table, opening-mode errors, rotation guide and threat
model live in [README.md](README.md#encryption). This section documents the
implementation contracts that must not drift.

### Key derivation

A Harbor password never reaches the database. When the root `harbor` package
creates a store it performs this pipeline:

```text
password
   │  Argon2id(salt, time, memory, threads)
   ▼
32-byte seed
   │  SLIP-0021 master node
   ▼
master key
   ├── harbor/canary/v1  → canary key
   ├── harbor/db/v1      → record store key
   ├── harbor/fs/v1      → file store key
   ├── harbor/ts/v1      → series store key
   ├── harbor/vec/v1     → vector store key
   └── harbor/fts/v1     → search store key
```

- The salt is 16 random bytes (`harbor.SaltSize`) generated for each new store.
- Argon2id produces a 32-byte seed (`harbor.KeySize`). The default parameters
  follow RFC 9106's second recommendation: `t=3`, `m=64 MiB`, `p=4`.
- SLIP-0021 derives independent child keys. Compromise of one sub-store key
  does not expose the others. Full-text values reuse the record encryption
  flag, under their own `harbor/fts/v1` key.
- Temporary byte slices are wiped after use. The password is a Go string and
  cannot be reliably wiped from memory; keep it out of long-lived state.

The salt and cost parameters are stored in an unencrypted metadata record, since
they are needed before any key exists. The metadata record has a fixed layout:
salt, KDF parameters, encryption flags, and a sealed canary.

### KDF parameter sets

| Set | Time | Memory | Threads | Use |
|-----|------|--------|---------|-----|
| `FastKDF*` | 1 | 64 MiB | 1 | Tests and short-lived development data only |
| `DefaultKDF*` | 3 | 64 MiB | 4 | New stores |
| `StrongKDF*` | 4 | 256 MiB | 8 | High-value secrets on capable hardware |

Partial KDF settings are rejected. The reader validates the persisted
parameters before deriving a key, and caps them at `t=100`, `m=1 GiB`,
`p=64`, with `m >= 8*p`, so a tampered metadata record cannot force an
unbounded Argon2id allocation.

### The canary

Every store contains a canary: `"harbor/canary/v1/0x<flags>"` sealed with the
canary key. Opening the store decrypts it and compares the result. This proves:

- the password derives the expected key, and
- the persisted encryption flags are the ones the canary was sealed with.

Because the flags are part of the authenticated plaintext, flipping a flag in
the metadata record causes an authentication failure instead of silently
opening the store in a different mode. The canary exists for plaintext stores
too, which is why a password is required even when encryption is disabled.

### Ciphertext format

- Each sealed value is `nonce || ciphertext || tag`, where the nonce is 12
  random bytes and the tag is 16 bytes. The nonce is generated per write.
- Empty plaintext is still sealed. An empty payload at rest is rejected rather
  than treated as an empty plaintext, so truncating a value to zero bytes
  cannot bypass authentication.
- Deterministic AES-SIV is used where a value must be looked up or listed by a
  hidden name: encrypted index keys, primary keys, series names, collection
  names, point IDs, and key/value keys.
- Every store writes an unencrypted marker that records its encryption mode.
  The root store records the flags in the metadata record; the sub-stores write
  their own versioned markers (`DENC`, `FENC`, `TENC`, `VENC`).

### Store-specific implementation behavior

#### db

`EncryptDBKeys` makes exact lookups work over ciphertext but destroys meaningful
ordering. Ordered and prefix lookups return `record.ErrEncryptedOrdering` instead of
returning wrong results. The three options — a plaintext order index, an
open-time in-memory ordered map, or order-preserving encryption — are weighed in
[README.md](README.md#ordered-queries-under-encryption). This release keeps the
fail-closed behaviour; option 2 is the planned path because it needs no format
change. Do not add OPE without a security review. `EncryptDBValues` seals record and key/value values;
`EncryptDBMetadata` seals auto-increment counters. Bucket names and the codec
name stay readable.

#### fs

`Config.ChunkSize` enables chunked content encryption and implies
`EncryptContent`. A positive chunk size splits content into independently
sealed chunks of that many plaintext bytes, so a write or truncation only
rewrites the affected chunks. Without chunking, the whole file is one sealed
blob and every write re-seals all of it. `EncryptMetadata` seals inode blobs,
hiding names, modes, sizes and times.

#### ts

`Config.BlockDuration` groups points into sealed blocks. It enables encryption
on its own (so `Config.Secret` is required) and is strongly recommended for
high-volume encrypted series: a block is sealed and opened once instead of once
per point. `EncryptSeriesNames` encrypts physical bucket and metadata names with
AES-SIV while listing still returns logical names.

#### vec

`EncryptPointIDs` encrypts point IDs and the stored point value (which contains
the ID). `EncryptCollectionNames` encrypts collection bucket and metadata names.
`EncryptPoints` seals point blobs and collection metadata. Quantized vectors are
sealed like any other point data.

### Rotation internals

Key rotation rewrites existing data under a new raw 32-byte key. The store's
`Keychain` writes with the primary key and tries fallback keys in order when
decrypting. Per-entity rotation installs the new key while keeping the previous
keys as fallbacks and deliberately leaves the marker under the current key;
the store is only self-consistent for a fresh process after `RotateAll`.

Full rotation (`record.ReEncrypt`/`RotateAll`, `file.RotateAll`, `series.RotateAll`,
`vector.RotateAll`) rewrites data atomically within the store, adopts the new key,
and drops the old one. It requires exclusive access. `VerifyAll` re-reads every
value and fails with the authentication sentinel if any value does not decrypt.

Partial rotation (`file.ReEncryptFS`, `series.ReEncryptSeries`,
`vector.ReEncryptCollection`) must be followed by `RotateAll` before close. It
cannot run when deterministic names are encrypted (`series.EncryptSeriesNames`,
`vector.EncryptPointIDs`, `vector.EncryptCollectionNames`), because the physical names
are derived from one master key; those paths return `ErrInvalidConfig` rather
than leaving the database unrecoverable.

There is no plaintext ↔ encrypted in-place migration. Export and re-import.

### Security invariants

- The salt and KDF parameters are public; the canary is ciphertext.
- The canary plaintext binds the encryption flags. Never change the binding
  without versioning the on-disk format.
- A sealed empty value is distinguishable from an empty payload at rest.
- Deterministic encryption is only used for values that must be looked up by
  ciphertext; never use it for free-form data with low entropy unless the
  leakage is acceptable.
- Metadata leakage (bucket structure, counts, sizes, timestamps) is documented
  in [README.md](README.md#threat-model). Do not add stronger claims to docs.
- The root package derives keys from the password and does not expose rotation.
  Rotate only through sub-stores opened with caller-managed keys.

### Application schema migrations

`Store.Migrate` applies a `Schema` of ordered `Step`s inside one
`WriteTransaction`; the application version lives in its own top-level
`harbor.schema` bucket (8-byte big-endian), not in `harbor.Metadata`, so
existing stores keep opening. Steps must be contiguous, end at `Schema.Version`
and be deterministic/idempotent. A newer stored version fails with
`ErrSchemaTooNew`; malformed plans fail with `ErrInvalidSchema`. The version is
re-read inside the transaction, so concurrent `Migrate` calls apply each step
once.

### Backup and verification

`Store.Backup`/`BackupTo`/`BackupFile` copy the on-disk database under one bbolt
read transaction after flushing vec buffers. `BackupFile` hashes the stream,
returns `BackupInfo` and can write a JSON sidecar
(`<path>.manifest.json`). `VerifyBackup` reopens the copy with
`bolt.Options{ReadOnly: true}` and validates the metadata canary plus each
sub-store's marker and version through the normal `record.Open`/`file.Open`/`series.Open`/
`vector.Open` paths. Those paths therefore must not write when the data is intact:
`series.ensureFormatVersion` reads before it writes, `file.OpenReadOnly` and
`search.OpenReadOnly` exist for read-only traversal, and fs filesystems are opened
with `OpenReadOnly` per name. The manifest is a sidecar, so the backup bytes are
unchanged. Verification never mutates the copy.

## Development workflow

```sh
# Formatting, vet and build
test -z "$(gofmt -l .)"
go vet ./...
go build ./...

# Tests
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./...
CGO_ENABLED=0 go test -count=1 -timeout 180s ./...

# Static analysis
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...

# Coverage
go test -count=1 -timeout 180s -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out

# Benchmarks (distance kernels, encrypted overhead, filtered search, FS I/O,
# FastWalk, TS aggregates/retention, db queries and S3 multipart)
go test -run '^$' -bench . -benchmem -benchtime=100ms ./store/vector ./store/series ./store/file ./store/record ./store/file/s3fs
```

CI runs formatting, vet, build, tests, race tests, coverage, staticcheck,
`govulncheck` and `go mod tidy -diff` on every push to `master` and every pull
request. A compatibility matrix builds with `CGO_ENABLED=0` for linux/amd64,
linux/arm64, linux/loong64, darwin/arm64, windows/amd64 and windows/arm64, and
runs the native CGO-free test suite. The benchmark job posts a pinned
`benchstat` comparison against the base commit for pull requests.
See [.github/workflows/ci.yml](.github/workflows/ci.yml).

## Documentation conventions

- [README.md](README.md) is the user-facing entry point: install, quick start,
  usage for every store, encryption behavior, lifecycle and performance.
- `AGENTS.md` (this file) is developer/agent-facing: architecture, on-disk
  layouts, encryption internals, invariants, development workflow.
- Per-package guides live in `store/record/README.md`, `store/file/README.md`, `store/series/README.md`,
  `store/vector/README.md`, `store/file/s3fs/README.md`, `pkg/audit/README.md`,
  `pkg/consensus/README.md`, `pkg/store/README.md`, `pkg/blob/README.md` and
  `pkg/snapshot/README.md`. Package doc comments are the source of truth for
  behavior; package guides are the source of truth for usage.
- Every public package has a package-level doc comment, and every exported
  identifier is documented. Keep comments in English.
- Code examples must compile against the current public API. The `record` package
  has compile-checked examples in `db/examples_test.go`; add examples there
  when behavior is easier to demonstrate than to describe.
- When behavior changes, update the affected package guide and user-facing
  README section in the same change. Do not add a `docs/` directory again;
  documentation lives in these files and next to the packages.
- Encryption claims are scoped: "protects data at rest" is not the same as
  "protects a running process". Link to the threat model rather than restating
  a stronger claim.

## Agent guidelines

- Keep the public API and on-disk formats stable. A format change requires a
  version bump and a compatibility path; unknown versions must fail closed.
- Preserve ownership contracts: `harbor.Store.Close` closes the handle it
  opened; `record.Open`, `file.Open`, `series.Open` and `vector.Open` borrow a caller-owned
  `*bolt.DB` and must never close it.
- Preserve context semantics: check at boundaries and loop checkpoints, do not
  spawn goroutines to fake cancellation, and never rewrite committed writes or
  published read results to cancellation errors.
- Preserve encryption invariants above. Do not weaken the canary binding, add
  unauthenticated plaintext fields, or change marker framing without a version
  bump.
- Add focused regression tests for bug fixes and run the race detector for
  concurrency changes. Keep benchmarks for hot paths.
- Run `gofmt`, `go vet`, `go build`, the test suite and `staticcheck` before
  declaring work done. Scale verification to the change.
- Keep scratch programs, generated output, `coverage.out` and database files
  out of the repository. Use the session scratch directory instead.
- When changing documentation, verify relative links and that code snippets
  still compile against the public API.

## Package guides and API reference

- [store/record/README.md](store/record/README.md)
- [store/file/README.md](store/file/README.md)
- [store/series/README.md](store/series/README.md)
- [store/vector/README.md](store/vector/README.md)
- [store/search/README.md](store/search/README.md)
- [store/file/s3fs/README.md](store/file/s3fs/README.md)
- [store/vector/doc/README.md](store/vector/doc/README.md)
- [graph/README.md](graph/README.md)
- [pkg/audit/README.md](pkg/audit/README.md)
- [pkg/consensus/README.md](pkg/consensus/README.md)
- [pkg/store/README.md](pkg/store/README.md)
- [pkg/blob/README.md](pkg/blob/README.md)
- [pkg/snapshot/README.md](pkg/snapshot/README.md)

Generated API documentation:

- [github.com/malivvan/harbor](https://pkg.go.dev/github.com/malivvan/harbor)
- [github.com/malivvan/harbor/store/record](https://pkg.go.dev/github.com/malivvan/harbor/store/record)
- [github.com/malivvan/harbor/store/file](https://pkg.go.dev/github.com/malivvan/harbor/store/file)
- [github.com/malivvan/harbor/store/series](https://pkg.go.dev/github.com/malivvan/harbor/store/series)
- [github.com/malivvan/harbor/store/vector](https://pkg.go.dev/github.com/malivvan/harbor/store/vector)
- [github.com/malivvan/harbor/store/search](https://pkg.go.dev/github.com/malivvan/harbor/store/search)
- [github.com/malivvan/harbor/pkg/audit](https://pkg.go.dev/github.com/malivvan/harbor/pkg/audit)
- [github.com/malivvan/harbor/pkg/blob](https://pkg.go.dev/github.com/malivvan/harbor/pkg/blob)
- [github.com/malivvan/harbor/pkg/consensus](https://pkg.go.dev/github.com/malivvan/harbor/pkg/consensus)
- [github.com/malivvan/harbor/pkg/id](https://pkg.go.dev/github.com/malivvan/harbor/pkg/id)
- [github.com/malivvan/harbor/pkg/metrics](https://pkg.go.dev/github.com/malivvan/harbor/pkg/metrics)
- [github.com/malivvan/harbor/pkg/sign/canonical](https://pkg.go.dev/github.com/malivvan/harbor/pkg/sign/canonical)
- [github.com/malivvan/harbor/pkg/snapshot](https://pkg.go.dev/github.com/malivvan/harbor/pkg/snapshot)
- [github.com/malivvan/harbor/pkg/snapshot/blobstore](https://pkg.go.dev/github.com/malivvan/harbor/pkg/snapshot/blobstore)
- [github.com/malivvan/harbor/pkg/store](https://pkg.go.dev/github.com/malivvan/harbor/pkg/store)

## Pre-release checklist

- Run the full development workflow above on a clean tree.
- Check that documented snippets still compile and relative links resolve.
- Confirm on-disk versions and encryption markers are unchanged, or bump them
  with a compatibility plan.
- Update README/AGENTS/package guides for any behavior change.
- Tag the release from a commit whose CI is green.
