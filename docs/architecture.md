# Architecture

Harbor is a Go module with four storage engines and a root package that
composes them behind one bbolt file. This document describes the package map,
both on-disk layouts, the transaction and concurrency model, and the
compatibility rules that keep data readable across releases.

## Package map

| Package | Responsibility |
|---------|----------------|
| `github.com/malivvan/harbor` | Opens one bbolt file, derives per-store keys from a password, and exposes `DB()`, `FS()`, `TS()`, `Vec()`, `Flags()` and `Meta()`. |
| `.../db` | Struct-to-bucket mapping, indexes, matchers, managed transactions, key/value access, codecs. |
| `.../fs` | Named file systems implementing the `absfs` interfaces, with symlinks, snapshots, inode cache and chunked content. |
| `.../ts` | Named time series with ordered scans, aggregation, labels and retention. |
| `.../vec` | Vector collections with Flat/HNSW indexes, payload filters and quantization. |
| `.../fs/s3fs` | An `absfs.Filer` backed by S3-compatible object storage. |
| `.../vec/hnsw` | The vendored HNSW graph implementation used by `vec`. |
| `.../internal/...` | Shared bbolt helpers, AEAD primitives, Argon2id/BLAKE2b and CPU detection. Not public API. |

The four stores are independent. The root package is a convenience composition,
not a required layer: each store can be opened directly with a caller-owned
`*bolt.DB` and its own 32-byte key.

## One file, four key spaces

The root package creates one bbolt database with this top-level shape:

```text
harbor.Metadata            unencrypted configuration record
db/                        db store root bucket
fs/                        fs store root bucket
ts/                        ts store root bucket
vec/                       vec store root bucket
```

`harbor.Metadata/Metadata` contains the Argon2id salt and cost parameters, the
encryption flags, and a sealed canary. It is the only record that must be read
before any key exists. See [encryption.md](encryption.md) for the key pipeline.

Each sub-store is opened under its own root bucket, so names cannot collide
with application buckets. A store used directly can choose any root path, or
the database root for backward compatibility with Storm-style layouts.

## db layout

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

## fs layout

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

## ts layout

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

## vec layout

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

## Transactions and concurrency

bbolt provides one writer and many readers, and Harbor follows that model:

- A `db.Store` is safe for concurrent use. CRUD operations use short
  transactions; `ReadTransaction` and `WriteTransaction` provide explicit
  callback boundaries.
- `Attach` lets a caller own the bbolt transaction and mix Harbor operations
  with raw bucket writes. Harbor never opens a nested managed transaction for
  an attached handle.
- `db.TransactionManager`, `fs.Walk`, `ts.Store`, and `vec.Collection` all use
  the same bbolt handle and therefore share the single-writer constraint.
- The root `Store` guards close against in-flight access with a read/write
  mutex. After `Close`, the sub-store accessors return `nil`.
- Vector writes are buffered; `Store.Close` flushes the vec store before
  closing the shared handle. When embedding `vec` with a shared handle, call
  `Flush` instead of `Close`, or call `SetOwnsDB(false)`.
- Contexts are checked at operation boundaries and loop checkpoints.
  Cancellation never interrupts a bbolt lock wait, a syscall, codec code, or
  an unreturned callback, and it never rewrites a committed write or a
  published read result.

## Buffering and fsync

Every bbolt commit is an fsync. Harbor batches where it can:

- `db.WriteTransaction` groups entity operations into one write transaction.
- `ts.Series.InsertMany` writes a batch in one transaction.
- `vec.Collection.Upsert` buffers points and flushes in batches.
- `fs` merges the inode and content writes of a single `Write` into one
  transaction, and chunking avoids rewriting unaffected content.

## Compatibility and versioning

- The module requires Go 1.27.4 or later.
- `db.Version` (`"1.0.0"`) is stored in `__database`. Opening a different
  version returns `ErrIncompatibleVersion`.
- `fs` format version 1, `ts` format version 1 and `vec` collection format
  versions are checked on open.
- Encryption markers are versioned (`DENC`, `FENC`, `TENC`, `VENC`), and the
  sealed canary binds the mode flags.
- Vector schema fingerprints detect a reordered or otherwise incompatible
  point codec.
- A first release refuses unknown versions instead of misinterpreting them.
  There is no plaintext ↔ encrypted in-place migration; export and re-import.

## Testing and benchmarks

The repository runs formatting, vet, build, tests, race tests, coverage,
staticcheck and `govulncheck` in CI. Benchmarks cover the vector and time-series
hot paths and CI compares pull requests against the base commit with
`benchstat`.

```sh
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./...
go test -run '^$' -bench . -benchmem ./vec ./ts
```
