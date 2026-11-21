# Changelog

## [1.0.0] - 2026-11-21

Initial release of Storm v1.

### Added

- **BoltDB toolkit** with typed struct storage, indexes, unique indexes,
  nested buckets, key/value operations, and a query system.
- **Public API package** at `github.com/malivvan/storm/db`.
- **Context-aware APIs**: `Open`, CRUD, finder, query, scanner, KV, and managed
  transactions accept `context.Context`. A nil context returns
  `ErrNilContext`.
- **Managed transactions**: `ReadTransaction` and `WriteTransaction` with
  callback-managed boundaries. Manual `Begin`/`Commit`/`Rollback` APIs are not
  provided.
- **Caller-owned database lifecycle**: `Open(ctx, db *bolt.DB, root []byte,
  c codec.MarshalUnmarshaler)` wraps an existing bbolt database. Storm borrows
  the database and never closes it; callers close the native `*bolt.DB`.
- **Codecs**: JSON by default, plus Gob and an AES-GCM codec wrapper.
- **Bucket store (`kv`)**: a `Store` that manages cached, named `Bucket`
  structs with `Open(ctx, db, root, codec)`, key/value operations, raw
  iteration, and bucket deletion.
- **File system store (`fs`)**: a `Store` over a caller-owned bbolt database
  that manages named file systems under a root bucket, with `Open(ctx, db,
  root)`, `Open`, `FileSystems`, and `DeleteFS`.
- **Operation-wrapped errors** with `errors.Is`-compatible causes.
- **Destination safety**: read operations decode before publishing to caller
  destinations.
- **Cooperative cancellation** at operation boundaries and loop checkpoints.
- **Runtime-generated struct support** and custom bucket names through
  `BucketNamer`.
- **Native bbolt escape hatch** through `NativeDB()`.
- **Benchmarks and tests** covering CRUD, indexes, queries, scanners, KV,
  cancellation, managed transactions, and codecs.

### Requirements

- Go 1.24 or later.
- `go.etcd.io/bbolt` v1.4.3.
