# tempest

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/tempest.svg)](https://pkg.go.dev/github.com/malivvan/tempest)
[![CI](https://github.com/malivvan/tempest/actions/workflows/ci.yml/badge.svg)](https://github.com/malivvan/tempest/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/malivvan/tempest/badge.svg?branch=master)](https://coveralls.io/github/malivvan/tempest?branch=master)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

tempest is a self-contained, encrypted storage toolkit built on
[BoltDB](https://github.com/etcd-io/bbolt). One database file, one password, and
stores you can use together or on their own:

- **`record`** — a structured record store with indexes, unique constraints, a
  matcher-based query API, and callback-managed transactions.
- **`fstree`** — a complete in-database file system with symbolic links,
  snapshots, an inode cache, and chunked content.
- **`series`** — a time-series store with ordered range scans, aggregation,
  labels, and retention policies.
- **`vector`** — a vector store with Flat and HNSW indexes, payload filters, SQ8
  quantization, and buffered writes.
- **`search`** — a full-text index with BM25 ranking and word/CJK tokenizers.
- **`graphs`** — an independent bitemporal property-graph store.
- **`object`** — an independent flat object store with buckets and atomic puts.

The root `tempest` package opens a single bbolt file behind one password, derives
a separate key for each store, and exposes them through `Record()`, `File()`, `Series()`, `Vector()`,
`Search()`, `Graphs()` and `Objects()`. Each accessor accepts optional
bucket-path segments that select a rooted view of the already-open store. Every store can also be used directly with its own `*bolt.DB`.

## Table of contents

- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Opening a store](#opening-a-store)
- [Records with `record`](#records-with-record)
- [Files with `fstree`](#files-with-fstree)
- [Time series with `series`](#time-series-with-series)
- [Vectors with `vector`](#vectors-with-vector)
- [Full-text search with `search`](#full-text-search-with-search)
- [Object storage: `object` and `backup`](#object-storage-object-and-backup)
- [Utilities](#utilities)
- [Clustering](#clustering)
- [Encryption](#encryption)
- [Audit trail](#audit-trail)
- [Cross-store transactions](#cross-store-transactions)
- [Database lifecycle](#database-lifecycle)
- [Error handling](#error-handling)
- [Contexts and cancellation](#contexts-and-cancellation)
- [Performance notes](#performance-notes)
- [Documentation](#documentation)
- [Status](#status)
- [License](#license)
- [Credits](#credits)

## Features

- **One file, one password.** Argon2id stretches the password; SLIP-0021
  domain separation derives one independent key per sub-store.
- **Encryption is built in, not bolted on.** AES-256-GCM seals records, files,
  points and documents; deterministic AES-SIV keeps exact lookups working when
  keys themselves must be hidden.
- **Tamper-evident audit trail.** Optionally record every write as an Ed25519
  signed, hash-linked event, so the history cannot be forged, altered or
  reordered without the store key.
- **Context-aware.** Database operations accept `context.Context` and honor
  cancellation at operation boundaries and loop checkpoints.
- **Transactional.** Managed read/write transactions, plus `Attach` for mixing
  tempest operations with raw bbolt writes in one atomic transaction.
- **Concurrency-tested.** The suite runs with `-race`, and CI also benchmarks
  the hot paths on every pull request.
- **No C dependencies and no external database.** tempest is pure Go, builds
  with `CGO_ENABLED=0`, and needs only one bbolt file.

## Installation

tempest requires Go 1.27.4 or later and a writable path for the database file.
It is pure Go and builds and tests with `CGO_ENABLED=0`.

```sh
go get github.com/malivvan/tempest
```

The module contains the root `tempest` package and the store packages under
`store/`:

| Package | Import path | Guide |
|---------|-------------|-------|
| `tempest` | `github.com/malivvan/tempest` | this file |
| `record` | `github.com/malivvan/tempest/store/record` | [store/record/README.md](store/record/README.md) |
| `fstree` | `github.com/malivvan/tempest/store/fstree` | [store/fstree/README.md](store/fstree/README.md) |
| `series` | `github.com/malivvan/tempest/store/series` | [store/series/README.md](store/series/README.md) |
| `vector` | `github.com/malivvan/tempest/store/vector` | [store/vector/README.md](store/vector/README.md) |
| `search` | `github.com/malivvan/tempest/store/search` | [store/search/README.md](store/search/README.md) |
| `graphs` | `github.com/malivvan/tempest/store/graphs` | [store/graphs/README.md](store/graphs/README.md) |
| `object` | `github.com/malivvan/tempest/store/object` | [store/object/README.md](store/object/README.md) |
| `system` | `github.com/malivvan/tempest/store/system` | [store/system/README.md](store/system/README.md) |

API reference: [pkg.go.dev/github.com/malivvan/tempest](https://pkg.go.dev/github.com/malivvan/tempest).

### Migrating from Harbor

The project was renamed from Harbor to tempest, and the persisted identifiers
moved with it: the key-derivation labels are now `tempest/...` (so every derived
key changed), and the schema bucket, index sentinel and prefix, legacy graph
bucket names, vector payload key and default file names use `tempest` too. A
store written by Harbor v0.3.0 or earlier therefore cannot be opened by this
build, and `Import` accepts the `tempest-export` container rather than the old
`harbor-export` one, so old exports do not import directly either. Keep a
v0.3.0 checkout if you still need to read data written by it.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/store/series"
)

type User struct {
	ID    int    `db:"id,increment"`
	Name  string `db:"index"`
	Email string `db:"unique"`
}

func main() {
	ctx := context.Background()

	store, err := tempest.Open(ctx, "app.db", 0o600, &tempest.Options{
		Password: "correct horse battery staple",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Println("close:", err)
		}
	}()

	// Structured records and indexes.
	user := User{Name: "alice", Email: "alice@example.com"}
	if err := store.Record().Save(ctx, &user); err != nil {
		log.Fatal(err)
	}

	var found User
	if err := store.Record().One(ctx, "Email", "alice@example.com", &found); err != nil {
		log.Fatal(err)
	}
	fmt.Println("saved user:", found.ID, found.Name)

	// Time-series points.
	cpuSeries := store.Series().Series("cpu")
	if err := cpuSeries.Insert(ctx, series.Point{
		Timestamp: time.Now().UnixNano(),
		Value:     0.42,
	}); err != nil {
		log.Fatal(err)
	}

	// A file system inside the same database.
	vfs, err := store.File().Open("documents")
	if err != nil {
		log.Fatal(err)
	}
	f, err := vfs.Create("/notes.txt")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte("hello from tempest")); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	// `store.Close` flushes buffered vector writes and closes the database.
}
```

`tempest.Open` takes ownership of the bbolt handle it opens: `Store.Close` flushes
pending vector writes and closes it. The sub-store packages (`record.Open`, `file.Open`,
`series.Open`, `vector.Open`) borrow a caller-owned `*bolt.DB` instead and never close
it. See [Database lifecycle](#database-lifecycle).

## Opening a store

`tempest.Open` opens or creates the database and returns `*tempest.Store`:

```go
store, err := tempest.Open(ctx, "app.db", 0o600, &tempest.Options{
	Password: "correct horse battery staple",

	// Optional: tune Argon2id for a new store. These are the defaults.
	KDFTime:    3,
	KDFMemory:  64 * tempest.MiB, // KiB
	KDFThreads: 4,
})
if err != nil {
	log.Fatal(err)
}
defer func() {
	if err := store.Close(); err != nil {
		log.Println("close:", err)
	}
}()
```

`tempest.Open` builds the underlying bbolt handle from
`tempest.DefaultBoltOptions`, so a default store is already crash-safe:

- `NoSync`, `NoGrowSync` and `NoFreelistSync` stay `false`. Each commit
  fdatasyncs its data pages and then its meta page, file growth is truncated and
  synced before pages are written into it, and the free-page list is durable.
  After a crash the database reopens from the newest valid meta page and loses
  at most the transaction that had not committed.
- `FreelistType` is `bolt.FreelistMapType`, which keeps allocation fast once a
  long-lived store fragments instead of degrading to a linear scan.
- `Timeout` is two seconds, so a lock held by another process fails the open
  instead of hanging it forever.

Every other bbolt field is set explicitly too — `MmapFlags`, `InitialMmapSize`,
`PageSize` and `MaxSize` to `0`, `OpenFile` and `Logger` to `nil`,
`NoStatistics` to `false`, `PreLoadFreelist` to `true` — so a bbolt release
cannot change the store's behaviour quietly. Each field carries a comment in
`tempest.DefaultBoltOptions` explaining the choice.

`tempest.Options` passes through the settings a caller may reasonably need —
`Timeout`, `InitialMmapSize`, `MaxSize`, `Mlock`, `NoSync` and `Logger` — and
every other bbolt setting stays at the default. Their zero values select the
default, so `&tempest.Options{Password: "..."}` is complete. `NoSync: true`
trades durability for write speed, while `Mlock` and `InitialMmapSize` tune
memory rather than consistency. `DefaultBoltOptions` is a variable: copy it
before changing a field when you open a bbolt handle yourself (for example to
pass the same database to `record.Open`), and do not mutate it in place, because
it is the default for every store in the process.

A nil `options` value is accepted and selects every default, but the password
has no default, so pass at least `&tempest.Options{Password: "..."}`; a nil value
fails with `ErrInvalidPassword`.

Important properties:

- The first call creates the file and writes unencrypted metadata: the salt,
  Argon2id parameters, encryption flags, and a sealed canary.
- Later calls use the persisted settings. `KDFTime`, `KDFMemory`, `KDFThreads`,
  `Encrypt` and `Plaintext` are ignored for an existing store.
- The password is always required, including for plaintext stores, because the
  canary is used to detect a wrong password.
- `Store.Close` flushes buffered vector writes and closes the database that
  `Open` created. It is safe to call twice, and the sub-store accessors return
  `nil` afterwards.
- The accessors are `Record`, `File`, `Series`, `Vector`, `Search`, `Graphs`
  and `Objects`. Each takes optional bucket-path segments:
  `store.Record("tenant", "records")` returns a rooted view of the already-open
  store, while `store.Record()` keeps the conventional `record` root so existing
  databases stay readable.

`Options.Validate()` performs the checks `Open` applies when creating a store —
password, complete and bounded Argon2id parameters, encryption-flag conflicts
and audit flags — without touching the file. The sub-store configs expose the
same `Validate()`/`Normalize()` pair, and their `Open` functions use it.

Use `Options{Plaintext: true}` for an unencrypted store, or select individual
encryption flags with `Options.Encrypt`. See [Encryption](#encryption) for the
flags, rotation rules and threat model.

## Records with `record`

Map Go structs to buckets, declare indexes with struct tags, and query with
`One`, `Find`, `All`, `Range`, `Prefix`, or the matcher API.

Declare a struct and describe indexes with the `db` struct-tag namespace:

```go
type User struct {
	ID        int       `db:"id,increment"`
	Name      string    `db:"index"`
	Email     string    `db:"unique"`
	Team      string    `db:"index"`
	CreatedAt time.Time `db:"index"`
}
```

Supported tags are:

| Tag | Meaning |
|-----|---------|
| `id` | Primary key. A field named `ID` is used when no tag is present. |
| `increment` | Auto-increments integer fields. `increment=100` sets the start. |
| `index` | Creates a list index for the field. |
| `unique` | Creates a unique index for the field. |
| `inline` | Flattens an embedded struct into the parent. |

Implement `record.BucketNamer` (`TempestBucketName() string`) for a custom bucket
name. Runtime-created structs can use `records.From("bucket")` to provide an
explicit bucket.

### Save and fetch

```go
users := store.Record()

u := User{Name: "alice", Email: "alice@example.com", Team: "core"}
if err := users.Save(ctx, &u); err != nil {
	return err // duplicate unique values match record.ErrAlreadyExists
}

var found User
if err := users.One(ctx, "Email", "alice@example.com", &found); err != nil {
	return err // no match matches record.ErrNotFound
}
```

`Save` creates the bucket and indexes on first use, assigns auto-increment
values, and enforces unique constraints. `Update` updates non-zero fields;
`UpdateField` updates one field, including zero values. The primary key cannot
be updated (`record.ErrImmutableID`).

### Find, range, prefix

```go
var team []User
err := users.Find(ctx, "Team", "core", &team, record.Limit(20), record.Skip(0))

var all []User
err = users.All(ctx, &all)
err = users.AllByIndex(ctx, "CreatedAt", &all, record.Reverse())

var recent []User
err = users.Range(ctx, "CreatedAt", since, time.Now(), &recent)

var prefixed []User
err = users.Prefix(ctx, "Name", "al", &prefixed)
```

`Find`, `Range` and `Prefix` return `record.ErrNotFound` when nothing matches; use
`All` if you want an empty slice contract. Pagination and ordering are
`FindOption` values: `record.Limit`, `record.Skip` and `record.Reverse`.

### Multi-get by key

`GetMany` reads a batch of records by primary key in one transaction, in request
order, returning the missing IDs separately:

```go
var users []User
missing, err := store.Record().GetMany(ctx, []any{"u1", "u2"}, &users)
```

### Projection

`Project` limits decoding to the fields you need:

```go
var docs []Doc
err := store.Record().Select().Project("ID", "Metadata").Find(ctx, &docs)
```

The JSON codec decodes only the requested top-level fields; a custom codec
without projected decoding falls back to a full decode and zeroes the rest.
Matchers and ordering cannot be combined with a projection. See
[store/record/README.md](store/record/README.md#projection).

### References

A `ref` tag ties a record to another bucket's primary key:

```go
type Chunk struct {
	ID    string `db:"id"`
	DocID string `db:"index,ref=Doc,ondelete=cascade"`
}
```

Saving a chunk requires the referenced `Doc` to exist. Deleting or dropping a
`Doc` is blocked by default (`ErrReferenceViolation`) or cascades to the
referencing records when `ondelete=cascade` is set; `ValidateSchema` catches a
misspelled target at startup. See
[store/record/README.md](store/record/README.md#references-and-cascades).

### Aggregation

`Aggregate` groups records and computes measures without materializing them:

```go
res, err := store.Record().From("Sale").Aggregate(ctx, record.AggregateSpec{
	GroupBy:  []string{"Kind"},
	Measures: []record.Measure{{Alias: "total", Type: record.MeasureSum, Field: "Amount"}},
	Having:   []record.Having{{Alias: "total", Op: record.HavingGTE, Value: 100}},
	OrderBy:  "total",
	Desc:     true,
})
```

Measures include count, sum, average, min/max, median, percentile, and sample
or population standard deviation. `TimeBucket` groups a timestamp by second,
minute, hour, day or week; `Missing` and `NonNumeric` choose how unusable values
are treated. See [store/record/README.md](store/record/README.md#aggregation).

### Matcher queries

```go
import "github.com/malivvan/tempest/store/record/q"

var admins []User
err := users.Select(q.In("Team", []string{"core", "admin"})).
	OrderBy("CreatedAt").
	Limit(50).
	Find(ctx, &admins)

count, err := users.Select(q.Lt("CreatedAt", cutoff)).Count(ctx, &User{})
err = users.Select(q.Eq("Team", "blocked")).Delete(ctx, &User{})
```

Matchers include `Eq`, `StrictEq`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `Re`,
`EqF`, `GtF`, `LtF`, and the combinators `And`, `Or`, `Not`.

### Transactions

tempest exposes callback-managed transactions only:

```go
err := users.WriteTransaction(ctx, func(tx record.Node) error {
	if err := tx.Save(ctx, &from); err != nil {
		return err
	}
	return tx.Save(ctx, &to)
})
```

A returned error rolls back. If the context is cancelled before commit, the
transaction is rolled back and `ctx.Err()` is returned. Panics propagate after
bbolt unwinds the transaction. Read-only work uses `ReadTransaction`.

To mix tempest operations with raw bbolt writes, attach tempest to a transaction
you own:

```go
tx, err := brecord.Begin(true)
if err != nil {
	return err
}
defer tx.Rollback()

txn, err := users.Attach(tx)
if err != nil {
	return err
}
if err := txn.Save(ctx, &record); err != nil {
	return err
}
if _, err := tx.CreateBucketIfNotExists([]byte("audit")); err != nil {
	return err
}
return tx.Commit()
```

tempest never commits or rolls back an attached transaction.

### Nested buckets and key/value access

```go
notes := users.From("notes")       // nested bucket
private := notes.From("private")   // deeper still

err := users.Set(ctx, "sessions", sessionID, &session)
var session Session
err = users.Get(ctx, "sessions", sessionID, &session)

raw, err := users.GetBytes(ctx, "cache", "key")
err = users.SetBytes(ctx, "cache", "key", []byte("value"))
exists, err := users.KeyExists(ctx, "sessions", sessionID)
err = users.Delete(ctx, "sessions", sessionID)
```

Full reference: [store/record/README.md](store/record/README.md).

## Files with `fstree`

An `absfs`-compatible file system stored in the same bbolt file. File systems
are named, so one store can hold many independent roots.

```go
vfs, err := store.File().Open("documents")
if err != nil {
	return err
}

if err := vfs.MkdirAll("/reports/2026", 0o755); err != nil {
	return err
}

f, err := vfs.Create("/reports/2026/q1.txt")
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
```

Common operations:

```go
err = vfs.Rename("/reports/2026/q1.txt", "/reports/2026/q1-final.txt")
err = vfs.Truncate("/reports/2026/q1-final.txt", 0)
err = vfs.Symlink("/reports/2026/q1-final.txt", "/latest.txt")

names, err := vfs.ReadDir("/reports")
info, err := vfs.Stat("/latest.txt")
link, err := vfs.Readlink("/latest.txt")
```

`file` supports symbolic links, `filepath.Walk`-style traversal, concurrent
`FastWalk`, read-only snapshots, an inode cache, external content back ends, and
chunked encrypted content.

### Application files and sessions

A named file system is a good home for application state that is naturally a
file: JSON sessions, uploaded documents, generated reports. Keep the meaning of
those files in the application; tempest only stores them.

```go
app, err := store.File().Open("app")
err = app.MkdirAll("/sessions", 0o755)

f, err := app.Create("/sessions/alice.json")
_, err = f.Write(sessionJSON)
err = f.Close()

// Later, or after reopening the store:
body, err := app.ReadFile("/sessions/alice.json")
```

The files live in the same encrypted database, participate in cross-store
transactions, and are included in `Backup`/`Export` and fs snapshots. A session
file is not a security boundary: anything holding the password can read it, so
do not put a bearer token there that the application would not otherwise trust.

### Walk

`file.Walk` has the shape of `filepath.Walk`:

```go
err := file.Walk(vfs, "/", func(path string, info os.FileInfo, err error) error {
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

For large trees, `file.FastWalk` traverses concurrently and passes an
`io/fs.DirEntry` instead of a full `FileInfo`:

```go
err := file.FastWalk(vfs, "/", func(path string, d iofs.DirEntry, err error) error {
	if err != nil {
		return err
	}
	fmt.Println(path)
	return nil
})
```

### Snapshots

```go
snap, err := vfs.CreateSnapshot("before-import")
if err != nil {
	return err
}
defer snap.Release()

data, err := snap.ReadFile("/reports/2026/q1-final.txt")
err = snap.CopyToFS("/reports/2026/q1-final.txt", "/restored.txt")
```

Snapshots are read-only and materialized in memory when created. They do not
pin the bbolt read transaction, so they do not block writers or grow the
database file.

### Encryption and content storage

`file.Config` enables encryption per store; `Config.ChunkSize` stores encrypted
content as independently sealed chunks. When using the root package, encryption
is selected at `tempest.Open` time instead:

```go
store, err := tempest.Open(ctx, "encrypted.db", 0o600, &tempest.Options{
	Password: password,
	Encrypt:  tempest.EncryptFile,
})
```

Opening `file` directly with a caller-owned `*bolt.DB` gives full control over
the key and chunk size:

```go
fsStore, err := file.Open(ctx, bdb, file.Config{
	Root:            [][]byte{[]byte("fstree")},
	Secret:          key, // 32 bytes
	EncryptMetadata: true,
	EncryptContent:  true,
	ChunkSize:       64 * 1024,
})

vfs, err := fsStore.Open("vault")
```

### The `absfs` interface and paths

`file` implements the module's internal `absfs` interfaces (`Filer`,
`FileSystem` and `SymlinkFileSystem`). The in-tree `store/fstree/s3fs` adapter implements
the `Filer` subset, and the interfaces make file systems work with standard
`io/fs` functions. The interface package itself is internal and cannot be
imported by other modules.

All tempest file systems use Unix-style `/` paths on every platform. On
Windows, drives are `/c/`, `/d/`, etc., and UNC paths are `//server/share`.
Use `path.Join` for virtual paths (`filepath.Join` can introduce backslashes);
convert to native paths only when calling OS APIs. `ReadDir`, `ReadFile` and
`Sub` follow `io/fs` conventions, and `Sub` returns a read-only view.

As with upstream absfs, a `FileSystem` wrapper keeps per-instance working
directory state. Avoid sharing one wrapper across goroutines that use `Chdir`
or relative paths; prefer absolute paths, one wrapper per goroutine, or
external synchronization. Individual `File` values should be used by a single
goroutine. See [AGENTS.md](AGENTS.md#internalabsfs) for the interface contracts
and implementer guidance.

Full reference: [store/fstree/README.md](store/fstree/README.md).

## Time series with `series`

Points are keyed by their timestamp, so no secondary index is needed for range
scans. Series carry labels and an optional retention policy.

```go
series := store.Series().Series("requests")
now := time.Now().UnixNano()

if err := series.Insert(ctx, series.Point{Timestamp: now, Value: 148}); err != nil {
	return err
}

if err := series.InsertMany(ctx, []series.Point{
	{Timestamp: now - int64(time.Minute), Value: 120},
	{Timestamp: now - 2*int64(time.Minute), Value: 133},
}); err != nil {
	return err
}

points, err := series.Range(ctx, now-int64(time.Hour), now)
first, err := series.First(ctx)
last, err := series.Last(ctx)
count, err := series.Count(ctx)
```

Timestamps are Unix nanoseconds and ranges are inclusive on both ends. Points
are stored in chronological key order, so `Range` and `RangeEach` stream in
time order without an index. `Insert` replaces a point with the same timestamp;
`InsertMany` accepts out-of-order timestamps and the last value wins.

### Multi-get by key

`GetMany` reads a batch of records by primary key in one transaction, in request
order, returning the missing IDs separately:

```go
var users []User
missing, err := store.Record().GetMany(ctx, []any{"u1", "u2"}, &users)
```

### Projection

`Project` limits decoding to the fields you need:

```go
var docs []Doc
err := store.Record().Select().Project("ID", "Metadata").Find(ctx, &docs)
```

The JSON codec decodes only the requested top-level fields; a custom codec
without projected decoding falls back to a full decode and zeroes the rest.
Matchers and ordering cannot be combined with a projection. See
[store/record/README.md](store/record/README.md#projection).

### References

A `ref` tag ties a record to another bucket's primary key:

```go
type Chunk struct {
	ID    string `db:"id"`
	DocID string `db:"index,ref=Doc,ondelete=cascade"`
}
```

Saving a chunk requires the referenced `Doc` to exist. Deleting or dropping a
`Doc` is blocked by default (`ErrReferenceViolation`) or cascades to the
referencing records when `ondelete=cascade` is set; `ValidateSchema` catches a
misspelled target at startup. See
[store/record/README.md](store/record/README.md#references-and-cascades).

### Aggregation

```go
samples, err := series.Aggregate(ctx, start, end, int64(time.Minute), series.AggAvg)
```

Aggregations are `AggMin`, `AggMax`, `AggAvg`, `AggSum`, `AggCount`,
`AggFirst` and `AggLast`. Each returned sample timestamp is the aligned start
of its window.

### Labels and retention

```go
err = series.SetLabels(ctx, map[string]string{"host": "web-1"})
labels, err := series.Labels(ctx)

err = series.SetRetention(ctx, series.Retention{
	MaxAge:    24 * time.Hour,
	MaxPoints: 100_000,
})
removed, err := series.EnforceRetention(ctx, time.Now().UnixNano())
```

Retention policies are persisted with the series and enforced automatically by
`Insert`/`InsertMany`. `Store.EnforceRetention` applies every persisted policy.
`Store.SetNow` injects a clock for deterministic tests. Full reference:
[store/series/README.md](store/series/README.md).

## Vectors with `vector`

Collections store fixed-dimension vectors with payload filters. Choose HNSW for
large collections or `FlatIndex` for exact search.

```go
docs, err := store.Vector().NewCollection("docs", 3, vector.Cosine, true)
if err != nil {
	return err
}

err = docs.Upsert([]vector.PointStruct{
	{
		ID:      "doc-1",
		Vector:  []float32{1, 0, 0},
		Payload: vector.Payload{"lang": "en", "year": 2026},
	},
	{
		ID:      "doc-2",
		Vector:  []float32{0, 1, 0},
		Payload: vector.Payload{"lang": "de", "year": 2025},
	},
})
if err != nil {
	return err
}

hits, err := docs.Search([]float32{1, 0, 0}, nil, 5)
```

Distance metrics are `vector.Cosine`, `vector.Euclid` and `vector.Dot`. Pass `true` as
the last `NewCollection` argument for HNSW; pass `false` for exact flat search.
Results are ordered by relevance: descending for Cosine/Dot, ascending for
Euclid.

### Filters

```go
filter := &vector.Filter{
	Must: []vector.Condition{
		{Key: "lang", Type: vector.MatchTypeExact, Match: vector.MatchValue{Value: "en"}},
		{Key: "year", Type: vector.MatchTypeRange, Range: &vector.RangeValue{GTE: 2020}},
	},
	MustNot: []vector.Condition{
		{Key: "status", Match: vector.MatchValue{Value: "deleted"}},
	},
}

hits, err := docs.Search(query, filter, 10)
_, err = docs.Delete(nil, filter) // delete every match
```

`Must` conditions all have to match; no `MustNot` condition may match. Condition
types are `MatchTypeExact`, `MatchTypeRange`, `MatchTypePrefix`,
`MatchTypeContains` and `MatchTypeRegex`. Numbers compare numerically across
Go types, so a filter built with `5` keeps matching after a round-trip that
returns `int64`.

### Radius search

`RangeSearch` returns every point within a distance rather than a fixed top-K:

```go
hits, err := docs.RangeSearch(query, 0.25, nil)
capped, err := docs.RangeSearchN(query, 0.25, nil, 100)
```

The radius is a distance in the collection's metric space (Euclidean distance,
`1 - cosine similarity`, or `-dot`), the score is the same value `Search`
reports, and the scan is exact for both Flat and HNSW collections.

`FacetCounts` counts string payload values over the points a filter selects:

```go
facets, err := docs.FacetCounts(vector.FacetRequest{
	Fields:    []string{"kind", "lang"},
	MaxValues: 10, // 0 keeps every value
})
```

Values are ordered by count then value, `MaxValues` keeps the most frequent,
and `Total` reports the matching points before truncation. Counts cover the
filtered collection, not a previous search result.

### Buffering and lifecycle

`Upsert` updates the in-memory index immediately and buffers points for bbolt.
The buffer is flushed when `vector.DefaultFlushThreshold` points accumulate, or
when you call `docs.Flush()` / `store.Close()`. The root `Store.Close` flushes
the vector store before closing the shared handle.

If you open `vector` directly with your own bbolt handle, call `vecStore.Flush()`
before closing that handle, or use `SetOwnsDB(false)` when the handle is shared.

### Quantization

Enable scalar quantization to reduce stored vector size:

```go
vstore, err := vector.Open(ctx, bdb, vector.Config{
	Root:     [][]byte{[]byte("vector")},
	UseQuant: true, // nil Quantizer selects SQ8
})
```

`bdb` here is a caller-owned `*bolt.DB` opened with `bolt.Open`. The root
package exposes the same setting as `tempest.Options{UseQuant: true}`.

The quantization mode is persisted in collection metadata: reopening a
quantized collection without quantization, or a plaintext collection with
quantization, is rejected. The quantizer implementation itself is not recorded;
use the same one for every process that opens the collection.

Full reference: [store/vector/README.md](store/vector/README.md).

## Full-text search with `search`

`search` indexes documents by id and ranks matches with BM25. It lives in the same
database and participates in cross-store transactions.

```go
idx, err := store.Search().Open("articles", search.Config{})
if err != nil {
	return err
}

err = idx.Upsert(ctx, "article-42", "tempest keeps records, files, series and vectors")
hits, err := idx.Search(ctx, "vectors", 10) // []search.Hit, best first
```

Two tokenizers are built in: `search.Words()` (the default) splits on Unicode
letters and digits and lowercases; `search.WordsWithCJK()` adds overlapping
trigrams for Han, Hiragana, Katakana and Hangul so CJK text matches substrings
without a dictionary segmenter. The tokenizer and BM25 parameters (`K1` 1.2,
`B` 0.75 by default) are fixed when an index is created and checked on reopen.

Values are sealed when value encryption is enabled, following the
`EncryptRecordValues` flag. Term keys are plaintext unless `Config.EncryptTokens` is
set; encrypting them hides the vocabulary but still leaks equality and result
sizes. See [store/search/README.md](store/search/README.md) for the layout, errors and limits.

## Object storage: `object` and `backup`

tempest has an independent flat object store and a `backup` package built on it:

- **`store/object`** owns a dedicated root bucket, so it works without the file
  store. A bucket is a namespace and a key is a slash-separated name inside it.
  Keys containing a `..` segment are rejected, so a key can never address a
  sibling bucket. `Put` consumes the whole reader before writing, so a failed
  reader leaves the previous content unchanged.
- **`backup`** takes content-addressed, incremental point-in-time
  snapshots of the objects under a prefix and restores them. Uploads dedupe by
  content hash, each snapshot carries a complete gzipped manifest, and restore
  verifies the manifest hash before writing anything.

```go
import (
	"github.com/malivvan/tempest/backup"
	"github.com/malivvan/tempest/backup/blobstore"
)

objects := store.Objects()
_, err := objects.Put(ctx, "tenant-a", "reports/q3.pdf", reader, 0, "application/pdf")

snap := backup.New(blobstore.New(objects, "os"), backup.Config{DataPrefix: "os/"})
idx, err := snap.Create(ctx, backup.KindManual, "") // capture
err = snap.Restore(ctx, idx.ID)                      // roll back
```

Data written by the previous file-backed blob implementation is not abandoned:
it remains in the file store, and `object.Store.MigrateFromFile` copies a bucket
into the independent store explicitly. See
[store/object/README.md](store/object/README.md) and
[backup/README.md](backup/README.md).

## Utilities

Small packages shared by applications and by tempest itself:

- **`metrics`** defines the dependency-free `Observer` interface the stores
  report counters, histograms, gauges and spans to. A nil observer disables
  instrumentation, and `tempest.Options{Observer: ...}` installs one.
- **`ranking`** combines ranked lists with reciprocal rank fusion, the usual
  way to merge full-text and vector results whose scores are not comparable.
  Ranking policy and list selection stay in the caller.
- **`vec/doc`** is a document-oriented facade over `vector`: id, content,
  embedding and string metadata, with the collection dimension taken from the
  first upsert instead of being known up front. See
  [store/vector/doc/README.md](store/vector/doc/README.md).

## Clustering

tempest ships a `cluster` package that replicates a whole store across a
cluster of processes using the [Raft](https://github.com/hashicorp/raft)
consensus algorithm. Each node keeps its own tempest store; writes are
proposed through raft on the leader and applied, in log order, to the same
logical state on every node. Reads are served from the local replica.

```go
node, err := cluster.Open(ctx, cluster.Config{
    NodeID:     "node-1",
    BindAddr:   "127.0.0.1:7001",
    Dir:        "./data/node-1",
    Bootstrap:  true, // first node forms the cluster
    StorePath:  "./data/node-1/tempest.db",
    StoreOpts:  &tempest.Options{Password: "shared-secret"},
})

// Writes go through the replicated facades (leader only).
err = node.Record().Save(ctx, &User{ID: "u1", Name: "alice"})
err = node.Series().Insert(ctx, "cpu", series.Point{Timestamp: now, Value: 42})

// Reads hit the local store directly.
var u User
_ = node.Store().Record().One(ctx, "ID", "u1", &u)
```

Every node in a cluster must use the same password. A second node opens with
`Bootstrap: false` and joins the leader with `Join`; nodes leave with `Leave`.
Off-leader writes return `cluster.ErrNotLeader`, and `Leader` reports the
current leader's address for client-side redirect. See
[cluster/README.md](cluster/README.md) for the full API, the consistency
model and deployment notes.

## Encryption

A new store is encrypted by default with `EncryptAll`. The password is never
stored: it is stretched with Argon2id, then SLIP-0021 derives one key per
sub-store. Encryption flags are persisted in unencrypted metadata and bound to
a sealed canary, so opening a store with the wrong password, key, or mode fails
loudly instead of returning garbage.

```go
store, err := tempest.Open(ctx, "app.db", 0o600, &tempest.Options{
	Password: "correct horse battery staple",
	Encrypt:  tempest.EncryptRecord | tempest.EncryptSeries,
})
```

Use `Plaintext: true` to create an unencrypted store, or choose individual
flags:

| Flag | Protects |
|------|----------|
| `EncryptRecordKeys` | db primary keys, index values, key/value keys |
| `EncryptRecordValues` | db record and key/value values |
| `EncryptRecordMetadata` | db auto-increment counters |
| `EncryptFileContent` | fs file content and symlink targets |
| `EncryptFileMetadata` | fs inode blobs (names, modes, sizes, times) |
| `EncryptSeriesValues` | ts point values and series metadata |
| `EncryptSeriesNames` | ts series and metadata names |
| `EncryptVectorPointIDs` | vec point IDs; also the stored point value |
| `EncryptVectorCollectionNames` | vec collection names |
| `EncryptVectorPoints` | vec point blobs and collection metadata |
| `EncryptGraphValues` | graph records |
| `EncryptObjectValues` | object records |

Aggregates (`EncryptRecord`, `EncryptFile`, `EncryptSeries`, `EncryptVector`,
`EncryptGraph`, `EncryptObject`) and `EncryptAll` combine these bits. The encryption and KDF fields of `Options`
only apply when creating a store; an existing store always uses its persisted
settings.

### Opening modes

- Opening an encrypted store with a plaintext config or a different mode fails
  with `ErrWrongMode`.
- Opening an encrypted store with the wrong key fails with `ErrAuthFailed`.
- Asking for encryption on a store that already holds plaintext data fails with
  `ErrWrongMode`.
- `ErrInvalidKey` reports a missing or malformed key; a key must be exactly 32
  bytes.
- `ErrInvalidConfig` reports an internally inconsistent configuration.
- The root package reports an empty or wrong password as `ErrInvalidPassword`
  and malformed metadata as `ErrCorruptStore`.

Migration from plaintext to encrypted storage is not supported. Export the data
and import it into a new encrypted store.

### Rotation

Key rotation rewrites existing data under a new raw 32-byte key. It does not
change the password: the root `tempest` package derives its keys from the
password and does not expose a rotation API. Rotate keys only when you open a
sub-store directly with your own key, or export and re-import to change the
password.

Full rotation uses the whole-store methods:

| Store | Method |
|-------|--------|
| `record` | `ReEncrypt` or `RotateAll` (aliases) |
| `file` | `RotateAll` |
| `series` | `RotateAll` |
| `vector` | `RotateAll` |

A full rotation is atomic within its store and adopts the new key when it
completes. Run it with **exclusive access** to the store; do not write from
other goroutines while it runs. `VerifyAll` re-reads every value and confirms
it decrypts with the configured keys.

The per-entity methods (`file.ReEncryptFS`, `series.ReEncryptSeries`,
`vector.ReEncryptCollection`) are **partial** migrations. The marker stays under
the current key and the previous key remains available in the running process,
so entities that have not been rewritten keep working. Rewrite every entity,
call `RotateAll`, and only then close the store or reopen it from a fresh
process. Closing a store between a partial rotation and `RotateAll` is
unsupported.

Partial rotation cannot run when deterministic names are encrypted: `series` with
`EncryptSeriesNames`, or `vector` with `EncryptPointIDs` or
`EncryptVectorCollectionNames`. In those modes the physical names are derived from
one master key, so the remaining entities could neither be found nor decrypted
after the new key is installed. The methods return `ErrInvalidConfig`; use
`RotateAll`.

### Ordered queries under encryption

When `EncryptRecordKeys` is enabled, primary keys and index values are AES-SIV
ciphertext. Exact lookups still work, but ciphertext order is meaningless, so
`OrderBy`, `Range` and `Prefix` return `record.ErrEncryptedOrdering` rather than
wrong results. There are three honest options:

1. **Plaintext index for non-secret ordering fields.** Fast, but deliberately
   leaks the order of that field.
2. **An in-memory ordered index built at open.** No on-disk format change and
   the same order leak, bounded by the number of distinct indexed values;
   writes must update it.
3. **Order-preserving encryption.** Research only; it needs a security review
   and is not promised here.

tempest keeps the current fail-closed behaviour for this release. Option 2 is
the planned path when a consumer needs ordering, because it needs no format
change and can be enabled per index.

### Threat model

tempest's encryption protects **data at rest** against someone who obtains the
database file without the password or key.

It does **not** protect against:

- a compromised running process, memory disclosure, or debugger access to keys,
- an attacker who can call the application's API with valid credentials,
- traffic analysis of metadata that is intentionally left readable,
- rollback or deletion of whole database pages (values are authenticated, but
  bbolt's page structure is not a globally authenticated log),
- weak passwords. Use a high-entropy password and consider `StrongKDF*` for
  high-value data.

Metadata that remains visible includes the salt, KDF parameters, encryption
flags, the sealed canary, bucket structure, record counts, and the sizes and
timestamps of encrypted values. Use the per-component flags above to reduce
what is exposed. Database file permissions (for example `0600`) are still your
responsibility.

### Operational checklist

- Create the database with restrictive permissions; the lock timeout is part of
  `DefaultBoltOptions` and can be overridden with `Options.Timeout`.
- Keep the password out of logs, crash reports, and command-line arguments.
- Back up the database file and the password/key separately.
- Test restores and run `VerifyAll` after moving data between environments.
- Do not copy a database out of a running rotation or partial migration.

For the key-derivation pipeline, canary design, ciphertext format and
store-specific encryption internals, see
[AGENTS.md](AGENTS.md#encryption-internals).

## Audit trail

tempest can record every mutating operation as a **tamper-evident, append-only
audit trail**. Each event is signed with an Ed25519 key derived from the store
password (SLIP-0010, one key per sub-store) and linked to its predecessor by a
hash chain, so the history cannot be forged, altered, reordered or spliced
without the key material.

Auditing is enabled per open with `Options.Audit`, mirroring the encryption
flags:

```go
store, err := tempest.Open(ctx, "app.db", 0o600, &tempest.Options{
	Password: "correct horse battery staple",
	Audit:    tempest.AuditAll, // or AuditRecord | AuditSeries, …
})
if err != nil {
	log.Fatal(err)
}

// … writes through the sub-stores are recorded automatically …

res, err := store.VerifyAudit()
if err != nil {
	log.Fatal(err) // forged signature, broken link, or no trail
}
log.Printf("audit: %d events, tip %x", res.Count, res.Tip)
```

| Flag | Records |
|------|---------|
| `AuditNone` | nothing (default) |
| `AuditRecord` | `record` writes |
| `AuditFile` | `file` writes |
| `AuditSeries` | `series` writes |
| `AuditVector` | `vector` writes |
| `AuditGraphs` | `graphs` writes |
| `AuditObject` | `object` writes |
| `AuditAll` | every store |

Unlike encryption flags, audit flags are **not persisted**: they are a per-open
runtime choice, so enabling or disabling auditing never changes the on-disk
format. The trail lives in a `ledger/` bucket in the same file, so it travels
with snapshots and backups.

The trail detects interior tampering but **cannot by itself detect tail
truncation** — deleting the last N events leaves a self-consistent prefix.
Anchor `store.Audit().Head()` out of band on a schedule to detect that. The
signing keys derive from the password, so anyone holding it can produce valid
events; the trail proves integrity against an attacker *without* the key, the
same trust boundary as the encryption itself.

See [store/ledger/README.md](store/ledger/README.md) for the event model, the recorded
operations, the full threat model and how to use the `ledger` package directly.

## Cross-store transactions

`WriteTransaction` runs a callback in one bbolt transaction bound to every
sub-store, so a record, a file, a time-series point, a vector upsert, a graph
write and an object write commit or roll back together:

```go
err := store.WriteTransaction(ctx, func(tx *tempest.Tx) error {
	if err := tx.Record().Save(ctx, &doc); err != nil {
		return err
	}

	fsys, err := tx.File().Open("app")
	if err != nil {
		return err
	}
	f, err := fsys.Create("/doc.json")
	if err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	col, err := tx.Vector().NewCollection("documents", len(embedding), vector.Cosine, false)
	if err != nil {
		return err
	}
	if err := col.Upsert([]vector.PointStruct{{ID: doc.ID, Vector: embedding}}); err != nil {
		return err
	}

	if _, err := tx.Objects().Put(ctx, "app-data", "doc.json", payloadReader, 0, "application/json"); err != nil {
		return err
	}

	g, err := tx.Graphs().Open("documents")
	if err != nil {
		return err
	}
	if err := g.UpsertNode(ctx, &graphs.Node{ID: doc.ID}); err != nil {
		return err
	}

	return tx.Series().Series("writes").Insert(ctx, series.Point{Timestamp: now, Value: 1})
})
```

Returning `nil` commits every change; returning an error (or canceling the
context) rolls all of them back. `ReadTransaction` runs the same views against
a consistent read-only snapshot.

Behaviour worth knowing:

- **One fsync.** All writes share one bbolt transaction instead of one
  transaction per store.
- **Vector staging.** Points upserted through `tx.Vector()` are visible to searches
  through the same view immediately. The live collection indexes adopt them when
  the transaction commits, so searches outside the transaction do not see them
  early. Points still buffered from earlier calls are flushed before the
  transaction starts.
- **Audit after commit.** Audit events for transaction writes are collected and
  appended to the trail once the transaction commits, never from inside it.
- **Single writer.** Write transactions are serialized. Do not start a
  transaction from inside another transaction's callback; pass `tx` down
  instead. A read transaction blocks while a write transaction is open, as
  bbolt's single-writer model requires.

## Database lifecycle

The two entry points differ in who owns the bbolt handle:

- `tempest.Open` opens the file itself; `Store.Close` flushes buffered vector
  writes and closes the database. It is safe to call more than once, and the
  sub-store accessors return `nil` afterwards.
- `record.Open`, `file.Open`, `series.Open` and `vector.Open` wrap an already-open
  `*bolt.DB`. Their `Close` methods release the wrapper only; the caller keeps
  ownership of the database. If several stores share one handle, close the
  handle after all wrappers are closed.

When embedding `vector` on a shared handle, set `vector.Config.SharedDB` (or call
`SetOwnsDB(false)`): `vector.Store.Close` then flushes buffered points without
closing the handle. Use `Store.Vector().Flush()` to drain explicitly and
`Store.Vector().Closed()` to check the state. Per collection, `Pending()` reports
buffered points and `Close()` flushes and retires that handle. The root store
does this automatically: `tempest.Store.Close` and `tempest.Store.Backup` flush
the vector buffer before touching the database.

A `nil` context is rejected by context-aware operations with `ErrNilContext`
(or the store's equivalent sentinel); `Close` is a lifecycle exception and
takes no context. `tempest.Open` is the one exception: it treats a nil context
as `context.Background()`.

### Backup

`Backup` writes a consistent, point-in-time snapshot of the whole database to
an `io.Writer`; `BackupTo` writes the same snapshot to a file atomically, and
`BackupFile` also reports its size and SHA-256:

```go
var buf bytes.Buffer
err := store.Backup(ctx, &buf)

err = store.BackupTo(ctx, "/backups/tempest-2026-11-23.db")

info, err := store.BackupFile(ctx, "/backups/tempest-2026-11-24.db", tempest.BackupOptions{
	Manifest: true, // also write <path>.manifest.json
})
fmt.Println(info.Size, info.SHA256, info.Flags)
```

- Buffered vector points are flushed first, so the snapshot includes every
  upsert the store has accepted.
- The snapshot comes from one bbolt read transaction: concurrent writers are
  not blocked and do not appear in it. The store lock is held for the
  duration, so `Close` waits for a running backup and a backup after `Close`
  returns `ErrClosed`.
- The bytes are the on-disk representation. An encrypted store produces an
  encrypted backup that is opened with the same password; nothing is exported
  in plaintext.
- `Backup` is only complete when it returns `nil`. `BackupTo` writes to a
  temporary sibling, syncs, and renames it into place, so a failed or canceled
  backup never corrupts an existing file and leaves no partial output.

To restore, open the snapshot file with `tempest.Open` and the same password.
`View` remains available for lower-level raw bbolt snapshots.

`VerifyBackup` validates a copy before restoring it. It opens the file
read-only and checks the metadata canary (so a wrong password is
`ErrInvalidPassword`), every sub-store marker and format version, and the
sidecar manifest when one exists. A truncated or corrupted copy fails; the
verification never writes to the file.

```go
info, err := tempest.VerifyBackup(ctx, "/backups/tempest-2026-11-24.db", password, nil)
```

`BackupFile` writes `<path>.manifest.json` with the checksum, creation time,
encryption flags and format versions. `BackupOptions.Overwrite` controls whether
an existing destination is replaced (`BackupTo` always replaces).

### Schema migrations

`Migrate` applies application schema steps in one cross-store transaction:

```go
err := store.Migrate(ctx, tempest.Schema{
	Version: 2,
	Steps: []tempest.Step{
		{From: 0, To: 1, Apply: func(ctx context.Context, tx *tempest.Tx) error {
			return tx.Record().Save(ctx, &Backfill{})
		}},
		{From: 1, To: 2, Apply: migrateDocuments},
	},
})
```

The version lives in its own `tempest.schema` bucket, so existing stores open
unchanged. A failing step rolls back the whole migration; a store written by a
newer build is refused with `ErrSchemaTooNew` instead of being downgraded; an
up-to-date store is a no-op. Steps must be deterministic and idempotent.
`SchemaVersion` reports the current version.

### Export, import and compaction

`Backup` copies the database file; `Export` writes a versioned, checksummed
*logical* snapshot that `Import` applies to another store. That is the migration
path between encryption modes and formats:

```go
var buf bytes.Buffer
summary, err := tempest.Export(ctx, store, &buf, tempest.ExportOptions{
	RecordTypes: []any{(*User)(nil)}, // db record types to include
	Progress:    func(p tempest.Progress) { log.Printf("%s: %d", p.Section, p.Records) },
})

restored, err := tempest.Open(ctx, "encrypted.db", 0o600, &tempest.Options{Password: "new"})
err = tempest.Import(ctx, restored, bytes.NewReader(buf.Bytes()), tempest.ImportOptions{
	RecordTypes: []any{(*User)(nil)},
})
```

- The container is newline-delimited JSON: a header, one record per line, and a
  footer with the record count and a SHA-256 checksum. `Import` rejects an
  unknown format or version (`ErrExportFormat`/`ErrExportVersion`), a truncated
  stream (`ErrExportTruncated`) and a checksum mismatch (`ErrExportChecksum`).
- Exported: db records of the listed types, every named file system's tree,
  every time series (points, labels, retention) and every vector collection
  (metadata and points; buffered points are flushed first).
- `ImportOptions.DryRun` validates the whole stream without writing, and accepts
  a nil destination. Because the checksum is verified at the footer, run a dry
  run first on untrusted input: earlier records can be applied before a later
  mismatch is detected.
- File ownership is not exported (the portable `FileInfo` has no owner), and db
  records are encoded with `encoding/json`.

`Store.CompactTo(ctx, path, mode)` rewrites the live pages into a new file and
drops free space. The result is the same on-disk representation as `Backup`
(encryption is preserved), an existing destination is refused, and buffered
vector points are flushed first. Compaction cannot be interrupted part-way.

## Error handling

Classify errors with `errors.Is`, never by comparing strings:

```go
var user User
err := store.Record().One(ctx, "Email", "missing@example.com", &user)
switch {
case errors.Is(err, record.ErrNotFound):
	// Record does not exist.
case errors.Is(err, context.Canceled):
	// The request was cancelled.
}
```

Errors are wrapped as `tempest <operation>: <cause>` while preserving the
original cause, context errors, bbolt errors and callback errors in the chain.
The package guides list each store's sentinels; use `errors.Is` with those
values rather than parsing messages.

## Contexts and cancellation

CRUD, query, scan, key/value and managed-transaction operations accept a
context. Cancellation is cooperative: tempest checks at operation boundaries and
loop checkpoints, but cannot interrupt a bbolt lock wait, a syscall, codec code,
or a callback that has not returned. No background goroutines are used to
simulate cancellation, and successfully committed writes or published read
results are not retroactively changed to cancellation errors.

## Performance notes

The defaults favor portability and safety. For throughput-sensitive workloads:

- Pass a faster `record.Codec` than JSON for decode-heavy scans.
- Batch writes with `record.WriteTransaction`, `series.Series.InsertMany`, or
  `vector.Collection.Upsert` instead of writing one record per loop; every bbolt
  commit is an fsync.
- For encrypted time series, set `series.Config.BlockDuration` so blocks are sealed
  and opened once instead of once per point.
- Use `file.Config.ChunkSize` for large files so writes and truncations touch only
  the affected chunks.

The repository ships benchmarks for the hot paths:

```sh
go test -run '^$' -bench . -benchmem ./vec ./ts
```

## Documentation

- [store/record/README.md](store/record/README.md) — models, tags, CRUD, queries, transactions,
  key/value access, and codecs.
- [store/fstree/README.md](store/fstree/README.md) — file systems, snapshots, external content, and
  encryption.
- [store/series/README.md](store/series/README.md) — series, aggregation, retention, and encrypted
  blocks.
- [store/vector/README.md](store/vector/README.md) — collections, indexes, filters, quantization,
  and buffering.
- [store/search/README.md](store/search/README.md) — full-text indexes, tokenizers, BM25 and
  encryption.
- [store/graphs/README.md](store/graphs/README.md) — the property-graph store and its
  bitemporal graph core.
- [store/system/README.md](store/system/README.md) — the store header, KDF settings,
  flags and key derivation.
- [store/fstree/s3fs/README.md](store/fstree/s3fs/README.md) — the S3-backed `absfs.Filer`.
- [store/ledger/README.md](store/ledger/README.md) — the tamper-evident audit trail:
  event model, recorded operations, threat model, and direct package use.
- [store/README.md](store/README.md) — the process-wide database
  manager, directory layout, and password file.
- [store/object/README.md](store/object/README.md) — the flat object store, containment,
  and grants.
- [backup/README.md](backup/README.md) — content-addressed
  incremental snapshots, restore, retention, and scheduling.
- [store/vector/doc/README.md](store/vector/doc/README.md) — the document-oriented vector
  facade.
- [AGENTS.md](AGENTS.md) — architecture, on-disk layout, encryption internals,
  development workflow, and repository conventions for contributors and coding
  agents.

Generated API documentation is published on pkg.go.dev:

- [github.com/malivvan/tempest](https://pkg.go.dev/github.com/malivvan/tempest)
- [github.com/malivvan/tempest/store/record](https://pkg.go.dev/github.com/malivvan/tempest/store/record)
- [github.com/malivvan/tempest/store/fstree](https://pkg.go.dev/github.com/malivvan/tempest/store/fstree)
- [github.com/malivvan/tempest/store/series](https://pkg.go.dev/github.com/malivvan/tempest/store/series)
- [github.com/malivvan/tempest/store/vector](https://pkg.go.dev/github.com/malivvan/tempest/store/vector)
- [github.com/malivvan/tempest/store/search](https://pkg.go.dev/github.com/malivvan/tempest/store/search)
- [github.com/malivvan/tempest/store/ledger](https://pkg.go.dev/github.com/malivvan/tempest/store/ledger)
- [github.com/malivvan/tempest/store/object](https://pkg.go.dev/github.com/malivvan/tempest/store/object)
- [github.com/malivvan/tempest/store/graphs](https://pkg.go.dev/github.com/malivvan/tempest/store/graphs)
- [github.com/malivvan/tempest/store/system](https://pkg.go.dev/github.com/malivvan/tempest/store/system)
- [github.com/malivvan/tempest/cluster](https://pkg.go.dev/github.com/malivvan/tempest/cluster)
- [github.com/malivvan/tempest/ranking](https://pkg.go.dev/github.com/malivvan/tempest/ranking)
- [github.com/malivvan/tempest/metrics](https://pkg.go.dev/github.com/malivvan/tempest/metrics)
- [github.com/malivvan/tempest/backup](https://pkg.go.dev/github.com/malivvan/tempest/backup)
- [github.com/malivvan/tempest/backup/blobstore](https://pkg.go.dev/github.com/malivvan/tempest/backup/blobstore)
- [github.com/malivvan/tempest/store](https://pkg.go.dev/github.com/malivvan/tempest/store)
- [github.com/malivvan/tempest/store/vector/doc](https://pkg.go.dev/github.com/malivvan/tempest/store/vector/doc)

Where to start:

- **New to tempest:** [Quick start](#quick-start), then the guide sections for
  the stores you need.
- **Records, indexes and transactions:** [store/record/README.md](store/record/README.md).
- **Full-text search:** [Full-text search with `search`](#full-text-search-with-search)
  and [store/search/README.md](store/search/README.md).
- **Files, snapshots or external content:** [store/fstree/README.md](store/fstree/README.md) and
  [store/fstree/s3fs/README.md](store/fstree/s3fs/README.md).
- **Metrics and time series:** [store/series/README.md](store/series/README.md).
- **Embeddings and similarity search:** [store/vector/README.md](store/vector/README.md).
- **Process-wide database, blobs or object snapshots:**
  [store/README.md](store/README.md),
  [store/object/README.md](store/object/README.md) and
  [backup/README.md](backup/README.md).
- **IDs, metrics or a document vector facade:**
  [Utilities](#utilities) and [store/vector/doc/README.md](store/vector/doc/README.md).
- **Security review or operations:** [Encryption](#encryption) and
  [AGENTS.md](AGENTS.md#architecture).

Every public package has a package-level overview in its doc comment, and every
exported identifier is documented. Compile-checked examples live next to the
code where they are most useful (for the `record` package, see
`store/record/examples_test.go`).

## Status

tempest is preparing its first public release (`v0.1.x`). The on-disk formats
are versioned and refuse data from an incompatible build instead of misreading
it. Every persisted format is version 1 (`record.Version`, `file.FormatVersion`,
`series.FormatVersion`, `vector.CollectionFormatVersion`, `ledger.FormatVersion`);
there is no v2 or dual-version compatibility path. Pin a module version, and
treat pre-1.0 API changes as potentially breaking.

## License

MIT — see [LICENSE](LICENSE).

tempest is a fork of [asdine/storm](https://github.com/asdine/storm) and keeps
the original MIT copyright notices. It also vendors code derived from the Go
project (Argon2, BLAKE2b, CPU feature detection), the `absfs` interfaces, and
[`coder/hnsw`](https://github.com/coder/hnsw); their licenses are preserved next
to the code.

## Credits

- [Asdine El Hrychy](https://github.com/asdine)
- [Anderson Bargas](https://github.com/andersonbargas)
- [Bjørn Erik Pedersen](https://github.com/bep)
