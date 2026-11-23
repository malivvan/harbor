# harbor

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/harbor.svg)](https://pkg.go.dev/github.com/malivvan/harbor)
[![CI](https://github.com/malivvan/harbor/actions/workflows/ci.yml/badge.svg)](https://github.com/malivvan/harbor/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Harbor is a self-contained, encrypted storage toolkit built on
[BoltDB](https://github.com/etcd-io/bbolt). One database file, one password, and
four stores you can use together or on their own:

- **`db`** — a structured record store with indexes, unique constraints, a
  matcher-based query API, and callback-managed transactions.
- **`fs`** — a complete in-database file system with symbolic links, snapshots,
  an inode cache, and chunked content.
- **`ts`** — a time-series store with ordered range scans, aggregation, labels,
  and retention policies.
- **`vec`** — a vector store with Flat and HNSW indexes, payload filters, SQ8
  quantization, and buffered writes.

The root `harbor` package opens a single bbolt file behind one password, derives
a separate key for each store, and exposes them through `DB()`, `FS()`, `TS()`
and `Vec()`. Every store can also be used directly with its own `*bolt.DB`.

## Table of contents

- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Opening a store](#opening-a-store)
- [Records with `db`](#records-with-db)
- [Files with `fs`](#files-with-fs)
- [Time series with `ts`](#time-series-with-ts)
- [Vectors with `vec`](#vectors-with-vec)
- [Clustering](#clustering)
- [Encryption](#encryption)
- [Audit trail](#audit-trail)
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
  Harbor operations with raw bbolt writes in one atomic transaction.
- **Concurrency-tested.** The suite runs with `-race`, and CI also benchmarks
  the hot paths on every pull request.
- **No C dependencies and no external database.** Harbor is pure Go, builds
  with `CGO_ENABLED=0`, and needs only one bbolt file.

## Installation

Harbor requires Go 1.27.4 or later and a writable path for the database file.
It is pure Go and builds and tests with `CGO_ENABLED=0`.

```sh
go get github.com/malivvan/harbor
```

The module contains the root `harbor` package and four sub-packages:

| Package | Import path | Guide |
|---------|-------------|-------|
| `harbor` | `github.com/malivvan/harbor` | this file |
| `db` | `github.com/malivvan/harbor/db` | [db/README.md](db/README.md) |
| `fs` | `github.com/malivvan/harbor/fs` | [fs/README.md](fs/README.md) |
| `ts` | `github.com/malivvan/harbor/ts` | [ts/README.md](ts/README.md) |
| `vec` | `github.com/malivvan/harbor/vec` | [vec/README.md](vec/README.md) |

API reference: [pkg.go.dev/github.com/malivvan/harbor](https://pkg.go.dev/github.com/malivvan/harbor).

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/malivvan/harbor"
	"github.com/malivvan/harbor/ts"
)

type User struct {
	ID    int    `db:"id,increment"`
	Name  string `db:"index"`
	Email string `db:"unique"`
}

func main() {
	ctx := context.Background()

	store, err := harbor.Open(ctx, "app.db", 0o600, &harbor.Options{
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
	if err := store.DB().Save(ctx, &user); err != nil {
		log.Fatal(err)
	}

	var found User
	if err := store.DB().One(ctx, "Email", "alice@example.com", &found); err != nil {
		log.Fatal(err)
	}
	fmt.Println("saved user:", found.ID, found.Name)

	// Time-series points.
	series := store.TS().Series("cpu")
	if err := series.Insert(ctx, ts.Point{
		Timestamp: time.Now().UnixNano(),
		Value:     0.42,
	}); err != nil {
		log.Fatal(err)
	}

	// A file system inside the same database.
	vfs, err := store.FS().Open("documents")
	if err != nil {
		log.Fatal(err)
	}
	f, err := vfs.Create("/notes.txt")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte("hello from harbor")); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	// `store.Close` flushes buffered vector writes and closes the database.
}
```

`harbor.Open` takes ownership of the bbolt handle it opens: `Store.Close` flushes
pending vector writes and closes it. The sub-store packages (`db.Open`, `fs.Open`,
`ts.Open`, `vec.Open`) borrow a caller-owned `*bolt.DB` instead and never close
it. See [Database lifecycle](#database-lifecycle).

## Opening a store

`harbor.Open` opens or creates the database and returns `*harbor.Store`:

```go
store, err := harbor.Open(ctx, "app.db", 0o600, &harbor.Options{
	// Set a lock timeout so a second process fails instead of hanging.
	Options:  bolt.Options{Timeout: 2 * time.Second},
	Password: "correct horse battery staple",

	// Optional: tune Argon2id for a new store. These are the defaults.
	KDFTime:    3,
	KDFMemory:  64 * harbor.MiB, // KiB
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

`Options.Validate()` performs the checks `Open` applies when creating a store —
password, complete and bounded Argon2id parameters, encryption-flag conflicts
and audit flags — without touching the file. The sub-store configs expose the
same `Validate()`/`Normalize()` pair, and their `Open` functions use it.

Use `Options{Plaintext: true}` for an unencrypted store, or select individual
encryption flags with `Options.Encrypt`. See [Encryption](#encryption) for the
flags, rotation rules and threat model.

## Records with `db`

Map Go structs to buckets, declare indexes with struct tags, and query with
`One`, `Find`, `All`, `Range`, `Prefix`, or the matcher API.

Declare a struct and describe indexes with the `db` tag namespace:

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

Implement `db.BucketNamer` (`HarborBucketName() string`) for a custom bucket
name. Runtime-created structs can use `records.From("bucket")` to provide an
explicit bucket.

### Save and fetch

```go
users := store.DB()

u := User{Name: "alice", Email: "alice@example.com", Team: "core"}
if err := users.Save(ctx, &u); err != nil {
	return err // duplicate unique values match db.ErrAlreadyExists
}

var found User
if err := users.One(ctx, "Email", "alice@example.com", &found); err != nil {
	return err // no match matches db.ErrNotFound
}
```

`Save` creates the bucket and indexes on first use, assigns auto-increment
values, and enforces unique constraints. `Update` updates non-zero fields;
`UpdateField` updates one field, including zero values. The primary key cannot
be updated (`db.ErrImmutableID`).

### Find, range, prefix

```go
var team []User
err := users.Find(ctx, "Team", "core", &team, db.Limit(20), db.Skip(0))

var all []User
err = users.All(ctx, &all)
err = users.AllByIndex(ctx, "CreatedAt", &all, db.Reverse())

var recent []User
err = users.Range(ctx, "CreatedAt", since, time.Now(), &recent)

var prefixed []User
err = users.Prefix(ctx, "Name", "al", &prefixed)
```

`Find`, `Range` and `Prefix` return `db.ErrNotFound` when nothing matches; use
`All` if you want an empty slice contract. Pagination and ordering are
`FindOption` values: `db.Limit`, `db.Skip` and `db.Reverse`.

### Matcher queries

```go
import "github.com/malivvan/harbor/db/q"

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

Harbor exposes callback-managed transactions only:

```go
err := users.WriteTransaction(ctx, func(tx db.Node) error {
	if err := tx.Save(ctx, &from); err != nil {
		return err
	}
	return tx.Save(ctx, &to)
})
```

A returned error rolls back. If the context is cancelled before commit, the
transaction is rolled back and `ctx.Err()` is returned. Panics propagate after
bbolt unwinds the transaction. Read-only work uses `ReadTransaction`.

To mix Harbor operations with raw bbolt writes, attach Harbor to a transaction
you own:

```go
tx, err := bdb.Begin(true)
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

Harbor never commits or rolls back an attached transaction.

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

Full reference: [db/README.md](db/README.md).

## Files with `fs`

An `absfs`-compatible file system stored in the same bbolt file. File systems
are named, so one store can hold many independent roots.

```go
vfs, err := store.FS().Open("documents")
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

`fs` supports symbolic links, `filepath.Walk`-style traversal, concurrent
`FastWalk`, read-only snapshots, an inode cache, external content back ends, and
chunked encrypted content.

### Walk

`fs.Walk` has the shape of `filepath.Walk`:

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

For large trees, `fs.FastWalk` traverses concurrently and passes an
`io/fs.DirEntry` instead of a full `FileInfo`:

```go
err := fs.FastWalk(vfs, "/", func(path string, d iofs.DirEntry, err error) error {
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

`fs.Config` enables encryption per store; `Config.ChunkSize` stores encrypted
content as independently sealed chunks. When using the root package, encryption
is selected at `harbor.Open` time instead:

```go
store, err := harbor.Open(ctx, "encrypted.db", 0o600, &harbor.Options{
	Password: password,
	Encrypt:  harbor.EncryptFS,
})
```

Opening `fs` directly with a caller-owned `*bolt.DB` gives full control over
the key and chunk size:

```go
fsStore, err := fs.Open(ctx, bdb, fs.Config{
	Root:            [][]byte{[]byte("fs")},
	Secret:          key, // 32 bytes
	EncryptMetadata: true,
	EncryptContent:  true,
	ChunkSize:       64 * 1024,
})

vfs, err := fsStore.Open("vault")
```

### The `absfs` interface and paths

`fs` implements the module's internal `absfs` interfaces (`Filer`,
`FileSystem` and `SymlinkFileSystem`). The in-tree `fs/s3fs` adapter implements
the `Filer` subset, and the interfaces make file systems work with standard
`io/fs` functions. The interface package itself is internal and cannot be
imported by other modules.

All Harbor file systems use Unix-style `/` paths on every platform. On
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

Full reference: [fs/README.md](fs/README.md).

## Time series with `ts`

Points are keyed by their timestamp, so no secondary index is needed for range
scans. Series carry labels and an optional retention policy.

```go
series := store.TS().Series("requests")
now := time.Now().UnixNano()

if err := series.Insert(ctx, ts.Point{Timestamp: now, Value: 148}); err != nil {
	return err
}

if err := series.InsertMany(ctx, []ts.Point{
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

### Aggregation

```go
samples, err := series.Aggregate(ctx, start, end, int64(time.Minute), ts.AggAvg)
```

Aggregations are `AggMin`, `AggMax`, `AggAvg`, `AggSum`, `AggCount`,
`AggFirst` and `AggLast`. Each returned sample timestamp is the aligned start
of its window.

### Labels and retention

```go
err = series.SetLabels(ctx, map[string]string{"host": "web-1"})
labels, err := series.Labels(ctx)

err = series.SetRetention(ctx, ts.Retention{
	MaxAge:    24 * time.Hour,
	MaxPoints: 100_000,
})
removed, err := series.EnforceRetention(ctx, time.Now().UnixNano())
```

Retention policies are persisted with the series and enforced automatically by
`Insert`/`InsertMany`. `Store.EnforceRetention` applies every persisted policy.
`Store.SetNow` injects a clock for deterministic tests. Full reference:
[ts/README.md](ts/README.md).

## Vectors with `vec`

Collections store fixed-dimension vectors with payload filters. Choose HNSW for
large collections or `FlatIndex` for exact search.

```go
docs, err := store.Vec().NewCollection("docs", 3, vec.Cosine, true)
if err != nil {
	return err
}

err = docs.Upsert([]vec.PointStruct{
	{
		ID:      "doc-1",
		Vector:  []float32{1, 0, 0},
		Payload: vec.Payload{"lang": "en", "year": 2026},
	},
	{
		ID:      "doc-2",
		Vector:  []float32{0, 1, 0},
		Payload: vec.Payload{"lang": "de", "year": 2025},
	},
})
if err != nil {
	return err
}

hits, err := docs.Search([]float32{1, 0, 0}, nil, 5)
```

Distance metrics are `vec.Cosine`, `vec.Euclid` and `vec.Dot`. Pass `true` as
the last `NewCollection` argument for HNSW; pass `false` for exact flat search.
Results are ordered by relevance: descending for Cosine/Dot, ascending for
Euclid.

### Filters

```go
filter := &vec.Filter{
	Must: []vec.Condition{
		{Key: "lang", Type: vec.MatchTypeExact, Match: vec.MatchValue{Value: "en"}},
		{Key: "year", Type: vec.MatchTypeRange, Range: &vec.RangeValue{GTE: 2020}},
	},
	MustNot: []vec.Condition{
		{Key: "status", Match: vec.MatchValue{Value: "deleted"}},
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

### Buffering and lifecycle

`Upsert` updates the in-memory index immediately and buffers points for bbolt.
The buffer is flushed when `vec.DefaultFlushThreshold` points accumulate, or
when you call `docs.Flush()` / `store.Close()`. The root `Store.Close` flushes
the vec store before closing the shared handle.

If you open `vec` directly with your own bbolt handle, call `vecStore.Flush()`
before closing that handle, or use `SetOwnsDB(false)` when the handle is shared.

### Quantization

Enable scalar quantization to reduce stored vector size:

```go
vstore, err := vec.Open(ctx, bdb, vec.Config{
	Root:     [][]byte{[]byte("vec")},
	UseQuant: true, // nil Quantizer selects SQ8
})
```

`bdb` here is a caller-owned `*bolt.DB` opened with `bolt.Open`. The root
package exposes the same setting as `harbor.Options{UseQuant: true}`.

The quantization mode is persisted in collection metadata: reopening a
quantized collection without quantization, or a plaintext collection with
quantization, is rejected. The quantizer implementation itself is not recorded;
use the same one for every process that opens the collection.

Full reference: [vec/README.md](vec/README.md).

## Clustering

Harbor ships a `consensus` package that replicates a whole store across a
cluster of processes using the [Raft](https://github.com/hashicorp/raft)
consensus algorithm. Each node keeps its own harbor database; writes are
proposed through raft on the leader and applied, in log order, to the same
logical state on every node. Reads are served from the local replica.

```go
node, err := consensus.Open(ctx, consensus.Config{
    NodeID:     "node-1",
    BindAddr:   "127.0.0.1:7001",
    Dir:        "./data/node-1",
    Bootstrap:  true, // first node forms the cluster
    StorePath:  "./data/node-1/harbor.db",
    StoreOpts:  &harbor.Options{Password: "shared-secret"},
})

// Writes go through the replicated facades (leader only).
err = node.DB().Save(ctx, &User{ID: "u1", Name: "alice"})
err = node.TS().Insert(ctx, "cpu", ts.Point{Timestamp: now, Value: 42})

// Reads hit the local store directly.
var u User
_ = node.Store().DB().One(ctx, "ID", "u1", &u)
```

Every node in a cluster must use the same password. A second node opens with
`Bootstrap: false` and joins the leader with `Join`; nodes leave with `Leave`.
Off-leader writes return `consensus.ErrNotLeader`, and `Leader` reports the
current leader's address for client-side redirect. See
[pkg/consensus/README.md](pkg/consensus/README.md) for the full API, the consistency
model and deployment notes.

## Encryption

A new store is encrypted by default with `EncryptAll`. The password is never
stored: it is stretched with Argon2id, then SLIP-0021 derives one key per
sub-store. Encryption flags are persisted in unencrypted metadata and bound to
a sealed canary, so opening a store with the wrong password, key, or mode fails
loudly instead of returning garbage.

```go
store, err := harbor.Open(ctx, "app.db", 0o600, &harbor.Options{
	Password: "correct horse battery staple",
	Encrypt:  harbor.EncryptDB | harbor.EncryptTS,
})
```

Use `Plaintext: true` to create an unencrypted store, or choose individual
flags:

| Flag | Protects |
|------|----------|
| `EncryptDBKeys` | db primary keys, index values, key/value keys |
| `EncryptDBValues` | db record and key/value values |
| `EncryptDBMetadata` | db auto-increment counters |
| `EncryptFSContent` | fs file content and symlink targets |
| `EncryptFSMetadata` | fs inode blobs (names, modes, sizes, times) |
| `EncryptTSValues` | ts point values and series metadata |
| `EncryptTSSeriesNames` | ts series and metadata names |
| `EncryptVecPointIDs` | vec point IDs; also the stored point value |
| `EncryptVecCollectionNames` | vec collection names |
| `EncryptVecPoints` | vec point blobs and collection metadata |

Aggregates (`EncryptDB`, `EncryptFS`, `EncryptTS`, `EncryptVec`) and
`EncryptAll` combine these bits. The encryption and KDF fields of `Options`
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
change the password: the root `harbor` package derives its keys from the
password and does not expose a rotation API. Rotate keys only when you open a
sub-store directly with your own key, or export and re-import to change the
password.

Full rotation uses the whole-store methods:

| Store | Method |
|-------|--------|
| `db` | `ReEncrypt` or `RotateAll` (aliases) |
| `fs` | `RotateAll` |
| `ts` | `RotateAll` |
| `vec` | `RotateAll` |

A full rotation is atomic within its store and adopts the new key when it
completes. Run it with **exclusive access** to the store; do not write from
other goroutines while it runs. `VerifyAll` re-reads every value and confirms
it decrypts with the configured keys.

The per-entity methods (`fs.ReEncryptFS`, `ts.ReEncryptSeries`,
`vec.ReEncryptCollection`) are **partial** migrations. The marker stays under
the current key and the previous key remains available in the running process,
so entities that have not been rewritten keep working. Rewrite every entity,
call `RotateAll`, and only then close the store or reopen it from a fresh
process. Closing a store between a partial rotation and `RotateAll` is
unsupported.

Partial rotation cannot run when deterministic names are encrypted: `ts` with
`EncryptSeriesNames`, or `vec` with `EncryptPointIDs` or
`EncryptVecCollectionNames`. In those modes the physical names are derived from
one master key, so the remaining entities could neither be found nor decrypted
after the new key is installed. The methods return `ErrInvalidConfig`; use
`RotateAll`.

### Threat model

Harbor's encryption protects **data at rest** against someone who obtains the
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

- Create the database with restrictive permissions and pass a lock timeout in
  `bolt.Options`.
- Keep the password out of logs, crash reports, and command-line arguments.
- Back up the database file and the password/key separately.
- Test restores and run `VerifyAll` after moving data between environments.
- Do not copy a database out of a running rotation or partial migration.

For the key-derivation pipeline, canary design, ciphertext format and
store-specific encryption internals, see
[AGENTS.md](AGENTS.md#encryption-internals).

## Audit trail

Harbor can record every mutating operation as a **tamper-evident, append-only
audit trail**. Each event is signed with an Ed25519 key derived from the store
password (SLIP-0010, one key per sub-store) and linked to its predecessor by a
hash chain, so the history cannot be forged, altered, reordered or spliced
without the key material.

Auditing is enabled per open with `Options.Audit`, mirroring the encryption
flags:

```go
store, err := harbor.Open(ctx, "app.db", 0o600, &harbor.Options{
	Password: "correct horse battery staple",
	Audit:    harbor.AuditAll, // or AuditDB | AuditTS, …
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
| `AuditDB` | `db` writes |
| `AuditFS` | `fs` writes |
| `AuditTS` | `ts` writes |
| `AuditVec` | `vec` writes |
| `AuditAll` | all four |

Unlike encryption flags, audit flags are **not persisted**: they are a per-open
runtime choice, so enabling or disabling auditing never changes the on-disk
format. The trail lives in an `audit/` bucket in the same file, so it travels
with snapshots and backups.

The trail detects interior tampering but **cannot by itself detect tail
truncation** — deleting the last N events leaves a self-consistent prefix.
Anchor `store.Audit().Head()` out of band on a schedule to detect that. The
signing keys derive from the password, so anyone holding it can produce valid
events; the trail proves integrity against an attacker *without* the key, the
same trust boundary as the encryption itself.

See [pkg/audit/README.md](pkg/audit/README.md) for the event model, the recorded
operations, the full threat model and how to use the `audit` package directly.

## Database lifecycle

The two entry points differ in who owns the bbolt handle:

- `harbor.Open` opens the file itself; `Store.Close` flushes buffered vector
  writes and closes the database. It is safe to call more than once, and the
  sub-store accessors return `nil` afterwards.
- `db.Open`, `fs.Open`, `ts.Open` and `vec.Open` wrap an already-open
  `*bolt.DB`. Their `Close` methods release the wrapper only; the caller keeps
  ownership of the database. If several stores share one handle, close the
  handle after all wrappers are closed.

When embedding `vec` on a shared handle, set `vec.Config.SharedDB` (or call
`SetOwnsDB(false)`): `vec.Store.Close` then flushes buffered points without
closing the handle. Use `Store.Vec().Flush()` to drain explicitly and
`Store.Vec().Closed()` to check the state. Per collection, `Pending()` reports
buffered points and `Close()` flushes and retires that handle. The root store
does this automatically: `harbor.Store.Close` and `harbor.Store.Backup` flush
the vector buffer before touching the database.

A `nil` context is rejected by context-aware operations with `ErrNilContext`
(or the store's equivalent sentinel); `Close` is a lifecycle exception and
takes no context. `harbor.Open` is the one exception: it treats a nil context
as `context.Background()`.

### Backup

`Backup` writes a consistent, point-in-time snapshot of the whole database to
an `io.Writer`; `BackupTo` writes the same snapshot to a file atomically.

```go
var buf bytes.Buffer
err := store.Backup(ctx, &buf)

err = store.BackupTo(ctx, "/backups/harbor-2026-11-23.db")
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

To restore, open the snapshot file with `harbor.Open` and the same password.
`View` remains available for lower-level raw bbolt snapshots.

### Export, import and compaction

`Backup` copies the database file; `Export` writes a versioned, checksummed
*logical* snapshot that `Import` applies to another store. That is the migration
path between encryption modes and formats:

```go
var buf bytes.Buffer
summary, err := harbor.Export(ctx, store, &buf, harbor.ExportOptions{
	RecordTypes: []any{(*User)(nil)}, // db record types to include
	Progress:    func(p harbor.Progress) { log.Printf("%s: %d", p.Section, p.Records) },
})

restored, err := harbor.Open(ctx, "encrypted.db", 0o600, &harbor.Options{Password: "new"})
err = harbor.Import(ctx, restored, bytes.NewReader(buf.Bytes()), harbor.ImportOptions{
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
err := store.DB().One(ctx, "Email", "missing@example.com", &user)
switch {
case errors.Is(err, db.ErrNotFound):
	// Record does not exist.
case errors.Is(err, context.Canceled):
	// The request was cancelled.
}
```

Errors are wrapped as `harbor <operation>: <cause>` while preserving the
original cause, context errors, bbolt errors and callback errors in the chain.
The package guides list each store's sentinels; use `errors.Is` with those
values rather than parsing messages.

## Contexts and cancellation

CRUD, query, scan, key/value and managed-transaction operations accept a
context. Cancellation is cooperative: Harbor checks at operation boundaries and
loop checkpoints, but cannot interrupt a bbolt lock wait, a syscall, codec code,
or a callback that has not returned. No background goroutines are used to
simulate cancellation, and successfully committed writes or published read
results are not retroactively changed to cancellation errors.

## Performance notes

The defaults favor portability and safety. For throughput-sensitive workloads:

- Pass a faster `db.Codec` than JSON for decode-heavy scans.
- Batch writes with `db.WriteTransaction`, `ts.Series.InsertMany`, or
  `vec.Collection.Upsert` instead of writing one record per loop; every bbolt
  commit is an fsync.
- For encrypted time series, set `ts.Config.BlockDuration` so blocks are sealed
  and opened once instead of once per point.
- Use `fs.Config.ChunkSize` for large files so writes and truncations touch only
  the affected chunks.

The repository ships benchmarks for the hot paths:

```sh
go test -run '^$' -bench . -benchmem ./vec ./ts
```

## Documentation

- [db/README.md](db/README.md) — models, tags, CRUD, queries, transactions,
  key/value access, and codecs.
- [fs/README.md](fs/README.md) — file systems, snapshots, external content, and
  encryption.
- [ts/README.md](ts/README.md) — series, aggregation, retention, and encrypted
  blocks.
- [vec/README.md](vec/README.md) — collections, indexes, filters, quantization,
  and buffering.
- [fs/s3fs/README.md](fs/s3fs/README.md) — the S3-backed `absfs.Filer`.
- [pkg/audit/README.md](pkg/audit/README.md) — the tamper-evident audit trail:
  event model, recorded operations, threat model, and direct package use.
- [pkg/store/README.md](pkg/store/README.md) — the process-wide database
  manager, directory layout, and password file.
- [pkg/blob/README.md](pkg/blob/README.md) — the flat object store, containment,
  and grants.
- [pkg/snapshot/README.md](pkg/snapshot/README.md) — content-addressed
  incremental snapshots, restore, retention, and scheduling.
- [AGENTS.md](AGENTS.md) — architecture, on-disk layout, encryption internals,
  development workflow, and repository conventions for contributors and coding
  agents.

Generated API documentation is published on pkg.go.dev:

- [github.com/malivvan/harbor](https://pkg.go.dev/github.com/malivvan/harbor)
- [github.com/malivvan/harbor/db](https://pkg.go.dev/github.com/malivvan/harbor/db)
- [github.com/malivvan/harbor/fs](https://pkg.go.dev/github.com/malivvan/harbor/fs)
- [github.com/malivvan/harbor/ts](https://pkg.go.dev/github.com/malivvan/harbor/ts)
- [github.com/malivvan/harbor/vec](https://pkg.go.dev/github.com/malivvan/harbor/vec)
- [github.com/malivvan/harbor/pkg/audit](https://pkg.go.dev/github.com/malivvan/harbor/pkg/audit)
- [github.com/malivvan/harbor/pkg/blob](https://pkg.go.dev/github.com/malivvan/harbor/pkg/blob)
- [github.com/malivvan/harbor/pkg/consensus](https://pkg.go.dev/github.com/malivvan/harbor/pkg/consensus)
- [github.com/malivvan/harbor/pkg/id](https://pkg.go.dev/github.com/malivvan/harbor/pkg/id)
- [github.com/malivvan/harbor/pkg/metrics](https://pkg.go.dev/github.com/malivvan/harbor/pkg/metrics)
- [github.com/malivvan/harbor/pkg/sign/canonical](https://pkg.go.dev/github.com/malivvan/harbor/pkg/sign/canonical)
- [github.com/malivvan/harbor/pkg/snapshot](https://pkg.go.dev/github.com/malivvan/harbor/pkg/snapshot)
- [github.com/malivvan/harbor/pkg/snapshot/blobstore](https://pkg.go.dev/github.com/malivvan/harbor/pkg/snapshot/blobstore)
- [github.com/malivvan/harbor/pkg/store](https://pkg.go.dev/github.com/malivvan/harbor/pkg/store)

Where to start:

- **New to Harbor:** [Quick start](#quick-start), then the guide sections for
  the stores you need.
- **Records, indexes and transactions:** [db/README.md](db/README.md).
- **Files, snapshots or external content:** [fs/README.md](fs/README.md) and
  [fs/s3fs/README.md](fs/s3fs/README.md).
- **Metrics and time series:** [ts/README.md](ts/README.md).
- **Embeddings and similarity search:** [vec/README.md](vec/README.md).
- **Process-wide database, blobs or object snapshots:**
  [pkg/store/README.md](pkg/store/README.md),
  [pkg/blob/README.md](pkg/blob/README.md) and
  [pkg/snapshot/README.md](pkg/snapshot/README.md).
- **Security review or operations:** [Encryption](#encryption) and
  [AGENTS.md](AGENTS.md#architecture).

Every public package has a package-level overview in its doc comment, and every
exported identifier is documented. Compile-checked examples live next to the
code where they are most useful (for the `db` package, see
`db/examples_test.go`).

## Status

Harbor is preparing its first public release (`v0.1.x`). The on-disk formats
are versioned and refuse data from an incompatible build instead of misreading
it. Every persisted format is version 1 (`db.Version`, `fs.FormatVersion`,
`ts.FormatVersion`, `vec.CollectionFormatVersion`, `audit.FormatVersion`);
there is no v2 or dual-version compatibility path. Pin a module version, and
treat pre-1.0 API changes as potentially breaking.

## License

MIT — see [LICENSE](LICENSE).

Harbor is a fork of [asdine/storm](https://github.com/asdine/storm) and keeps
the original MIT copyright notices. It also vendors code derived from the Go
project (Argon2, BLAKE2b, CPU feature detection), the `absfs` interfaces, and
[`coder/hnsw`](https://github.com/coder/hnsw); their licenses are preserved next
to the code.

## Credits

- [Asdine El Hrychy](https://github.com/asdine)
- [Anderson Bargas](https://github.com/andersonbargas)
- [Bjørn Erik Pedersen](https://github.com/bep)
