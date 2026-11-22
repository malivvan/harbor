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

## Features

- **One file, one password.** Argon2id stretches the password; SLIP-0021
  domain separation derives one independent key per sub-store.
- **Encryption is built in, not bolted on.** AES-256-GCM seals records, files,
  points and documents; deterministic AES-SIV keeps exact lookups working when
  keys themselves must be hidden.
- **Context-aware.** Database operations accept `context.Context` and honor
  cancellation at operation boundaries and loop checkpoints.
- **Transactional.** Managed read/write transactions, plus `Attach` for mixing
  Harbor operations with raw bbolt writes in one atomic transaction.
- **Concurrency-tested.** The suite runs with `-race`, and CI also benchmarks
  the hot paths on every pull request.
- **No C dependencies and no external database.** A single Go module and one
  bbolt file.

## Installation

Harbor requires Go 1.27.4 or later.

```sh
go get github.com/malivvan/harbor
```

The module contains the root `harbor` package and four sub-packages:

| Package | Import path | Guide |
|---------|-------------|-------|
| `harbor` | `github.com/malivvan/harbor` | this file, [docs/getting-started.md](docs/getting-started.md) |
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

## What each store gives you

### Records — `db`

Map Go structs to buckets, declare indexes with struct tags, and query with
`One`, `Find`, `All`, `Range`, `Prefix`, or the matcher API.

```go
type User struct {
	ID        int       `db:"id,increment"`
	Team      string    `db:"index"`
	Email     string    `db:"unique"`
	Name      string
	CreatedAt time.Time `db:"index"`
}

users := store.DB()
if err := users.Save(ctx, &User{Team: "core", Email: "a@example.com"}); err != nil {
	return err
}

var team []User
err := users.Find(ctx, "Team", "core", &team, db.Limit(20), db.Skip(0))
```

```go
import (
	"github.com/malivvan/harbor/db"
	"github.com/malivvan/harbor/db/q"
)

var recent []User
err := users.Select(q.Gte("CreatedAt", cutoff)).
	OrderBy("CreatedAt").Reverse().Limit(50).Find(ctx, &recent)
```

Full guide: [db/README.md](db/README.md). Supported tags are `id`, `increment`,
`index`, `unique` and `inline`; implement `db.BucketNamer` for a custom bucket
name.

### Files — `fs`

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
if _, err := f.Write([]byte("ok")); err != nil {
	return err
}
if err := f.Close(); err != nil {
	return err
}

data, err := vfs.ReadFile("/reports/2026/q1.txt")
snapshot, err := vfs.CreateSnapshot("before-import")
defer snapshot.Release()
```

`fs` supports symbolic links, `filepath.Walk`-style traversal, concurrent
`FastWalk`, read-only snapshots, an inode cache, external content back ends, and
chunked encrypted content. Full guide: [fs/README.md](fs/README.md).

### Time series — `ts`

Points are keyed by their timestamp, so no secondary index is needed for range
scans. Series carry labels and an optional retention policy.

```go
import "github.com/malivvan/harbor/ts"

series := store.TS().Series("requests")
now := time.Now().UnixNano()

if err := series.InsertMany(ctx, []ts.Point{
	{Timestamp: now - int64(time.Minute), Value: 120},
	{Timestamp: now, Value: 148},
}); err != nil {
	return err
}

points, err := series.Range(ctx, now-int64(time.Hour), now)
samples, err := series.Aggregate(ctx, now-int64(time.Hour), now,
	int64(time.Minute), ts.AggAvg)

err = series.SetRetention(ctx, ts.Retention{
	MaxAge:    24 * time.Hour,
	MaxPoints: 100_000,
})
```

Full guide: [ts/README.md](ts/README.md).

### Vectors — `vec`

Collections store fixed-dimension vectors with payload filters. Choose HNSW for
large collections or `FlatIndex` for exact search.

```go
import "github.com/malivvan/harbor/vec"

docs, err := store.Vec().NewCollection("docs", 3, vec.Cosine, true)
if err != nil {
	return err
}

err = docs.Upsert([]vec.PointStruct{
	{ID: "a", Vector: []float32{1, 0, 0}, Payload: vec.Payload{"lang": "en"}},
	{ID: "b", Vector: []float32{0, 1, 0}, Payload: vec.Payload{"lang": "de"}},
})
if err != nil {
	return err
}

filter := &vec.Filter{Must: []vec.Condition{
	{Key: "lang", Match: vec.MatchValue{Value: "en"}},
}}
hits, err := docs.Search([]float32{1, 0, 0}, filter, 5)
```

Writes are buffered and flushed in batches; call `docs.Flush()` or
`store.Close()` before shutting down. Full guide: [vec/README.md](vec/README.md).

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
| `EncryptVecPointIDs` | vec point IDs |
| `EncryptVecCollectionNames` | vec collection names |
| `EncryptVecPoints` | vec point blobs and collection metadata |

Aggregates (`EncryptDB`, `EncryptFS`, `EncryptTS`, `EncryptVec`) and
`EncryptAll` combine these bits. The encryption and KDF fields of `Options`
only apply when creating a store; an existing store always uses its persisted
settings.

Key rotation is available on the sub-store packages through `RotateAll` /
`ReEncrypt` (and `VerifyAll` verifies that every value decrypts). A full
rotation requires exclusive access to the store. The root `harbor` package
derives its keys from the password and does not expose rotation: use the
sub-store packages directly with keys you manage, or export and re-import to
change the password. Migrating a plaintext store to an encrypted one in place
is not supported. See [docs/encryption.md](docs/encryption.md) for the threat
model and the partial rotation caveats.

## Database lifecycle

The two entry points differ in who owns the bbolt handle:

- `harbor.Open` opens the file itself; `Store.Close` flushes buffered vector
  writes and closes the database. It is safe to call more than once, and the
  sub-store accessors return `nil` afterwards.
- `db.Open`, `fs.Open`, `ts.Open` and `vec.Open` wrap an already-open
  `*bolt.DB`. Their `Close` methods release the wrapper only; the caller keeps
  ownership of the database. If several stores share one handle, close the
  handle after all wrappers are closed.

A `nil` context is rejected by context-aware operations with `ErrNilContext`
(or the store's equivalent sentinel); `Close` is a lifecycle exception and
takes no context.

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

- [docs/getting-started.md](docs/getting-started.md) — a guided tour of all four
  stores, transactions, and lifecycle.
- [docs/encryption.md](docs/encryption.md) — key derivation, encryption flags,
  rotation, and threat model.
- [docs/architecture.md](docs/architecture.md) — package map, on-disk layout,
  concurrency model, and compatibility.
- [db/README.md](db/README.md) — models, tags, CRUD, queries, transactions,
  key/value access, and codecs.
- [fs/README.md](fs/README.md) — file systems, snapshots, external content, and
  encryption.
- [ts/README.md](ts/README.md) — series, aggregation, retention, and encrypted
  blocks.
- [vec/README.md](vec/README.md) — collections, indexes, filters, quantization,
  and buffering.
- [fs/s3fs/README.md](fs/s3fs/README.md) — the S3-backed `absfs.Filer`.

## Development

```sh
# Formatting, vet and build
test -z "$(gofmt -l .)"
go vet ./...
go build ./...

# Tests
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./...

# Static analysis
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...

# Coverage
go test -count=1 -timeout 180s -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

CI runs the same checks on every push to `master` and every pull request; see
[.github/workflows/ci.yml](.github/workflows/ci.yml).

## Status

Harbor is at its first public release (`v0.1.x`). The on-disk formats are
versioned and refuse data from an incompatible build instead of misreading it.
Pin a module version, and treat pre-1.0 API changes as potentially breaking.

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
