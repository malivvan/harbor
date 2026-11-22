# Getting started with Harbor

This guide walks through the four Harbor stores using the root `harbor`
package, which puts them behind one bbolt file and one password. If you only
need one store, the same APIs are available directly through `db`, `fs`, `ts`
and `vec` with your own `*bolt.DB`.

## Requirements

- Go 1.27.4 or later
- A writable path for the database file

```sh
go get github.com/malivvan/harbor
```

## Open a store

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/malivvan/harbor"
	bolt "go.etcd.io/bbolt"
)

func main() {
	ctx := context.Background()

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

	_ = ctx
}
```

Important properties of `harbor.Open`:

- The first call creates the file and writes unencrypted metadata (salt,
  Argon2id parameters, encryption flags, and a sealed canary).
- Later calls use the persisted settings. `KDFTime`, `KDFMemory`, `KDFThreads`,
  `Encrypt` and `Plaintext` are ignored for an existing store.
- The password is always required, including for plaintext stores, because the
  canary is used to detect a wrong password.
- `Store.Close` flushes buffered vector writes and closes the database that
  `Open` created. It is safe to call twice.

Use `Options{Plaintext: true}` for an unencrypted store, or select individual
encryption flags with `Options.Encrypt` (see
[encryption.md](encryption.md)).

## Records with `db`

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

Save a record and fetch it back:

```go
users := store.DB()

u := User{Name: "alice", Email: "alice@example.com", Team: "core"}
if err := users.Save(ctx, &u); err != nil {
	return err // unique violations match db.ErrAlreadyExists
}

var found User
if err := users.One(ctx, "Email", "alice@example.com", &found); err != nil {
	return err // no match matches db.ErrNotFound
}
```

`Save` creates the bucket and indexes on first use. `Update` updates non-zero
fields; `UpdateField` updates one field, including zero values. The primary key
cannot be updated (`db.ErrImmutableID`).

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
`All` if you want an empty slice contract.

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
bbolt unwinds the transaction.

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
notes := users.From("notes")             // nested bucket
private := notes.From("private")         // deeper still

err := users.Set(ctx, "sessions", sessionID, &session)
var session Session
err = users.Get(ctx, "sessions", sessionID, &session)

raw, err := users.GetBytes(ctx, "cache", "key")
err = users.SetBytes(ctx, "cache", "key", []byte("value"))
exists, err := users.KeyExists(ctx, "sessions", sessionID)
err = users.Delete(ctx, "sessions", sessionID)
```

Full reference: [db/README.md](../db/README.md).

## Files with `fs`

A `fs.Store` manages named file systems below one root bucket. Each file system
implements the `absfs` interfaces, including symbolic links.

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
store3, err := fs.Open(ctx, bdb, fs.Config{
	Root:            [][]byte{[]byte("fs")},
	Secret:          key, // 32 bytes
	EncryptMetadata: true,
	EncryptContent:  true,
	ChunkSize:       64 * 1024,
})
```

Full reference: [fs/README.md](../fs/README.md).

## Time series with `ts`

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
time order without an index.

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
`Store.SetNow` injects a clock for deterministic tests.

Full reference: [ts/README.md](../ts/README.md).

## Vectors with `vec`

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

Condition types are `MatchTypeExact`, `MatchTypeRange`, `MatchTypePrefix`,
`MatchTypeContains` and `MatchTypeRegex`.

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

A quantized collection must be reopened with the same quantizer; the mode is
persisted and validated.

Full reference: [vec/README.md](../vec/README.md).

## Errors and contexts

Use `errors.Is` with the package sentinels:

```go
var user User
err := users.One(ctx, "Email", "missing@example.com", &user)
if errors.Is(err, db.ErrNotFound) {
	// Handle the missing record.
}
if errors.Is(err, context.Canceled) {
	// The caller went away.
}
```

Errors are wrapped as `harbor <operation>: <cause>` and keep the original error
in the chain. A nil context passed to a context-aware operation is rejected
with the store's `ErrNilContext` sentinel.

## Next steps

- [encryption.md](encryption.md) — how keys are derived, what each flag
  protects, and how to rotate keys safely.
- [architecture.md](architecture.md) — on-disk layout and concurrency model.
- [docs/README.md](README.md) — the documentation index.
- [db/README.md](../db/README.md), [fs/README.md](../fs/README.md),
  [ts/README.md](../ts/README.md), [vec/README.md](../vec/README.md) — complete
  package guides.
