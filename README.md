# Storm

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/storm/db.svg)](https://pkg.go.dev/github.com/malivvan/storm/db)
[![CI](https://github.com/malivvan/storm/actions/workflows/main.yml/badge.svg)](https://github.com/malivvan/storm/actions/workflows/main.yml)

Storm is a simple and powerful toolkit for [BoltDB](https://github.com/etcd-io/bbolt), forked from [Storm](https://github.com/asdine/storm). It provides indexes, a wide range of methods to store and fetch data, an advanced query system, and much more.

## Table of Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Declaring models](#declaring-models)
- [Model tags](#model-tags)
- [Custom bucket names](#custom-bucket-names)
- [Opening a database](#opening-a-database)
- [CRUD operations](#crud-operations)
  - [Save](#save)
  - [Fetch by ID or field](#fetch-by-id-or-field)
  - [Fetch multiple records](#fetch-multiple-records)
  - [Query API](#query-api)
  - [Update](#update)
  - [Delete](#delete)
  - [Count, Init, Drop, ReIndex](#count-init-drop-reindex)
- [Managed transactions](#managed-transactions)
  - [Attaching to a caller-owned transaction](#attaching-to-a-caller-owned-transaction)
- [Nodes and nested buckets](#nodes-and-nested-buckets)
- [Key/value store](#keyvalue-store)
- [Context and cancellation](#context-and-cancellation)
- [Error handling](#error-handling)
- [Database lifecycle](#database-lifecycle)
- [Native BoltDB access](#native-boltdb-access)
- [Codecs](#codecs)
- [Testing and CI](#testing-and-ci)
- [License](#license)
- [Credits](#credits)

## Installation

Storm requires **Go 1.24** or later.

```sh
go get github.com/malivvan/storm
```

```go
import storm "github.com/malivvan/storm/db"
```

The public API lives in the `db` package.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	storm "github.com/malivvan/storm/db"
	bolt "go.etcd.io/bbolt"
)

type User struct {
	ID    int    `storm:"id,increment"`
	Name  string `storm:"index"`
	Email string `storm:"unique"`
	Age   int    `storm:"index"`
}

func main() {
	ctx := context.Background()

	bDB, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := bDB.Close(); err != nil {
			log.Println("close:", err)
		}
	}()

	db, err := storm.Open(ctx, bDB, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close:", err)
		}
	}()

	// Save
	u := User{Name: "Alice", Email: "alice@example.com", Age: 30}
	if err := db.Save(ctx, &u); err != nil {
		log.Fatal(err)
	}
	fmt.Println("saved ID:", u.ID)

	// Fetch by indexed field
	var found User
	if err := db.One(ctx, "Email", "alice@example.com", &found); err != nil {
		log.Fatal(err)
	}
	fmt.Println("found:", found.Name)
}
```

## Declaring models

Storm maps Go structs to BoltDB buckets. The struct name becomes the bucket name unless a `BucketNamer` implementation provides a custom name.

```go
type User struct {
	ID        int       `storm:"id,increment"`
	Group     string    `storm:"index"`
	Email     string    `storm:"unique"`
	Name      string
	Age       int       `storm:"index"`
	CreatedAt time.Time `storm:"index"`
}
```

The primary key is identified by the `id` tag or — if no `id` tag is present — by a field named `ID`. It must be non-zero and consistently serializable by the configured codec. Auto-increment is available only for integer fields.

### Inline structs

Use the `inline` tag to flatten embedded structs. The container's bucket name is used.

```go
type Base struct {
	Ident string `storm:"id"`
}

type User struct {
	Base      `storm:"inline"`
	Group     string `storm:"index"`
	Email     string `storm:"unique"`
	Name      string
	CreatedAt time.Time `storm:"index"`
}
```

## Model tags

| Tag          | Description |
|--------------|-------------|
| `id`         | Marks the primary key field. |
| `increment`  | Auto-increments integer fields. `increment=100` sets a starting value. |
| `index`      | Creates a list index for the field. |
| `unique`     | Creates a unique index for the field. |
| `inline`     | Flattens an embedded struct into the parent. |

All tags belong to the `storm` struct-tag namespace: `` `storm:"id,increment"` ``.

## Custom bucket names

### `BucketNamer` interface

Structs can implement `BucketNamer` to provide a custom bucket name:

```go
type User struct {
	ID   int    `storm:"id,increment"`
	Name string `storm:"index"`
}

func (u User) StormBucketName() string {
	return "customers"
}
```

The custom bucket name is used consistently across all list and point operations.

### Runtime-generated structs

Anonymous types created via `reflect.StructOf` can be used with `db.From()` to provide an explicit bucket name:

```go
dynType := reflect.StructOf([]reflect.StructField{
	{
		Name: "ID",
		Type: reflect.TypeOf(uint64(0)),
		Tag:  reflect.StructTag(`storm:"id,increment"`),
	},
	{
		Name: "Name",
		Type: reflect.TypeOf(""),
		Tag:  reflect.StructTag(`storm:"index"`),
	},
})

record := reflect.New(dynType)
record.Elem().FieldByName("Name").SetString("Dynamic Alice")
if err := db.From("dynamic_users").Save(ctx, record.Interface()); err != nil {
	log.Fatal(err)
}
```

Use the same reconstructed runtime type and explicit node path for subsequent reads and mutations.

## Opening a database

`Open` wraps an already-open bbolt database:

```go
func Open(ctx context.Context, db *bolt.DB, root []byte, c codec.MarshalUnmarshaler) (*DB, error)
```

The parameters are:

- `db` — a caller-owned `*bolt.DB`. It must be non-nil and remain open while Storm is in use.
- `root` — an optional root bucket key. Pass `nil` or an empty slice to operate from the database root.
- `c` — the codec used for entities. Pass `nil` to use the default JSON codec.

`Open` does not take ownership of `db`. `DB.Close()` releases the Storm wrapper but
leaves the bbolt database open; the caller remains responsible for closing it.

```go
bDB, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: 10 * time.Second})
if err != nil {
	log.Fatal(err)
}
defer func() {
	if err := bDB.Close(); err != nil {
		log.Println("native close:", err)
	}
}()

db, err := storm.Open(ctx, bDB, nil, nil)
if err != nil {
	log.Fatal(err)
}
defer func() {
	if err := db.Close(); err != nil {
		log.Println("storm close:", err)
	}
}()
```

A nil `db` is rejected with an error matching `ErrNilParam`. Use `From` to
operate below a root bucket or `WithCodec` to select a different codec for a
node.

## CRUD operations

Database open, CRUD, finder, query, scanner, KV, and managed-transaction operations accept `context.Context`. Lifecycle and escape-hatch methods such as `Close` and `NativeDB` do not.

### Save

```go
user := User{Name: "John", Email: "john@example.com", Age: 21}
err := db.Save(ctx, &user)
// user.ID is now set if auto-increment is configured
```

`Save` creates the bucket and indexes if needed, enforces unique constraints, and persists the record. Saving a record with a duplicate unique value returns `ErrAlreadyExists`.

### Auto-increment

```go
type Product struct {
	ID                 int    `storm:"id,increment"`
	Name               string
	Count              uint64 `storm:"increment"`
	IndexedCount       uint32 `storm:"index,increment"`
	UniqueCount        int16  `storm:"unique,increment=100"`
}

p := Product{Name: "Widget"}
db.Save(ctx, &p)
// p.ID == 1, p.Count == 1, p.IndexedCount == 1, p.UniqueCount == 100
```

### Fetch by ID or field

```go
var user User

// By indexed unique field
err := db.One(ctx, "Email", "john@example.com", &user)

// By unindexed field (full scan)
err = db.One(ctx, "Name", "John", &user)

// By primary key
err = db.One(ctx, "ID", 10, &user)
```

`One` uses the index when available, otherwise performs a full scan.

### Fetch multiple records

```go
var users []User
err := db.Find(ctx, "Group", "staff", &users)

// All records
err = db.All(ctx, &users)

// All sorted by index
err = db.AllByIndex(ctx, "CreatedAt", &users)

// Range query on indexed field
err = db.Range(ctx, "Age", 21, 30, &users)

// Prefix query; indexed fields use their index, while Name falls back to a scan.
err = db.Prefix(ctx, "Name", "Jo", &users)
```

Pagination is available via `FindOption`:

```go
err := db.Find(ctx, "Group", "staff", &users, storm.Limit(10), storm.Skip(5))
err = db.All(ctx, &users, storm.Limit(10), storm.Skip(5), storm.Reverse())
```

### Query API

For complex queries, use `Select` with matchers from the `q` package:

```go
import "github.com/malivvan/storm/q"

// Matchers
q.Eq("Name", "John")
q.Gt("Age", 7)
q.Gte("Age", 21)
q.Lt("Age", 77)
q.Lte("Age", 77)
q.In("Group", []string{"staff", "admin"})
q.Re("Name", "^Jo")
q.StrictEq("Field", value)

// Combined matchers
q.And(q.Gt("Age", 7), q.Re("Name", "^J"))
q.Or(q.Eq("Group", "staff"), q.Eq("Group", "admin"))
q.Not(q.Eq("Group", "blocked"))

// Build and execute
var users []User
err := db.Select(q.Gte("ID", 10), q.Lte("ID", 100)).
	Limit(10).Skip(5).Reverse().OrderBy("Age").
	Find(ctx, &users)

var single User
err = db.Select(q.Eq("Email", "john@example.com")).First(ctx, &single)

// Delete matching records
err = db.Select(q.Lt("Age", 18)).Delete(ctx, &User{})

// Count matching records
count, err := db.Select(q.Eq("Group", "staff")).Count(ctx, &User{})

// Iterate one by one
err = db.Select(q.Gte("ID", 10)).Each(ctx, &User{}, func(record any) error {
	u := record.(*User)
	fmt.Println(u.Name)
	return nil
})
```

### Update

```go
// Update non-zero fields
err := db.Update(ctx, &User{ID: 10, Name: "Jack", Age: 45})

// Update a single field (including zero values)
err := db.UpdateField(ctx, &User{ID: 10}, "Age", 0)
```

### Delete

```go
var user User
db.One(ctx, "ID", 10, &user)
err := db.DeleteStruct(ctx, &user)
```

### Count, Init, Drop, ReIndex

```go
// Count records
count, err := db.Count(ctx, &User{})

// Pre-create buckets and indexes
err = db.Init(ctx, &User{})

// Drop a bucket
err = db.Drop(ctx, "User")
err = db.Drop(ctx, &User{})

// Rebuild indexes
err = db.ReIndex(ctx, &User{})
```

## Managed transactions

Storm provides callback-managed transactions only. Manual `Begin`/`Commit`/`Rollback` APIs do not exist.

### ReadTransaction

```go
err := db.ReadTransaction(ctx, func(tx storm.Node) error {
	var u User
	if err := tx.One(ctx, "ID", 1, &u); err != nil {
		return err
	}
	fmt.Println(u.Name)
	return nil
})
```

### WriteTransaction

```go
err := db.WriteTransaction(ctx, func(tx storm.Node) error {
	if err := tx.Save(ctx, &accountA); err != nil {
		return err
	}
	if err := tx.Save(ctx, &accountB); err != nil {
		return err
	}
	return nil
})
```

**Semantics:**

- The callback executes exactly once (`bbolt.Update` never retries).
- If the callback returns an error, the transaction rolls back and the callback error is returned.
- If the context is cancelled before commit, the transaction rolls back and `ctx.Err()` is returned.
- If the callback cancels the context and also returns an error, the callback error remains primary.
- After a successful commit, cancellation is not retroactively applied.
- Panics propagate unchanged after bbolt unwinds the transaction.

### Attaching to a caller-owned transaction

For units of work that must own their bbolt transaction (for example, to mix
Storm record/index operations with raw BoltDB bucket writes), use `Attach`
to bind a Storm facade to an existing transaction:

```go
tx, err := db.NativeDB().Begin(true)
if err != nil {
	return err
}
defer tx.Rollback()

txn, err := db.Attach(tx) // storm.Tx
if err != nil {
	return err
}

// Storm record write (record + indexes) in the caller's transaction…
if err := txn.Save(ctx, &account); err != nil {
	return err
}

// …and a raw BoltDB write in the same transaction, committed atomically.
audit, err := tx.CreateBucketIfNotExists([]byte("audit"))
if err != nil {
	return err
}
if err := audit.Put([]byte("event"), []byte("created")); err != nil {
	return err
}

return tx.Commit()
```

**Semantics:**

- `Tx` exposes the full `Node` surface (`Save`, `Update`, `One`, `Find`,
  `All`, queries, scans, KV access); descendants from `From`, `WithCodec`,
  `PrefixScan`, and `RangeScan` remain transaction-bound.
- Storm never commits, rolls back, closes, or replaces the supplied
  transaction, and it never opens a managed transaction for a bound `Tx`.
- Reads and writes through the bound `Tx` use the supplied transaction:
  uncommitted writes are visible to reads through the same `Tx` and invisible
  to every other transaction until the caller commits.
- `Attach` accepts writable and read-only transactions. Writes through a
  read-only transaction fail with bbolt's `ErrTxNotWritable`.
- The transaction must belong to this database: otherwise `Attach` returns
  `ErrTxFromDifferentDB`. A nil transaction or nil receiver returns
  `ErrNilParam`.
- A bound `Tx` is valid only for the lifetime of its transaction. Using it
  after `Commit` or `Rollback` is undefined behavior, exactly like using the
  bbolt transaction itself (writes fail with `ErrTxClosed`; reads may panic).

## Nodes and nested buckets

Use `From` to work with nested buckets. Every node supports the full API:

```go
notes := db.From("notes")
privateNotes := notes.From("private")
workNotes := notes.From("work")

privateNotes.Save(ctx, &Note{ID: "p1", Text: "private"})
workNotes.Save(ctx, &Note{ID: "w1", Text: "work"})

// From accepts any number of path segments
personalNotes := notes.From("private", "personal")
```

A node can be further configured with `WithCodec`:

```go
node := db.From("data").WithCodec(gob.Codec)
```

The `Bucket()` method returns the bucket path as a `[]string`. The `BucketScanner` interface provides `PrefixScan` and `RangeScan` for discovering sub-buckets.

## Key/value store

Storm can also be used as a simple key/value store:

```go
// Store
err := db.Set(ctx, "logs", time.Now(), "breakfast started")
err = db.Set(ctx, "sessions", sessionID, &someUser)

// Retrieve
var user User
err = db.Get(ctx, "sessions", sessionID, &user)

// Raw bytes
raw, err := db.GetBytes(ctx, "logs", key)
err = db.SetBytes(ctx, "cache", "key", []byte("value"))

// Existence check
exists, err := db.KeyExists(ctx, "sessions", sessionID)

// Delete
err = db.Delete(ctx, "sessions", sessionID)
```

## Context and cancellation

Database open, CRUD, finder, query, scanner, KV, and managed-transaction paths accept `context.Context`; passing nil to those context-aware operations returns an error matching `ErrNilContext`. `Close` and `NativeDB` are lifecycle/escape-hatch exceptions.

Cancellation is cooperative, not preemptive. Storm checks the context at operation boundaries and loop checkpoints. It cannot interrupt:

- a filesystem syscall,
- a bbolt mutex or lock wait,
- codec code that does not observe a context,
- a user callback that has not returned control.

No abandoned goroutines are used to simulate cancellation. Published read destinations and successfully committed writes are not retroactively changed to cancellation errors.

Real applications should pass request or job contexts rather than creating a new `context.Background()` at each persistence call.

## Error handling

Always use `errors.Is` for sentinel classification, never direct equality or string comparison:

```go
var user User
if err := db.One(ctx, "ID", 999, &user); err != nil {
	if errors.Is(err, storm.ErrNotFound) {
		// handle missing record
	}
	if errors.Is(err, context.Canceled) {
		// handle cancellation
	}
}
```

### Operation wrapping

Errors are wrapped with an operation label:

```
storm <operation>: <cause>
```

Wrapping preserves Storm sentinels, `context.Canceled`, `context.DeadlineExceeded`, bbolt errors, callback errors, and codec errors in the error chain. Callers can match the innermost cause with `errors.Is` through the wrapper.

### Sentinels

| Sentinel | Meaning |
|----------|---------|
| `ErrNotFound` | Record not found |
| `ErrAlreadyExists` | Duplicate unique value |
| `ErrNilContext` | Nil context passed to an operation |
| `ErrNilParam` | Nil required non-context argument |
| `ErrNoID` | No ID field or `id` tag found |
| `ErrZeroID` | ID field is a zero value |
| `ErrBadType` | Unexpected value type |
| `ErrUnknownTag` | Unrecognised struct tag |
| `ErrIdxNotFound` | Index not found |
| `ErrSlicePtrNeeded` | Expected pointer to slice |
| `ErrStructPtrNeeded` | Expected pointer to struct |
| `ErrPtrNeeded` | Expected pointer |
| `ErrNoName` | Struct has no name and no bucket specified |
| `ErrIncompatibleValue` | Value type does not match field type |
| `ErrDifferentCodec` | Codec incompatible with existing bucket |

Do not parse error strings. Use `errors.Is` (and `errors.As` if a typed error is introduced with structured fields).

## Database lifecycle

Storm always borrows the bbolt database supplied to `Open`:

- `Open` accepts an already-open `*bolt.DB`.
- A nil database returns an error matching `ErrNilParam`.
- Storm does not close the database.
- `DB.Close()` returns nil and does not close the underlying database.
- The caller owns the lifecycle and must keep the database open while Storm is active.

Close the bbolt database after `DB.Close()` when it is no longer needed.

## Native BoltDB access

`NativeDB()` returns the underlying `*bbolt.DB` pointer. This is an **advanced interoperability escape hatch**.

**Warnings:**

- Native operations bypass Storm context checkpoints.
- Native writes can bypass codecs, indexes, metadata, and invariants.
- Storm cannot guarantee index consistency or cancellation for native operations.
- Callers must coordinate native and Storm transactions internally.
- Callers must not close the native database while Storm is active.

Normal application code should prefer the managed Storm APIs.

## Codecs

Storm marshals data to BoltDB using a codec. The default is JSON. Pass a codec as
the fourth argument to `Open`:

```go
import "github.com/malivvan/storm/codec/gob"

db, err := storm.Open(ctx, bDB, nil, gob.Codec)
```

### Built-in codecs

| Package | Import path | Codec variable |
|---------|-------------|----------------|
| JSON (default) | `storm/codec/json` | `json.Codec` |
| Gob | `storm/codec/gob` | `gob.Codec` |
| AES | `storm/codec/aes` | `NewAES(subCodec, key)` |

AES wraps another codec with AES-GCM encryption.

### Custom codecs

Implement the `codec.MarshalUnmarshaler` interface:

```go
type MarshalUnmarshaler interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
	Name() string
}
```

### Codec compatibility

Storm records codec metadata for record/KV leaf buckets and an encoded database-version value used during `Open`. An incompatible root codec is classified with `ErrDifferentCodec`; metadata-validating initialization and write operations also classify a mismatched node codec this way. Read paths may instead return the underlying decode error, so nodes configured with `WithCodec` must continue using their matching codec.

## Testing and CI

```sh
# Formatting and build
test -z "$(gofmt -l .)"
go vet ./...
go build ./...

# Tests
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./...

# Staticcheck
go run honnef.co/go/tools/cmd/staticcheck@2026.1 ./...

# Coverage
go test -count=1 -timeout 180s -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out > coverage.txt
```

## License

MIT

## Credits

- [Asdine El Hrychy](https://github.com/asdine)
- [Bjørn Erik Pedersen](https://github.com/bep)
