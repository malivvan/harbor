# db — structured records on BoltDB

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/harbor/db.svg)](https://pkg.go.dev/github.com/malivvan/harbor/db)

`db` maps Go structs to bbolt buckets and adds indexes, unique constraints, a
matcher-based query API, managed transactions, and key/value access. It is the
record store used by the root `harbor` package.

It can be used in two ways:

```go
// 1. Through the root package (one file, one password, all four stores).
store, err := harbor.Open(ctx, "app.db", 0o600, &harbor.Options{Password: password})
records := store.DB()

// 2. Directly with a caller-owned bbolt handle.
bdb, err := bolt.Open("app.db", 0o600, &bolt.Options{Timeout: time.Second})
defer bdb.Close()

records, err := db.Open(ctx, bdb, db.Config{})
defer records.Close() // does not close bdb
```

`db.Open` borrows the database; the caller must keep it open for the lifetime of
the store and close it afterwards. `Store.Close` releases the wrapper only.

`db.Config` exposes `Validate()` and `Normalize()`. `db.Open` uses the
normalized configuration, so the same checks apply: a nil `Codec` selects the
default JSON codec, and any encryption flag requires a 32-byte `Secret`.

## Models

The struct name becomes the bucket name. The primary key is the field tagged
`id`, or a field named `ID` when no tag is present. It must be non-zero and
serializable by the configured codec.

```go
type User struct {
	ID        int       `db:"id,increment"`
	Team      string    `db:"index"`
	Email     string    `db:"unique"`
	Name      string
	Age       int       `db:"index"`
	CreatedAt time.Time `db:"index"`
}
```

### Tags

All tags use the `db` namespace:

| Tag | Meaning |
|-----|---------|
| `id` | Marks the primary key. |
| `increment` | Auto-increments integer fields. `increment=100` starts at 100. |
| `index` | Creates a list index for the field. |
| `unique` | Creates a unique index for the field. |
| `inline` | Flattens an embedded struct into the parent. |

Auto-increment works for integer fields and is assigned by `Save` when the field
is zero:

```go
type Product struct {
	ID           int    `db:"id,increment"`
	Name         string
	Count        uint64 `db:"increment"`
	IndexedCount uint32 `db:"index,increment"`
	UniqueCount  int16  `db:"unique,increment=100"`
}

p := Product{Name: "Widget"}
err := records.Save(ctx, &p)
// p.ID == 1, p.Count == 1, p.IndexedCount == 1, p.UniqueCount == 100
```

### Embedded structs

```go
type Base struct {
	Ident string `db:"id"`
}

type User struct {
	Base      `db:"inline"`
	Team      string `db:"index"`
	Email     string `db:"unique"`
	CreatedAt time.Time `db:"index"`
}
```

### Custom bucket names

Implement `db.BucketNamer`:

```go
func (u User) HarborBucketName() string {
	return "customers"
}
```

The custom name is used consistently by every list and point operation.

Runtime-created types (`reflect.StructOf`) have no static name. Give them an
explicit bucket and reuse the same reconstructed type for later reads:

```go
dynType := reflect.StructOf([]reflect.StructField{
	{Name: "ID", Type: reflect.TypeOf(uint64(0)), Tag: `db:"id,increment"`},
	{Name: "Name", Type: reflect.TypeOf(""), Tag: `db:"index"`},
})

record := reflect.New(dynType)
record.Elem().FieldByName("Name").SetString("Dynamic Alice")

err := records.From("dynamic_users").Save(ctx, record.Interface())
```

## CRUD

### Save

```go
user := User{Team: "core", Email: "alice@example.com", Age: 30}
err := records.Save(ctx, &user)
```

`Save` creates the bucket and indexes if needed, enforces unique constraints,
and assigns auto-increment values. A duplicate unique value returns
`db.ErrAlreadyExists`.

### Fetch one record

```go
var user User

err = records.One(ctx, "Email", "alice@example.com", &user) // indexed field
err = records.One(ctx, "Name", "Alice", &user)               // full scan
err = records.One(ctx, "ID", 10, &user)                      // primary key
```

`One` uses an index when one exists and falls back to a scan otherwise.

### Fetch many records

```go
var users []User

err = records.Find(ctx, "Team", "core", &users)
err = records.All(ctx, &users)
err = records.AllByIndex(ctx, "CreatedAt", &users)
err = records.Range(ctx, "Age", 21, 30, &users)
err = records.Prefix(ctx, "Name", "Al", &users)
```

`Find`, `Range` and `Prefix` return `db.ErrNotFound` when nothing matches. `All`
returns an empty slice and no error.

Pagination and ordering are `FindOption` values:

```go
err = records.Find(ctx, "Team", "core", &users,
	db.Limit(10), db.Skip(5), db.Reverse())
err = records.All(ctx, &users,
	db.Limit(10), db.Skip(5), db.Reverse())
```

### Query API

For richer predicates, use `Select` with matchers from `db/q`:

```go
import (
	"github.com/malivvan/harbor/db"
	"github.com/malivvan/harbor/db/q"
)

var users []User

err := records.Select(q.Gte("Age", 21), q.In("Team", []string{"core", "admin"})).
	OrderBy("Age").
	Limit(50).
	Skip(10).
	Reverse().
	Find(ctx, &users)

var one User
err = records.Select(q.Eq("Email", "alice@example.com")).First(ctx, &one)

count, err := records.Select(q.Lt("Age", 18)).Count(ctx, &User{})
err = records.Select(q.Eq("Team", "blocked")).Delete(ctx, &User{})
```

Builder methods (`Skip`, `Limit`, `OrderBy`, `Reverse` and `Bucket`) return a
new query and never modify the receiver, so one base query can be reused for
several variants:

```go
base := records.Select(q.Gte("Age", 18))

var firstPage []User
err := base.Limit(20).Find(ctx, &firstPage)   // base keeps no limit

var all []User
err = base.Find(ctx, &all)                    // still the full result set
```

`First` limits an internal copy for the same reason: a query used for `First`
can still `Find` every match afterwards.

Available matchers: `q.Eq`, `q.StrictEq`, `q.Gt`, `q.Gte`, `q.Lt`, `q.Lte`,
`q.In`, `q.Re`, `q.EqF`, `q.GtF`, `q.GteF`, `q.LtF`, `q.LteF`, `q.True`, plus
the combinators `q.And`, `q.Or` and `q.Not`.

Iterate without materializing a slice:

```go
err := records.Select(q.Gte("ID", 10)).Each(ctx, &User{}, func(record any) error {
	u := record.(*User)
	fmt.Println(u.Name)
	return nil
})
```

`Each` decodes every record. `Raw` and `RawEach` expose the stored bytes:

```go
values, err := records.Select(q.Eq("Team", "core")).Raw(ctx, &User{})

err = records.Select(q.Eq("Team", "core")).
	RawEach(ctx, &User{}, func(key, value []byte) error {
		fmt.Println(string(key), len(value))
		return nil
	})
```

A non-nil sample record (`&User{}`) is required when the query has matchers or
ordering, because those are evaluated against decoded records. Pass `nil` only
for a matcher-less, unordered dump; otherwise `Iter`, `Raw` and `RawEach` return
`db.ErrSampleRequired`.

Stream records one at a time when the result set is large or the consumer may
stop early:

```go
it, err := records.Select(q.Gte("Age", 21)).Iter(ctx, &User{})
if err != nil {
	return err
}
defer it.Close()

for it.Next() {
	var u User
	if err := it.Decode(&u); err != nil {
		return err
	}
	fmt.Println(u.Name)
}
if err := it.Err(); err != nil {
	return err
}
```

`Iter` keeps a read-only transaction (and the store's rotation guard) open
until `Close`, so always close it, including on an early break. `Key` and
`Value` expose the raw stored bytes of the current record. `Skip`, `Limit`,
`Reverse` and `Bucket` apply; `OrderBy` does not, because ordering requires
materializing the result set: an ordered query returns
`db.ErrStreamOrdered`. Cancellation is checked before every `Next` and reported
by `Err`. An iterator obtained from a `Store.Attach` transaction reads through
that transaction and never closes it.

`Query.Bucket(name)` targets a bucket explicitly, which is useful for
runtime-generated types.

### References and cascades

A field can declare a reference to a record in another bucket:

```go
type Doc struct {
	ID    string `db:"id"`
	Title string `db:"index"`
}

type Chunk struct {
	ID    string `db:"id"`
	DocID string `db:"index,ref=Doc,ondelete=cascade"`
	Body  string
}
```

- `ref=<bucket>` names the target bucket (the target type's bucket name), and
  `ondelete=cascade|restrict` selects the delete behaviour. `restrict` is the
  default: deleting a referenced record fails with `db.ErrReferenceViolation`.
  `cascade` deletes the referencing records in the same transaction, recursively.
- Referencing fields are indexed automatically, so `Find(ctx, "DocID", id, &out)`
  works without an explicit `index` tag.
- `Save` requires a non-zero reference to name an existing record: a missing
  target bucket returns `ErrReferenceTargetMissing`, a missing record
  `ErrDanglingReference`. A zero value (empty string, 0, nil) means "no
  reference" and is not validated.
- The same rules apply to `DeleteStruct`, `Select(...).Delete`, and `Drop`.
  `Drop` with `cascade` deletes the referencing records before dropping the
  bucket; with `restrict` it fails if any remain. Cascades are guarded against
  cycles and stop after 32 levels with `ErrReferenceCycle`.
- References are single scalar keys in v1; slices, maps and composite keys are
  not supported.
- `Node.ValidateSchema(ctx, &Doc{}, &Chunk{})` checks at startup that every
  reference names an existing bucket, catching a typo before the first save.
- The relation registry (`__harbor_refs__`) is metadata: it stores bucket and
  field names, not record values.

### Aggregation

`Aggregate` computes grouped measures over a bucket without materializing the
records:

```go
res, err := records.From("Sale").Aggregate(ctx, db.AggregateSpec{
	GroupBy: []string{"Kind"},
	Measures: []db.Measure{
		{Alias: "rows", Type: db.MeasureCount},
		{Alias: "total", Type: db.MeasureSum, Field: "Amount"},
		{Alias: "avg", Type: db.MeasureAvg, Field: "Amount"},
	},
	Having:  []db.Having{{Alias: "total", Op: db.HavingGTE, Value: 100}},
	OrderBy: "total",
	Desc:    true,
	Limit:   10,
})
for _, g := range res.Groups {
	fmt.Println(g.Keys, g.Count, g.Measures)
}
```

- Measures are `Count`, `Sum`, `Avg`, `Min`, `Max`, `Median`, `Percentile`
  (`P` in 0–100), `StdDev` (sample) and `StdDevPop`.
- Fields are the encoded (JSON) field names of the record; nested paths are not
  supported in v1.
- `TimeBucket{Field, Unit}` adds a bucketed timestamp to the group key
  (`second`, `minute`, `hour`, `day`, `week`). The field may hold a
  `time.Time`, an RFC 3339 string, or numeric Unix seconds; the result key is a
  `time.Time` in UTC.
- `Missing` controls a missing numeric field (`MissingExclude` by default,
  `MissingZero`), and `NonNumeric` controls a value that is not a number
  (`NonNumericError` by default, `NonNumericSkip`, `NonNumericZero`).
  `AggregateResult.Skipped` counts the values a measure ignored.
- `Having` filters on a measure alias, `count`, or a group key. `OrderBy`
  names a measure alias or group key; ties order by group key.
- Only median/percentile values are retained per group; the scan itself holds
  group accumulators, not the records.
- An empty bucket returns no groups. Invalid specs return `db.ErrInvalidAggregate`.

### Update

```go
err := records.Update(ctx, &User{ID: 10, Name: "Alice", Age: 31})
err = records.UpdateField(ctx, &User{ID: 10}, "Age", 0)
```

`Update` updates non-zero fields. `UpdateField` updates one field, including
zero values. The primary key is immutable: updating it returns
`db.ErrImmutableID`.

### Delete

```go
var user User
if err := records.One(ctx, "ID", 10, &user); err != nil {
	return err
}
err := records.DeleteStruct(ctx, &user)
```

### Count, Init, Drop, ReIndex

```go
count, err := records.Count(ctx, &User{})
err = records.Init(ctx, &User{})    // create buckets and indexes
err = records.Drop(ctx, &User{})    // or records.Drop(ctx, "User")
err = records.ReIndex(ctx, &User{}) // rebuild indexes
```

## Transactions

Harbor exposes callback-managed transactions only; there is no
`Begin`/`Commit`/`Rollback` API.

```go
err := records.ReadTransaction(ctx, func(tx db.Node) error {
	var u User
	return tx.One(ctx, "ID", 1, &u)
})

err = records.WriteTransaction(ctx, func(tx db.Node) error {
	if err := tx.Save(ctx, &from); err != nil {
		return err
	}
	return tx.Save(ctx, &to)
})
```

Semantics:

- `WriteTransaction` uses `bbolt.Update`, so the callback runs exactly once;
  bbolt does not retry.
- Returning an error rolls the transaction back and returns that error.
- If the context is cancelled before commit, the transaction is rolled back and
  `ctx.Err()` is returned. A callback error takes precedence over a
  cancellation caused by the callback itself.
- A cancellation after a successful commit is not applied retroactively.
- Panics propagate unchanged after bbolt unwinds the transaction. Harbor does
  not recover panics.

### Attach to a caller-owned transaction

Use `Attach` to mix Harbor operations with raw bbolt writes atomically:

```go
tx, err := bdb.Begin(true)
if err != nil {
	return err
}
defer tx.Rollback()

txn, err := records.Attach(tx)
if err != nil {
	return err
}
if err := txn.Save(ctx, &account); err != nil {
	return err
}

audit, err := tx.CreateBucketIfNotExists([]byte("audit"))
if err != nil {
	return err
}
if err := audit.Put([]byte("event"), []byte("created")); err != nil {
	return err
}

return tx.Commit()
```

- Harbor never commits, rolls back, closes, or replaces the supplied
  transaction, and never opens a managed transaction for a bound `Tx`.
- The full `Node` surface is available, including descendants from `From`,
  `WithCodec`, `PrefixScan` and `RangeScan`.
- Writable and read-only transactions are accepted. Writes through a read-only
  transaction fail with bbolt's `ErrTxNotWritable`.
- A transaction from another database returns `db.ErrTxFromDifferentDB`; a nil
  transaction or receiver returns `db.ErrNilParam`.
- A bound `Tx` is valid only for the lifetime of its transaction. Using it
  after commit or rollback is undefined behavior, exactly like using the bbolt
  transaction itself.

## Nested buckets and scanning

```go
notes := records.From("notes")
private := notes.From("private")
work := notes.From("work")

err := private.Save(ctx, &Note{ID: "p1", Text: "private"})
err = work.Save(ctx, &Note{ID: "w1", Text: "work"})

path := notes.Bucket() // []string{"notes"}
```

Each node supports the full API and can select its own codec:

```go
node := records.From("archive").WithCodec(db.JSONCodec)
```

`BucketScanner` discovers child nodes:

```go
children, err := records.PrefixScan(ctx, "notes/")
children, err = records.RangeScan(ctx, "notes/a", "notes/z")
```

## Key/value access

```go
err := records.Set(ctx, "sessions", sessionID, &session)

var session Session
err = records.Get(ctx, "sessions", sessionID, &session)

err = records.SetBytes(ctx, "cache", "key", []byte("value"))
raw, err := records.GetBytes(ctx, "cache", "key")

exists, err := records.KeyExists(ctx, "sessions", sessionID)
err = records.Delete(ctx, "sessions", sessionID)
```

A nil value stores a zero-length value; `Get` then yields the destination's
zero value.

## Codecs

The default codec is JSON (`db.JSONCodec`). Select another codec per store or
node:

```go
records, err := db.Open(ctx, bdb, db.Config{Codec: db.JSONCodec})
node := records.From("archive").WithCodec(myCodec)
```

A custom codec implements:

```go
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
	Name() string
}
```

The codec name is stored in each bucket's metadata. Opening a bucket with a
different codec returns `db.ErrDifferentCodec`. Read paths may instead surface
the underlying decode error, so keep a node bound to the codec it was written
with.

## Encryption

Enable encryption by passing a raw 32-byte key and one or more flags:

```go
records, err := db.Open(ctx, bdb, db.Config{
	Root:            [][]byte{[]byte("db")},
	Secret:          key,
	EncryptKeys:     true,
	EncryptValues:   true,
	EncryptMetadata: true,
})
```

- `EncryptKeys` deterministically encrypts primary keys, index values and
  key/value keys with AES-SIV, so exact lookups keep working.
- `EncryptValues` seals record and key/value values. Bucket names and the codec
  name stay readable.
- `EncryptMetadata` seals auto-increment counters.
- Ordered and prefix lookups depend on index ordering, which ciphertext does not
  preserve. They return `db.ErrEncryptedOrdering` when keys are encrypted.
- Opening an encrypted store without the key or with a different mode returns
  `db.ErrWrongMode`; a wrong key returns `db.ErrAuthFailed`.
- `ReEncrypt` / `RotateAll` rewrite the whole store under a new key atomically.
  Run them with exclusive access. `VerifyAll` checks every value decrypts.
- In-place migration from plaintext to encrypted storage is not supported.

See [Encryption](../README.md#encryption) for the complete model, and [AGENTS.md](../AGENTS.md#encryption-internals) for the implementation details.

## Errors

Classify with `errors.Is`, not string comparison:

| Sentinel | Meaning |
|----------|---------|
| `ErrNotFound` | No record matches. |
| `ErrAlreadyExists` | Duplicate unique value. |
| `ErrNoID` | No `id` tag or `ID` field. |
| `ErrZeroID` | Primary key is zero. |
| `ErrImmutableID` | `UpdateField` targeted the primary key. |
| `ErrBadType` | Wrong value type. |
| `ErrUnknownTag` | Unrecognized struct tag. |
| `ErrIdxNotFound` | Index does not exist. |
| `ErrSlicePtrNeeded` | Expected a pointer to a slice. |
| `ErrStructPtrNeeded` | Expected a pointer to a struct. |
| `ErrPtrNeeded` | Expected a pointer. |
| `ErrNoName` | No struct name and no explicit bucket. |
| `ErrIncompatibleValue` | Value does not match the field type. |
| `ErrSampleRequired` | `Iter`/`Raw`/`RawEach` needs a sample for matchers/ordering. |
| `ErrStreamOrdered` | `Iter` cannot stream an `OrderBy` query. |
| `ErrDifferentCodec` | Bucket was written with another codec. |
| `ErrIncompatibleVersion` | Unsupported on-disk format version. |
| `ErrTxFromDifferentDB` | Attached transaction belongs to another database. |
| `ErrNilParam` | Nil required argument. |
| `ErrNilContext` | Nil context. |
| `ErrEncryptedOrdering` | Ordered/prefix query on encrypted keys. |
| `ErrAuthFailed` | Ciphertext failed authentication. |
| `ErrInvalidKey` | Missing or malformed key. |
| `ErrInvalidConfig` | Inconsistent encryption config. |
| `ErrWrongMode` | Store mode does not match the persisted marker. |

Errors are wrapped as `harbor <operation>: <cause>` while preserving the cause
in the chain.

## Contexts

Database open, CRUD, finder, query, scanner, key/value and transaction methods
accept a context. A nil context returns `ErrNilContext`. Cancellation is
cooperative: it is checked at operation boundaries and loop checkpoints, but
cannot interrupt a bbolt lock wait, codec code, or an unreturned callback.
Committed writes and published read results are not retroactively changed to
cancellation errors.

## Performance

- JSON decoding dominates unindexed scans, `Count`, and matcher-less `Delete`.
  For high-throughput workloads, use a faster codec.
- `Count` and matcher-less `Delete` skip decoding entirely.
- Every bbolt commit is an fsync. Batch writes with `WriteTransaction` instead
  of saving one record per loop.
- Index the fields you filter on. `One` falls back to a full scan when the field
  is not indexed.

## License and compatibility

`db.Version` (`"1.0.0"`) is written to new databases. Unknown versions are
rejected with `ErrIncompatibleVersion` instead of being misread. MIT licensed;
see [../LICENSE](../LICENSE).

For a guided tour of the whole module, start at
[../README.md](../README.md).
