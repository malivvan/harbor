# Plan: `ts` — Time Series Store

## Goal

Add a new sub-package `ts` that implements a time series database `Store`,
functionally and stylistically equivalent to the existing sub-stores
(`kv`, `vec`, `fs`, `db`):

- it wraps a **caller-owned** bbolt database under a dedicated root bucket,
- it exposes a `Store` that manages named child entities (a *series*, in the
  same way `kv.Store` manages `Bucket`s and `vec.Store` manages `Collection`s),
- it is wired into the top-level `storm.Store` and reachable via a new accessor.

No other functional changes are made; the only edits outside the new `ts/`
package are the minimal integration points (top-level store wiring, tests, and
docs) required to make `ts` a first-class store.

## Context: how the existing stores are structured

All four sibling stores share the same shape, which `ts` must follow:

| Store | `Open` signature | Managed child | Root key used by `storm.go` |
|-------|------------------|---------------|------------------------------|
| `kv`  | `Open(ctx, db, root []byte, codec)` | `Bucket` (cached by name) | `[]byte{1}` |
| `db`  | `Open(ctx, db, root []byte, codec)` | `Node`/`From(name)` | `[]byte{2}` |
| `vec` | `Open(ctx, db, root []byte, useQuant bool, quantizer)` | `Collection` (via `NewCollection`) | `[]byte{3}` |
| `fs`  | `Open(ctx, db, root []byte)` | `FileSystem` (via `Open(name)`) | `[]byte{4}` |

Common conventions observed across `kv`, `db`, `vec`, `fs` and adopted here:

1. **`Open(ctx context.Context, db *bolt.DB, root []byte, …) (*Store, error)`**
   - `nil` context → error matching `ErrNilContext` (`errors.Is`).
   - `nil` db → error matching `ErrNilParam`.
   - `root` is defensively copied (`append([]byte(nil), root...)`).
   - The `Store` **borrows** the database and never closes it.
2. **Lifecycle / escape-hatch methods**: `NativeDB() *bolt.DB`, `Root() []byte`,
   `Close() error` (returns `ErrNilParam` on nil receiver, never closes the db).
3. **Context-aware operations** check `checkContext(ctx)` at entry and at
   transaction boundaries; nil context → `ErrNilContext`.
4. **Package-local sentinel errors** (`errors.New("<pkg>: …")`) matched with
   `errors.Is`; no string parsing.
5. **Metadata in a reserved sub-bucket** so it never collides with data:
   `kv` uses `__storm_metadata`, `vec` uses `__collections_meta__`. `ts` uses
   `__ts_meta__`.
6. **`encoding/json` for small metadata** (as `vec` does for `CollectionMeta`);
   no `codec` package dependency for the numeric point payload.
7. **Tests with `testing` + `testify`** (`require`/`assert`), plus a benchmark
   file where the sibling stores provide one (`kv/bench` … `vec`, `fs` all have
   `bench_test.go`).
8. **Docs**: a README section and a CHANGELOG bullet.

## Design overview

`ts` is a lightweight, fixed-schema time series store:

- A **series** is a named, ordered sequence of `(timestamp, value)` points, plus
  optional string **labels** (tags) stored as series metadata.
- Values are `float64`; timestamps are `int64` Unix **nanoseconds**.
- Points are persisted as fixed-width binary records in a bbolt bucket keyed by
  the series name. Keys are timestamp-encoded so bbolt's lexicographic key order
  **equals chronological order**, giving O(log n) point lookup and efficient
  ordered range scans without any in-memory index.
- Writes are append-optimized; out-of-order inserts are allowed (last write
  wins for a given timestamp).
- The `Store` caches `Series` values by name (like `kv` caches `Bucket`s), so
  repeated `Store.Series("cpu")` calls return the same value.
- Reads (`Range`, aggregates, `First`/`Last`/`Get`) are streaming and
  context-checked; aggregation runs directly over the ordered scan.

## Data model

```go
// Point is a single timestamped sample.
type Point struct {
    Timestamp int64   // Unix time in nanoseconds
    Value     float64 // sample value
}

// Sample is one aggregated value over a time bucket.
type Sample struct {
    Timestamp int64   // start of the bucket (Unix nanoseconds)
    Value     float64 // aggregate result
}
```

Series metadata (JSON) holds the labels map; everything else is derived from
the point bucket content.

## On-disk layout

Under the store's root bucket (top-level `storm.go` passes `[]byte{5}`):

```
<root = []byte{5}>
├── __ts_meta__                    (bucket)
│     └── "<series name>"  ->  JSON: {"labels": {...}}
└── "<series name>"                (bucket, one per series)
      └── <8-byte ts key>  ->  <8-byte float64 value>
```

- A series exists iff its point bucket exists (or its metadata entry exists);
  `Insert` creates both lazily, mirroring `kv.Bucket`'s `createBucket`.
- `SeriesNames` iterates root sub-buckets and skips `__ts_meta__`
  (same pattern as `vec.ListCollections` skipping `collectionsMetaBucket`).
- `DeleteSeries` removes the point bucket **and** the metadata entry.

### Key/value encoding (order-preserving, fixed width)

```go
const keyLen = 8

// encodeKey maps an int64 timestamp to 8 bytes that sort chronologically:
// the sign bit is flipped so big-endian byte order == int64 order.
func encodeKey(ts int64) [8]byte {
    var b [8]byte
    binary.BigEndian.PutUint64(b[:], uint64(ts)^(1<<63))
    return b
}

// decodeKey is the inverse of encodeKey.
func decodeKey(b []byte) int64 { /* uint64(big-endian) ^ (1<<63), as int64 */ }

// encodeValue stores a float64 as its IEEE-754 bits, big-endian.
func encodeValue(v float64) [8]byte {
    var b [8]byte
    binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
    return b
}

func decodeValue(b []byte) float64 { /* math.Float64frombits(...) */ }
```

Range scans use `Cursor.Seek(encodeKey(start)[:])` and iterate while
`decodeKey(k) <= end`. Because keys are monotonic, no post-sort is needed.

## Package API

### Files

```
ts/
├── doc.go         // package documentation (mirrors fs/doc.go)
├── store.go       // Store type, Open, NativeDB, Root, Close, Series, SeriesNames, DeleteSeries, bucket helpers
├── series.go      // Series type and point CRUD: Insert/InsertMany/Get/First/Last/Range/RangeEach/Count/DeleteRange/Drop
├── aggregate.go   // AggregateFunc, Aggregate, and internal aggregation implementations
├── encode.go      // encodeKey/decodeKey/encodeValue/decodeValue (internal)
├── errors.go      // sentinel errors
├── ts_test.go     // unit tests
└── bench_test.go  // benchmarks (Insert, Range, Aggregate)
```

### `Store`

```go
type Store struct {
    b     *bolt.DB
    root  []byte
    mu    sync.RWMutex
    series map[string]*Series
    order []string
}

// Open wraps an already-open bbolt database in a time series Store.
func Open(ctx context.Context, db *bolt.DB, root []byte) (*Store, error)

func (s *Store) NativeDB() *bolt.DB
func (s *Store) Root() []byte
func (s *Store) Close() error

// Series returns the named series, creating and caching it on demand.
func (s *Store) Series(name string) *Series

// SeriesNames returns the names of series created through this Store,
// in creation order.
func (s *Store) SeriesNames() []string

// DeleteSeries drops a series bucket and its metadata.
func (s *Store) DeleteSeries(ctx context.Context, name string) error
```

### `Series`

```go
type Series struct {
    store *Store
    name  string
}

func (se *Series) Name() string
func (se *Series) Store() *Store

// Labels returns the series' labels (tags); empty map when unset.
func (se *Series) Labels(ctx context.Context) (map[string]string, error)
// SetLabels replaces the series' labels.
func (se *Series) SetLabels(ctx context.Context, labels map[string]string) error

// Insert stores one point (upsert by timestamp).
func (se *Series) Insert(ctx context.Context, p Point) error
// InsertMany stores multiple points in a single transaction.
func (se *Series) InsertMany(ctx context.Context, points []Point) error

// Get returns the point exactly at ts, or ErrNotFound.
func (se *Series) Get(ctx context.Context, ts int64) (Point, error)
// First/Last return the earliest/latest point, or ErrNotFound when empty.
func (se *Series) First(ctx context.Context) (Point, error)
func (se *Series) Last(ctx context.Context) (Point, error)

// Range returns points with start <= Timestamp <= end, in time order.
func (se *Series) Range(ctx context.Context, start, end int64) ([]Point, error)
// RangeEach streams the same range, calling fn per point; fn must not retain
// the Point after returning. Stops at the first fn error.
func (se *Series) RangeEach(ctx context.Context, start, end int64, fn func(Point) error) error

// Count returns the number of points in the series.
func (se *Series) Count(ctx context.Context) (int, error)

// Aggregate buckets [start, end] into windows of width step and computes fn
// per window, returning one Sample per non-empty window (bucket start as ts).
func (se *Series) Aggregate(ctx context.Context, start, end int64, step int64, fn AggregateFunc) ([]Sample, error)

// DeleteRange deletes points with start <= Timestamp <= end and returns the
// number deleted.
func (se *Series) DeleteRange(ctx context.Context, start, end int64) (int, error)

// Drop removes the series and its metadata from the store.
func (se *Series) Drop(ctx context.Context) error
```

### Aggregation

```go
type AggregateFunc string

const (
    AggMin   AggregateFunc = "min"
    AggMax   AggregateFunc = "max"
    AggAvg   AggregateFunc = "avg"   // arithmetic mean
    AggSum   AggregateFunc = "sum"
    AggCount AggregateFunc = "count" // number of points in the window
    AggFirst AggregateFunc = "first" // earliest point value in the window
    AggLast  AggregateFunc = "last"  // latest point value in the window
)
```

`Aggregate` semantics:

- `step` must be > 0, else `ErrInvalidStep`.
- `start <= end`, else `ErrInvalidRange`.
- Windows are aligned to `step` boundaries: `b = floor(ts/step)*step`.
- A window with no points produces no `Sample`.
- `AggCount` and `AggFirst`/`AggLast` are resolved during the scan; `AggMin`/
  `AggMax`/`AggAvg`/`AggSum` use a running accumulator to keep memory O(1)
  per window (points are already time-ordered).

### Errors (`errors.go`)

```go
var (
    ErrNilParam    = errors.New("ts: nil parameter")
    ErrNilContext  = errors.New("ts: nil context")
    ErrNotFound    = errors.New("ts: not found")          // series or point missing
    ErrInvalidRange = errors.New("ts: invalid range")     // start > end
    ErrInvalidStep  = errors.New("ts: invalid step")      // step <= 0 in Aggregate
)
```

All context-aware methods validate the context first (`checkContext`, same as
`kv`/`fs`), returning `ErrNilContext` or `ctx.Err()`.

## Implementation details

- **`Open`** mirrors `fs.Open`: nil-context check, nil-db check, clone `root`,
  and initialize `series` map. No database writes at open time (series buckets
  are created lazily on first `Insert`).
- **Bucket helpers** mirror `vec`:
  - `rootBucket(tx)`: `tx.Bucket(s.root)` (nil when root is empty).
  - `seriesBucket(tx, name)` / `createSeriesBucket(tx, name)`: resolve or create
    the series bucket under the root, creating the root bucket when needed.
  - `metaBucket(tx, create)`: resolve/create `__ts_meta__` under the root.
- **Context checkpoints**: every `View`/`Update` re-checks `checkContext(ctx)`
  inside the transaction (as `kv` does), so cancelled contexts abort promptly.
- **Nil-receiver safety**: exported methods return `ErrNilParam` (or nil for
  `NativeDB`/`Root`) on a nil receiver, matching the sibling stores.
- **Concurrency**: bbolt serializes transactions; the `Store.mu` guards only the
  `series`/`order` caches (same as `kv`). `Series` itself is stateless.
- **No codec dependency**: point values are fixed-width binary; series labels use
  `encoding/json`. `ts` therefore depends only on `bbolt` and the standard
  library (consistent with `fs`, which does not depend on `codec`).

## Integration into the top-level `Store` (`storm.go`)

Minimal, additive changes only:

1. Add import `"github.com/malivvan/storm/ts"`.
2. Add field `ts *ts.Store` to `storm.Store`.
3. In `Open`, after `fs.Open`, add:

   ```go
   s.ts, err = ts.Open(ctx, s.b, []byte{5})
   if err != nil {
       return nil, err
   }
   ```

4. Add accessor:

   ```go
   func (s *Store) TS() *ts.Store { return s.ts }
   ```

No change to existing buckets `[]byte{0}`–`[]byte{4}`; `[]byte{5}` is the next
free root key. No behavioral change to the existing stores.

## Tests

### `ts/ts_test.go`

Cover, with `testify` and a per-test temp bbolt db (opened in `t.TempDir()` so no
`*.db` files leak — note `.gitignore` already excludes `*.db`):

- `Open` rejects nil context (`ErrNilContext`) and nil db (`ErrNilParam`).
- `Series` caching: repeated `Series("x")` returns the same instance; `SeriesNames`
  reflects creation order.
- `Insert`/`Get`/`Last`/`First` round-trip; `Get` on a missing timestamp →
  `ErrNotFound`.
- Out-of-order `InsertMany` still yields time-ordered `Range`.
- `Range` with `[start, end]` bounds is inclusive on both ends; empty range →
  empty slice.
- `RangeEach` iterates in order and stops on callback error.
- `Count` and `DeleteRange` (returns the correct count; deleted points no longer
  read).
- `SetLabels`/`Labels` round-trip; persists across re-open.
- `Aggregate` for every `AggregateFunc` against a hand-computed fixture,
  including bucket alignment and empty-window skipping.
- `Aggregate` with `step <= 0` → `ErrInvalidStep`; `start > end` →
  `ErrInvalidRange`.
- `Drop` removes the bucket and metadata; subsequent `Get` → `ErrNotFound`.
- `DeleteSeries` removes a series and its metadata entry.
- Persistence across `Close`/re-open: reopen the same file and verify points
  and labels survive (store `Close` is a no-op wrapper; the top-level test
  covers the shared db lifecycle).
- Cancellation: pass an already-cancelled context to a read/write op → error
  matching `context.Canceled` (and `checkContext`).

### `storm_test.go`

Add a short section (after the `vec` block) exercising `s.TS()` end-to-end:

```go
cpu := s.TS().Series("cpu")
err = cpu.Insert(ctx, ts.Point{Timestamp: t, Value: 0.5})
// ... then Range/Aggregate assertions
```

### `ts/bench_test.go`

`BenchmarkInsert`, `BenchmarkInsertMany`, `BenchmarkRange`, `BenchmarkAggregate`
on an in-memory/temp db, mirroring the style of `vec/bench_test.go`.

## Documentation

- **`README.md`**: add a "Time series store (ts)" subsection under the stores
  section, with the `Open` signature, a short `Series` insert/range/aggregate
  example, and a note that it borrows the bbolt db (never closes it).
- **`CHANGELOG.md`**: add a bullet under `### Added` describing the `ts` store
  (named series, ordered fixed-width point storage, range scans, aggregation,
  labels), mirroring the existing `kv`/`fs` bullets.

## Verification checklist

Run from the repository root (no `*.db` files committed):

```sh
gofmt -l ts storm.go storm_test.go            # expect empty output
go vet ./...
go build ./...
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./ts/...
```

Acceptance criteria:

- `go build ./...` and `go vet ./...` pass.
- `go test ./...` passes, including the new `ts` tests and the extended
  `storm_test.go`.
- `ts` follows the same `Open`/`NativeDB`/`Root`/`Close`/sentinel-error
  conventions as `kv`/`fs`/`vec`, and is reachable via `store.TS()`.

## Ordered task list

1. **Scaffold the package** — create `ts/` with `doc.go`, `errors.go`, `encode.go`,
   `store.go`, `series.go`, `aggregate.go`.
2. **Implement encoding + errors** — `encodeKey`/`decodeKey`/`encodeValue`/
   `decodeValue` and the sentinel errors, with unit coverage.
3. **Implement `Store`** — `Open`, `NativeDB`, `Root`, `Close`, `Series`,
   `SeriesNames`, `DeleteSeries`, and the internal root/series/meta bucket
   helpers.
4. **Implement `Series` CRUD** — labels, `Insert`/`InsertMany`, `Get`/`First`/
   `Last`, `Range`/`RangeEach`, `Count`, `DeleteRange`, `Drop`.
5. **Implement aggregation** — `AggregateFunc` constants and `Aggregate`.
6. **Wire into `storm.go`** — import, field, `ts.Open(ctx, s.b, []byte{5})`, and
   `TS()` accessor.
7. **Add tests** — `ts/ts_test.go` and `ts/bench_test.go`.
8. **Extend integration test** — `storm_test.go` exercises `s.TS()`.
9. **Document** — README section and CHANGELOG bullet.
10. **Verify** — run the verification checklist; fix any gofmt/vet/test issues.
