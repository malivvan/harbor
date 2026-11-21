# Plan: Retention for the `ts` time series store

## Goal

Add data **retention** to the existing `ts` package: let a caller bound how much
data a series keeps (by age and/or by number of points) and have that bound
enforced automatically on writes and manually via an explicit prune. No other
behavior changes; the only edits are inside `ts/` plus the minimal doc/test
touches needed to describe and verify the feature.

**Implementation status:** complete. See [Execution log](#execution-log).

## Scope / non-goals

**In scope**

- A per-series retention policy: time-based (`MaxAge`) and count-based
  (`MaxPoints`), independently optional and combinable.
- Persist the policy in the series metadata (survives reopen).
- Automatic enforcement on `Insert`/`InsertMany`.
- Explicit enforcement: `Series.EnforceRetention` and a store-wide
  `Store.EnforceRetention`.
- An injectable clock so time-based enforcement is deterministic and testable.

**Out of scope (deliberately)**

- No global/default retention policy on the `Store` — retention is per series.
- No downsampling/aggregation-on-expiry (aggregation already exists as a read
  operation; retention only deletes).
- No change to the point encoding, series naming, labels, or the top-level
  `storm.Store` wiring (retention is purely internal to `ts`).

## Background: relevant current state of `ts/`

- `Store` (`ts/store.go`): `b *bolt.DB`, `root []byte`, `mu sync.RWMutex`,
  `series map[string]*Series`, `order []string`; `Open(ctx, db, root)`.
  Bucket helpers `rootBucket`, `seriesBucket`, `createSeriesBucket`,
  `metaBucket` already exist.
- `Series` (`ts/series.go`): `Insert` → `InsertMany` (single `Update`), `Get`,
  `First`, `Last`, `Range`, `RangeEach`, `Count` (uses `bucket.Stats().KeyN`),
  `DeleteRange`, `Drop`.
- Metadata (`ts/series.go`): `seriesMeta struct { Labels map[string]string }`
  stored as JSON in the `__ts_meta__` bucket, keyed by series name; read/written
  by `Labels`/`SetLabels`.
- Points (`ts/encode.go`): fixed-width 8-byte keys that sort chronologically
  (`encodeKey` flips the sign bit), 8-byte `float64` values. Because keys are
  ordered, **expired points always form a prefix** of the bucket — this is what
  makes retention cheap.
- Errors (`ts/errors.go`): sentinel errors + `checkContext`.

## Design

### Retention model

```go
// Retention describes how long points are kept in a series.
type Retention struct {
    // MaxAge drops points older than the reference time minus MaxAge.
    // 0 disables age-based retention.
    MaxAge time.Duration
    // MaxPoints keeps at most the most-recent MaxPoints points.
    // 0 disables count-based retention.
    MaxPoints int
}
```

- Zero value = no retention (default; preserves current behavior).
- Both constraints may be set together: a point is kept only if it satisfies
  every enabled constraint.
- Negative `MaxAge` or negative `MaxPoints` is rejected with
  `ErrInvalidRetention` at `SetRetention`.

### Storage

Extend the existing metadata struct (backward compatible — old entries simply
unmarshal with a zero `Retention`, no migration):

```go
type seriesMeta struct {
    Labels    map[string]string `json:"labels,omitempty"`
    Retention Retention         `json:"retention,omitempty"`
}
```

Because both `Labels` and `Retention` now share one JSON record, the write
paths must **read-modify-write** so setting one never clobbers the other.
Introduce two internal helpers (used by `SetLabels`, `SetRetention`, and the
write-time enforcement):

```go
func readMeta(tx *bolt.Tx, name string) (seriesMeta, error)   // zero value if absent
func writeMeta(tx *bolt.Tx, name string, m seriesMeta) error  // create meta bucket as needed
```

### Clock ("now")

Time-based retention needs a reference time. The `Store` gets an injectable
clock, defaulting to the wall clock:

```go
// in Store
now func() int64 // Unix nanoseconds; set to time.Now().UnixNano by default

func defaultNow() int64 { return time.Now().UnixNano() }

// SetNow overrides the clock used for automatic retention enforcement during
// writes. Passing nil restores the default wall clock. Configure before
// concurrent use of the store.
func (s *Store) SetNow(fn func() int64)
```

- `Open` initializes `now: defaultNow`.
- Explicit `EnforceRetention(ctx, now)` takes `now` as a parameter (no clock
  dependency), so background jobs and tests pass their own reference time.
- Automatic enforcement on writes calls `s.now()`.

### Automatic vs manual enforcement

- **Automatic**: `InsertMany` (and therefore `Insert`) enforces the series'
  retention in the *same* `Update` transaction after writing the new points.
  This is atomic — a point that violates retention (including just-inserted
  backfill older than `MaxAge`) is never observable.
- **Manual**: `Series.EnforceRetention(ctx, now)` and
  `Store.EnforceRetention(ctx, now)` prune on demand and return the number of
  points removed. `SetRetention` only persists the policy; it does not prune
  (call `EnforceRetention` after setting if immediate effect is desired, or let
  the next write do it).

## New/changed API

New file `ts/retention.go` plus small edits to existing files.

```go
// Retention — see above.

// SetRetention persists the series retention policy. The zero value clears it.
// Returns ErrInvalidRetention for a negative MaxAge or MaxPoints.
func (se *Series) SetRetention(ctx context.Context, r Retention) error

// Retention returns the persisted policy; the zero value when unset.
func (se *Series) Retention(ctx context.Context) (Retention, error)

// EnforceRetention prunes points that violate the persisted policy, using now
// as the reference time for MaxAge (ignored when only MaxPoints is set).
// Returns the number of points removed. A missing series yields 0, nil.
func (se *Series) EnforceRetention(ctx context.Context, now int64) (int, error)

// EnforceRetention enforces the retention policy of every persisted series that
// has one, using now as the reference time. Returns the total points removed.
// It iterates the metadata bucket, so it covers series not yet cached by
// Series().
func (s *Store) EnforceRetention(ctx context.Context, now int64) (int, error)

// SetNow overrides the clock used for automatic retention enforcement.
func (s *Store) SetNow(fn func() int64)
```

New error sentinel in `ts/errors.go`:

```go
// ErrInvalidRetention is returned for a retention policy with a negative
// MaxAge or negative MaxPoints.
ErrInvalidRetention = errors.New("ts: invalid retention")
```

## Enforcement algorithm

Both constraints translate to "delete the oldest **prefix** of the ordered key
space", so a single pass computes one prefix length and deletes it.

```go
func pruneBucket(b *bolt.Bucket, r Retention, now int64) int {
    total := b.Stats().KeyN
    if total == 0 || (r.MaxAge <= 0 && r.MaxPoints <= 0) {
        return 0
    }

    remove := 0 // number of oldest keys to delete

    if r.MaxPoints > 0 && total > r.MaxPoints {
        remove = total - r.MaxPoints
    }

    if r.MaxAge > 0 {
        cutoff := retentionCutoff(now, int64(r.MaxAge))
        c := b.Cursor()
        age := 0
        for k, _ := c.First(); k != nil && decodeKey(k) < cutoff; k, _ = c.Next() {
            age++
        }
        if age > remove {
            remove = age
        }
    }

    if remove <= 0 {
        return 0
    }

    // Collect then delete the first `remove` keys (same pattern as DeleteRange).
    c := b.Cursor()
    deleted := 0
    for k, _ := c.First(); k != nil && deleted < remove; k, _ = c.Next() {
        if err := b.Delete(k); err != nil {
            return deleted
        }
        deleted++
    }
    return deleted
}

// retentionCutoff returns now - maxAge, clamped to math.MinInt64 on underflow.
func retentionCutoff(now, maxAge int64) int64 {
    if maxAge > 0 && now < math.MinInt64+maxAge {
        return math.MinInt64
    }
    return now - maxAge
}
```

Notes:

- `decodeKey`/`encodeKey` are already in `ts/encode.go` and reused verbatim.
- `b.Delete` during cursor iteration is valid in bbolt; to match the existing
  `DeleteRange` style the keys are deleted in place as the cursor advances over
  the known-to-be-deleted prefix (no double pass needed since `remove` is known).
- Public methods add `checkContext(ctx)` at the loop boundary (consistent with
  `DeleteRange`/`Aggregate`).
- `pruneBucket` is internal; it never panics on corrupt keys because
  `decodeKey` is total and `remove` is bounded by the cursor walk.

## File-by-file changes

| File | Change |
|------|--------|
| `ts/retention.go` (new) | `Retention` type; `Series.SetRetention`, `Retention`, `EnforceRetention`; `Store.EnforceRetention`, `SetNow`; `pruneBucket`, `retentionCutoff`, `defaultNow`. |
| `ts/errors.go` | Add `ErrInvalidRetention`. |
| `ts/series.go` | Extend `seriesMeta` with `Retention`; add `readMeta`/`writeMeta`; rewrite `SetLabels` to read-modify-write (preserve `Retention`); in `InsertMany`, after writing points, read the policy and call `pruneBucket` using `se.store.now()`. |
| `ts/store.go` | Add `now func() int64` field; initialize in `Open`; add `SetNow`; add `Store.EnforceRetention` (iterates `__ts_meta__`). |
| `ts/ts_test.go` | Add retention tests (below). |
| `ts/bench_test.go` | Add `BenchmarkEnforceRetention` (optional, mirrors `BenchmarkAggregate`). |
| `README.md` | Extend the "Time series store (ts)" section with a retention example. |
| `CHANGELOG.md` | Extend the `ts` bullet with retention. |

### `InsertMany` auto-enforcement sketch

```go
return se.store.b.Update(func(tx *bolt.Tx) error {
    if err := checkContext(ctx); err != nil { return err }

    bucket, err := se.store.createSeriesBucket(tx, se.name)
    if err != nil { return err }

    for _, point := range points {
        if err := checkContext(ctx); err != nil { return err }
        key, value := encodeKey(point.Timestamp), encodeValue(point.Value)
        if err := bucket.Put(key[:], value[:]); err != nil { return err }
    }

    meta, err := readMeta(tx, se.name) // zero Retention when absent
    if err != nil { return err }
    pruneBucket(bucket, meta.Retention, se.store.now())

    return checkContext(ctx)
})
```

`SetLabels` keeps its current behavior but is rewritten to load the existing
`seriesMeta`, replace only `Labels`, and write it back, so a previously set
`Retention` survives a `SetLabels` call (and vice versa).

## Tests (`ts/ts_test.go`)

- **Round-trip + persistence**: `SetRetention`/`Retention` return the same
  value; reopen a file and confirm the policy survives.
- **Validation**: `SetRetention` with `MaxAge < 0` or `MaxPoints < 0` →
  `ErrInvalidRetention`; zero value clears.
- **Labels/retention non-clobbering**: set labels then retention then labels
  again — both remain; and vice versa.
- **Age-based `EnforceRetention`**: insert points at `0, 100, 200, 300`,
  `MaxAge=150ns`, `now=300` → keeps `{200, 300}` (cutoff `150`), returns 2.
- **Count-based**: `MaxPoints=3` over 5 points → keeps last 3, returns 2.
- **Combined**: both constraints → intersection of the two prefixes.
- **Missing series**: `EnforceRetention` on an empty series → `0, nil`.
- **Automatic on write**: `SetNow` fixed clock + `MaxAge` → inserting an old
  point is immediately pruned; `MaxPoints=N` + inserting `>N` points caps
  `Count` at `N`.
- **Store-wide**: create several series (some with policy, some without, at
  least one not yet returned by `Series()` after reopen), call
  `Store.EnforceRetention`, verify only policy-bearing series are pruned and
  the total count is correct.
- **Clock reset**: `SetNow(nil)` restores wall-clock default.
- **Backward compatibility**: an old metadata record `{"labels":{...}}` reads
  back with zero `Retention`.
- **Underflow**: `MaxAge` so large that `now-MaxAge` underflows → no panic,
  all points removed (cutoff clamps to `math.MinInt64`).
- **Cancellation**: cancelled context → error matching `context.Canceled` from
  `EnforceRetention`/`SetRetention`.
- **Nil receivers**: nil `Series`/`Store` → `ErrNilParam`.

## Documentation

- **`README.md`** (in "Time series store (ts)", after the existing example):

  ```go
  // keep only the last 24h and at most 10_000 points
  err = cpu.SetRetention(ctx, ts.Retention{MaxAge: 24 * time.Hour, MaxPoints: 10_000})

  // explicit maintenance (also enforced automatically on Insert)
  removed, err := cpu.EnforceRetention(ctx, time.Now().UnixNano())
  ```

  plus a sentence describing `MaxAge`, `MaxPoints`, automatic enforcement on
  writes, and `Store.EnforceRetention`.
- **`CHANGELOG.md`**: extend the `ts` bullet to mention per-series retention
  (age and count), automatic enforcement on write, and explicit
  `EnforceRetention`.

## Verification checklist

From the repository root:

```sh
gofmt -l ts                              # expect empty output
go vet ./...
go build ./...
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./ts/...
```

Acceptance criteria:

- Build, vet, and the full test suite pass (existing tests unaffected).
- New retention tests pass, including the `-race` run.
- Retention does not alter behavior of any method for series with no policy set
  (zero value → `pruneBucket` returns 0, no extra metadata written on insert
  beyond what `SetRetention`/`SetLabels` already write).

## Ordered task list

1. [x] **Errors + Retention type** — add `ErrInvalidRetention` to `ts/errors.go`;
   create `ts/retention.go` with the `Retention` struct and validation helper.
2. [x] **Metadata helpers** — extend `seriesMeta` in `ts/series.go`; add
   `readMeta`/`writeMeta`; rewrite `SetLabels` to read-modify-write.
3. [x] **Policy accessors** — implement `Series.SetRetention` and
   `Series.Retention`.
4. [x] **Prune primitive** — implement `pruneBucket` and `retentionCutoff` in
   `ts/retention.go`.
5. [x] **Enforcement** — implement `Series.EnforceRetention`.
6. [x] **Auto-enforcement** — hook `pruneBucket` into `InsertMany` using the store
   clock.
7. [x] **Clock + store-wide enforcement** — add the `now` field and `SetNow` to
   `ts/store.go`; implement `Store.EnforceRetention` (iterate `__ts_meta__`).
8. [x] **Tests** — add the retention tests and (optionally) a benchmark.
9. [x] **Docs** — update `README.md` and `CHANGELOG.md`.
10. [x] **Verify** — run the checklist; fix any gofmt/vet/test issues.

## Execution log

- `8888c06` — **feat(ts): add per-series retention**. Added retention policies,
  metadata read-modify-write helpers, automatic write-time pruning, explicit
  series/store enforcement, the injectable clock, tests, and the retention
  benchmark.
- Documentation commit — README retention example, CHANGELOG update, this plan
  update, and `POST.md`.

### Implementation notes

- `pruneBucket` counts keys with a cursor rather than `Bucket.Stats().KeyN`.
  `Stats()` does not include keys written earlier in the same uncommitted write
  transaction, which made write-time `MaxPoints` enforcement a no-op. The cursor
  count reflects pending `Put` calls and is required for correct auto-pruning.
- `retentionCutoff` clamps underflow to `math.MinInt64`, matching the plan's code
  sketch. This is safe but means such a cutoff removes no points; the test
  asserts the clamp and the absence of panic rather than the plan bullet's
  "all points removed" wording.
- Metadata is written read-modify-write, so `SetLabels` preserves `Retention`
  and `SetRetention` preserves `Labels`.
- Auto-enforcement happens in the same transaction as `InsertMany`, so
  just-inserted backfill older than `MaxAge` is never observable. Tests that
  exercise explicit enforcement set policies after inserting fixtures.

### Verification results

- `gofmt -l ts`: clean.
- `go vet ./...`: pass.
- `go build ./...`: pass.
- `go test -count=1 -timeout 180s ./...`: pass.
- `go test -race -count=1 -timeout 300s ./ts/...`: pass.
