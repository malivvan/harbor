# series — time series on BoltDB

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/tempest/store/series.svg)](https://pkg.go.dev/github.com/malivvan/tempest/store/series)

`series` stores named series of timestamped `float64` points in bbolt. Point keys are
chronological, so range scans and aggregations stream in time order without a
secondary index. Series can carry labels and a persisted retention policy, and
an optional encryption mode seals values or whole time blocks.

```go
// Through the root package.
series := store.Series().Series("cpu")

// Or directly, with a caller-owned bbolt handle.
tsStore, err := series.Open(ctx, bdb, series.Config{
	Root: [][]byte{[]byte("ts")},
})
defer tsStore.Close() // does not close bdb
series := tsStore.Series("cpu")
```

`series.Open` borrows the database; the caller owns its lifecycle. `Store.Close`
releases the wrapper only.

`series.Config` exposes `Validate()` and `Normalize()`, matching the checks in
`series.Open`: `BlockDuration` must be non-negative, and encryption needs a
non-empty `Root` plus a 32-byte `Secret`.

## Points and series

```go
type Point struct {
	Timestamp int64   // Unix nanoseconds
	Value     float64
}
```

A series is a named, ordered sequence of poinseries. Timestamps are Unix
nanoseconds. Ranges are inclusive on both ends.

```go
now := time.Now().UnixNano()

err := series.Insert(ctx, series.Point{Timestamp: now, Value: 0.42})

err = series.InsertMany(ctx, []series.Point{
	{Timestamp: now - int64(time.Minute), Value: 0.38},
	{Timestamp: now - 2*int64(time.Minute), Value: 0.40},
})
```

- `Insert` replaces a point with the same timestamp.
- `InsertMany` writes a batch in one transaction. Out-of-order timestamps are
  accepted, and the last value for a timestamp wins.
- `Get` returns the exact point at a timestamp or `series.ErrNotFound`.
- `Count` returns the number of live poinseries.

## Reading

```go
points, err := series.Range(ctx, start, end)
first, err := series.First(ctx)
last, err := series.Last(ctx)

err = series.RangeEach(ctx, start, end, func(p series.Point) error {
	fmt.Println(p.Timestamp, p.Value)
	return nil
})
```

`Range` returns points in time order. `RangeEach` streams them and stops at the
first callback error. `First` and `Last` return `series.ErrNotFound` for an empty
series.

Because keys are chronological, no in-memory index is needed for ranges.

## Aggregation

```go
samples, err := series.Aggregate(ctx, start, end, int64(time.Minute), series.AggAvg)
```

`Aggregate` buckets `[start, end]` into windows of width `step` and computes
one value per window. Each returned `Sample` carries the aligned window start.

| Function | Result |
|----------|--------|
| `series.AggMin` | minimum value |
| `series.AggMax` | maximum value |
| `series.AggAvg` | arithmetic mean |
| `series.AggSum` | sum |
| `series.AggCount` | number of points |
| `series.AggFirst` | earliest value in the window |
| `series.AggLast` | latest value in the window |

A non-positive step returns `series.ErrInvalidStep`; `start > end` returns
`series.ErrInvalidRange`.

## Labels

```go
err := series.SetLabels(ctx, map[string]string{
	"host": "web-1",
	"env":  "prod",
})

labels, err := series.Labels(ctx)
```

`SetLabels` replaces the labels. `Labels` returns an empty map when none are
set, never a nil map.

## Retention

```go
err := series.SetRetention(ctx, series.Retention{
	MaxAge:    24 * time.Hour,
	MaxPoints: 100_000,
})
```

- `MaxAge` drops points older than the reference time minus the age. Zero
  disables age-based retention.
- `MaxPoints` keeps only the most recent `MaxPoints` poinseries. Zero disables
  count-based retention.
- A negative value returns `series.ErrInvalidRetention`.
- The zero `Retention` clears the policy.

Policies are persisted with the series and enforced automatically by `Insert`
and `InsertMany`. Apply them explicitly with:

```go
removed, err := series.EnforceRetention(ctx, time.Now().UnixNano())
removed, err = tsStore.EnforceRetention(ctx, time.Now().UnixNano()) // all series
```

`Store.SetNow` injects a clock for deterministic tests:

```go
tsStore.SetNow(func() int64 { return fixedUnixNano })
tsStore.SetNow(nil) // restore the wall clock
```

## Deleting data

```go
removed, err := series.DeleteRange(ctx, start, end) // returns points deleted
err = series.Drop(ctx)                              // series and metadata
err = tsStore.DeleteSeries(ctx, "old-series")
```

## Managing series

```go
s := tsStore.Series("cpu")
name := s.Name()
owner := s.Store()

names := tsStore.SeriesNames() // names created through this Store, in order
root := tsStore.Root()         // copy of the root bucket path
```

## Configuration and encryption

```go
tsStore, err := series.Open(ctx, bdb, series.Config{
	Root:               [][]byte{[]byte("ts")},
	Secret:             key, // 32 bytes
	EncryptValues:      true,
	EncryptSeriesNames: true,
	BlockDuration:      int64(time.Minute),
})
```

A zero `Config`, or one with no encryption enabled, keeps the plaintext format.

| Field | Effect |
|-------|--------|
| `EncryptValues` | Seals point values and series metadata. |
| `EncryptSeriesNames` | Encrypts series bucket and metadata names with deterministic AES-SIV. Listing still returns the logical names. |
| `BlockDuration` | Groups points into sealed blocks of this width in nanoseconds. Zero disables block mode. A positive value enables encryption on its own, so `Secret` is required. |

Block mode is strongly recommended for high-volume encrypted series. A block is
sealed and opened once, instead of every point being sealed and opened
separately. A few seconds to a minute is a good starting point for streaming
metrics.

Encryption requires a non-empty `Root`: at the database root the store cannot
distinguish its own series buckets from unrelated top-level buckeseries.

### Rotation

```go
err = tsStore.RotateAll(ctx, newKey)
err = tsStore.VerifyAll(ctx)
```

`RotateAll` re-encrypts every known series and the marker under `newKey`, then
drops the old key. Run it with exclusive access.

`ReEncryptSeries(ctx, name, newKey)` is a **partial** rotation: the previous key
stays available in the process and the marker remains under the old key, so the
series that were not rewritten keep working. Finish with `RotateAll` before
closing the store; a fresh process cannot read the whole store in between.

Partial rotation is impossible with `EncryptSeriesNames`, because the physical
names are derived from one master key. `ReEncryptSeries` returns
`series.ErrInvalidConfig` in that mode instead of leaving the database
unrecoverable. Use `RotateAll`.

Rotation requires an already encrypted store. There is no in-place plaintext
migration. See [Encryption](../../README.md#encryption) and [AGENTS.md](../../AGENTS.md#encryption-internals).

## Compression

`Config.Compress` Zstd-compresses point values, block payloads and series
metadata before sealing (`CompressLevel`, `MinCompress` tune it). Block mode is
where it pays off: the whole block is compressed once. Single 8-byte points are
never wrapped, because an envelope would make them larger. Reads auto-detect the
envelope, so opening the store without compression still works and rotation
carries the codec.

## Errors

Use `errors.Is`:

| Sentinel | Meaning |
|----------|---------|
| `series.ErrNilParam` | Required argument is nil. |
| `series.ErrNilContext` | Nil context. |
| `series.ErrNotFound` | Series or point does not exist. |
| `series.ErrInvalidRange` | `start > end`. |
| `series.ErrInvalidStep` | Aggregation step is not positive. |
| `series.ErrInvalidRetention` | Negative `MaxAge` or `MaxPoints`. |
| `series.ErrIncompatibleVersion` | Unsupported on-disk format. |
| `series.ErrAuthFailed` | Ciphertext failed authentication. |
| `series.ErrInvalidKey` | Missing or malformed key. |
| `series.ErrInvalidConfig` | Inconsistent encryption configuration. |
| `series.ErrWrongMode` | Store mode does not match the marker. |

## Concurrency and lifecycle

- `Store` and `Series` are safe for concurrent use. bbolt serializes writers;
  readers proceed concurrently.
- The store borrows the caller's `*bolt.DB`. Closing the wrapper does not close
  the database.
- Inside the root `tempest` store, `tempest.Store.Close` closes the shared handle;
  do not close the `series` store separately.
- A nil context returns `ErrNilContext`. Cancellation is checked between
  operations and during iteration, but cannot interrupt a bbolt lock wait.

## Performance

- Ranges, first/last and aggregation use chronological keys only; there is no
  secondary index to maintain.
- Use `InsertMany` instead of one `Insert` per point to amortize bbolt fsyncs.
- For encrypted series, set `BlockDuration`; otherwise each value is sealed
  individually.
- Retention keeps a live point count in metadata, so `Count` and already
  satisfied policies avoid decoding data.

## License

MIT — see [../LICENSE](../../LICENSE). Start the whole-module tour at
[../README.md](../../README.md).
