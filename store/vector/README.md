# vector — vector storage and similarity search

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/harbor/store/vector.svg)](https://pkg.go.dev/github.com/malivvan/harbor/store/vector)

`vector` stores float32 vectors with JSON-like payloads in bbolt and searches them
with either an exact Flat index or an approximate HNSW graph. It supports
payload filters, scalar quantization, buffered writes and optional encryption.

```go
// Through the root package.
docs, err := store.Vector().NewCollection("docs", 3, vector.Cosine, true)

// Or directly, with a caller-owned bbolt handle.
vstore, err := vector.Open(ctx, bdb, vector.Config{
	Root: [][]byte{[]byte("vec")},
})
defer vstore.Close()
docs, err := vstore.NewCollection("docs", 3, vector.Cosine, true)
```

`vector.Open` borrows the database and rejects a nil handle with
`vector.ErrInvalidConfig`. By default `Store.Close` flushes buffered points and
closes the handle. An embedding store that shares the handle sets
`Config.SharedDB` (or calls `SetOwnsDB(false)`); `Close` then only flushes and
releases the wrapper, and the caller closes the handle. `Store.Closed()`
reports the state.

`vector.Config` exposes `Validate()` and `Normalize()` with the same checks as
`vector.Open`: encryption needs a 32-byte `Secret`, `UseQuant` fills in the default
quantizer, and empty `Root` elements are dropped. `CollectionOptions.Validate()`
checks a collection's name, dimension and metric before creation, and
`HNSWParams.Normalize()` previews the effective index parameters.

## Collections

A collection has a fixed vector dimension, a distance metric, and an index
strategy:

```go
col, err := vstore.NewCollection(
	"documents", // name
	1536,        // vector dimension
	vector.Cosine,  // metric
	true,        // use HNSW; false selects the exact Flat index
)
```

Metrics:

| Metric | Meaning | Better scores |
|--------|---------|---------------|
| `vector.Cosine` | Cosine similarity (normalized by magnitude) | higher |
| `vector.Euclid` | Euclidean distance | lower |
| `vector.Dot` | Dot product | higher |

Existing collections keep their configuration. Reopening a collection with a
different dimension, metric, index type, quantization mode or HNSW parameters
returns `vector.ErrInvalidConfig` instead of reinterpreting old vectors.

To tune HNSW:

```go
col, err := vstore.NewCollectionWithParams("documents", 1536, vector.Cosine, true, vector.HNSWParams{
	M:              16,  // neighbors per node
	EfConstruction: 200, // candidate list while inserting
	EfSearch:       64,  // candidate list while searching
	K:              10,  // advisory default top-k
})
```

`vector.DefaultHNSWParams()` returns the defaults shown above. `K` is advisory:
`Search` always returns the number of results requested by its `topK` argument.

## Points

```go
type PointStruct struct {
	ID      string
	Version uint64    // store-managed; caller values are not trusted
	Vector  []float32
	Payload Payload   // map[string]any
}
```

Payload values support:

| Go type | Stored as | Returned as |
|---------|-----------|-------------|
| `string` | string | `string` |
| `int`, `int64` | integer | `int64` |
| `float32`, `float64` | double | `float64` |
| `bool` | bool | `bool` |
| `[]byte` | bytes | `[]byte` |
| maps, slices, other complex values | JSON | decoded Go values (integral numbers stay `int64`) |

The payload key `__quantized_vector` is reserved and rejected.

### Upsert and buffering

```go
err := col.Upsert([]vector.PointStruct{
	{
		ID:      "doc-1",
		Vector:  []float32{0.1, 0.2, 0.3},
		Payload: vector.Payload{"lang": "en", "year": 2026},
	},
})
```

`Upsert` validates dimensions, deep-copies the points, assigns a store-managed
version, updates the in-memory index immediately (so searches see the writes),
and buffers them for bbolt. Buffered points are flushed when
`vector.DefaultFlushThreshold` (64) accumulate. Force a flush with:

```go
err := col.Flush()
err = vstore.Flush() // every collection created by this Store
```

A failed flush re-buffers the batch, while preventing points deleted in the
meantime from being resurrected. `Collection.Pending()` reports how many
points are buffered and not yet durable.

`Collection.Close` flushes the collection, retires the handle, and deliberately
does not close the shared bbolt handle. After Close, `Closed()` is true and
later calls on the collection return `vector.ErrClosed`; Close is idempotent, and
`Store.Flush`/`Store.Close` skip retired collections. `Store.Close` flushes
every remaining collection and closes the handle only when it owns it (the
default; `Config.SharedDB` or `SetOwnsDB(false)` for embeddings). Use
`Store.Closed()` to check the store state.

## Search

```go
hits, err := col.Search([]float32{0.1, 0.2, 0.3}, nil, 5)
for _, hit := range hits {
	fmt.Println(hit.ID, hit.Score, hit.Payload)
}
```

- `queryVector` must match the collection dimension.
- `topK` must be greater than zero.
- Results are ordered by relevance: descending for Cosine/Dot, ascending for
  Euclid.
- `ScoredPoint` carries `ID`, `Version`, `Score` and `Payload`.

HNSW search uses post-filtering: it over-fetches candidates, applies the
payload filter, then returns up to `topK` matches. The Flat index is exhaustive
and exact.

### Range search

`RangeSearch` returns every point within a distance instead of a fixed top-K:

```go
hits, err := col.RangeSearch(query, 0.25, filter)
capped, err := col.RangeSearchN(query, 0.25, filter, 100)
```

- `radius` is a distance in the collection's metric space: Euclidean distance,
  `1 - cosine similarity`, or `-dot` product. A negative radius is
  `ErrInvalidOptions`.
- `Score` is the same public score `Search` reports (cosine similarity, dot
  product, or Euclidean distance), and results are ordered the same way.
- The scan is exact for both index types: it evaluates the filter against every
  indexed point instead of walking the HNSW graph. That bounds the result set
  correctly for an open radius.
- A non-positive limit (`RangeSearchN`) returns every match; ties order by id.

### Vectors and custom scoring

`SearchWith` adds per-call options to `Search`:

```go
hits, err := col.SearchWith(query, filter, 10,
	vector.WithVectors(),
	vector.WithScorer(func(query, vector []float32) float64 {
		return customScore(query, vector)
	}),
)
```

- `WithVectors` populates `ScoredPoint.Vector` with a copy of the stored
  embedding. Without it the field stays nil, so a normal search allocates
  nothing extra.
- `WithScorer` replaces the final score (and the ordering) using
  `fn(query, vector)`; a higher score is better regardless of the metric.
- Candidate selection still uses the collection's index and metric, so filters
  and HNSW recall are unchanged.

### Facet counts

`FacetCounts` counts the string values of payload fields over the points that
match a filter:

```go
facets, err := col.FacetCounts(vector.FacetRequest{
	Fields:    []string{"kind", "lang"},
	Filter:    filter, // optional
	MaxValues: 10,     // 0 keeps every value
})
for _, f := range facets {
	fmt.Println(f.Field, f.Total, f.Values)
}
```

- Only string values are counted; missing keys and numeric values are skipped.
- Values are ordered by count descending then value, and `MaxValues` keeps the
  most frequent. `Total` is the number of matching points before truncation.
- Counts cover the filtered collection, not a previous search result. To facet
  a result set, pass a filter that selects it or count the result ids yourself.

## Filters

```go
filter := &vector.Filter{
	Must: []vector.Condition{
		{Key: "lang", Type: vector.MatchTypeExact, Match: vector.MatchValue{Value: "en"}},
		{Key: "year", Type: vector.MatchTypeRange, Range: &vector.RangeValue{GTE: 2020}},
	},
	MustNot: []vector.Condition{
		{Key: "status", Type: vector.MatchTypeExact, Match: vector.MatchValue{Value: "deleted"}},
	},
}

hits, err := col.Search(query, filter, 10)

matching, err := col.GetPointsByFilter(filter)
deleted, err := col.Delete(nil, filter)
```

Semantics:

- `Must`: every condition must match.
- `MustNot`: no condition may match.
- A missing payload key never matches.
- Numeric comparisons are type-agnostic: `int`, `int64`, `float32`, `float64`
  and unsigned integers compare numerically, so a filter built with `5` keeps
  matching after a round-trip that returns `int64`.

Condition types:

| Type | Uses | Behavior |
|------|------|----------|
| `vector.MatchTypeExact` | `Match.Value` | Equality (numeric types compared numerically) |
| `vector.MatchTypeRange` | `Range` | `GT`/`GTE`/`LT`/`LTE` numeric bounds |
| `vector.MatchTypePrefix` | `Match.Value` | String or byte prefix |
| `vector.MatchTypeContains` | `Match.Value` | Array/string containment |
| `vector.MatchTypeRegex` | `Match.Value` | Regex over strings or bytes |

An empty `Type` behaves like `MatchTypeExact`.

### Delete

```go
removed, err := col.Delete([]string{"doc-1", "doc-2"}, nil) // by ID
removed, err = col.Delete(nil, filter)                      // by filter
```

A non-empty `points` slice takes precedence; otherwise `filter` is used. If
both are empty, `Delete` returns an error. Storage is updated first, then the
in-memory index, keeping both consistent.

## Store-level APIs

The `Collection` API is the usual entry point. The `Store` also exposes the
lower-level storage surface:

```go
err := vstore.EnsureCollection("documents")
names, err := vstore.ListCollections()
metas, err := vstore.ListCollectionMetas()
meta, err := vstore.LoadCollectionMeta("documents")

points, err := vstore.LoadCollection("documents") // map[string]*PointStruct
err = vstore.UpsertPoints("documents", points2)
err = vstore.DeletePoints("documents", []string{"doc-1"})
err = vstore.DropCollection("documents")
```

`ListCollections` excludes the internal metadata bucket. `LoadCollection`
returns an empty map for a missing collection. `DeletePoints` silently skips
IDs that do not exist and succeeds when the collection itself is missing;
`DropCollection` also succeeds when the collection is already gone.

## Quantization

Scalar quantization shrinks stored vectors from 32-bit floats to 8-bit
integers:

```go
vstore, err := vector.Open(ctx, bdb, vector.Config{
	Root:     [][]byte{[]byte("vec")},
	UseQuant: true, // nil Quantizer selects SQ8
})
```

`vector.NewSQ8Quantizer()` creates the default quantizer explicitly. The
quantization *mode* is persisted in collection metadata: reopening a quantized
collection without quantization, or a plaintext collection with quantization,
is rejected so vectors are never silently returned as `nil`. The quantizer
implementation itself is not recorded; use the same one for every process that
opens the collection.

SQ8 scales each vector to `[0, 255]` using its own min/max. Non-finite
components (`NaN`, `±Inf`) are stored as the vector minimum, so dequantizing
always yields finite values.

## Encryption

```go
vstore, err := vector.Open(ctx, bdb, vector.Config{
	Root:                   [][]byte{[]byte("vec")},
	Secret:                 key, // 32 bytes
	EncryptPoints:          true,
	EncryptPointIDs:        true,
	EncryptCollectionNames: true,
})
```

- `EncryptPoints` seals point blobs and collection metadata.
- `EncryptPointIDs` deterministically encrypts point IDs with AES-SIV. It also
  encrypts the stored point value, because that value contains the ID.
- `EncryptCollectionNames` deterministically encrypts collection bucket and
  metadata names. Listing still returns the logical names.
- Wrong mode returns `vector.ErrWrongMode`; a wrong key returns
  `vector.ErrAuthFailed`; a malformed key returns `vector.ErrInvalidKey`.
- `RotateAll` re-encrypts every collection and the marker, then adopts the new
  key. Run it with exclusive access, and call `VerifyAll` to confirm every
  point and metadata entry decrypts.

`ReEncryptCollection` is a partial rotation. It keeps the old key available in
the process and must be followed by `RotateAll` before closing the store. It
cannot run when `EncryptPointIDs` or `EncryptCollectionNames` is enabled,
because those physical names are derived from the master key; in that case it
returns `vector.ErrInvalidConfig`. Plaintext stores cannot be migrated in place.

See [Encryption](../../README.md#encryption) and [AGENTS.md](../../AGENTS.md#encryption-internals).

## Persistence format

- Point values use the hand-written positional format in `vec/binary.go`.
  Payload keys are sorted, so identical points encode identically.
- Collection metadata stores the format version and schema fingerprints.
  Reopening detects a reordered or incompatible codec instead of misreading old
  blobs.
- Collections are buckets; each point is keyed by its ID.
- Search indexes live in memory and are rebuilt from stored points on open.

## Errors

| Sentinel | Meaning |
|----------|---------|
| `vector.ErrClosed` | Operation on a closed store. |
| `vector.ErrStorageNotOpen` | Store has no underlying database. |
| `vector.ErrNilContext` | Nil context. |
| `vector.ErrInvalidConfig` | Invalid or inconsistent configuration. |
| `vector.ErrCollectionNotFound` | Collection bucket is missing. |
| `vector.ErrMetadataNotFound` | Collection metadata is missing. |
| `vector.ErrAuthFailed` | Ciphertext failed authentication. |
| `vector.ErrInvalidKey` | Missing or malformed key. |
| `vector.ErrWrongMode` | Store mode does not match the marker. |

## Performance

- Use HNSW for large collections; Flat search is exact but O(n) per query.
- Batch points in `Upsert`; the store merges writes up to the flush threshold
  into one transaction.
- Call `Flush` (or `Store.Close` with `Config.SharedDB`) before closing your own
  bbolt handle.
- Quantization trades recall for storage size; validate the trade-off on your
  data.
- Payload filters are applied after candidate retrieval in HNSW mode, so very
  selective filters may need a larger over-fetch or a Flat index.

## Related packages

- `vec/hnsw` — the vendored HNSW graph used by the HNSW index.
- `vec/hnsw/heap` — the heap used on the search hot path.

## License

MIT — see [../LICENSE](../../LICENSE). Start the whole-module tour at
[../README.md](../../README.md).
