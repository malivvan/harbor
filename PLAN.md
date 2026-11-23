# Proposed Improvements — release backlog

This is the working backlog after the final pre-release review. The previous
29-item plan is complete, and the audit-trail plan (`PLAN.md`) is implemented,
tested and removed. `HARBOR.md` — a consuming package's list of generic storage
gaps — has been folded into this document and removed; every item below states
the current state, the proposed API, the design constraints, the implementation
steps and the acceptance tests.

Priorities:

- **Blocker** — must land before the first public tag; CI or the release is
  wrong without it.
- **P0** — required for downstream adoption; a consumer implements these today
  with workarounds.
- **P1** — high value; do after P0.
- **P2** — important but deferrable, or needs a design decision first.
- **P3** — polish, documentation and research.

Current review status: `gofmt`, `go vet`, `go build`, `go test ./...`,
`go test -race ./...` and `CGO_ENABLED=0 go test ./...` are green.
`go mod tidy -diff` is the only failing release check (see 0.1).

## 0. Release blockers

### 0.1 Tidy the module (CI `lint` job) — DONE

Done: `go mod tidy` removed the four unused modules; `go build`, `go vet` and
the test suite still pass; `go mod tidy -diff` is empty.

`go mod tidy -diff` fails. Tidy removes `github.com/malivvan/jolt`,
`github.com/MoeForge/moego`, `github.com/MoeForge/moejs` and
`github.com/tetratelabs/wazero` from `go.mod`/`go.sum`; nothing in the module
imports them.

Steps: run `go mod tidy`; confirm `go build ./...`, `go vet ./...` and
`go test ./...`; commit the `go.mod`/`go.sum` diff on its own.
Acceptance: `go mod tidy -diff` is empty.

### 0.2 Make `blob.Put` atomic — DONE

Done: `Store.Put` streams into a temporary sibling (`.harbor-put-*`) and
`fs.Rename`s it into place, removing the temporary key on any error. Tests cover
a mid-stream reader failure (no final key, no leftover temp, previous content
intact) and the content-address dedupe path. The `pkg/snapshot/blobstore` suite
(item 0.6) exercises the adapter end to end.

`blob.Store.Put` (`pkg/blob/blob.go`) opens the destination with `fs.Create` and
streams the reader straight into it. A reader that fails mid-copy leaves a
partial object at the final key. `snapshot.Create` (`pkg/snapshot/snapshot.go`)
skips an upload when `Stat` says the content-addressed blob exists, so a failed
earlier upload can be treated as present; `Restore`/`Verify` then fail closed and
the snapshot is permanently unrestorable.

Proposal: write to a temporary sibling key in the same bucket, then `fs.Rename`
it into place (fs rename is atomic); remove the temporary key on any error. No
public API change.

Steps: helper `putAtomic` in `pkg/blob/blob.go`; failing-reader test in
`pkg/blob/blob_test.go` (final key absent or previous content unchanged); snapshot
test that reuses a blob only after a complete upload.
Acceptance: fault-injected upload leaves no object and no dedupe entry;
snapshot create/restore still passes.

### 0.3 Document the new packages — DONE

Done: README gained "Object storage: `blob` and `snapshot`" and "Utilities"
sections (id, metrics, sign/canonical, vec/doc) with a table-of-contents entry,
a `vec/doc` guide link and pkg.go.dev links. AGENTS.md's repository map now lists
`vec/doc` and notes the password-file lifecycle, blob atomicity/containment and
snapshot manifest verification. Relative links resolve.

`pkg/store`, `pkg/blob`, `pkg/snapshot`, `pkg/snapshot/blobstore`, `pkg/id`,
`pkg/sign/canonical` and `vec/doc` now have AGENTS map entries and pkg.go.dev
links (added with the package move). Still needed: short README sections for
`store`, `blob` and `snapshot`, and a usage paragraph or guide for `id`,
`metrics`, `sign/canonical` and `vec/doc`.

Steps: one row per package in the AGENTS map plus a short note where behavior is
non-obvious (`store` password-file lifecycle, `blob` containment and atomicity,
`snapshot` manifests/verify/restore, `vec/doc` lazy dimension); README TOC,
sections and documentation links; keep per-package READMEs as the usage source.
Verify relative links and that snippets compile.
Acceptance: every package is discoverable from AGENTS.md and README.

### 0.4 `store` parity: expose `TS()` — DONE

Done: `Manager.TS()` and the package-level `store.TS()` were added, with a test
that every accessor (DB, FS, TS, Vec) is populated for both shapes. The store
README table and example list all four.

`store.Manager` exposes `DB()`, `FS()` and `Vec()`, but not `TS()`; the
package-level singleton has the same gap. A manager user must go through
`Store()` for time series.

Steps: add `Manager.TS() *ts.Store` and `store.TS()`; add a test; update the
`store` README table. No lifecycle change.
Acceptance: manager and singleton expose all four stores.

### 0.5 Decide `pkg/sign/canonical` — DONE

Done: `audit` now signs `canonical.Bytes(event)`; the same bytes are stored and
hashed into the chain. `pkg/audit/README.md` gained an "Event encoding" section,
and tests pin the stored bytes to the canonical form and prove that a
map-carrying event has one signature regardless of insertion order. The package
is no longer unexplained.

`pkg/sign/canonical` (deterministic JSON: sorted keys, stable numbers) has no
importer. `audit` signs `json.Marshal(event)`; struct field order is stable, but
a future event carrying a map would not be canonical.

Recommendation: sign `canonical.Bytes(event)` in `audit`, pin the encoding in
`pkg/audit/README.md`, and test that reordered map fields produce the same signature.
If it stays standalone, document it in README/AGENTS as a general utility.
Acceptance: no unexplained exported package; audit encoding is canonical or
explicitly documented.

### 0.6 `pkg/snapshot/blobstore` tests — DONE

Done: `blobstore_test.go` covers `List` (recursive, prefix-scoped, absent
prefix), `Get`/`Put` (including `size == -1`), `Delete` (idempotent), `Stat`
(missing key is not an error), the not-configured and nil-receiver paths, a full
snapshot create/verify/restore round trip through the adapter, and a failed
upload staying invisible.

`pkg/snapshot/blobstore` has no test files. Add a conformance test over a temporary
`fs`/`blob` store: `List`, `Get`, `Put`, `Delete`, `Stat`, prefix behavior,
missing keys, and the snapshot suite running through the adapter.
Acceptance: `go test ./pkg/snapshot/...` exercises the adapter.

### 0.7 Release gate — DONE

Recorded after blockers 0.1–0.6: `gofmt -l .` empty, `go vet ./...`,
`go build ./...`, `go test ./...`, `go test -race ./...`,
`CGO_ENABLED=0 go test ./...`, `go mod tidy -diff`, staticcheck and
govulncheck all green. Two staticcheck findings were fixed on the way
(dead `changeHookMu`, `copyNode`, `observeTSDecoded` helpers; a deprecated
bbolt error; a deliberately nil context now carries a `//lint:ignore`).
A single `TestReplicateFSMetadata` failure was observed once under the
whole-repository race run but did not reproduce in targeted stress (20×) or
subsequent whole-repository runs; it is recorded in `POST.md` for follow-up.

Run and record: `gofmt -l .`, `go vet ./...`, `go build ./...`,
`go test ./...`, `go test -race ./...`, `CGO_ENABLED=0 go test ./...`,
`go mod tidy -diff`, staticcheck and govulncheck per AGENTS.md; verify README
links and examples. Tag only from a green commit.

## 1. P0 — cross-store transaction — DONE

Done: `Store.WriteTransaction`/`ReadTransaction` and `Tx` are implemented in
`transaction.go`. `Tx.DB()` is `db.AttachWithAudit`, and `fs`, `ts` and `vec`
gained a `tx` field plus `WithTransaction`/`SetAuditSink`: their existing
`view`/`update` helpers now run against the caller's transaction, bound views
are not tracked by the parent store, and rotation is rejected
(`ErrInTransaction`). Vec stages points in the bound view and
`AdoptTransaction` installs the updated in-memory indexes after commit, so no
point is re-read. Audit events are staged by a collecting sink and replayed
after commit; `db.AttachWithAudit` covers the record store. Read transactions
use bbolt's read snapshot and construct existing file systems/collections
without writing (`readOnly` handling in `newFS` and `vec.newCollection`).

Deviation: write transactions are serialized with `Store.txMu` rather than
returning `ErrTxAlreadyOpen`. A nested call from inside a callback would block
instead of being detected (Go has no supported goroutine identity), so nesting
is documented as unsupported. Concurrent writers serialize as the plan asked.

Tests in `transaction_test.go`: cross-store commit visibility (including a
search over the adopted index), abort leaving db/fs/ts/vec untouched, context
cancellation rollback, read-your-writes inside the callback, read-only
enforcement, vector snapshot reads, concurrent writer serialization, and
post-commit audit. `go test -race` is green for the root package and the four
sub-stores.

### Current state

`db.Store.WriteTransaction` spans only the db sub-store. `fs`, `ts` and `vec`
each open their own bbolt transactions per operation (`fs.Store`/`ts.Store`
hold a `*bolt.DB`; `vec.Store.update/view` wrap `Update`/`View`). Vector writes
are buffered and flush on a threshold. A consumer that writes a record, a
vector point and a full-text posting must use compensating deletes; one failure
leaves a partial result.

### Proposed API

```go
// root package
func (s *Store) WriteTransaction(ctx context.Context, fn func(tx *Tx) error) error
func (s *Store) ReadTransaction(ctx context.Context, fn func(tx *Tx) error) error

type Tx struct{ /* one *bolt.Tx + tx-bound sub-store views */ }

func (tx *Tx) DB() db.Node        // db.Attach semantics
func (tx *Tx) FS() *fs.Store      // bound to tx; no nested transactions
func (tx *Tx) TS() *ts.Store      // bound to tx
func (tx *Tx) Vec() *vec.Store    // stages writes; commit applies them
```

### Design

- **One bbolt transaction.** `WriteTransaction` uses `s.bs.Update` under
  `rotMu.RLock`; every sub-store operation runs against that transaction.
- **Sub-store binding.** Introduce an internal runner interface
  (`Update(func(*bolt.Tx) error) error`, `View(func(*bolt.Tx) error) error`) and
  implement a tx-bound runner that executes against the shared transaction
  without committing. `fs.Store`, `ts.Store` and `vec.Store` already funnel
  writes through a small number of helpers, so this is a contained refactor.
  `db` already has `Attach`.
- **Vector staging.** Inside a transaction, `Collection.Upsert` clones and
  stages points; the in-memory index is *not* updated until commit, and
  `Search`/`Get` consult a staged overlay. Commit applies the index delta and
  writes the batch in the same bbolt transaction; abort discards the overlay
  (no HNSW rebuild). Outside a transaction the current buffering behavior is
  unchanged. If staging is deferred to a later phase, the fallback is an index
  snapshot/rebuild on abort; document its cost.
- **No nesting.** A transaction inside a transaction returns a new
  `ErrTxAlreadyOpen` (or joins the outer transaction — pick one and document it).
  Repositories elsewhere use the explicit error.
- **Audit.** Audit events remain post-commit; the recorder must never be called
  inside the transaction.
- **Cancellation.** A callback error or a canceled context rolls the bbolt
  transaction back; in-memory side effects are discarded with it.

### Implementation steps

1. Add `Tx`, `WriteTransaction`, `ReadTransaction`; open/close the transaction
   and hold the rotation guard.
2. db: `tx.DB()` returns `s.db.Attach(tx)`.
3. fs/ts: refactor their `*bolt.DB` field behind the runner interface; add
   `store.Tx(tx)`-style internal constructors; route all read/write helpers
   through the runner. Keep public behavior outside transactions identical.
4. vec: add the staged overlay and commit/abort plumbing; route writes through
   the runner; ensure `Flush` inside a transaction writes within it.
5. Root wiring and docs (README "Transactions"); add a compile-checked example.
6. Tests: commit makes db/fs/ts/vec state visible and atomic; abort leaves every
   store unchanged (including search results); fault-injected vec write rolls
   back db/ts/fs; nested transaction errors; cancellation rolls back; concurrent
   writers stay serialized; run with `-race`.

### Compatibility

New API only; no on-disk format change. `db.WriteTransaction`, `Attach` and the
existing sub-store APIs keep working. Document that `vec` staging changes when
searches observe a write inside a transaction (commit time), and that the
transaction is a single-writer unit by bbolt design.

## 2. P0 — full-text search (`fts`) — DONE

Done: new package `github.com/malivvan/harbor/fts` with `Store`/`Index`,
`Words`/`WordsWithCJK` tokenizers, BM25 ranking, `Upsert`, `Delete`, `Search`,
`Count` and `Indexes`. The root store opens it under `fts/`, derives a
`harbor/fts/v1` key, exposes `Store.FTS()` and `Tx.FTS()`, and value encryption
follows the `EncryptDBValues` flag (term encryption is opt-in via
`Config.EncryptTokens`). Each index has its own format version (1), a plaintext
mode record that fails closed on an encryption mismatch, and persisted
tokenizer/BM25 settings checked on reopen. `Upsert` removes old postings and
adjusts corpus statistics in one transaction; `Delete` is idempotent; results
are ordered by score then id with a non-positive limit meaning all matches.
Tests cover tokenizers, ranking, re-index, delete, persistence, CJK, limits,
tie order, encryption mode, token encryption, format-version rejection, and
transaction commit/abort through `Tx.FTS()`.

### Current state

No full-text index. A consumer maintains an inverted index with BM25 over db
buckets (terms/posting, document lengths, corpus statistics, word and CJK
tokenizers, re-index and delete).

### Proposed API

```go
// new package github.com/malivvan/harbor/fts
type Tokenizer interface{ Tokenize(string) []string }
func Words() Tokenizer          // Unicode letters/digits, lowercased
func WordsWithCJK() Tokenizer   // Words + Han/Hiragana/Katakana/Hangul trigrams

type Config struct {
    Tokenizer Tokenizer
    K1, B     float64
}

type Store struct{ /* bound to db.Node or the store root */ }
func (s *harbor.Store) FTS() *fts.Store
func (s *fts.Store) Open(name string, cfg fts.Config) (*fts.Index, error)

func (idx *fts.Index) Upsert(ctx context.Context, id, text string) error
func (idx *fts.Index) Delete(ctx context.Context, ids ...string) error
func (idx *fts.Index) Search(ctx context.Context, query string, limit int) ([]fts.Hit, error)
func (idx *fts.Index) Count(ctx context.Context) (int, error)
```

`Hit{ID string, Score float64}`; results ordered by score then ID for
determinism.

### Design

- Storage: one namespace per index under a harbor-owned root (`harbor/fts/<name>`
  or a dedicated bucket under the root), with `terms/`, `docs/` and `meta/`
  records. Add an `FTS` format-version record (version 1) and fail closed on
  anything else.
- BM25 with configurable `k1`/`b`; default `1.2`/`0.75`. Updates remove old
  postings before adding new ones; deletes decrement corpus statistics.
- Tokenization: word tokenizer, plus CJK trigrams (a run of n CJK characters
  contributes its n−2 trigrams, or the run itself when shorter).
- Encryption: postings follow the db value flags (`EncryptDBValues`); token
  encryption is optional and deterministic, and its vocabulary/ordering leakage
  must be documented. Do not enable it by default.
- Transactions: participate in the cross-store transaction (item 1); a failed
  transaction rolls postings and statistics back together.
- Root: `FTS()` is always available; no new persisted option.

### Implementation steps

1. Package skeleton, encoding and version marker; tokenizers with tests.
2. Index writes (`Upsert`/`Delete`) and corpus statistics; re-index and delete
   semantics.
3. `Search` (BM25, deterministic ties), `Count`.
4. Root wiring (`FTS()`, transaction surface) and docs.
5. Port the consumer's lexical suite: ranking, re-index, delete, CJK,
   persistence; add encrypted and transaction-rollback tests; run `-race`.

### Compatibility

New package; existing stores are untouched. The FTS root is new, so no on-disk
migration is required. Version the FTS format independently (version 1).

## 3. P0 — backup verification and metadata — DONE

Done: `BackupInfo`, `BackupOptions`, `Store.BackupFile` and `VerifyBackup`.
`BackupFile` streams through SHA-256 into a temporary sibling, syncs, renames,
and optionally writes `<path>.manifest.json` (size, checksum, creation time,
flags, per-store format versions). `BackupTo` now delegates to it with
`Overwrite: true`, keeping its signature and behaviour. `VerifyBackup` hashes
the file, reads the sidecar (if present), opens the copy read-only and validates
the metadata canary (wrong password → `ErrInvalidPassword`) and every
sub-store's marker/version. To make that read-only, `ts` now only writes its
format version when it is missing, and `fs`/`fts` gained `OpenReadOnly`
constructors used for per-filesystem and per-index validation. Tests cover
verify + restore of all stores, wrong password, truncation, manifest tampering,
overwrite policy, cancellation leaving no file, and a backup taken while a
writer is active. The manifest is a sidecar; the backup bytes are unchanged.

### Current state

`harbor.Store.Backup(ctx, w)` and `BackupTo(ctx, path)` write a consistent bbolt
snapshot (flushing vec first). They return only an error: no size, checksum,
timestamp or manifest, and there is no supported way to validate a copy before
restoring. The `snapshot` package snapshots logical objects in a blob store, not
the database file.

### Proposed API

```go
type BackupInfo struct {
    Path      string
    Size      int64
    SHA256    string
    CreatedAt time.Time
    Flags     uint16 // encryption selection of the source
    Version   string // db.Version
}

// BackupTo keeps its signature; add an info-returning sibling:
func (s *Store) BackupFile(ctx context.Context, path string, opts BackupOptions) (BackupInfo, error)

// VerifyBackup opens the copy read-only, checks the metadata/canary and every
// sub-store marker/version, and hashes the file.
func VerifyBackup(ctx context.Context, path, password string, opts *Options) (BackupInfo, error)
```

`BackupOptions{Manifest bool, Overwrite bool}`; a manifest sidecar (JSON) records
format versions, flags, checksum and creation time. Default: checksum returned,
manifest optional.

### Design

- `BackupFile` streams the snapshot through a SHA-256 writer (or hashes the
  finished file) and returns `BackupInfo`; atomic temp-file + rename as today.
- `VerifyBackup` opens the file with `bolt.Open(..., ReadOnly: true)` and
  validates: metadata record, canary (wrong password → `ErrInvalidPassword`),
  db/fs/ts/vec markers and format versions, and the checksum/manifest. It never
  writes. Foreign/corrupt files fail closed.
- Online backup keeps the existing one-read-transaction model (`Tx.WriteTo`);
  concurrent writers are not blocked and do not appear. Document that the copy
  is the on-disk representation (encrypted if the source is).
- The `snapshot` package remains the tool for object-store snapshots; document
  the combination (database backup + blob snapshot) in the operator docs.

### Implementation steps

1. `BackupInfo`/`BackupOptions`; checksum and manifest plumbing in `BackupFile`;
   keep `Backup`/`BackupTo` behavior.
2. Read-only verification path (internal open without writes) and
   `VerifyBackup`; validate canary/markers/versions.
3. Tests: online backup under writes, restore reads all four stores; corrupt
   copy refused; wrong password; encrypted store; checksum stability; manifest
   validation; cancellation; atomic replace.
4. Docs: README backup section, operational checklist.

### Compatibility

Additive. The backup bytes are unchanged; the manifest is a sidecar. `VerifyBackup`
is read-only and does not modify the copy.

## 4. P1 — vector radius search — DONE

Done: `Collection.RangeSearch` and `RangeSearchN` in `vec/range.go`. The radius
is a distance in the metric space, the returned score is the same value
`Search` reports, filters apply before scoring, results are ordered by score
then id, a non-positive limit returns every match, negative radius/limit return
`ErrInvalidOptions`, and a dimension mismatch uses the existing error.

Deviation: the scan is exact for both Flat and HNSW collections rather than an
approximate HNSW over-fetch. An open radius has no bounded candidate set, and
exactness is the documented trade-off for `RangeSearch`; a future `RangeOptions`
can add an approximate mode without changing this API.

Tests: Flat/HNSW Cosine parity, Euclid and Dot distances and ordering, filters,
limits, zero and empty radii, negative arguments, dimension mismatch, score
agreement with `Search`, and deterministic tie ordering. `go test -race ./vec`
is green.

### Current state

`vec.Collection` offers only top-K `Search`; a consumer loads every point and
computes distances itself for radius queries.

### Proposed API

```go
func (c *Collection) RangeSearch(query []float32, radius float32, filter *Filter) ([]ScoredPoint, error)
func (c *Collection) RangeSearchN(query []float32, radius float32, filter *Filter, limit int) ([]ScoredPoint, error)
```

### Design

- `radius` is a distance in the collection's metric space: Euclidean distance,
  `1 − cosine similarity`, or `−dot` (the same internal distance HNSW uses).
  `Score` in the result is the metric's public score, exactly as `Search`
  returns it.
- Flat/exact index: full scan, filter, sort by relevance, optional limit.
- HNSW: bounded over-fetch around the radius (documented approximate); offer an
  exact option (`RangeOptions{Exact: true}` or a method suffix) that scans the
  points. Negative radius returns `ErrInvalidOptions`; zero returns distance-zero
  matches.
- Filters apply before distance collection; dimension mismatch uses the same
  error as `Search`.

### Steps and tests

Add an exact scan path using `GetPointsByFilter`/the index; add the HNSW path;
tests for flat-vs-scan parity, exact HNSW mode, filters, limit, empty result,
negative radius, dimension mismatch; benchmark.

### Compatibility

New methods; no format change.

## 5. P1 — vector facet counts — DONE

Done: `Collection.FacetCounts` with `FacetRequest`/`FacetResult`. It streams the
filtered points (no full-result slice), counts string values per requested
field, skips missing and non-string values, deduplicates fields, orders values
by count then value and truncates at `MaxValues` (zero = all), reports `Total`
before truncation, and validates negative caps and empty field names. Facets
cover the filtered collection; the README documents how to facet a result set.

Tests: counts against a manual pass, filter scoping, `MaxValues`, missing
fields, numeric values, empty collection and no fields, duplicate fields,
negative/empty validation, and concurrent upserts with reads (race).

### Current state

Counting payload values (optionally filtered) lives in the consumer.

### Proposed API

```go
type FacetRequest struct {
    Fields    []string
    Filter    *Filter
    MaxValues int
}
type FacetResult struct {
    Field  string
    Values map[string]int
    Total  int
}
func (c *Collection) FacetCounts(req FacetRequest) ([]FacetResult, error)
```

### Design

Stream points through the index (no full slice), apply the optional filter,
count string values of each requested field, skip missing/non-string values,
sort by count descending then value, truncate to `MaxValues`. `Total` is the
number of matching points (before value truncation). Document that counts cover
the filtered collection, not a result set; consumers that want result-set facets
pass the result IDs as a filter (or count themselves).

Tests: counts vs. a manual pass, filter scoping, `MaxValues`, missing fields,
numeric values, empty collection, concurrent upsert/read; benchmark.

## 6. P1 — metadata aggregation in `db` — DONE

Done: `Node.Aggregate` with `AggregateSpec`, `Measure`, `Having`, `TimeBucket`,
`AggregateResult` and `Group`. It streams decoded records (holding only group
accumulators; values are retained per group only for median/percentile),
supports count/sum/avg/min/max/median/percentile/stddev/stddev_pop, multiple
group keys, day/week/... time buckets from `time.Time`, RFC 3339 or Unix-second
fields, explicit missing and non-numeric policies with a `Skipped` count,
HAVING on measures/count/group keys, ordering by measure or key with
deterministic ties, and a limit. Invalid specs fail with `ErrInvalidAggregate`
before any record is read. The `Node` interface gained `Aggregate`, so
transaction-bound nodes aggregate inside the caller's transaction.

Tests: basic measures, multiple keys/order/limit, HAVING, median/percentile/
std dev, time buckets, missing and non-numeric policies, empty bucket,
validation, and cancellation.

### Current state

The `db` store has no aggregation; a consumer reimplements group-by, measures,
HAVING, time buckets, operator filters and SQL-compat quirks.

### Proposed API

```go
type AggregateSpec struct {
    GroupBy    []string
    Measures   []Measure
    Having     []Having
    OrderBy    string
    Desc       bool
    Limit      int
    TimeBucket *TimeBucket
    Missing    MissingPolicy    // MissingExclude | MissingZero
    NonNumeric NonNumericPolicy // NonNumericError | NonNumericSkip | NonNumericZero
}
type Measure struct {
    Alias string
    Type  MeasureType // Count, Sum, Avg, Min, Max, Median, Percentile, StdDev, StdDevPop
    Field string
    P     float64 // percentile
}
type TimeBucket struct { Field string; Unit TimeUnit }
type AggregateResult struct {
    Groups []Group
    Skipped int64 // values that could not be aggregated
}
type Group struct {
    Keys     map[string]any
    Measures map[string]float64
    Count    int64
}

func (n Node) Aggregate(ctx context.Context, spec AggregateSpec) (AggregateResult, error)
```

### Design

- Stream matching records via the query iterator; no full materialization except
  the values needed for median/percentile per group (bounded by the group's
  values; document memory).
- Group keys are field values; time buckets use creation or a named timestamp
  field with a fixed unit.
- Missing/non-numeric behavior is explicit policy, not a harbor default; expose
  both so a consumer can select SQL-compatible semantics.
- `Having` references measure aliases (and count); `OrderBy` accepts a measure
  alias or group key; `Limit` applies after ordering.
- Field resolution uses cached reflect paths; errors for unknown fields,
  invalid percentiles, or mismatched measure types. Context checked per record.

### Steps and tests

Port the consumer's planner/aggregator behind the API; test count/sum/avg/min/max,
multiple group keys, having/order/limit, measures, time buckets, operator
filters, skipped/missing/non-numeric policies, empty groups, unknown field,
cancellation, encrypted stores; compare against a manual aggregation; benchmark
on a large fixture.

### Compatibility

New API; no format change. Do not change existing matcher semantics.

## 7. P1 — references and cascades in `db` — DONE

Done: `ref=<bucket>` and `ondelete=cascade|restrict` tags are parsed (and
carried through the struct-meta cache), `ref` implies an index, and saves
validate that a non-zero reference names an existing record
(`ErrReferenceTargetMissing`, `ErrDanglingReference`). A per-store registry
(`__harbor_refs__`, metadata only) records referencing fields so
`DeleteStruct`, `Select(...).Delete` and `Drop` can enforce `restrict` or
cascade in the same transaction, recursively, with a cycle/depth guard
(`ErrReferenceCycle`). Cascaded deletes remove index entries generically.
`Node.ValidateSchema` checks sample records' reference targets at startup.
References are single scalar keys; zero values mean "no reference".

Tests: dangling/missing target, zero references, restrict, cascade, recursive
cascade, bulk delete, drop (restrict and cascade), schema validation and a bad
`ondelete` tag. The existing db suite is green, including the tests that assert
`Select.Delete` does not decode records when nothing references the bucket.

### Current state

No foreign keys. A consumer validates parent existence and deletes children by
hand.

### Proposed API (tags only)

```go
type Chunk struct {
    ID    string `db:"id"`
    DocID string `db:"index,ref=documents"`
}
type Chunk struct {
    DocID string `db:"index,ref=documents,ondelete=cascade"` // or restrict
}
```

### Design

- Parse `ref` and `ondelete` in the existing tag parser (`db/extract.go`); a
  malformed or unknown target is a schema error at first use.
- On save, require the referenced record to exist (exact key lookup; works with
  encrypted keys). Create an index on the referencing field automatically.
- On delete/drop of a target: `cascade` deletes referencing records (recursively,
  with a depth/cycle guard); `restrict` fails with a clear error. Perform the
  whole operation in one db transaction (and later the cross-store transaction).
- Optional `ValidateSchema` helper to catch typos at startup; consumers can call
  it once.
- Document that references are single scalar keys in v1 (no slices/maps).

### Steps and tests

Tag parsing + field config; save validation; cascade/restrict delete; re-pointed
references; missing target bucket; metadata/index creation; encrypted stores;
transaction rollback; docs. Acceptance: an equivalent dangling-reference test
fails with the new tag and succeeds with a valid parent, with no consumer code in
the path.

## 8. P2 — vectors and custom scoring in results — DONE

Done: `ScoredPoint.Vector` (nil unless requested), `Collection.SearchWith`,
`WithVectors` and `WithScorer`. Candidate selection stays with the index metric;
the scorer replaces the final score and ordering (higher is better), so HNSW
recall and filtering are unchanged. Tests cover Flat and HNSW vector
population and copy isolation, scorer ordering, filters, tie ordering, nil
options and validation. The vec README documents the options.

### Current state

`Search` returns `ScoredPoint{ID, Version, Score, Payload}`; the vector is not
returned and the metric score cannot be replaced, so a consumer re-reads points
and re-scores.

### Proposed API

```go
func (c *Collection) SearchWith(query []float32, filter *Filter, topK int, opts ...SearchOption) ([]ScoredPoint, error)
func WithVectors() SearchOption
func WithScorer(fn func(query, vector []float32) float64) SearchOption
```

Optionally add `Vector []float32` to `ScoredPoint` (populated only with
`WithVectors`). Candidate selection stays the index metric; the scorer replaces
only the final score, so HNSW recall is unchanged (documented). Tests: vectors
match stored points, scorer ranks by the supplied function, no vectors by default
(allocation check), filters and HNSW/exact paths.

## 9. P2 — streaming projection in `db` — DONE

Done: `Query.Project(fields...)` and an optional `db.ProjectedCodec`
capability. The JSON codec decodes only the requested top-level fields (Go or
JSON tag names); other codecs fall back to a full decode plus zeroing, which
preserves correctness but not the allocation win. Unprojected fields stay zero,
unknown/unexported names are errors, and `Project` with matchers or ordering is
refused with `ErrProjectionUnsupported`. `Find`, `First`, `Each` and
`Iter`/`Decode` honor it. Tests cover Find/Each/Iter, JSON tag names, unknown
fields, matcher/order rejection and the custom-codec fallback.

### Current state

`Query.Iter` streams raw values but always decodes the full record; linear scans
in a consumer pay the full decode.

### Proposed API

```go
query.Project("ID", "Metadata").Each(ctx, &T{}, fn)
it, _ := query.Project("ID").Iter(ctx, &T{})
```

### Design

- Add `Project(fields ...string) Query` (immutable like other builders).
- Optional codec capability: `UnmarshalProjected(b []byte, fields []string, v any) error`.
  The JSON codec decodes into `map[string]json.RawMessage` and assigns only the
  requested top-level fields; other codecs fall back to a full decode followed
  by zeroing unprojected fields (correctness preserved, no allocation win).
- Matchers and `OrderBy` need their fields; the query refuses (or auto-includes)
  a projection that omits them. Missing/unexported field names are errors.
- Nested field paths are out of scope for v1 (document).
- Tests: unprojected fields zero, requested fields set, matcher/order fields
  enforced, custom codec fallback, missing field error, iterator decode, and an
  allocation benchmark.

## 10. P2 — schema migrations — DONE

Done: `Schema`, `Step` and `Store.Migrate`, with `SchemaVersion`. The
application schema version lives in a dedicated top-level `harbor.schema`
bucket rather than changing `harbor.Metadata`, so existing stores open
unchanged. Steps are validated for contiguity and target version, applied in
order inside one cross-store transaction, and the version is re-read inside the
transaction so concurrent calls apply each step once. A newer store is refused
with `ErrSchemaTooNew`; an equal version is a no-op; a failing step rolls back.
Tests: forward migration, idempotent re-run, too-new refusal, failing-step
rollback, malformed schemas and concurrent calls.

### Current state

`harbor.Metadata` holds encryption/KDF settings; `db.Version` is the on-disk
codec version. There is no application schema version or step runner.

### Proposed API

```go
type Schema struct {
    Version uint64
    Steps   []Step
}
type Step struct {
    From, To uint64
    Apply    func(ctx context.Context, tx *Tx) error
}
func (s *Store) Migrate(ctx context.Context, schema Schema) error
```

### Design

- Store the application schema version in a new versioned record (for example a
  `harbor.schema` bucket) rather than changing the `harbor.Metadata` layout, so
  existing stores keep opening.
- Apply steps in order inside the cross-store transaction (item 1) so a failed
  step rolls back whole; a version lower than the binary's is upgraded, an equal
  version is a no-op, and a higher version is refused (store written by a newer
  build).
- Steps must be deterministic and idempotent; document that they must not read
  wall-clock time or randomness unless pinned.
- Tests: forward migration, idempotent re-run, too-new refusal, failing step
  rollback, concurrent open, empty schema.

## 11. P2 — input-ordered multi-get in `db` — DONE

Done: `Node.GetMany(ctx, ids, to)` in `db/getmany.go`. It validates a pointer
to a slice of structs/pointers, reads every key in one read transaction, keeps
request order, repeats duplicates, returns missing IDs in request order, and
reports a missing bucket as "all missing". Encrypted keys work through the
existing key encoding. Tests cover order/missing/duplicates, pointer and value
slices, empty input, wrong destinations and an encrypted store.

### Current state

Batch reads are loops of `One`, each with its own transaction and decode; a graph
consumer needs input order and a missing-ID report.

### Proposed API

```go
func (n Node) GetMany(ctx context.Context, ids []any, to any) (missing []any, err error)
```

`to` is a pointer to a slice of structs or pointers; results follow `ids` order,
duplicates are returned in request order (or deduped — decide and document),
missing IDs are returned in request order. One read transaction; direct key
lookups; encrypted keys work through the existing key encoding. Tests: order,
missing, duplicates, empty input, wrong destination type, encrypted store, and a
benchmark against the loop.

## 12. P2 — append-only history helper (research) — DESIGN NOTE RECORDED

No code, as the plan requires. The design note is in
[Appendix A](#appendix-a-append-only-history-design-note). The shape still needs
a consumer to validate it before it becomes API.

A generic bitemporal pattern (monotonic version keys, as-of reads, retraction)
recurs in consumers. Before implementing, write a short design note covering:
key layout (`key/version` vs. a separate history bucket), timestamp source,
encryption/order trade-offs, retention/compaction, and as-of lookup semantics.
Candidate API (not final):

```go
h := node.History("documents")
version, err := h.Append(ctx, id, value)
snapshot, err := h.AsOf(ctx, id, at)
```

No code until a consumer validates the shape. Keep application-specific
intervals and meanings out of harbor.

## 13. P2 — `fs` for application files and sessions — DONE

Done: the pattern is documented in the root README ("Application files and
sessions") and `fs/README.md`, and `TestFSSessionSurvivesReopen` writes a JSON
session file, closes the store, reopens it and reads the state back. fs
operations already participate in `tx.FS()`, so no new primitive was needed.

No new harbor primitive is needed: `fs` already provides named file systems with
snapshots, symlinks, content encryption and external content. Harbor work:

- Document the pattern (`store.FS().Open("app.fs")`, sessions as JSON files on
  the virtual FS, optional separate database file if the application requires
  it) in README and `fs/README.md`.
- Ensure fs operations participate in the cross-store transaction (item 1).
- Add an example/test that writes a session file, reopens the store and reads it
  back; keep the application's session semantics outside harbor.

## 14. P2 — reciprocal-rank fusion helper (optional) — DONE

Done: `pkg/fusion` with `RRF(k, lists...)`, deterministic score-then-id ordering,
duplicate suppression within a list, `DefaultRRFK` (60), and no weighting or
list-selection policy. It is a small standalone package because FTS and vector
results now coexist; the caller still decides which lists to fuse. Tests cover
merging, k handling, empty lists, ties and duplicates.

Consider a small `search` package only if the FTS and vector APIs coexist:

```go
func RRF(k int, lists ...[]Result) []Result
```

Weights, list selection and ranking policy stay in the caller. Low value until
FTS exists; do not add a package for it alone.

## 15. P2 — ordered and prefix queries under encryption — DOCUMENTED, OPTION 2 CHOSEN

Done: README "Ordered queries under encryption" and the AGENTS encryption
section document the three options (plaintext order index, open-time in-memory
ordered map, OPE), the leak each incurs, and why Harbor keeps failing closed in
this release. Option 2 is the planned path: no format change, per-index opt-in,
bounded memory. No OPE without a security review; no code in this release.

`db` returns `ErrEncryptedOrdering` for ordered/prefix lookups when keys are
encrypted, because ciphertext order is meaningless. Harbor should document the
three options and pick one rather than promise order-preserving encryption:

1. a documented plaintext index mode for non-secret ordering fields (accepting
   the order leak);
2. an in-memory ordered map built at open (same leak, no format change);
3. order-preserving encryption (research only; requires a security review).

Deliverable: a README/AGENTS subsection and, if option 1 is chosen, an opt-in
index flag with tests proving deterministic ordering and documenting leakage. Do
not implement OPE without review.

## 16. P3 — audit follow-ups — DONE

Done: `slip10.Node` gained `Destroy`, and `audit.DeriveKey` wipes the derived
node (previously only the returned keys were wiped). `fs.Copy`/`CopyWithOptions`
now record one `copy` event for the destination. `db.Set` records `set` and
`db.SetBytes` `set_bytes` instead of both recording `set_bytes`. The audit README
documents the higher-level write mapping (blob/snapshot via fs, store manager
records nothing), the out-of-scope `VerifyData`, the runtime-only audit flags,
the tail-truncation anchoring pattern, and the copy coverage. Canonical event
encoding was completed in 0.5. Tests cover the copy event and the set/set_bytes
distinction.

The trail (audit package, PLAN.md) is implemented and tested: signed events,
hash chain, genesis anchor, head reporting, per-store flags, verification, and
wrong-key/wrong-password behavior. Remaining polish:

- **SLIP-10 node wiping:** `slip10` does not wipe intermediate nodes; derived
  audit keys are wiped with `storeKeys`, but the node values are not. Add a
  `Wipe`/`Destroy` or derive-and-wipe helper and document it. (`PLAN.md` §12.)
- **Canonical event encoding:** see blocker 0.5.
- **Tail-truncation anchoring:** document a scheduler pattern that records
  `Audit().Head()` outside the store; optionally provide a tiny helper. Tail
  truncation is intentionally not detectable from the store alone.
- **Audit coverage of copy paths:** `fs.Copy`/`CopyWithOptions` currently write
  content and inodes without an audit event (they were not in the audit table).
  Decide whether to record a `copy` event (source → destination) or document the
  gap; add tests either way.
- **Operation naming:** `db.Set` records as `set_bytes` because it routes through
  `SetBytes`. Decide whether to distinguish `set` from `set_bytes`.
- **Higher-level writes:** `blob` and `snapshot` writes go through `fs` and are
  audited there; document that mapping. The `store` manager only opens stores.
- **Optional data re-verification:** `VerifyData` (re-hash current records and
  compare to event digests) is out of scope for v1; document it as a possible
  follow-up.
- **Persisted audit flags:** audit flags are runtime-only by design; if users
  want them persisted, that needs a metadata version bump and canary binding.

## 17. Findings already resolved (no action) — CONFIRMED

- Ordered `Select(...).OrderBy(...).Find` into `[]*T` aliasing: fixed in
  `db/sink.go` with `TestOrderedQueryKeepsTheMatcher`
  (`db/ordered_filter_regression_test.go`). Keep the `[]T` / `[]*T` / pre-sized
  slice coverage when changing the sink.
- Single on-disk version 1 across stores, markers and `/v1` key domains; pinned
  by `TestSingleVersionOnePolicy`.
- Audit trail: PLAN.md phases implemented; this review added the missing
  concurrent-record and wrong-key tests plus `create`/O_TRUNC audit events.
  `PLAN.md` removed.
- `db.Codec` pointer semantics (marshal receives the entity pointer; reads
  decode into `reflect.New(elem)` — a pointer to a pointer for `[]*T` sinks) are
  still undocumented; see item 18.

## 18. P2 — codec contract documentation and test — DONE

Done: `db.Codec` now documents exactly what `Marshal` and `Unmarshal` receive
for Save/Update, One, Find into `[]T` and `[]*T`, Each, Iterator/Decode, Set,
and the internal `*map[string]any` decodes used by aggregation and references;
the destination is zeroed first. `db/codec_contract_test.go` runs the full
read/write matrix with a pointer-strict codec (nil and extra indirection
rejected), so a codec that only breaks `[]*T` reads fails loudly. The matrix is
also summarised in `db/README.md`.

Document in `db.Codec` (and `db/README.md`) exactly what `Marshal` receives and
what `Unmarshal` receives for `Save`, `One`, `Find` into `[]T` and `[]*T`, and
`RawEach`/`Each`. Add a test codec that is pointer-strict (rejects nil/extra
indirection) and run the full read/write matrix, so a custom codec fails loudly
instead of only on pointer-slice reads. This is documentation plus test work; no
behavior change.

## 19. What stays in the application (boundary)

Do not move these into harbor: domain semantics (scoring/decay, opinions,
identity, episodes, prompt assembly), SQL-compatibility policy (expose it as
aggregation knobs instead), application schema meaning (migrations run steps,
but steps are application code), and facade-specific document scoping. Harbor
owns the primitive; the application owns the meaning.

## 20. Release checklist — COMPLETE

- [x] `go mod tidy` clean (0.1).
- [x] `gofmt -l .`, `go vet ./...`, `go build ./...` clean.
- [x] `go test ./...`, `go test -race ./...`, `CGO_ENABLED=0 go test ./...` green.
- [x] staticcheck and govulncheck clean (AGENTS.md workflow).
- [x] New packages documented and linked; README snippets compile.
- [x] `blob.Put` atomicity (0.2) and `pkg/snapshot/blobstore` tests (0.6) done.
- [x] `store.TS()` parity (0.4); `pkg/sign/canonical` decision (0.5).
- [x] Internal planning documents: `HARBOR.md` is absent; `PLAN.md` is retained
      as the single backlog/record at the user's request.
- [x] On-disk versions: every pre-existing format stays at version 1 with its
      markers unchanged. FTS is a new root with its own version 1 and a new
      `harbor/fts/v1` key label; no existing key or record changed.
- [ ] Tag from a commit whose CI is green. Not tagged here: the local gate is
      green, but CI has not run on this working tree. See `POST.md`.

Execution summary: all blockers and P0/P1 items are implemented; P2 items 8–15
are implemented or documented with a decision; P3 item 16 is implemented; item
17 needed no action; item 18 is implemented. The follow-ups that remain are
listed in `POST.md`.

## Appendix A — append-only history design note

This is a design note for the deferred history helper (PLAN.md item 12). It is
not API and no code ships for it until a consumer validates the shape.

**Key layout.** Prefer a sibling bucket per tracked record bucket:
`<bucket>__history/<primary key>/<version>`, where `<version>` is a 16-byte
big-endian counter (or a ULID suffix when ordering across writers matters).
A separate bucket keeps current-record scans and index maintenance untouched and
lets retention drop history independently. Storing history records inside the
record bucket would make every normal scan filter them out and would break
primary-key ordering.

**Timestamp source.** A caller-supplied logical time (not the wall clock) should
be the as-of key, because backfills and imports are not monotonic in wall time.
Persist it alongside the version and use it for `AsOf`; document that two writes
with the same logical time order by version. The store-managed version remains
the tiebreaker.

**Encryption and ordering.** History keys contain the primary key and a
monotonic version, so the same ordering trade-off as item 15 applies: with
`EncryptDBKeys`, exact lookup at a known version works but range/as-of scans by
version do not. Options are (a) store the version outside the encrypted key,
(b) keep the history bucket plaintext-keyed and encrypt only values, or (c) keep
a plaintext ordered index for the version. Each leaks something; do not enable
OPE without review.

**Retention and compaction.** Per-key retention: keep the latest version, the
latest version older than N days, and any version referenced by an external
anchor. Compaction rewrites the history bucket in one transaction. Never delete
the newest version while the current record exists, or `AsOf` cannot distinguish
"unchanged" from "no record".

**As-of lookup.** `AsOf(id, t)` finds the greatest version with logical time
`<= t`, or reports that the record did not exist. Retractions are explicit
tombstone versions; absence is different from a tombstone. `Append` returns the
new version so a caller can pin it. The API candidate from the plan
(`h.Append`, `h.AsOf`) is a reasonable starting point, but no shape should be
frozen before a consumer validates interval semantics and retention.
