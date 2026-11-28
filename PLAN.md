# Implementation plan — closing the GAPS audit for tempest

This document is a complete, executable plan that resolves every item in the
GAPS audit (GAP-01 … GAP-33). It is written for a coding agent (e.g. Reasonix +
deepseek-flash) to execute one gap at a time, each as one reviewable commit.

Conventions used throughout:

- **"trace"** and **"ledger"** are the same store (`store/ledger`): the
  signed, hash-linked append-only trail. `Store.Trace()` returns the recorder,
  `Store.VerifyTrace()` verifies it, per the current tree.
- **"Fail closed"** always means: a build that does not implement a persisted
  feature must refuse to open the store, never silently serve data that should
  be hidden, expired, or unscoped.
- Signatures below are the shape of the change (from GAPS.md), not code that
  compiles against today's tree. The implementer fills in exact types.

---

## 0. Progress

Last updated: 2026-11-28. Status is `pending`, `in progress`, or `done`. A gap
is `done` only when its acceptance tests pass and the tree is green.

| Gap | Status | Note |
|---|---|---|
| GAP-01 | pending | |
| GAP-02 | pending | |
| GAP-03 | pending | |
| GAP-04 | done | record, vector and search expiry (`SetExpiry`/`Expiry`/`SweepExpired`), fail-closed format/capability markers |
| GAP-05 | pending | |
| GAP-06 | done | soft delete/tombstones, `Restore`, `Deleted`, `PurgeDeleted`, capability bit |
| GAP-07 | done | record revisions, `UpdateIf`/`DeleteIf`/`Revision`, `ErrConflict` |
| GAP-08 | pending | |
| GAP-09 | pending | |
| GAP-10 | done | `Store.HybridSearch`, RRF + weighted fusion, filters, cursor, `Why` |
| GAP-11 | done | `store/rag` splitter, offsets/digests, provenance keys, window filter |
| GAP-12 | done | collection model metadata, mixed-model guard, `StalePoints` and resumable `Reembed` |
| GAP-13 | pending | |
| GAP-14 | pending | |
| GAP-15 | pending | |
| GAP-16 | pending | |
| GAP-17 | done | search/vector explain APIs and the root retrieval-event sink |
| GAP-18 | done | record `AllPage`, search `SearchPage`, vector `ListPointsPage` with shape-checked cursors |
| GAP-19 | pending | |
| GAP-20 | pending | |
| GAP-21 | pending | |
| GAP-22 | pending | |
| GAP-23 | pending | |
| GAP-24 | pending | |
| GAP-25 | pending | |
| GAP-26 | pending | |
| GAP-27 | pending | |
| GAP-28 | pending | |
| GAP-29 | pending | |
| GAP-30 | pending | |
| GAP-31 | pending | |
| GAP-32 | pending | |
| GAP-33 | done | `internal/evals` metrics/corpus and the HNSW recall floor in `store/vector/quality_test.go` |

## 1. Purpose and how to use this plan

Each gap is written to be landed independently. Every gap section contains:

1. **Goal** — one paragraph on the concrete capability and its general-store shape.
2. **API** — the public surface (types/functions) to add.
3. **Storage & format** — what changes on disk and how compatibility is kept
   fail-closed (following the audit's Appendix C decisions).
4. **Files touched.**
5. **Implementation steps** — ordered, concrete work items.
6. **Acceptance tests** — the testable conditions, mapped to the audit's
   acceptance bullets.

Cross-cutting mechanisms shared by several gaps are defined once in
[§3 Cross-cutting conventions](#3-cross-cutting-conventions) and referenced from
the gap sections.

## 2. Guardrails → implementation invariants

The audit's four "generality guardrails" become these hard invariants that every
gap must satisfy. A change that violates one is out of scope and must be pushed
to the companion.

- **G1 Zero-value-off.** The zero value of every new `Config`/`Options` field
  keeps today's byte-identical behaviour, storage and results. Every new
  persisted structure is created only when the caller opts in.
- **G2 No domain vocabulary.** No `Memory`, `Persona`, `Mood`, `Salience`,
  `Person`, `Episode` type or field. The vocabulary is `Scope`, `Subject`,
  `Tenant`, `ExpiresAt`, `Revision`, `Tombstone`, `Partition`, `Log`, `Job`,
  `Lease`, `Rollup`, `Analyzer`, `Query`, `Cursor`, `Index`, `Schema`.
- **G3 Policy stays in the caller.** Decay curves, importance, conflict
  heuristics, extraction prompts, consolidation, prompt assembly, emotion and
  drift scoring, embedding-provider calls, and product job scheduling are all
  out of scope (Appendix B is authoritative and must not be re-opened).
- **G4 Versioned and fail-closed.** Any new persisted structure is additive and
  documented, or bumps the relevant format with a compatibility plan. Unknown
  versions/feature bits are never reinterpreted; they fail the open.

## 3. Cross-cutting conventions

### 3.1 Feature-bitmask compatibility record (record store and friends)

The audit flags three *fail-open* risks where an older build would silently
return data it must hide: scoping (GAP-01), TTL (GAP-04), and — in a different
form — trace replication (GAP-30). Since this tree still has exactly one on-disk
version per store (release 1), do **not** introduce a second version string.
Instead, introduce a per-store **capabilities bitmask** that is written only when
a caller opts into a semantics-changing feature, and validated on open:

- `record`: a `capabilities` record in the `__database` bucket (same place as
  `version`), a big-endian `uint64` bitmask. Reserved bits are assigned in
  §4/§5 (scope, expiry, history, tombstone, CAS, field-policy).
- `search`: extend the existing always-written `__mode__` byte with capability
  bits, or add a `__caps__` record. Prefer a separate `__caps__` record so the
  mode byte keeps its encryption meaning.
- `vector`: a `caps` field inside `CollectionMeta` (JSON), so per-collection
  opt-in features (named/sparse vectors, model) are detected.
- `graphs`: a `caps` record under the graph root, alongside the mode byte.
- `fstree`: the existing `FENC` marker flags + the format version.
- `object`: the existing mode byte (already carries encryption + dedup).

Rule: `Open` computes the store's known feature set; if the persisted bitmask
has any bit the build does not implement, return `ErrIncompatibleVersion` (or a
store-specific `ErrUnknownFeature`) instead of opening. Because the feature is
opt-in, an existing store has a zero bitmask and opens unchanged. This is the
concrete implementation of guardrail G4 and covers every gap that changes read
semantics.

### 3.2 Every new sealed structure participates in rotation + VerifyAll

Anything stored under a store's root and sealed with the store key must be
walked by that store's `RotateAll`/`ReEncrypt*` and `VerifyAll`, and must be
covered by `Export`/`Import` where the store is exported. New buckets listed
below (history, tombstones, expiry index, property indexes, snapshots, blobs,
log, jobs, rollups) must each get:
- a bullet in the store's `RotateAll`/`VerifyAll` walker,
- a bullet in `Export`/`Import` (or an explicit decision to exclude, documented),
- a test proving the sealed form survives a round trip and `VerifyAll`.

### 3.3 Injectable clock

For deterministic tests of any time-dependent feature (TTL, tombstones, history,
leases, jobs, expiry), reuse the existing per-store `SetNow` where present
(series, vector, graphs) and add the same one-liner to `record`, `search`,
`object` and `store/log`/`store/jobs` where missing.

### 3.4 Generality test

Before merging any gap, add one test that exercises it from a *non-companion*
caller (a cache with TTL, a tenant-scoped todo list, a product search with
hybrid retrieval, an audit log with retention). If the feature only makes sense
for a companion, it is in the wrong repository.

---

# Part A — Isolation, privacy and lifecycle

## GAP-01 — Store-enforced ownership and visibility scoping

**Priority P0 · Effort L · Depends on none**

### Goal

Multi-tenancy for `record`, `vector`, `search` and `object`: a `Scope` carried
in the context stamps writes and constrains reads, so a silent unscoped read of
opted-in data is impossible. No domain vocabulary (`Scope`/`Subject`/`Tenant`,
never `Person`).

### API

```go
// record package
type Visibility int // 0 = private (default), 1 = shared, 2 = tenant

type Scope struct {
    Tenant  string
    Subject string
    Visible Visibility
}

func WithScope(ctx context.Context, s Scope) context.Context
func ScopeFrom(ctx context.Context) (Scope, bool)

type ScopeFields struct {
    Tenant, Subject, Visible string // struct field names; "" = store stamp
}
// record.Config gains: ScopeFields ScopeFields  // zero disables scoping
```

Enforcement points (all in one place, a `scopePredicate` helper shared by every
read path):

- `record`: `Save`/`Update`/`UpdateField` stamp the reserved scope fields and
  reject a write whose context scope mismatches the stored scope (except an
  explicit shared/tenant write). `Get`/`Find`/`All`/`Select`/`Range`/`Prefix`/
  `GetMany`/`Count` inject the predicate; `q` matchers and index-backed `Find`
  compose the scope condition.
- `vector`: scope fields are payload keys (`__tenant__`, `__subject__`,
  `__visibility__`); `Upsert` stamps them, `Search`/`ListPoints`/`RangeSearch`
  inject them into the `Filter`.
- `search`: scope becomes document metadata; `Upsert` stamps, `Search` filters.
- `object`: scope is a per-key stamp; `Put`/`List` filter.

### Storage & format

- `record`: a reserved child bucket `__tempest_scope_` (or reserved fields plus a
  secondary index on `(tenant, subject, visible)`). Set the `capabilities` bit
  `scope` (§3.1). **Fail closed**: an old build that does not know the bit
  refuses to open — do not rely on "old builds ignore the bucket".
- `vector`/`search`/`object`: reserved payload/meta keys; no format bump (they
  filter on explicit fields already), but record their scope mode in the store's
  caps/mode so a scoped store fails closed.

### Files

`store/record` (Config, node implementations, q matchers, index find),
`store/vector`, `store/search`, `store/object`, root `Options` (scope fields
passthrough) and `Tx` (scope carried on the bound context).

### Steps

1. Add `Scope`/`WithScope`/`ScopeFrom`/`Visibility`/`ScopeFields` in `store/record`.
2. Add the capabilities-bitmask record + validation in `store/record` open (§3.1).
3. Implement stamp-on-write and predicate-on-read in `record` behind
   `ScopeFields`; add the secondary index and wire `Find`/`Select`/`Range`/`Prefix`.
4. Mirror stamp/predicate in `vector` (payload), `search` (doc meta), `object`
   (key stamp) using the same `Scope` type.
5. Propagate `Options.ScopeFields` from the root and bind `Scope` onto the `Tx`
   context so cross-store writes share the scope.
6. Rotation/VerifyAll/Export walk the new index bucket.

### Acceptance tests

- `TestScopeIsolationRecord`/`Vector`/`Search`/`Object`: `Subject=alice` cannot
  read a `bob` row through any read path, including `Select` matchers,
  index-backed `Find`, `vector.Search` with a permissive filter, `search.Search`.
- `TestScopeMissingFailsClosed`: opted-in store + no scope → reads fail closed.
- `TestScopeZeroValueIdentical`: non-opted-in store has byte-identical layout
  and results (guardrail G1).
- `TestScopeConcurrent`: two goroutines with different scopes never observe each
  other's rows.
- `TestScopeOpenOldBuildRefused`: a scoped store with the caps bit set fails to
  open on a build that doesn't implement it (simulate by masking the known bits).
- Generality test: a tenant-scoped todo list with two tenants.

---

## GAP-02 — Per-subject keys and crypto-shredding

**Priority P1 · Effort XL · Depends on GAP-01**

### Goal

Envelope encryption with a key registry: a `Partition` is a key domain; data
written through it is sealed with a partition DEK, and dropping the DEK makes the
data unreadable (crypto-shredding) while bytes remain.

### API

```go
type PartitionOptions struct { Create, Rotate bool }
func (s *Store) Partition(id string, opts PartitionOptions) (*Store, error)
func (s *Store) DropPartition(ctx context.Context, id string, shred bool) error
```

### Storage & format

- A DEK registry in `system` (wrapped by the master key): `id → wrapped DEK +
  key id + tombstone flag`. `DropPartition(shred:true)` deletes the wrapped DEK
  and writes a tombstone; reads then fail with `crypto.ErrAuthFailed`.
- Every sealed value gains a small key-id prefix (or a per-bucket header) so the
  reader selects the DEK. This is a **format change** for every store that opts
  in — gate it behind `PartitionOptions` and a mode flag / key-id marker; unknown
  key ids fail closed (guardrail G4).
- `Partition` returns a `*Store` bound to that partition id (a derived
  `Scope`-like view, not a second database); all sub-store writes through it
  select the partition DEK.

### Files

`store/system` (key registry), every `seal`/`open` helper (key selection by
key-id), root `Options`, `Export`/`Import` (partition-aware), `backup` (backups
carry the wrapped registry so a pre-shred backup still cannot read a shredded
partition).

### Steps

1. Design the wrapped-DEK record and the key-id prefix format (a reviewable
   design note committed with the change).
2. Add the registry + `Partition`/`DropPartition` in `store/system`.
3. Thread key-id selection through every `seal`/`open` helper; reject unknown
   key ids with `ErrAuthFailed`.
4. Wire `Partition` into root accessors (scoped store views).
5. Partition-aware `Export`/`Import` and backup verification.

### Acceptance tests

- `TestPartitionIsolation`: two partitions cannot read each other's values with
  the same password.
- `TestDropPartitionShred`: after `DropPartition(shred)`, every read of that
  partition's data fails with `crypto.ErrAuthFailed`; reopen does not recover it.
- `TestShredBackupUnreadable`: a pre-shred backup opens, but the shredded
  partition is unreadable in it too (or the opposite is documented + tested —
  pick one and pin it).
- `TestPartitionRotate`: rotating one partition re-encrypts only that partition
  and leaves others readable.
- `TestPartitionZeroValueIdentical`: no partition → byte-identical to today.

---

## GAP-03 — Cascade forget/erasure and orphan GC

**Priority P0 · Effort L · Depends on none (GAP-01 makes it safer)**

### Goal

A link registry plus a cross-store `Forget(id)` that deletes every related row in
one transaction, and `Orphans` that reports/repairs dangling rows.

### API

```go
type EntityRef struct{ Store, Bucket, ID string }
type LinkSpec struct { /* how an entity id maps to each store */ }

func (s *Store) RegisterLink(ctx context.Context, l LinkSpec) error
func (s *Store) Forget(ctx context.Context, id string, opts ForgetOptions) (*ForgetReport, error)
func (s *Store) Orphans(ctx context.Context, opts OrphanOptions) (*OrphanReport, error)
```

### Storage & format

- The link registry lives in a reserved metadata bucket under the root (no new
  root bucket). **No store-format change** — it is derived metadata describing
  which record field/payload key holds the shared entity id.

### Files

root (`tx.go`, new `forget.go`), `store/record`, `store/vector`, `store/search`,
`store/graphs` (including closed versions), `store/object`, `store/fstree`
(dedup refcounts), `internal/bucket` (no new root).

### Steps

1. Add `LinkSpec`/`RegisterLink` and the registry bucket.
2. Implement `Forget`: within one `WriteTransaction`, delete the record, its
   index entries, vector points (and quantized blobs), search postings, graph
   nodes/edges *and their closed versions*, object keys, fstree paths and dedup
   refcounts. Reuse each store's existing delete, bound to the shared `Tx`.
3. Implement `Orphans`: scan registered links, report (and optionally remove)
   rows whose entity id is gone.
4. Return a `ForgetReport` listing what was removed and what was unreachable
   (e.g. another partition).

### Acceptance tests

- `TestForgetRemovesEveryStore`: after `Forget(id)`, record `Get`,
  `search.Search` (term unique to it), `vector.Search` (its embedding), graph
  `GetNode`/`NodeHistory`, object `Get` all not-found.
- `TestForgetDedupRefcountsZero`: dedup refcounts reach zero and GC reclaims the
  blob.
- `TestForgetMissingIsNoop`.
- `TestForgetRollbackOnPartialFailure`: a partial failure rolls back every store
  and reports nothing deleted.
- `TestOrphansFindsAndRemoves`: an orphaned vector + graph node are found and
  removed without touching live rows.

---

## GAP-04 — TTL, expiry and retention sweeps

**Priority P0 · Effort M · Depends on none (GAP-20 for scheduling)**

### Goal

Per-record/per-point/per-document `ExpiresAt` with an index-backed sweep and
default read-time exclusion. General TTL/cache/session primitive.

### API

```go
// record
type ExpiryOptions struct{ Field string }
func (n Node) SetExpiry(ctx context.Context, data any, at time.Time) error
func (n Node) SweepExpired(ctx context.Context, now time.Time, limit int) (int, error)
func (n Node) FindLive(ctx context.Context, ...) // existing finds exclude expired

// vector
func (c *Collection) SetExpiry(id string, at time.Time) error
func (c *Collection) SweepExpired(now time.Time, limit int) (int, error)

// search
func (idx *Index) UpsertWithExpiry(ctx context.Context, id, text string, at time.Time) error
func (idx *Index) SweepExpired(ctx context.Context, now time.Time) (int, error)
```

### Storage & format

- `record`: reserved `__tempest_expiry_` index (expiry time → key) + a reserved
  field; set the `expiry` capability bit (§3.1). **Fail closed**: the bit makes
  an old build refuse to open an expiring store (fix the fail-open risk).
- `vector`/`search`: reserved payload/meta key + a per-collection/index expiry
  index; record the mode in caps so it fails closed.
- Read paths exclude expired rows by default; `IncludeExpired` option for audit.

### Files

`store/record` (reserved field, index, read predicates), `store/vector`,
`store/search`, root `Options`, `Tx` (sweep inside a transaction), rotation/
VerifyAll walkers.

### Steps

1. Add the expiry index + capability bit in `record`; stamp `SetExpiry`.
2. Add read-time exclusion to `Get`/`Find`/`All`/`Select`/`Count` (+
   `IncludeExpired`).
3. Implement `SweepExpired` (deletes value + index + vector/search siblings in
   one transaction, bounded by `limit`).
4. Mirror in `vector` and `search` (excluding expired from `Search`,
   `ListPoints`, corpus stats after sweep).
5. Injectable clock (reuse `SetNow`).

### Acceptance tests

- `TestTTLRecordHiddenThenGone`: expired record invisible to every read before
  the sweep, absent after it.
- `TestTTLVectorExcluded`: expired point not returned by `Search`/`RangeSearch`,
  removed by `SweepExpired`.
- `TestTTLSearchCorpus`: expired document contributes no hit/stat after sweep.
- `TestSweepIdempotentBounded`.
- `TestTTLZeroValueIdentical` + `TestTTLOldBuildRefused` (caps bit).

---

## GAP-05 — Record revision history and as-of reads

**Priority P1 · Effort L · Depends on none**

### Goal

Per-record revision history with `History`/`AsOf`/`Diff`/`TrimHistory`, mirroring
the graphs store's versioning for records.

### API

```go
type ChangeOp int // created | updated | deleted
type Revision struct {
    Key []byte; Seq uint64; Op ChangeOp; At time.Time; Digest []byte; Value []byte
}
func (n Node) History(ctx context.Context, data any) ([]Revision, error)
func (n Node) AsOf(ctx context.Context, data any, at time.Time) (any, error)
func (n Node) Diff(ctx context.Context, from, to time.Time) (*RecordDiff, error)
func (n Node) TrimHistory(ctx context.Context, before time.Time) (int, error)
```

### Storage & format

- Reserved child bucket `__tempest_history_` per mapped bucket, keyed by
  `(primary key, seq)`; `Seq` monotonic per store. Sealed like the values it
  copies. **Additive**, opt-in via a `History` config knob; old builds ignore the
  bucket (history loss documented, acceptable only because opt-in).
- `AsOf` resolves by seq or time; `TrimHistory` only removes revisions older than
  the cutoff.

### Files

`store/record` (write path, history bucket, version counter), `ReEncrypt`/
`VerifyAll` (walk history), `Export`/`Import`.

### Steps

1. Add `History` config knob + capability bit; create the history bucket.
2. On `Save`/`Update`/`DeleteStruct`, append a `Revision` (op, seq, at, digest,
   value) in the same transaction.
3. Implement `History`/`AsOf`/`Diff`/`TrimHistory`.
4. Walk history in rotation/VerifyAll; export/import it.

### Acceptance tests

- `TestHistoryOps`: create/update/delete/update → three revisions with correct
  ops and original values recoverable.
- `TestAsOfBoundaries`: before-first-write not-found; between writes returns the
  correct prior value.
- `TestTrimHistory`.
- `TestHistoryDisabledNoWrite` + `TestHistorySurvivesReopenAndVerifyAll`.

---

## GAP-06 — Soft delete, tombstones and restore

**Priority P1 · Effort M · Depends on none**

### Goal

Tombstones + `Restore`/`Deleted` so deletes are undoable and sync can tell
"deleted" from "never existed".

### API

```go
type DeleteOptions struct { Soft, Cascade bool; At time.Time }
func (n Node) DeleteStructWith(ctx context.Context, data any, opts DeleteOptions) error
func (n Node) Restore(ctx context.Context, data any) error
func (n Node) Deleted(ctx context.Context, limit int) ([]DeletedRef, error)
func (n Node) PurgeDeleted(ctx context.Context, before time.Time) (int, error)
```

### Storage & format

- Reserved tombstone child bucket (key → at, digest), sealed, additive; a
  `tombstone` capability bit. Vector points and search documents get the same
  treatment (tombstoned but excluded from search, kept for restore).

### Files

`store/record`, `store/vector`, `store/search`, `Export`/`Import`
(`IncludeDeleted`).

### Steps

1. Add tombstone bucket + capability bit.
2. `DeleteStructWith(Soft)`: write tombstone, hide from normal reads.
3. `Restore` (from live value, or GAP-05 history when enabled), `Deleted`,
   `PurgeDeleted`.
4. Vector/search tombstone paths; export/import honors `IncludeDeleted`.

### Acceptance tests

- `TestSoftDeleteHideAndRestore`: hidden from `Get`/`Find`/`All`/`Select`/
  `Count`/`vector.Search`/`search.Search`; `Restore` brings it back byte-equal.
- `TestHardDeleteRemovesBytes`.
- `TestDeletedLists`.
- `TestSoftDeleteExcludedFromExport` + `TestTombstoneSurvivesReopen`.

---

## GAP-07 — Optimistic concurrency (revision/CAS)

**Priority P1 · Effort M · Depends on none**

### Goal

`If-Match`-style CAS via a per-record revision counter, so a multi-writer
companion detects lost updates.

### API

```go
type Precondition struct{ Revision uint64 } // 0 = must not exist; >0 = must match
func (n Node) UpdateIf(ctx context.Context, data any, pre Precondition) (uint64, error)
func (n Node) DeleteIf(ctx context.Context, data any, pre Precondition) error
var ErrConflict = errors.New("record: revision conflict")
```

### Storage & format

- Reserved per-record revision counter in the metadata bucket (not the value),
  additive. Vector upserts enforce `PointStruct.Version`; graph upserts use the
  version id.

### Files

`store/record`, `store/vector`, `store/graphs`.

### Steps

1. Add revision counter to the record write path; `UpdateIf`/`DeleteIf` compare
   and increment in one transaction, return `ErrConflict` + current revision.
2. Make `vector.PointStruct.Version` enforceable on `Upsert`.
3. Graph upsert precondition via version id.

### Acceptance tests

- `TestCASConcurrent`: two `UpdateIf` with same revision → exactly one wins,
  other gets `ErrConflict`, stored value is the winner's.
- `TestCASPreconditionZero`: on existing key conflicts; on new key creates
  revision 1.
- `TestCASDisabledUnconditional` + `TestRevisionDurableAcrossReopen`.

---

## GAP-08 — Write-time redaction and field-level encryption

**Priority P1 · Effort L · Depends on GAP-02 (for key scope)**

### Goal

Field-level policy: seal a field independently, exclude it from indexes/vectors/
search, and redact before storage. General regulated-data primitive.

### API

```go
type FieldPolicy struct { Sealed, Indexable bool; Redact func(string) string }
func (c Config) WithFieldPolicy(field string, p FieldPolicy) Config
```

### Storage & format

- Sealed fields get a marker inside the encoded value (or a separate sealed blob
  per field); additive but versioned. Existing values read under "no policy".
- Ledger records `Redact(digest)` (or a policy id), never the raw hash of a
  sealed field.

### Files

`store/record` codec, `store/search` ingestion (`Indexable:false` → placeholder
term), `store/vector` payload, `store/ledger` digest, `Export`/`Import`.

### Steps

1. Add `FieldPolicy`/`WithFieldPolicy`; apply before sealing and before any
   index/derived write.
2. Search ingestion respects `Indexable:false`; vector payload excludes sealed
   fields.
3. Ledger digest policy.
4. Fail closed on a policy change without re-index/rewrite (or document
   forward-only).

### Acceptance tests

- `TestFieldSealedUnreadableInDump`: sealed field unreadable in a raw dump while
  the rest is readable.
- `TestFieldNonIndexable`: never appears in `search.Terms`, vector payload
  filters, graph property indexes.
- `TestRedactOncePerWrite` + `TestPolicyChangeFailsClosed`.

---

## GAP-09 — Root-level key rotation (rekey)

**Priority P2 · Effort L · Depends on none**

### Goal

A root `Rotate` that re-keys every sub-store (canary, data, trace keys) in one
exclusive operation.

### API

```go
type RotateOptions struct { NewPassword string; NewKey []byte; Verify bool }
func (s *Store) Rotate(ctx context.Context, opts RotateOptions) error
```

### Storage & format

- A rotation marker + re-sealed `system` metadata; no data-layout change. Crash
  mid-rotation leaves old metadata → store reopens with old keys consistently.

### Files

root `db.go`, `store/system`, every `RotateAll`, `ledger` (re-derived trace keys).

### Steps

1. Take the exclusive write lock (refuse when a Tx/bound view is open).
2. Derive new keys (or accept raw `NewKey`), rotate every sub-store's key,
   re-seal the canary, write new salt/KDF parameters atomically.
3. Re-derive trace keys; verify the trace log.
4. Optional `Verify: true` runs `VerifyAll` before adopting.

### Acceptance tests

- `TestRotatePasswordChange`: old password fails `ErrInvalidPassword`, new one
  opens every store.
- `TestRotateVerifyAllPasses`.
- `TestRotateCancelLeavesOldUsable`.
- `TestRotateRefusesInTransaction` + `TestRotateTraceVerifies`.

---

# Part B — Retrieval

## GAP-10 — Hybrid retrieval end-to-end

**Priority P0 · Effort L · Depends on GAP-01 (scoped filters), GAP-12 (model-safe scores)**

### Goal

One `HybridSearch` call fusing lexical BM25 + dense vector, with filter
translation, RRF/weighted fusion, optional rerank, pagination and explanations.

### API

```go
type HybridRequest struct {
    Text string; Vector []float32; Filter *vector.Filter; TopK int
    TextIndex, VectorCol string
    Fusion ranking.Fusion; Rerank RerankOptions; Cursor *Cursor
}
type HybridHit struct { ID string; Score, Text, Dense float64; Rank int; Why []string }
func (s *Store) HybridSearch(ctx context.Context, req HybridRequest) ([]HybridHit, Cursor, error)
```

### Files

root (new `hybrid.go`), `ranking` (weighted fusion + cursor), `store/search`,
`store/vector`. **No format change.**

### Steps

1. Add weighted fusion + cursor to `ranking`.
2. Add a `ranking.Result`-producing shim so both arms share one namespace.
3. Translate the `vector.Filter` once and apply to both arms.
4. Fuse/rerank in memory over the union bounded by `TopK * oversample`.
5. Build `Why` from arm + rerank signals.

### Acceptance tests

- `TestHybridSingleArm`: text-only matches `search.Search` order; vector-only
  matches `vector.Search` order.
- `TestHybridFusionDeterministic` (ties break by ID).
- `TestHybridFilterAppliesToBothArms`.
- `TestHybridCursorResumes`.
- `TestHybridWhy`.

---

## GAP-11 — Chunking and provenance

**Priority P1 · Effort M · Depends on GAP-10**

### Goal

A pure `store/rag` (or `store/chunk`) package: deterministic, tokenizer-aware
text splitting with offsets, plus a provenance convention. **No format change.**

### API

```go
package rag
type Options struct { MaxRunes, Overlap int; Split SplitFunc; Tokenizer search.Tokenizer }
type Chunk struct { Index int; Text string; Start, End int; Digest [32]byte }
func Split(text string, opts Options) []Chunk

type Provenance struct { DocumentID string; Index, Start, End int }
func WithProvenance(p Provenance) vector.Payload
```

### Files

new `store/rag`, `store/vector` payload helpers, `search` tokenizer reuse.

### Steps

1. Implement `Split` (sentence/paragraph/none split funcs) with overlap + byte
   offsets + digest.
2. Add `WithProvenance` payload helper.
3. Make windows (`index±1` for a `DocumentID`) expressible as one vector filter.

### Acceptance tests

- `TestChunkReconstructsOriginal` (overlap-aware concat).
- `TestChunkStable`.
- `TestChunkOffsetsAddressParent`.
- `TestChunkEmptyAndShort`.
- `TestChunkCJKSafe`.

---

## GAP-12 — Embedding provenance and re-embedding

**Priority P1 · Effort L · Depends on none**

### Goal

Model metadata on a collection, `StalePoints`, and a resumable `Reembed`.

### API

```go
type EmbeddingModel struct { ID string; Dimensions int; Metric Distance; Normalized bool; Revision string }
// CollectionMeta gains: Model EmbeddingModel `json:"model,omitempty"`
func (c *Collection) SetModel(m EmbeddingModel) error
func (c *Collection) StalePoints(model string, limit int) ([]string, error)
func (c *Collection) Reembed(ctx context.Context, fn func(id string, old []float32)([]float32,error), opts ReembedOptions) (*ReembedReport, error)
```

### Storage & format

- Additive `model` JSON field in `CollectionMeta`; point codec unchanged unless a
  per-point override is added (prefer collection stamp + optional override).
  Collection `Search` refuses mixed models unless `AllowMixedModels`.

### Files

`store/vector` (CollectionMeta, Collection, Store, codec), root export/import.

### Steps

1. Add `EmbeddingModel` + `Model` field; validate on open (`AllowMixedModels`
   gate).
2. `SetModel`, `StalePoints` (iterate in ID order).
3. `Reembed` with batched writes + progress checkpoint for resume.
4. Export/import the model metadata.

### Acceptance tests

- `TestModelMismatchFailsClosed` (unless allowed).
- `TestStalePointsExact`.
- `TestReembedResumable`.
- `TestDimensionChangeRejected`.
- `TestModelExportImport`.

---

## GAP-13 — Search query language

**Priority P1 · Effort XL · Depends on GAP-14 (analyzers)**

### Goal

Data-driven query: AND/required, phrases (with positions), prefix, fuzzy,
boosts, snippets, stable cursor.

### API

```go
type Query struct { Text string; Terms []Term; Phrases [][]string; Prefixes []string; Fuzzy []Fuzzy; Boost map[string]float64; Require bool }
type Hit struct { ID string; Score float64; Snippets []Snippet }
func (idx *Index) SearchQuery(ctx context.Context, q Query, opts SearchOptions) ([]Hit, Cursor, error)
```

### Storage & format

- **`search.FormatVersion` 2**: doc records gain term positions; term records may
  gain a prefix index. There is no v1→v2 in-place reader; the compatibility path
  is a rebuild from `Docs` (which exists). Opening a v1 index fails closed (or
  rebuilds, per the chosen path — **decide and test**). Export/import carries
  positions.

### Files

`store/search` (doc record positions, term index, scoring, `DocEntry`, export/
import), `ranking`/`store/vector` (cursor reuse).

### Steps

1. Bump `search.FormatVersion` to 2; document + implement the rebuild path.
2. Add positions to `docRecord` and update `Upsert`/`Delete`.
3. Implement `SearchQuery`: AND/OR, phrase (adjacency over positions), prefix
   (bbolt term-prefix scan), fuzzy (bounded edit-distance / deletion index),
   boosts, snippets (byte ranges), cursor.
4. Reject unsupported syntax with a typed error (no silent OR fallback).

### Acceptance tests

- `TestSearchRequire` (AND vs OR).
- `TestSearchPhrase` (adjacent positions only).
- `TestSearchPrefixBounded`.
- `TestSearchSnippetsAddressOriginal`.
- `TestSearchCursorStable` (ties into GAP-18).
- `TestSearchUnsupportedSyntaxErrors` + `TestSearchV1OpensOrRebuilds`.

---

## GAP-14 — Analyzer chain

**Priority P1 · Effort M · Depends on none (GAP-13 for phrases)**

### Goal

Composable, serializable analyzer chains (tokenizer → filters): stopwords,
stemmer, synonyms, ngram.

### API

```go
type Analyzer struct { Tokenizer Tokenizer; Filters []TokenFilter }
type TokenFilter interface{ Apply([]string) []string }
func NewAnalyzer(t Tokenizer, f ...TokenFilter) Analyzer
func Stopwords(lang string) TokenFilter
func Stemmer(lang string) TokenFilter
func Synonyms(m map[string][]string) TokenFilter
// search.Config gains: Analyzer Analyzer  // zero = Words
```

### Storage & format

- Index metadata gains analyzer name/version; **additive** (default `Words`
  remains implicit so v1 indexes stay readable). A changed analyzer fails
  `ErrInvalidConfig` and exposes `Reindex` (rebuild from `Docs`).

### Files

`store/search` (tokenizer.go, config, meta, export/import).

### Steps

1. Add `TokenFilter` + built-ins (lowercase/stopwords/stemmer/synonyms/ngram).
2. Compose into `Analyzer`; run the chain at index and query time as configured.
3. Persist canonical analyzer name/version; validate on open; add `Reindex`.

### Acceptance tests

- `TestStemmerCollapsesForms` ("running"/"runs"/"ran").
- `TestStopwordsAbsent`.
- `TestSynonymsExpand`.
- `TestAnalyzerChangeFailsClosedAndReindex`.
- `TestDefaultWordsByteIdentical`.

---

## GAP-15 — Vector index persistence and rebuild

**Priority P1 · Effort XL · Depends on none**

### Goal

Persist HNSW/Flat snapshots with a digest, load-fast on open, and an explicit
`RebuildIndex`/`CompactIndex`.

### API

```go
type IndexOptions struct { Persist bool; RebuildAfter int }
type IndexStats struct { Points, Layers int; Bytes int64; Stale bool }
func (c *Collection) IndexStats() IndexStats
func (c *Collection) RebuildIndex(ctx context.Context, opts RebuildOptions) error
func (c *Collection) CompactIndex(ctx context.Context) error
```

### Storage & format

- New index-snapshot bucket under the collection root (topology + point-set
  digest). Additive; `CollectionMeta` records `IndexFormatVersion` so a
  mismatched snapshot is ignored (and triggers rebuild), never misread.

### Files

`store/vector` (collection.go, store.go, binary.go), `store/vector/hnsw`
(`SavedGraph` wiring).

### Steps

1. Wire `hnsw.SavedGraph` save/load for HNSW; snapshot Flat as a sorted list.
2. Persist snapshot + digest; on open, load when digest matches, else rebuild.
3. Invalidate lazily on write; `RebuildIndex`/`CompactIndex` rewrite crash-safely
   (keep previous snapshot usable until the new one commits).
4. `IndexStats`.

### Acceptance tests

- `TestSnapshotOpenSkipsUpsert` (spy/counter proves no per-point `Upsert`).
- `TestCorruptSnapshotFallsBack`.
- `TestRebuildCrashSafe`.
- `TestIndexStats`.
- `TestFlatPersistenceDecision` (persist Flat or reject with a clear error).

---

## GAP-16 — Vector model flexibility

**Priority P2 · Effort XL · Depends on GAP-12**

### Goal

Named/sparse vectors, per-name weighted search, payload patch, content-hash
idempotent upsert.

### API

```go
type Vector struct { Dense []float32; Sparse map[uint32]float32 }
type PointStruct struct { ID string; Version uint64; Vectors map[string]Vector; Payload Payload }
func (c *Collection) UpdatePayload(id string, patch Payload) error
func (c *Collection) UpsertIfChanged(id, contentHash string, p PointStruct) (bool, error)
```

### Storage & format

- Point codec gains named vectors + sparse payloads; **bump
  `CollectionFormatVersion`** and refuse to reinterpret v1 blobs. Single-vector
  points with only the `"default"` name encode byte-identically to today.

### Files

`store/vector` (models, binary codec, collection, index), `store/vector/doc`.

### Steps

1. Bump `CollectionFormatVersion`; add named/sparse encoding with a legacy path.
2. Single-vector API becomes a thin wrapper over `"default"`.
3. `Search` with a vector name + per-name weights.
4. `UpdatePayload` (payload-only, vector bytes untouched); `UpsertIfChanged`
   (skip when hash matches).

### Acceptance tests

- `TestMultiVectorSearch`.
- `TestUpdatePayloadLeavesVectorBytes`.
- `TestUpsertIfChangedNoOp`.
- `TestNamedVsSparseCollisionRejected` + `TestDefaultVectorByteIdentical`.

---

## GAP-17 — Retrieval explainability and recall events

**Priority P2 · Effort M · Depends on GAP-10, GAP-19**

### Goal

Per-term explanations and an optional durable retrieval-event sink.

### API

```go
type Explanation struct { Terms []TermContribution; Dense, Boost float64; Rank int }
func (idx *Index) SearchExplain(ctx context.Context, q Query, opts SearchOptions) ([]Hit, error)
func (c *Collection) SearchExplain(query []float32, filter *Filter, topK int) ([]ScoredPoint, error)
type RetrievalEvent struct { At time.Time; Query string; Hits []string; Latency time.Duration; Why []string }
func (s *Store) SetRetrievalSink(sink func(RetrievalEvent)) // or publish via GAP-19
```

### Files

`store/search`, `store/vector`, root `hybrid.go`, optional `store/ledger`/GAP-19.
**No format change.**

### Steps

1. Emit per-term contributions (idf/tf) in `SearchExplain`; dense contribution in
   `Collection.SearchExplain`.
2. Add `SetRetrievalSink` (nil = no allocation).

### Acceptance tests

- `TestContributionsSumToScore`.
- `TestSinkDisabledNoAlloc`.
- `TestSinkOnlyOnSuccess`.
- `TestExplainDeterministic`.

---

## GAP-18 — Stable pagination cursors and streaming iterators

**Priority P2 · Effort M · Depends on GAP-13 (search-order stability)**

### Goal

Opaque cursors (last sort key + id) and streaming iterators with bounded memory.

### API

```go
type Cursor struct{ /* opaque, versioned */ }
type Page struct { /* Size, After *Cursor, ... */ }
func (n Node) AllPage(ctx context.Context, to any, page Page) (Cursor, error)
func (idx *Index) SearchPage(ctx context.Context, q Query, page Page) ([]Hit, Cursor, error)
func (c *Collection) ListPointsPage(filter *Filter, page Page) ([]PointStruct, Cursor, error)
type Stream[T any] interface { Next(ctx context.Context) (T, bool, error); Close() error }
```

### Files

`store/record`, `store/search`, `store/vector`, root `HybridSearch`.
**No format change** (cursor is a wire type).

### Steps

1. Define the cursor encoding (sort key + ID) and query-shape fingerprint.
2. `AllPage`/`SearchPage`/`ListPointsPage` resolve `After` and return the next
   cursor; reject a cursor from a different query.
3. Add `Stream[T]` for `record.Query.Iter` and search/vector result streams.

### Acceptance tests

- `TestCursorStableUnderInsertDelete`.
- `TestCursorRejectedOnShapeChange`.
- `TestStreamBoundedMemory` (100k rows, small page).
- `TestStreamCancelClosesTransaction`.

---

# Part C — Events, sessions and jobs

## GAP-19 — Sequenced append-only log and durable subscriptions

**Priority P1 · Effort L · Depends on none**

### Goal

A durable, ordered, replayable append-only `Log` (WAL/outbox/session-log
primitive) with subscriptions.

### API

```go
type Log struct{}
func (s *Store) Log(name string) *Log
type AppendOptions struct { Kind, Key string; At time.Time; MaxSize int }
func (l *Log) Append(ctx context.Context, payload []byte, opts AppendOptions) (uint64, error)
func (l *Log) Read(ctx context.Context, from uint64, limit int) ([]LogEntry, error)
func (l *Log) Range(ctx context.Context, from, to uint64) Iterator[LogEntry]
func (l *Log) Subscribe(ctx context.Context, from uint64, filter LogFilter) (<-chan LogEntry, error)
func (l *Log) Trim(ctx context.Context, before uint64) (int, error)
```

### Storage & format

- New root bucket `LOG` (append to `internal/bucket`). 8-byte big-endian seq
  keys; each frame length-prefixed + CRC + sealed when encryption on. Additive
  and versioned. `Append` inside a `Tx` commits with the caller's data (outbox).

### Files

new `store/log`, root accessor, `internal/bucket` (new root), `Export`/`Import`,
`cluster` (new command kinds).

### Steps

1. Add `LOG` to `internal/bucket`; new `store/log` package with seq allocation,
   sealed frames, `Append`/`Read`/`Range`/`Subscribe`/`Trim`.
2. Root `Log(name)` accessor + key derivation (`tempest/log/v1`).
3. `Subscribe` replays on restart from an acked seq.
4. `Trim` returns typed `ErrTrimmed` for reads below the cut.
5. Export/import and cluster command coverage; walk the log in `VerifyAll`.

### Acceptance tests

- `TestLogOrderedGapFree` (concurrent appends).
- `TestSubscribeReplaysFromAck`.
- `TestAppendRollbackNoEntry`.
- `TestTrimmedReturnsTypedError`.
- `TestLogSealedAndVerifyAll`.

---

## GAP-20 — Background jobs and leases

**Priority P2 · Effort L · Depends on GAP-19 (durable events)**

### Goal

A durable job/lease registry: periodic jobs run exactly once per cluster and
resume after a crash.

### API

```go
type JobSpec struct { Name string; Interval time.Duration; Handler func(ctx context.Context) error }
type Jobs struct{}
func (s *Store) Jobs() *Jobs
func (j *Jobs) Register(spec JobSpec) error
func (j *Jobs) RunDue(ctx context.Context, now time.Time) ([]JobResult, error)
func (j *Jobs) Lease(ctx context.Context, name string, ttl time.Duration) (Lease, error)
```

### Storage & format

- Additive job-state bucket (last run, next due, lease holder + expiry),
  versioned. State can ride on the GAP-19 log. Lease is a replicated write in a
  cluster.

### Files

new `store/jobs`, root accessor, `backup.Scheduler` (migrate onto it), `cluster`.

### Steps

1. Add `store/jobs` with persisted schedule + lease state.
2. `Register`/`RunDue` (claim only unleased jobs; expiry releases crashed
   holders)/`Lease`.
3. Migrate `backup.Scheduler` to use it.

### Acceptance tests

- `TestRunDueExactlyOnce` (two concurrent processes).
- `TestHandlerFailureReleasesLease`.
- `TestMissedIntervalRunsOnce`.
- `TestJobStateSurvivesReopen`.
- `TestSchedulerStopsOnCancel`.

---

# Part D — Knowledge graph and series

## GAP-21 — Graph property indexes

**Priority P1 · Effort L · Depends on none**

### Goal

Index-backed property lookups and numeric ranges over graph nodes/edges.

### API

```go
type PropertyIndex struct { Key, Kind, Type string; Numeric bool }
func (g *Graph) CreatePropertyIndex(ctx context.Context, idx PropertyIndex) error
func (g *Graph) DropPropertyIndex(ctx context.Context, key, kind string) error
func (g *Graph) FindNodesByProperty(ctx context.Context, key string, value any, opts FindOptions) ([]*Node, error)
func (g *Graph) RangeNodesByProperty(ctx context.Context, key string, min, max any, opts FindOptions) ([]*Node, error)
```

### Storage & format

- Index buckets under the graph root (encoded value → id). **`graphs.FormatVersion`
  bump** with a rebuild-on-open fallback (the index existence changes the read
  path). Encrypted graphs store a blind index or ciphertext (decide + document).

### Files

`store/graphs` (new index buckets, write path, query path), export/import
(or rebuild on import).

### Steps

1. Bump `graphs.FormatVersion`; add index bucket encoding (numeric +
  order-preserving).
2. Maintain indexes in the same transaction as node/edge writes.
3. `FindNodesByProperty`/`RangeNodesByProperty` prefer the index, fall back to
   scan only when none exists.
4. Backfill on create, drop on `DropPropertyIndex`.

### Acceptance tests

- `TestFindByPropertyMatchesScan` + spy proves scan not used.
- `TestIndexBackfillAndDrop`.
- `TestIndexMaintainedUnderMutation` (100 random ops, no stale entries).
- `TestNumericRangeOrdering`.
- `TestIndexOnEncryptedGraph`.

---

## GAP-22 — Graph schema and constraints

**Priority P2 · Effort L · Depends on GAP-21**

### Goal

Opt-in per-graph schema: required fields, allowed endpoints, cardinality,
uniqueness.

### API

```go
type Schema struct { NodeTypes map[string]NodeTypeSpec; EdgeTypes map[string]EdgeTypeSpec }
type NodeTypeSpec struct { Required []string; Properties map[string]PropertySpec; Unique []string }
type EdgeTypeSpec struct { From, To []string; Required []string; MaxPerPair int }
func (g *Graph) SetSchema(ctx context.Context, s Schema) error
func (g *Graph) Schema(ctx context.Context) (*Schema, error)
func (g *Graph) Validate(ctx context.Context, dryRun bool) (*ValidationReport, error)
```

### Storage & format

- Reserved schema record under the graph root; additive. Included in export/
  import.

### Files

`store/graphs` (schema record, write validation).

### Steps

1. Add `Schema` types + `SetSchema`/`Schema`.
2. Validate on `UpsertNode`/`UpsertEdge` with a path-addressed error.
3. `Validate(dryRun)` reports existing violations without mutating.

### Acceptance tests

- `TestSchemaRequiredField`.
- `TestSchemaEndpointType`.
- `TestSchemaValidateDryRun`.
- `TestSchemaDisabledPermissive`.
- `TestSchemaSurvivesReopenAndExport`.

---

## GAP-23 — Graph analytics and declarative queries

**Priority P2 · Effort XL · Depends on GAP-21**

### Goal

Pure graph algorithms (PPR, communities, centrality, weighted path) over a
read-only snapshot. **No format change.**

### API

```go
// package graphs/analytics
func PersonalizedPageRank(ctx context.Context, g Reader, seeds []string, opts PPROptions) (*PPRResult, error)
func Communities(ctx context.Context, g Reader, opts CommunityOptions) (*CommunityResult, error)
func Centrality(ctx context.Context, g Reader, opts CentralityOptions) (map[string]float64, error)
func WeightedShortestPath(ctx context.Context, g Reader, from, to, weight string) (*Path, error)
```

### Files

new `store/graphs/analytics`, docs, benchmarks.

### Steps

1. Implement each algorithm over the existing `Reader`/snapshot (no write
   transaction held).
2. Add context checkpointing for cancellation.
3. Benchmarks with a concurrent-writer budget.

### Acceptance tests

- `TestPPRSumsToOneAndSymmetric`.
- `TestCommunitiesDeterministicWithModularity`.
- `TestWeightedPathRespectsWeights` (rejects negative weights).
- `TestAnalyticsDoesNotBlockWrites`.
- `TestCancellationCheckpoint`.

---

## GAP-24 — Series rollups and label queries

**Priority P2 · Effort L · Depends on none**

### Goal

Materialized rollups (continuous aggregates) and label-matcher queries.

### API

```go
type RollupSpec struct { Source, Target string; Step int64; Func AggregateFunc; Retain Retention; OnInsert bool }
func (s *Store) CreateRollup(ctx context.Context, spec RollupSpec) error
func (s *Store) DropRollup(ctx context.Context, target string) error
func (s *Store) Rollups(ctx context.Context) ([]RollupSpec, error)
func (s *Store) SeriesMatching(ctx context.Context, matchers ...LabelMatcher) ([]SeriesInfo, error)
```

### Storage & format

- Rollup specs + a label index over `__ts_meta__` (key/value → series); additive
  metadata, versioned. A rollup target uses the same point encoding.

### Files

`store/series` (meta, write path, label index), `internal/bucket`.

### Steps

1. Add label index; `SeriesMatching` over it (not a scan).
2. `CreateRollup`/`DropRollup`/`Rollups`; `OnInsert` updates the current window
   in the same transaction; a GAP-20 job closes past windows.

### Acceptance tests

- `TestRollupOnePointPerWindow`.
- `TestLatePointsHandled` (reject or recompute — pin the choice).
- `TestSeriesMatchingMatchesScan`.
- `TestDropRollupOwnedTarget`.
- `TestOriginalSeriesUnaffected`.

---

# Part E — Media and files

## GAP-25 — Object streaming, range reads and metadata

**Priority P1 · Effort L · Depends on none**

### Goal

Streaming `PutStream`, `GetRange`, and typed object metadata (content type,
user metadata, created-at).

### API

```go
type ObjectMeta struct { ContentType string; Metadata map[string]string; CreatedAt time.Time }
func (s *Store) PutStream(ctx context.Context, bucket, key string, r io.Reader, size int64, meta ObjectMeta) (Info, error)
func (s *Store) GetRange(ctx context.Context, bucket, key string, off, n int64) (io.ReadCloser, error)
func (s *Store) SetMetadata(ctx context.Context, bucket, key string, meta ObjectMeta) error
```

### Storage & format

- Additive record fields (`content_type`, `metadata`, `created_at`) and a
  chunked-layout flag; version the object mode byte. `PutStream` chunks the
  payload (reusing the fstree chunk format or dedup blob path).

### Files

`store/object` (write/read path, record, Info, List, migration), root export/
import.

### Steps

1. Add `ObjectMeta` + record fields; version the mode byte.
2. `PutStream` (chunked, size/hash verified before commit; a failed stream
   leaves the previous object unchanged).
3. `GetRange`/`SetMetadata`; `List` gains prefix + metadata filter.

### Acceptance tests

- `TestPutStreamBoundedMemory` (100 MB, memory guard).
- `TestGetRangeExact` (out-of-range → EOF semantics).
- `TestMetadataRoundTripAndExport`.
- `TestFailedStreamLeavesPrevious`.
- `TestDedupChunkSharing`.

---

## GAP-26 — Durable fstree snapshots and file history

**Priority P2 · Effort XL · Depends on GAP-25 (chunk reuse)**

### Goal

Durable point-in-time snapshots that survive restart, incremental snapshot
support, and per-file version history.

### API

```go
type SnapshotOptions struct { Name string; TTL time.Duration; From string }
func (s *Store) CreateSnapshot(ctx context.Context, fsName string, opts SnapshotOptions) (SnapshotInfo, error)
func (s *Store) Snapshots(ctx context.Context, fsName string) ([]SnapshotInfo, error)
func (s *Store) RestoreSnapshot(ctx context.Context, fsName, snapID string, opts RestoreOptions) error
func (fs *FS) FileHistory(ctx context.Context, path string, limit int) ([]FileVersion, error)
```

### Storage & format

- New `snapshots/` child bucket (manifests + content hashes) reusing dedup
  chunks; **`fstree.FormatVersion` bump** (a v1 filesystem opens read-only
  without snapshots, or is rebuilt).

### Files

`store/fstree` (snapshot.go, store.go, inode/content paths), dedup refcounts,
root export/import.

### Steps

1. Bump `fstree.FormatVersion`; add the `snapshots/` bucket + manifest encoding.
2. `CreateSnapshot` (incremental via `From`), `Snapshots`, `RestoreSnapshot`.
3. `FileHistory` (content hash + metadata blob per version).
4. TTL expiry via a GAP-20 job (removes manifest + unreferenced chunks only).

### Acceptance tests

- `TestSnapshotSurvivesReopenAndRestores`.
- `TestIncrementalStoresNoDuplicateChunks`.
- `TestFileHistoryOrder`.
- `TestRestoreDoesNotDisturbOtherFS`.
- `TestSnapshotTTLReclaimsOnlyUnreferenced`.

---

# Part F — Operations and portability

## GAP-27 — Store stats, usage and quotas

**Priority P2 · Effort M · Depends on none**

### Goal

Per-store usage accounting and opt-in quotas. **No format change** (counters are
derived or cached).

### API

```go
type StoreStats struct { Path string; Bytes, FreeBytes int64; Stores map[string]StoreUsage }
type StoreUsage struct { Buckets int; Keys, Bytes int64 }
func (s *Store) Stats(ctx context.Context) (*StoreStats, error)
type Quota struct { MaxBytes map[string]int64 }
```

### Files

root (`stats.go`), `internal/bucket`, every store's write path (quota check).

### Steps

1. `Stats`: walk the bbolt page tree per root bucket (document/benchmark cost)
   or read cached counters — pick one, document it.
2. Quota check on the write path before commit → typed `ErrQuotaExceeded`.

### Acceptance tests

- `TestStatsMatchesFullWalk`.
- `TestQuotaExceededFailsCleanly`.
- `TestStatsConcurrentSafe`.
- `TestStatsIncludesLogAndLedger`.
- `TestNoQuotaIdentical`.

---

## GAP-28 — Export/Import completeness and selectivity

**Priority P2 · Effort M · Depends on GAP-19 (the log)**

### Goal

Selective, resumable export including the trace log, with a manifest.

### API

```go
type ExportOptions struct {
    RecordTypes []any
    Stores      []string
    Selector    func(kind, id string) bool
    IncludeLog, IncludeTrace, Resume bool
}
```

### Storage & format

- **Export container version bump** (not the store). Add a head manifest with a
  stable export id, per-section record counts and checksums, so a resume skips
  completed sections. Ledger export when `IncludeTrace` (events are portable;
  signatures verify with derived public keys).

### Files

`export.go`, `import.go`, `store/ledger` (export/verify).

### Steps

1. Bump the container version; add the manifest + section framing.
2. `Stores`/`Selector`/`IncludeLog`/`IncludeTrace`/`Resume` in `Export`.
3. Import fails closed on an unknown section; ledger import + verify.

### Acceptance tests

- `TestSelectiveExportReproduces`.
- `TestIncludeTraceRoundTripsVerifiable` (tip hash matches).
- `TestResumeSkipsCompleted`.
- `TestDryRunValidatesWholeContainer`.
- `TestCrossModeExportImport`.

---

## GAP-29 — Trace log query, retention and export

**Priority P2 · Effort M · Depends on GAP-28**

### Goal

Queryable, prunable, exportable trace log with retention that preserves
verifiability.

### API

```go
type EventFilter struct { Store, Op, Entity string; Since, Until time.Time }
func QueryEvents(ctx context.Context, b *bolt.DB, f EventFilter, limit int) ([]Event, error)
func (r *Recorder) Prune(ctx context.Context, before time.Time) (int, error)
func (r *Recorder) Export(ctx context.Context, w io.Writer, from uint64) error
type Retention struct { MaxAge time.Duration; MaxCount uint64 }
```

### Storage & format

- A retained anchor/checkpoint record; additive. Pruning keeps the cut event's
  hash as a new anchor (verify-from-anchor) or re-signs a checkpoint — **decide
  and document**; silently breaking `Verify` is unacceptable.

### Files

`store/ledger`, root `db.go`, export/import.

### Steps

1. `QueryEvents` (index or scan by store/op/entity/time; document the access
   path).
2. `Prune` with anchor preservation + `Verify` against the anchor.
3. `Export` a portable event stream; `Retention` (MaxAge/MaxCount) enforced
   without removing the tip/anchor.

### Acceptance tests

- `TestQueryEventsSequenceOrder`.
- `TestPruneThenVerify`.
- `TestExportImportsAndVerifies`.
- `TestRetentionKeepsTipAndAnchor`.
- `TestEventTimestampsAbsolute`.

---

## GAP-30 — Cluster read parity and read-index

**Priority P2 · Effort L · Depends on GAP-19/GAP-04 (new commands)**

### Goal

Follower read guarantees (`ReadIndex`/leader) and a read-only query facade;
deterministic replicated trace.

### API

```go
type ReadConsistency int // ReadLocal | ReadIndex | ReadLeader
func (n *Node) ReadBarrier(ctx context.Context, c ReadConsistency) error
func (n *Node) Query() *Query // read-only facades over replicated views
```

### Storage & format

- New command kinds; no store-format change beyond the log/ledger. **Fix the
  ledger divergence**: pin `At` at the leader for every command that uses
  `time.Now` (series already does with `Now`), and make the ledger entry's `At`
  part of the command so trace logs are identical on every replica.

### Files

`cluster` (config, fsm, node, facades), `store/ledger`.

### Steps

1. Add `ReadIndex`/`ReadLeader` to the FSM/node; `ReadBarrier` waits for the
   committed index; a partition cannot serve `ReadIndex` indefinitely (typed
   error).
2. Add a read-only `Query` facade mirroring the write facades.
3. Pin timestamps at the leader; carry the ledger `At` in the command.
4. Replicate new operations (TTL sweep, log append, job lease) or mark them
   leader-only explicitly.

### Acceptance tests

- `TestReadIndexSeesLeaderWrite`.
- `TestReadLocalDefaultStale`.
- `TestTraceHeadsIdentical`.
- `TestNewOpsReplicatedOrLeaderOnly`.
- `TestPartitionReadIndexErrors`.

---

## GAP-31 — Independent-key backups and point-in-time recovery

**Priority P2 · Effort L · Depends on GAP-09**

### Goal

Backups re-sealed under a separate key, and PITR over an incremental snapshot
chain.

### API

```go
type BackupOptions struct { Manifest, Overwrite, Incremental bool; EncryptKey []byte }
func (s *Store) RestoreAt(ctx context.Context, path, password string, at time.Time) (BackupInfo, error)
```

### Storage & format

- A backup header recording the key id + snapshot chain; **version the backup
  container**. `EncryptKey` decrypts with the live key and re-seals with the
  backup key (streaming re-encrypt, not a logical export).

### Files

root `backup.go`, `backup` package, `store/system` (backup key).

### Steps

1. Add `EncryptKey` (streaming re-encrypt) + backup header/chain.
2. `RestoreAt`: replay the incremental chain to the requested instant when the
   store format supports replay; else select the newest snapshot ≤ `at` and say
   so.

### Acceptance tests

- `TestBackupEncryptKey`: opens with backup key, not live password.
- `TestRestoreAtGranularity`.
- `TestIncrementalStoresOnlyChanges`.
- `TestCorruptSnapshotFailsVerification`.
- `TestManifestFlagsSurviveReseal`.

---

## GAP-32 — Record schema validation and typed migrations

**Priority P2 · Effort L · Depends on none**

### Goal

Per-type schema (required, enums, defaults) and typed migration steps.

### API

```go
type TypeSchema struct { Type any; Version uint64; Required []string; Enums map[string][]string; Defaults map[string]any; Steps []TypeStep }
func (c Config) WithSchema(s TypeSchema) Config
func (n Node) Validate(ctx context.Context, data any) error
```

### Storage & format

- Type version in the bucket `__metadata` (or a reserved schema record);
  additive. A version bump is visible in the store's schema bucket.

### Files

`store/record` (config, encode path, migrate), root `migrate.go`, docs/examples.

### Steps

1. `WithSchema`/`Validate`; validation before encode (typed errors, no partial
   write; defaults only when absent, not zero-valued).
2. Per-type `Steps` applied in `Store.Migrate`, idempotent, version bumped
   atomically.

### Acceptance tests

- `TestRequiredFieldFails`.
- `TestEnumViolation`.
- `TestDefaultOnlyWhenAbsent`.
- `TestTypeMigrationIdempotentRollback`.
- `TestNoSchemaIdentical`.

---

## GAP-33 — Retrieval quality evaluation harness

**Priority P3 · Effort M · Depends on none**

### Goal

A test/benchmark package (never in the hot path) with fixtures + metrics and a
CI quality gate.

### API

```go
// package evals
func RecallAtK(exact, approx []string, k int) float64
func NDCG(grades map[string]int, ranked []string, k int) float64
func MRR(ranked []string, relevant map[string]bool) float64
func SyntheticCorpus(n, dim int, seed int64) (vectors, queries [][]float32)
```

### Files

new `internal/evals` (or `store/vector/quality`), CI workflow, benchmarks.

### Steps

1. Implement metrics + seed-stable fixtures.
2. CI small fixture (fast, deterministic); `-short=false` compares HNSW recall
   vs Flat for several `ef`/`M` settings with a declared floor.

### Acceptance tests

- `TestExactAndApproxMatch` (Flat trivial; HNSW floor).
- `TestMetricsKnownAnswers`.
- `TestFixturesSeedStable`.
- `TestCIBounded` (`-short` skips the large corpus).
- `TestRegressionCaught` (a deliberate `efSearch` regression trips the floor).

---

## 4. Consolidated format/version decisions

| Gap | On-disk impact | Decision |
|---|---|---|
| GAP-01 | scope stamps + index | `record` capability bit; fail closed |
| GAP-02 | key-id prefix on sealed values | opt-in mode byte; unknown key id fails closed |
| GAP-03 | none | link registry metadata bucket |
| GAP-04 | expiry index + field | capability bit; fail closed |
| GAP-05 | history bucket | additive, opt-in |
| GAP-06 | tombstone index | additive |
| GAP-07 | revision counter | additive metadata |
| GAP-08 | field-seal marker | additive, versioned |
| GAP-09 | re-sealed metadata + marker | no layout change |
| GAP-10 | none | — |
| GAP-11 | none | — |
| GAP-12 | collection meta `model` | additive JSON |
| GAP-13 | positions + prefix index | `search.FormatVersion` 2 + rebuild |
| GAP-14 | analyzer metadata | additive (default implicit) |
| GAP-15 | index snapshot bucket | additive, digest-validated |
| GAP-16 | named/sparse vectors | `CollectionFormatVersion` bump |
| GAP-17 | none | — |
| GAP-18 | none | — |
| GAP-19 | new root bucket `LOG` | additive, versioned |
| GAP-20 | job-state bucket | additive |
| GAP-21 | graph index buckets | `graphs.FormatVersion` bump + rebuild |
| GAP-22 | schema record | additive |
| GAP-23 | none | — |
| GAP-24 | rollup + label index | additive metadata |
| GAP-25 | object record fields + chunk flag | version object mode byte |
| GAP-26 | snapshot bucket | `fstree.FormatVersion` bump |
| GAP-27 | none | — |
| GAP-28 | export container version | bump container, not store |
| GAP-29 | anchor/checkpoint | additive; verify-from-anchor |
| GAP-30 | new command kinds | no store-format change |
| GAP-31 | backup header/chain | version backup container |
| GAP-32 | type schema version | additive |
| GAP-33 | none | — |

## 5. Suggested landing order (cherry-pick batches)

Land in this order; each batch is independently shippable and green on its own.

1. **Privacy & lifecycle (highest urgency):** GAP-01 → GAP-03 → GAP-04 → GAP-06 →
   GAP-07.
2. **Recall quality:** GAP-10 → GAP-12 → GAP-11 → GAP-13 → GAP-14 → GAP-15.
3. **Durability & reactivity:** GAP-19 → GAP-05 → GAP-02 → GAP-20.
4. **Knowledge & media:** GAP-21 → GAP-24 → GAP-25 → GAP-26.
5. **Operations:** GAP-27 → GAP-28 → GAP-29 → GAP-30 → GAP-31 → GAP-18 →
   GAP-17.
6. **Quality gates:** GAP-32 → GAP-33 → GAP-08 → GAP-09 → GAP-16 → GAP-22 →
   GAP-23.

Dependency notes: GAP-02 requires GAP-01 (scope model first); GAP-11/GAP-17
require GAP-10; GAP-13 requires GAP-14; GAP-16 requires GAP-12; GAP-20 requires
GAP-19; GAP-22 requires GAP-21; GAP-25 precedes GAP-26; GAP-29 requires GAP-28;
GAP-31 requires GAP-09; GAP-08 requires GAP-02.

## 6. Definition of done (per gap)

- Guardrails G1–G4 hold; the gap has a non-companion "generality" test.
- New persisted structures are walked by the store's `RotateAll`/`VerifyAll`
  and covered by `Export`/`Import` (or explicitly excluded, documented).
- Focused regression tests named in the gap section pass, including the
  fail-closed test where the gap changes read semantics.
- `gofmt`, `go vet`, `go build` clean; `go test -race` for any concurrency
  change (GAP-01, GAP-07, GAP-18, GAP-19, GAP-20, GAP-30, GAP-15, GAP-23).
- Documentation: update `README.md`, `AGENTS.md` and the affected package
  guide(s) in the same commit, and verify relative links + compile-checked
  examples.

## 7. Final verification (whole tree)

```sh
test -z "$(gofmt -l .)"
go vet ./...
go build ./...
CGO_ENABLED=0 go build ./...
go test -count=1 -timeout 300s ./...
CGO_ENABLED=0 go test -count=1 -timeout 300s ./...
go test -race -count=1 -timeout 600s ./...
CGO_ENABLED=0 GOARCH=386 go vet ./...
CGO_ENABLED=0 GOARCH=386 go test -count=1 -timeout 600s ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...
go mod tidy -diff
```

## 8. What must not be implemented (Appendix B, authoritative)

Salience/decay curves/tiers, importance/emotional-weight formulas, extraction
prompts and review workflow, conflict heuristics/resolution, consolidation and
knowledge promotion, prompt assembly/token budgeting, emotion derivation,
identity-drift scoring, embedding-provider calls, product job scheduling, and
HTTP/UI/MCP all stay in the caller. Any item that only makes sense for a
companion fails the generality test and is out of scope.
