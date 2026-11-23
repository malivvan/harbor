> **Post-backlog restructure.** The store packages were renamed and moved:
> `db` → `store/record`, `ts` → `store/series`, `fs` → `store/file`, `vec` →
> `store/vector`, `fts` → `store/search`. `harbor.Store` now exposes
> `Record/File/Series/Vector/Search`, each taking optional bucket-path
> segments; no-argument accessors keep the original on-disk roots. The
> compatibility notes below use the names from before that restructure where
> they describe historical behaviour.

# Post-implementation notes

The backlog in `PLAN.md` is implemented and the local release gate is green.
This file records what is left, what was decided by judgement, and what should
be watched before the first public tag.

## Verification performed

Run on the final tree:

- `test -z "$(gofmt -l .)"` — clean.
- `go vet ./...`, `go build ./...` — clean.
- `go test ./...` (timeout 300s) — all packages pass.
- `go test -race ./...` — all packages pass.
- `CGO_ENABLED=0 go test ./...` — all packages pass.
- `go mod tidy -diff` — empty.
- `staticcheck@v0.8.2 ./...` — clean.
- `govulncheck@v1.10.0 ./...` — no vulnerabilities.
- Relative README/AGENTS/package-guide links resolve; new snippets follow the
  public API (no compile-checked example harness was added for the new root
  packages beyond the tests).

CI has not run on this working tree. Tag only from a commit whose CI is green
(the checklist item that remains unchecked in `PLAN.md` §20).

## Deferred or judgement-call work

1. **Rare consensus test flake, not reproduced.** `TestReplicateFSMetadata`
   failed twice early in the work (once under the whole-repository race run,
   once with `-count=2`) with a follower that had not applied a replicated FS
   operation. It did not reproduce in 20 targeted stress runs, six whole-package
   race runs, or subsequent whole-repository runs. The FSM has no apply-error
   telemetry by default (it logs to a discarded writer), so if it recurs, first
   set `consensus.Config.LogOutput` and log FSM apply errors. Do not treat the
   green gate as proof that this is fixed; it is an open, low-frequency
   observation.
2. **Append-only history (PLAN item 12).** Only the design note in `PLAN.md`
   Appendix A ships. No API until a consumer validates key layout, logical time,
   retention and as-of semantics.
3. **Ordered/prefix queries under encryption (PLAN item 15).** Documented and
   decided: keep failing closed today; the planned path is an open-time
   in-memory ordered index (option 2). No code. OPE needs a security review.
4. **Vector range search is exact for HNSW (PLAN item 4).** An approximate
   HNSW over-fetch was not added because an open radius has no bounded candidate
   set. A future `RangeOptions` can add it without breaking the API.
5. **Cross-store transaction nesting (PLAN item 1).** Write transactions are
   serialized by `Store.txMu`; a nested call blocks instead of returning
   `ErrTxAlreadyOpen`, because Go has no supported goroutine identity. This is
   documented in `transaction.go` and the README. Concurrent writers serialize
   as the plan asked.
6. **FTS has no audit flag (PLAN item 2).** The other stores can be audited via
   `Options.Audit`; FTS writes are not in the trail. Add `AuditFTS` (and a
   matching key/flag) only if the audit model should cover search indexes.
7. **FTS search cost.** A query loads each candidate document record to read
   its term frequencies; postings store only document IDs. This is simple and
   correct, but a term-heavy index can be optimized by storing `(id, tf)` in
   the posting list. Benchmark before changing the format.
8. **Reference cascades scan the source bucket.** Cascaded deletes enumerate
   referencing records and decode them, then remove the primary key and generic
   index entries. That avoids re-encoding index values without the struct
   schema, but it is O(records) per cascade. A persisted per-bucket schema could
   use the index instead.
9. **`pkg/fusion` has no consumer yet.** It was added because FTS and vector
   results now coexist and RRF is the documented way to merge them. If the
   release should not carry an unused package, remove it; nothing imports it.
10. **`VerifyBackup` structure, not content.** It validates the canary, every
   sub-store marker/version, each filesystem, vector collection metadata and
   each FTS index, plus the sidecar checksum. It does not re-hash every record
   value; `VerifyAll` on the sub-stores remains the content-level check after
   opening the restored copy.
11. **`PLAN.md` is retained** as the single backlog/record at the user's
   request, even though the checklist line originally said planning documents
   should be removed. `HARBOR.md` remains absent.
12. **No tag was created.** The plan asks for a tag from a green CI commit; that
    is a release action, not an implementation step.

## Compatibility notes

- Existing on-disk formats are unchanged (version 1, same markers). The root
  store gained a `fts/` bucket, the `harbor/fts/v1` key label, and a
  `harbor.schema` bucket used only by `Migrate`; older builds ignore the new
  buckets, and a new build opens old stores with the same derived keys.
- `ScoredPoint` gained an optional `Vector` field, and `Node` gained
  `Aggregate`, `ValidateSchema` and `GetMany`. These are additive.
- `file.Store`, `series.Store`, `vector.Store` and `search.Store` gained transaction-bound
  views and read-only constructors; `record` gained `AttachWithAudit`. These are
  additive and do not change managed-transaction behaviour.
- The audit event encoding changed to canonical JSON (PLAN 0.5). The first
  public release has a single audit format; a pre-release trail written by an
  older build would fail verification. No released data exists yet.
- `record.Set` now records `set` (not `set_bytes`) in the audit trail. `file.Copy`
  records a `copy` event. Operations that were previously invisible now appear.

## Suggested next steps before tagging

1. Run the full gate on CI (all platforms in the matrix).
2. Re-run the race suite several times under load and watch for the consensus
   flake; add FSM apply-error logging to the test config if it recurs.
3. Decide whether FTS should be auditable, and whether `pkg/fusion` should ship
   unused.
4. Walk the README end to end against a fresh database, including the new
   transactions, backup/verify, FTS, aggregation, references, projection and
   migration sections.
5. Tag `v0.1.x` from that green commit.

## Post-backlog package restructure

The store packages were renamed and moved under `store/` after the backlog was
completed:

| Old | New |
|---|---|
| `db` (`DB()`) | `store/record` (`Record()`) |
| `fs` (`FS()`) | `store/file` (`File()`) |
| `ts` (`TS()`) | `store/series` (`Series()`) |
| `vec` (`Vec()`) | `store/vector` (`Vector()`) |
| `fts` (`FTS()`) | `store/search` (`Search()`) |

- Subpackages moved with their parents: `record/index`, `record/q`,
  `file/s3fs`, `vector/hnsw`, `vector/hnsw/heap`, `vector/doc`.
- The accessors take variadic bucket-path segments and return a rooted view of
  the already-open store (`store.Record("tenant", "records")`). No-argument
  accessors keep the legacy on-disk roots (`db`, `fs`, `ts`, `vec`, `fts`), so
  existing databases remain readable. Each store gained `WithRoot`; views share
  the handle, encryption state, rotation guard and (for vector) the flush
  registry.
- `Tx` accessors are now `Record/File/Series/Vector/Search`.
- `pkg/store` manager and package-level accessors were renamed and accept the
  same path segments; `Search` was added for parity.
- `pkg/consensus` keeps its own `Node.DB/FS/TS/Vec` facade names (they return
  consensus wrappers, not the stores); only its internal `harbor.Store` calls
  changed. Metric names (`harbor.db.*`, `harbor.fs.*`, …) and audit store
  identifiers (`db`, `fs`, `ts`, `vec`) are unchanged because they are part of
  externally observed formats.
- Documentation, package guides, links and snippets were updated to the new
  paths and names. `PLAN.md` keeps its historical text with a restructure note
  at the top.

Verification for this change: `gofmt`, `go vet`, `go build`, the full test
suite, the race suite, the CGO-free suite, `go mod tidy -diff`, staticcheck and
govulncheck are green. A rooted-view test proves custom roots are isolated from
the defaults, persist across a close/reopen, and that a base vector flush drains
its views.
