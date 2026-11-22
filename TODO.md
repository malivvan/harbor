# TODO — Storm codebase review

This file is the output of a full review of the current working tree. It is a
list of defects and unpolished areas to solve. Line numbers are from the reviewed
revision and may shift; use the named symbols and files to locate the code.

Scope reviewed:

- root package: `storm.go`, `meta.go`, `meta_test.go`, `storm_test.go`
- `db` (including `db/index`, `db/q`)
- `fs` (including `fs/absfs`, `fs/inode`, `fs/s3fs`)
- `ts`
- `vec` core (store, collections, indexes, filters, quantization, persistence,
  rotation, encryption)
- `vec/hnsw`, `vec/vek`, and `internal/*`
- repository health: README, go.mod, generated code, formatting, tests

## Current verification status

- `go build ./...` — passes.
- `go vet ./...` — passes.
- `gofmt -l .` — clean.
- `go test ./...` — **FAILS** in the root package:
  `TestKeyValueStore` (`storm_test.go`) asserts the wrong key order.
- `go test -race ./...` for the affected packages — mostly passes, but it does
  not exercise the race-prone paths listed below.
- The working tree is a large uncommitted refactor (root metadata encoding,
  `[][]byte` roots, sub-store APIs, test helpers). Do not assume the tests
  cover the current code merely because individual package suites are green.

## Priority order

1. Root/package blockers and security gaps: H1, H2, R1–R4.
2. Data-loss bugs: F1–F3, V1–V3, T1–T3, D1–D2.
3. Correctness bugs with silent wrong results: D3–D4, V4–V7, F4–F6, T4–T6.
4. Races/atomicity: T1, T3, F5, V3, V7.
5. API consistency, dead code, docs, and missing tests.

---

# 1. Repository health

All items in this section are resolved:

- **H1** — `TestKeyValueStore` now asserts byte order (`Metadata` before `kdf`).
- **H2** — the refactor is committed; `go build`, `go vet`, `gofmt` and
  `go test ./...` are green.
- **H3** — `go mod tidy` removed the unused `github.com/google/uuid`.
- **H4** — README now states Go 1.27.3, staticcheck is optional (`@latest`), and
  a real CI workflow lives in `.github/workflows/ci.yml`.
- **H5** — no `storm.db` artifact remains; `.gitignore` covers `*.db`.


# 2. Root package (`storm`)

All items in this section are resolved:

- **R1** — the key/value bucket now seals values with a dedicated
  `storm/kv/v1` AES-256-GCM key when the new `EncryptKVValues` flag is set
  (part of `EncryptAll`, the default). Keys stay in the clear so byte-ordered
  listing keeps working, and the package doc says so. New tests assert that the
  stored bytes are not the plaintext, and that `Plaintext` stores them verbatim.
- **R2** — the canary plaintext now embeds the persisted flags
  (`canaryPlaintextFor`), so flipping a flag bit fails the canary with
  `ErrAuthFailed`/`ErrInvalidPassword`. Covered by `TestCanaryBindsEncryptionFlags`.
- **R3** — a missing Metadata bucket is only treated as a new store when the
  database has no buckets at all; otherwise `Open` returns `ErrCorruptStore`.
  Covered by `TestMissingMetadataBucketIsCorrupt`.
- **R4** — `Get("")` now returns `ErrInvalidKey` like the other methods, and
  every KV method checks `ErrClosed` first. Covered by
  `TestEmptyKeyClosedPrecedence`.
- **R5** — `TestDeriveKeys` covers lengths, domain separation, determinism,
  password/salt sensitivity; `TestStoreKeysWipe` covers wiping.
- **R6** — `Options.Password` documents that Go strings cannot be reliably
  wiped and that only the derivation copy is zeroed.
- **R7** — metadata var-block comment, `Metadata()` docstring and the malformed
  fixture/subtest names are corrected.


# 3. `internal/encryption`, `internal/slip10`, `internal/slip21`

All items in this section are resolved:

- **E1** — `siv_test.go` now verifies RFC 5297 appendix A.1 and A.2 byte for
  byte. `NewSIVEngine` accepts the RFC key sizes (32/48/64) so the published
  vectors apply directly; storm still derives 64-byte keys. Empty plaintext and
  empty associated data are covered too.
- **E2** — a short ciphertext now wraps `ErrAuthFailed`, so
  `SIVNameCipher.OpenName` returns the documented sentinel for empty/short
  names (`TestSIVNameCipherRejectsShortNames`).
- **E3** — `NewSIVEngine` copies the key halves and propagates both
  `aes.NewCipher` errors; the k1 block is built once; `pad` cannot panic on a
  full block (`TestSIVKeySizesAndIsolation`, `TestSIVPadNeverPanics`).
- **E4** — the unreferenced `internal/slip10` package was removed.
- **E5** — the dead `internal/machine` package (wrong import comment,
  deprecated `ioutil`) was removed.


# 4. `db` package

All items in this section are resolved:

- **D1** — `generatePrefix` no longer allocates a 2-byte buffer for an empty
  value. `Get`, `All` and `Remove` treat an empty value as "no match"; they
  return `ErrNotFound`/nil instead of panicking.
- **D2** — the reverse `RangeCursor.First` falls back to `Cursor.Last()` when
  `Seek(max)` returns nil, so reverse "everything up to X" works when X sorts
  above every key.
- **D3** — `ListIndex.All`, `Get` and `Remove` skip nested buckets (`id ==
  nil`), so the `storm__ids` sentinel no longer shadows the value `"storm"`.
- **D4** — documented and tested the chosen contract: `Set`/`SetBytes` with a
  nil value store a zero-length value, `KeyExists` reports it, `GetBytes`
  returns empty and the typed `Get` yields the destination's zero value.
- **D5** — removed the unused `(*encState).enabled`, `deleteSink.removed` and
  `testBoltOptions`.
- **D6** — `Config.Root` and the node root comment now say an empty root is the
  database root; `["db"]` is the storm root package's explicit choice.
- **D7** — removed the nonexistent `Copy` operation references.
- **D8** — `node.readTx` checks the context after the callback in both the
  attached-transaction and `bolt.View` paths.

Regression tests: `TestFindEmptyIndexedValue`, `TestReverseRangeAboveMax`,
`TestListIndexValueMatchingSentinel`, `TestKVSetNilReadsBackAsZero`,
`TestReadTxChecksContextAfterCallback`.


# 5. `fs` package

All items in this section are resolved:

- **F1** — `Rename`/`Copy` reject the source itself and any descendant with
  `EINVAL` before mutating anything (`isDescendantPath` plus an inode
  comparison).
- **F2** — content written to an external `contentFS` is sealed and opened
  through the store's encryption state; `EncryptContent` is no longer silently
  ignored.
- **F3** — s3fs `OpenFile` only treats `ErrNotFound` as "new object"; other
  `GetObject`/`ReadAll`/`Close` errors abort the open, so `Close` cannot
  overwrite an existing object with a partial buffer.
- **F4** — the default umask is now `0022` and it is applied to file and
  directory creation (`Create` produces `0755`, not `0777`).
- **F5** — `RemoveAll` propagates parent-save errors instead of dropping them.
- **F6** — `extendedFS.MkdirAll` returns the first real error and rejects a
  path component that is an existing file (`ENOTDIR`).
- **F7** — `fileadapter.Truncate` rejects shrinking (no silent tail), and grows
  by appending instead of overwriting from offset 0.
- **F8** — s3fs `Remove` returns `ENOTEMPTY` for a non-empty directory and
  `Rename` copies/deletes every object under a directory prefix.
- **F9** — `Rename` replaces an existing non-directory target; directory
  replacement returns `EEXIST`/`EISDIR`.
- **F10** — cross-directory renames persist both parents and the replaced
  inode in a single bbolt transaction.
- **F11** — the `fs/inode` package was unreferenced (see F16), so it was
  removed together with its `..` resolution defect.
- **F12** — `Snapshot.Open` no longer rolls back the shared read transaction
  when one file cannot be read; only `Release` ends it.
- **F13** — `fs/README.md` examples use the 3-argument `fs.Open`,
  `Config{Root: ...}`, `filesystem.CreateSnapshot`, `fs.Walk(snapshot, ...)`
  and `filesystem.NewSnapshotManager`.
- **F14** — `fs.Config.Root` documents that an empty root is the database root.
- **F15** — `Remove("/")` returns `EBUSY`.
- **F16** — removed the dead `fs/inode` package and `createRootBucket`.
  `dirWriteFlags` is actually used, and the remaining `absfs` exports are the
  public compatibility surface, so they are kept.
- **F17** — the HTTP client closes the response body when `GetObject` gets an
  unexpected status.
- **F18** — s3fs `ReadDir` sorts its entries by name.
- **F19** — Store roots and file-system names reject `/`, `.` and `..` so the
  literal-bucket and slash-joined views cannot diverge.
- **F20** — regression tests added in `fs/todo_review_test.go`,
  `fs/absfs/todo_review_test.go` and `fs/s3fs/todo_review_test.go`
  (rename/copy into subtree, encrypted external content, umask, root removal,
  replacement rename, cross-directory rename, snapshot read errors, root
  validation, MkdirAll over a file, truncate shrink, s3fs non-empty
  remove/rename, s3fs read errors, sorted ReadDir and body closing).


# 6. `ts` package

All items in this section are resolved:

- **T1** — `Store.enc` is now an `atomic.Pointer[encState]`; every read loads
  it once and rotation swaps it atomically. Covered by
  `TestConcurrentRotationAndAccess` (also run with `-race`).
- **T2** — `ReEncryptSeries` keeps the marker under the current (old) key and
  documents that a partial rotation must be completed with `RotateAll` before
  `Close`. A fresh process with the old key can still open the store and read
  the un-rotated series (`TestReEncryptSeriesPartialRotation`).
- **T3** — `RotateAll` rewrites every series and publishes the new marker in a
  single bbolt transaction, so a cancellation or failure rolls the whole
  rotation back (`TestRotateAllRollsBackOnFailure`).
- **T4** — a zero retention policy returns before scanning or decrypting;
  block-mode pruning also checks the policy before unsealing
  (`TestZeroRetentionDoesNotScan`).
- **T5** — encryption requires a non-empty `Root`; at the database root
  unrelated top-level buckets cannot be distinguished from series
  (`TestEncryptionRequiresRoot`).
- **T6** — `DeleteSeries` branches on `len(root)` and treats a configured but
  missing root as `ErrNotFound` instead of deleting a same-named top-level
  bucket (`TestDeleteSeriesNeverEscapesAMissingRoot`).
- **T7** — empty `Config.Root` segments are filtered out in `Open`, and
  `createSeriesBucket` guards a nil root (`TestEmptyRootSegmentsDoNotPanic`).
- **T8** — `sealValue`/`decodePoint` gate point sealing on `encryptsValues()`,
  so a names-only store keeps 8-byte plaintext values
  (`TestEncryptValuesOffKeepsValuesPlaintext`).
- **T9** — the dead `Store.openValue` was removed; `decodePoint` is the single
  decode path.
- **T10** — `Config.Root` documents the database-root default and why
  encryption needs a root; `Name` documents that it is only a convention used
  by the storm root package.
- **T11** — `ctx` is threaded through `pruneSeriesBucket`, `pruneBucket` and
  `pruneBlocks`, with checks inside the loops.
- **T12** — regression tests added in `ts/todo_review_test.go` (plus the
  updated rotation tests).


# 7. `vec` core

## V1. [High] `Store.Close` drops buffered collection writes
**Location:** `vec/store.go` `Store.Close`, `vec/collection.go`
`Upsert`/`flushUnsafe`.
**Problem:** `Upsert` buffers points until `flushThreshold` (64), but `Store`
does not track collections, so `Store.Close` never flushes them despite its doc.
The root `storm.Store.Close` also never flushes vec buffers. Up to 63 points per
collection are silently lost.
**Fix:** Track created collections and flush in `Store.Close`, or make `Upsert`
persist synchronously, or correct the documented shutdown path.
**Test:** Upsert one point, close the store, reopen, assert it persisted.

## V2. [High] `Collection.Close`/`Store.Close` close the shared bbolt handle
**Location:** `vec/collection.go` `Collection.Close`, `vec/store.go`
`Store.Close`.
**Problem:** `Collection.Close` calls `c.storage.Close()` → `Store.Close` →
`s.db.Close()`. The root package passes one shared bbolt handle to db/fs/ts/vec;
closing one collection makes every sibling collection/sub-store unusable.
**Fix:** Separate “flush” from “own/close the database”. Collections must not
close a handle they do not own; only the top-level owner closes it.
**Test:** Two collections on one store; close one; the other and `ListCollections`
must still work.

## V3. [High] Search returns payload maps shared with the in-memory index
**Location:** `vec/flat_index.go`, `vec/hnsw_index.go` `Search`
(`Payload: point.Payload`).
**Problem:** Callers can mutate the index’s payload via a search result;
concurrent mutation races with filtering. `GetPointsByFilter` returns copies,
`Search` does not.
**Fix:** Return cloned payloads (use `clonePoint`/deep copy) from both indexes.
**Test:** Mutate `r[0].Payload` and assert a subsequent exact filter still
matches.

## V4. [Medium] Reopening an existing collection can silently overwrite its config
**Location:** `vec/collection.go` `NewCollectionWithParams`.
**Problem:** Only vector length is validated; a same-dimension metric,
`UseHNSW` or HNSWParams change passes and `SaveCollectionMeta` overwrites the
stored metadata. An empty existing collection accepts any dimension.
**Fix:** Load existing metadata first and reject any configuration mismatch
before saving.
**Test:** Reopen with a different metric/UseHNSW; expect an error and unchanged
metadata.

## V5. [Medium] HNSW filtered search misses deep matches
**Location:** `vec/hnsw_index.go` (`fetchK = topK*10`, cap) and break at
`topK`.
**Problem:** Post-filtering only examines the first `topK*10` graph candidates;
matching points farther away produce zero/short results, diverging from Flat.
**Fix:** Grow `fetchK` adaptively, use filter-aware graph search, or document the
limitation.
**Test:** Flat vs HNSW filtered search for a rare far match.

## V6. [Medium] `MatchTypeContains` breaks after reload and can panic
**Location:** `vec/models.go` `matchContains` (`[]any` branch uses `==`).
**Problem:** Reloaded JSON arrays contain `int64`/nested maps/slices; comparing
them with `==` fails for numeric cross-types and panics on uncomparable values.
`matchExact` already handles numeric coercion and DeepEqual.
**Fix:** Use the `matchExact` comparison logic in the `[]any` branch.
**Test:** Contains with `[]int` after reload; contains on nested arrays/objects.

## V7. [Medium] Partial `ReEncryptCollection` leaves the store unreadable after restart
**Location:** `vec/rotate.go` `withRotationKey`, marker rewrite, `adoptEnc`.
**Problem:** The marker is re-sealed with the new key while un-rotated
collections stay under the old key; a restart with a single `Config.Secret`
cannot read the old-key collections.
**Fix:** Keep the marker under the old key until RotateAll completes, support
multiple secrets/fallback keys, or document/enforce the operational constraint.
**Test:** Reopen after a partial collection rotation.

## V8. [Medium] `Upsert` aliases caller-owned vectors/payloads
**Location:** `vec/flat_index.go`, `vec/hnsw_index.go` (store `&points[i]`),
`vec/collection.go` (`writeBuf` appends structs sharing slices/maps).
**Problem:** Mutating/reusing the caller’s batch changes index results and
persisted data. It also mutates caller `Version` in place.
**Fix:** Copy vectors/payloads (clonePoint) before indexing/buffering, or
document ownership transfer.
**Test:** Mutate the input slice after Upsert; search results must not change.

## V9. [Medium] Quantization mutates the payload map
**Location:** `vec/store.go` quantized point path.
**Problem:** It shares `p.Payload`, then inserts `__quantized_vector` into the
caller/index map when the map is non-nil. The reserved key leaks into public
results and the caller’s data.
**Fix:** Clone the payload before adding the reserved key, or store the quantized
vector in a dedicated field.
**Test:** After Upsert+Flush with quantization, the caller/index payload has no
`__quantized_vector`.

## V10. [Low] Schema fingerprints are never checked
**Location:** `vec/binco.gen.go` (`ValueSchemaFingerprint`,
`StoredPointSchemaFingerprint`).
**Problem:** Fingerprints/format versions are generated but never stored or
compared; reordering fields silently reinterprets old blobs.
**Fix:** Persist a format version/fingerprint in collection metadata and verify
on load; add a migration/detection test.

## V11. [Low] `encState.enabled()` is dead
**Location:** `vec/aead.go`.
**Fix:** Remove it or use it consistently.

## V12. [Low] `Config.Root` docs are wrong
**Location:** `vec/store.go`, `vec/aead.go`.
**Problem:** Docs say empty Root uses `"vec"`, but empty Root means the database
root; only the root `storm` package passes `"vec"`.
**Fix:** Correct the docs and test the default location.

## V13. [Low] `HNSWIndex.Search` panics on wrong query dimension
**Location:** `vec/hnsw_index.go` → `vec/hnsw/graph.go` `assertDims`.
**Problem:** `Collection.Search` validates dimensions, but the exported index
does not; HNSW panics where Flat degrades. Inconsistent public API.
**Fix:** Validate in `HNSWIndex.Search` and return an error.
**Test:** Mismatched query dimension returns an error.

## V14. [Low] `matchRegex` recompiles per point and hides errors
**Location:** `vec/models.go`.
**Problem:** O(N) regexp compilations per filtered scan; invalid patterns are
silently treated as non-matches.
**Fix:** Precompile/cache per condition and propagate/validate invalid patterns.

## V15. [Low] `Version` is a coarse per-batch wall clock
**Location:** `vec/collection.go` `Upsert`.
**Problem:** One `time.Now().UnixNano()` is assigned to the whole batch,
overwriting caller versions; rapid batches can collide and clock rollback
breaks monotonicity. Direct index Upsert honors caller versions.
**Fix:** Use a per-collection monotonic counter or max(stored, now), and
document that Version is store-managed.

## V16. [Low] `toStoredPoint` silently drops unencodable payload values
**Location:** `vec/store.go`.
**Problem:** Values whose `json.Marshal` fails (map with non-string keys,
channels, funcs, cycles) are silently omitted from the persisted payload.
**Fix:** Return an error from the write path instead of dropping values.
**Test:** `map[int]string` payload returns an error.

## V17. [Low] `clonePoint` is shallow for nested payload values
**Location:** `vec/point.go`.
**Problem:** It copies the top-level map only; nested maps/slices remain shared
with the index in `GetPointsByFilter`.
**Fix:** Deep-copy nested JSON values, or document payload immutability.
**Test:** Mutate a nested value in a returned point and assert the index is
unchanged.

## V18. [Low] API/consistency polish
- `vec/store.go` returns a non-sentinel “collection bucket not found” error.
- `vec/store.go` uses `log.Printf` inside a library load path.
- `VectorIndex.DeleteByFilter` is implemented but unused by `Collection.Delete`.
- `Collection.GetPointsByFilter(nil)` errors while `Search(..., nil, ...)`
  matches all; inconsistent nil semantics.
- `vec/quant.go` comment says “shift to [-128, 127]” but the code clamps to
  `[0,255]`.
- `vec/models.go` doc typo “Copy Must conditions.”
**Fix:** Introduce a sentinel error, remove library logging, reconcile/filter
nil semantics, fix comments/typos, and either use or remove `DeleteByFilter`.

---

# 8. `vec/hnsw`, `vec/vek`, and remaining internal packages

## N1. [Medium] HNSW `Upsert` can panic on inconsistent dimensions in the first batch
**Location:** `vec/hnsw_index.go` (`Upsert` validation), `vec/hnsw/graph.go`
`assertDims`.
**Problem:** Validation is skipped when `graph.Dims()==0`; the first node sets
the dimension, a later node in the same batch with a different length panics.
**Fix:** Validate every point against `len(points[0].Vector)` even for an empty
graph; return an error.
**Test:** Mixed-dimension first batch returns an error, does not panic.

## N2. [Medium] `vec/vek`, `internal/math32`, `internal/partial`, `internal/constraints` are unwired
**Location:** `vec/vek/*`, `vec/vek/vek32/*`, `internal/math32`, `internal/partial`,
`internal/constraints`.
**Problem:** Nothing in the module imports `vec/vek`; the internal packages are
used only by vek. This is a large amount of generated SIMD/assembly and copied
code with no consumer. `vec/hnsw/distance.go` has a TODO asking whether to use
vek.
**Fix:** Decide whether vek is a supported utility (document it and test it) or
remove the subtree and its internal dependencies.
**Test:** If kept, add an integration test proving which public feature uses it.

## N3. [Low] `Analyzer.Height()` overcounts after deletions and is untested
**Location:** `vec/hnsw/analyzer.go`.
**Problem:** It returns `len(Graph.layers)`, including trailing empty layers left
by `Delete`; no analyzer tests exist.
**Fix:** Count non-empty layers (or document slot count) and add
Topography/Connectivity/Height tests after deleting all nodes.

## N4. [Low] `Graph.Search` lacks guards for nil Distance and non-positive EfSearch
**Location:** `vec/hnsw/graph.go`.
**Problem:** `Add` guards nil Distance; `Search` does not. `EfSearch<=0` collapses
the traversal to direct neighbors; a hand-built/imported graph degrades silently
or nil-panics.
**Fix:** Validate/default `EfSearch` and nil Distance in `Search`; add tests for
both.

## N5. [Low] Missing analyzer/graph guard tests
Add tests for N3/N4 and for a zero-value `Graph`.

---

# 9. Cross-cutting cleanup

## C1. [Low] Dead/unwired packages and exports
- `internal/slip10` (see E4)
- `internal/machine` (see E5)
- `vec/vek` + `internal/math32`/`partial`/`constraints` (see N2)
- `fs/inode` package and several `absfs` exports (see F16)
- `encState.enabled` in db/vec (see D5/V11)
- `deleteSink.removed`, `testBoltOptions` (see D5)

## C2. [Low] Stale artifacts and CI mismatch
- `storm.db` in the repository root (H5).
- No `.github` workflow despite README claims; README Go version/pinned
  staticcheck mismatch (H4).
- Uncommitted working tree (H2).

## C3. [Low] README coverage
The README documents `db` and `ts` only; it has no sections for the root
`storm` package (password, metadata, KV API, sub-store accessors), `fs`, or
`vec`. Add scoped docs or explicitly state that the README covers only `db`/`ts`.

---

# Suggested fix order

1. H1/H2: fix the test order, verify, and land a clean baseline commit.
2. R1–R3, E1: root KV encryption contract, canary flag binding, missing
   metadata handling, SIV RFC vectors.
3. F1–F3, V1–V3, T1–T3, D1–D2: data-loss/race/panic bugs.
4. F4–F12, V4–V9, T4–T8, D3–D4, N1: correctness and atomicity.
5. Remaining Low items, API polish, dead-code removal, docs, and missing tests.
