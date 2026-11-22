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

All items in this section are resolved:

- **V1** — `Store` tracks every collection it creates. `Store.Close` flushes
  their buffers before closing the handle, and the new `Store.Flush` lets an
  embedding owner drain them; the storm root package calls it in `Close`.
- **V2** — `Collection.Close` only flushes. It never closes the shared bbolt
  handle, so sibling collections and sub-stores stay usable.
- **V3/V17** — search results and `GetPointsByFilter` return deep copies of
  the payload (including nested maps/slices).
- **V4** — reopening an existing collection validates vector length, metric,
  `UseHNSW` and HNSW parameters before saving metadata, and rejects a mismatch
  with `ErrInvalidConfig`.
- **V5** — filtered HNSW search over-fetches, then falls back to an exact scan
  when the graph cannot fill the requested result set, matching Flat semantics
  for distant matches.
- **V6** — `MatchTypeContains` uses the exact-match comparison for reloaded
  `[]any`/numeric slices, so numbers coerce and nested values cannot panic.
- **V7** — `ReEncryptCollection` keeps the marker under the old key and
  documents that RotateAll must complete a partial rotation before Close.
- **V8** — `Upsert` deep-copies the batch before indexing/buffering; the
  caller's vectors and payloads are never aliased or mutated.
- **V9** — quantization clones the payload before adding
  `__quantized_vector`, so the reserved key stays out of public data.
- **V10** — collection metadata records `FormatVersion` and
  `SchemaFingerprint`; reopen rejects an incompatible fingerprint.
- **V11** — removed the dead `encState.enabled`.
- **V12** — `Config.Root` documents that an empty root is the database root.
- **V13** — `HNSWIndex.Search` validates the query dimension and returns an
  error instead of panicking.
- **V14** — regex filters cache compiled patterns (including invalid ones).
- **V15** — versions are store-managed and strictly increasing per collection.
- **V16** — `toStoredPoint` returns an error for an unencodable payload value
  instead of silently dropping it.
- **V18** — added `ErrCollectionNotFound`/`ErrMetadataNotFound`, removed the
  library `log.Printf`, `Collection.Delete` uses `DeleteByFilter`,
  `GetPointsByFilter(nil)` matches all, and the comment/typo fixes are applied.
- The concurrent workspace refactor that moved `fs/absfs` to `internal/absfs`
  was adopted (with its one stale import repaired) since it matches C1/F16.

Regression tests live in `vec/todo_review_test.go`.


# 8. `vec/hnsw`, `vec/vek`, and remaining internal packages

All items in this section are resolved:

- **N1** — `HNSWIndex.Upsert` validates every point against `len(points[0].Vector)`
  even when the graph is empty, so a mixed-dimension first batch returns an
  error instead of panicking (`TestHNSWMixedDimensionFirstBatch`).
- **N2** — the unwired `vec/vek` subtree and its only consumers
  (`internal/math32`, `internal/partial`, `internal/constraints`) were removed.
  The HNSW TODO that referenced vek is gone.
- **N3** — `Analyzer.Height` counts non-empty layers instead of every slot left
  by `Delete`, and handles a nil graph (`TestAnalyzerHeightCountsNonEmptyLayers`,
  `TestAnalyzerNilGraph`).
- **N4** — `Graph.Search` returns no results for a nil `Distance` instead of
  panicking, defaults a non-positive `EfSearch`, and never searches a candidate
  list smaller than `k` (`TestGraphSearchNilDistance`,
  `TestGraphSearchNonPositiveEfSearch`).
- **N5** — added the analyzer/graph guard tests above plus a zero-value graph
  search test (`TestZeroValueGraphSearch`).

# 9. Cross-cutting cleanup

All items in this section are resolved:

- **C1** — removed `internal/slip10`, `internal/machine`, `vec/vek` and its
  internal dependencies, the `fs/inode` package, `fs/store.go`'s
  `createRootBucket`, and the dead `encState.enabled`/`deleteSink.removed`/
  `testBoltOptions` helpers. The `absfs` compatibility exports were retained
  (and now live under `internal/absfs`, so they are no longer public surface).
  `dirWriteFlags` is actually used and stays.
- **C2** — no `storm.db` artifact remains, `.github/workflows/ci.yml` exists and
  matches the README, and the refactor is committed in coherent steps.
- **C3** — the README now documents the module's package layout, including the
  root `storm` store (password, metadata, KV API and sub-store accessors),
  `fs` and `vec`, and states explicitly that the API reference covers `db`/`ts`.

# Suggested fix order

1. H1/H2: fix the test order, verify, and land a clean baseline commit.
2. R1–R3, E1: root KV encryption contract, canary flag binding, missing
   metadata handling, SIV RFC vectors.
3. F1–F3, V1–V3, T1–T3, D1–D2: data-loss/race/panic bugs.
4. F4–F12, V4–V9, T4–T8, D3–D4, N1: correctness and atomicity.
5. Remaining Low items, API polish, dead-code removal, docs, and missing tests.
