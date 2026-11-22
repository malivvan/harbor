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

## D1. [High] `generatePrefix` panics for an empty indexed value
**Location:** `db/index/list.go` `generatePrefix`, called from `ListIndex.All`,
`Get`, `Remove`.
**Problem:** For `value == nil/[]`, the function allocates `len+2`: a 2-byte
slice, then writes `prefix[2]` → `index out of range`. Reachable through
`Find(ctx, field, "", &dst)` and `One(ctx, field, "", &dst)` for list-indexed
fields.
**Reproduced:** Both panic with
`runtime error: index out of range [2] with length 2`.
**Fix:** Use `append(append([]byte{}, value...), '_', '_')`, or treat empty
values as “no possible match”/reject them consistently.
**Test:** Find/One with an empty value on an indexed field returns
`ErrNotFound`, not a panic.

## D2. [High] Reverse range returns no rows when `max` is above all keys
**Location:** `internal/internal.go` `RangeCursor.First` (reverse branch), used
by `ListIndex.Range`, `UniqueIndex.Range`, `IDIndex.Range` and the public
`db.Range`/`rnge`.
**Problem:** `Seek(max)` returns `nil` when `max` is beyond every key; the code
returns `nil` instead of falling back to the last key, so descending
“everything up to X” queries return zero rows.
**Reproduced:** With `abc1..abc3`, reverse range `["abc1","zzz"]` returns 0;
forward returns 3.
**Fix:** In the reverse branch, `if k == nil { return c.C.Last() }` before the
`Prev` adjustment.
**Test:** Reverse ranges with both bounds beyond the dataset.

## D3. [Medium] `ListIndex.All`/`Get` include the internal `storm__ids` sentinel
**Location:** `db/index/list.go` `All`, `Get`; sentinel created in
`NewUniqueIndex(b, []byte("storm__ids"))`.
**Problem:** For an indexed value `"storm"`, the generated prefix `"storm__"`
matches the nested sentinel bucket `"storm__ids"` (nil value). `AllRecords`,
`Range` and `Prefix` skip it; `All` and `Get` do not. `Find`/`One` then report
`ErrNotFound` even when the matching record exists.
**Fix:** Skip nil IDs/sentinel keys in `All`; advance past them in `Get`.
**Test:** Save a record whose indexed value is `"storm"` and assert
`Find`/`One` find it.

## D4. [Medium] KV `Set(..., nil)` writes a value that `Get` cannot read
**Location:** `db/kv.go` `Set`, `Get`, `GetBytes`.
**Problem:** `Set(ctx, bucket, key, nil)` stores a zero-length value. `GetBytes`
and `KeyExists` succeed, but typed `Get` tries to decode empty bytes and fails
with a JSON/decode error.
**Fix:** Choose one contract: nil means delete, nil is rejected, or `Get`
returns the zero value for an empty stored value. Document and test it.
**Test:** `Set(nil) → Get` and `Set(nil) → KeyExists`.

## D5. [Low] Dead code and write-only state
**Location:** `db/aead.go` `(*encState).enabled`; `db/sink.go`
`deleteSink.removed`; `db/node_test.go` `testBoltOptions`.
**Problem:** `enabled` is unused; `removed` is incremented but never read;
`testBoltOptions` is unused.
**Fix:** Remove or wire them up.

## D6. [Low] Root-default docs are wrong
**Location:** `db/aead.go` `Config.Root`; `db/node.go` root comment.
**Problem:** Docs say nil/empty Root uses `"db"` and that the normal root is
`["db"]`. Actual `db.Open` uses the database root for empty Root; only the root
`storm` package passes a `"db"` bucket.
**Fix:** Correct both comments.

## D7. [Low] Docs mention a nonexistent `Copy` operation
**Location:** `db/attach.go`, `db/transaction.go`, `db/node.go`.
**Problem:** Comments refer to `Copy`/`Copy Store operations`, but no `Copy`
method exists. One comment also appears to be a bad search/replace of “All”.
**Fix:** Remove/replace the stale references.

## D8. [Low] `node.readTx` lacks the post-callback context check
**Location:** `db/node.go` `readTx` versus `readWriteTx`.
**Problem:** `readWriteTx` checks the context after the callback; `readTx` does
not (both in-tx and `bolt.View` paths), so cancellation after a completed read
callback is not reported, unlike the managed transaction API.
**Fix:** Add `checkContext(ctx)` after the callback and test cancellation after
the read body.

---

# 5. `fs` package

## F1. [High] Renaming/copying a directory into its own subtree destroys data
**Location:** `fs/fs.go` `Rename` (link step), `Copy`.
**Problem:** `Rename("/a", "/a/c")` links the directory into itself and unlinks
it from the root, returning success. The whole subtree becomes unreachable and
`RemoveAll` can loop on the cycle. `Copy("/a","/a/c")` creates a self-referential
duplicate.
**Fix:** Reject when the destination is the source or one of its descendants
(ancestry/inode comparison; `EINVAL`/`EBUSY`).
**Test:** `/a`→`/a/c`, `/a`→`/a/c/d`, self-rename; assert an error and intact
source.

## F2. [High] `EncryptContent` is ignored with an external `contentFS`
**Location:** `fs/fs.go` `saveData`, `loadData`.
**Problem:** When `contentFS != nil`, content is written/read verbatim; no
`sealContent`/`openContent`. The marker advertises encryption, so users believe
file bytes are encrypted while they are plaintext in the external store.
**Fix:** Seal before writing to `contentFS` and open after reading, or reject
`EncryptContent` with an external contentFS.
**Test:** Write a file with encryption + external contentFS; assert the external
object is not plaintext and can only be read with the key.

## F3. [High] s3fs `OpenFile` for writing ignores `GetObject`/`ReadAll` errors
**Location:** `fs/s3fs/s3fs.go` `OpenFile`.
**Problem:** A non-`ErrNotFound` error from `GetObject` leaves the buffer empty;
`Close` then overwrites the existing object with only the new bytes. `ReadAll`
errors are ignored.
**Fix:** Only treat `ErrNotFound` as empty; propagate all other errors and the
`ReadAll` error.
**Test:** Mock `GetObject` returning access-denied/5xx; assert open fails and no
`PutObject` occurs.

## F4. [Medium] Umask is never applied to created files/directories
**Location:** `fs/fs.go` `openFile`, `mkdir`; `Umask()`/`SetUmask`.
**Problem:** `perm` is used verbatim; docs say “before umask”; `Create` yields
mode `0777` under the default umask. This also makes world-writable files by
default.
**Fix:** Apply `fs.Umask()` at creation, or correct the docs and remove the
misleading contract.
**Test:** `SetUmask(0077)`, create file/dir, assert modes.

## F5. [Medium] `RemoveAll` is non-atomic and drops the parent save error
**Location:** `fs/fs.go` `RemoveAll`.
**Problem:** Content deletion, inode deletion and parent update run in separate
transactions; `saveInode(parent)` errors are ignored. A failure leaves the parent
listing removed inodes.
**Fix:** Delete inodes and update the parent in one bbolt transaction (or at
least propagate the save error).
**Test:** Inject a save failure mid-removal and assert a consistent tree.

## F6. [Medium] `absfs.extendedFS.MkdirAll` swallows all errors
**Location:** `fs/absfs/filesystem.go` `MkdirAll`.
**Problem:** The result of `Mkdir` is ignored; if a component exists as a file,
`MkdirAll` returns nil with nothing created. This is reachable through
`fs.saveData` with an extended s3fs contentFS.
**Fix:** Return the first non-`os.ErrExist` error.
**Test:** `MkdirAll` over an existing file path returns an error.

## F7. [Medium] `absfs.fileadapter.Truncate` cannot shrink
**Location:** `fs/absfs/fileadapter.go`.
**Problem:** The fallback seeks to 0 and writes `size` zero bytes, leaving any
tail intact when shrinking.
**Fix:** Return an error when shrinking is unsupported, or rewrite the file.
**Test:** Wrap a Seekable without native Truncate; truncate 10→4; assert length.

## F8. [Medium] s3fs `Remove`/`Rename` on a non-empty “directory” orphan children
**Location:** `fs/s3fs/s3fs.go`.
**Problem:** They delete/copy only the marker object, not keys under the prefix,
so children remain (or stay under the old prefix) while the call reports
success.
**Fix:** List and delete/copy the prefix; reject non-empty removal with
`ENOTEMPTY` if that is the intended contract.
**Test:** Remove/rename a directory containing an object.

## F9. [Medium] `fs.Rename` rejects replacing an existing target
**Location:** `fs/fs.go` `Rename`.
**Problem:** POSIX/absfs contract allows replacing a non-directory target;
this implementation always returns `EEXIST`, breaking the temp-file-and-rename
idiom.
**Fix:** If both source and destination are non-directories, unlink the
destination and proceed atomically.
**Test:** Rename over an existing file replaces it.

## F10. [Medium] `Rename` is not atomic across directories
**Location:** `fs/fs.go` cross-directory branch.
**Problem:** Destination parent save, source unlink, and source parent save are
separate transactions. A failure after the first save leaves two names for one
inode without an incremented link count, leading to dangling references/data
loss.
**Fix:** Link, unlink, and save both parents in one `db.Update`.
**Test:** Inject failure between saves and assert no double entry/dangling link.

## F11. [Medium] `inode.Resolve` mishandles `..` because `NewDir` points `..` to itself
**Location:** `fs/inode/inode.go` `NewDir`, `Resolve`.
**Problem:** `Resolve` does not normalize paths and relies on literal `..`
entries; with default `NewDir`, `/a/..` resolves to `a`, not the root. Tests
manually relink `..`.
**Fix:** Normalize `.`/`..` in `Resolve` or track a real parent.
**Test:** `NewDir` + link `a`; `Resolve("a/../x")` addresses the root child.

## F12. [Medium] Snapshot `Open` rolls back the shared transaction on one read error
**Location:** `fs/snapshot.go` `Open`.
**Problem:** `readContent` failure calls `s.tx.Rollback()` but leaves
`s.released=false`; the snapshot then uses a closed tx and `Release` double-rolls
back. One bad file invalidates the whole snapshot.
**Fix:** Do not roll back on a per-file read error; only `Release` ends the tx.
**Test:** Force a read error and assert other files remain readable.

## F13. [Medium] `fs/README.md` examples are stale and uncompilable
**Location:** `fs/README.md`.
**Problem:** Examples use the old 4-argument `fs.Open(ctx, bdb, root, cfg)`
signature, `fs.CreateSnapshot` on the package, and `snapshot.Walk` (no such
method).
**Fix:** Update to 3-argument `Open` with `Config{Root: [][]byte{...}}`, use
`FS.CreateSnapshot`, and `fs.Walk(snapshot, ...)`.

## F14. [Medium] `Config.Root` doc contradicts the implementation
**Location:** `fs/aead.go` `Config.Root`.
**Problem:** Doc says empty Root uses an `"fs"` bucket; actual empty Root is the
database root (top-level `state`/`inodes`/...).
**Fix:** Correct the doc and test the default location.

## F15. [Low] `Remove("/")` reports success without doing anything
**Location:** `fs/fs.go` `Remove`.
**Problem:** `os.Remove("/")` returns `EBUSY`; here it is a silent no-op.
**Fix:** Return `EINVAL`/`EBUSY`, or document the no-op.
**Test:** `Remove("/")` errors.

## F16. [Low] Dead code/unused APIs
**Location:** `fs/store.go` `createRootBucket`; entire `fs/inode` package;
`fs/absfs` exports such as `ExtendFiler`, `ExtendSeekable`, `ParseFileMode`,
`ParseFlags`, `BlockSymlinks`, `ExtendSymlinkFiler`, `SymlinkOverlay`;
`inode/store.go` `min`/`max`; `fs/file.go` `dirWriteFlags`; unreachable branches
in `fs/fs.go`.
**Problem:** Several APIs are unreferenced or unreachable; `min`/`max` shadow
builtins.
**Fix:** Remove or wire them up, and delete unreachable branches.

## F17. [Low] s3fs `http_client.GetObject` leaks the body on unexpected status
**Location:** `fs/s3fs/http_client.go`.
**Problem:** Non-2xx/non-404 returns `responseError(resp)` without closing the
body; every other method closes it.
**Fix:** Close the body before/inside the error path.
**Test:** Drive a 500 and assert the body is closed.

## F18. [Low] s3fs `ReadDir` claims sorted output but does not sort
**Location:** `fs/s3fs/s3fs.go`, `fs/s3fs/s3file.go`.
**Problem:** Display names derived by trimming at `/` can be out of order
(`a.txt` before `a`, because `.` < `/`), contradicting the doc.
**Fix:** Explicitly sort entries by name.
**Test:** Objects `a.txt` and `a/b` yield `["a","a.txt"]`.

## F19. [Low] Root-path handling is inconsistent for unusual segment names
**Location:** `fs/store.go` `bucketPath` vs `rootBucket`/`FileSystems`/`DeleteFS`.
**Problem:** `bucketPath` uses `path.Join` (treats root elements as
slash-delimited paths), while the other functions treat each element as a
literal bucket name. Elements containing `/`, `.` or `..` diverge; `name` may
contain `/`.
**Fix:** Validate and reject `/`, `.`, `..` in root/FS names, or make all paths
split consistently.
**Test:** Round-trip a nested root with unusual elements through
`Open`/`FileSystems`/`DeleteFS`.

## F20. [Low] Missing tests
No coverage for encryption × external contentFS, directory-into-subtree
rename/copy, umask, non-atomic RemoveAll/Rename failure, s3fs non-empty directory
Remove/Rename, s3fs write-open read errors, or `extendedFS.MkdirAll` errors.

---

# 6. `ts` package

## T1. [High] Data race on `Store.enc` during rotation
**Location:** `ts/rotate.go` `adopt`; readers in `ts/store.go`
`encryptsValues`/`blockDuration`/`bucketKey`/`logicalName`, `ts/encode.go`.
**Problem:** `enc` is a plain field mutated by rotation without a lock while
other goroutines read it. `go run -race` reports a race between `adopt` and
`bucketKey`/`decodePoint`.
**Fix:** Store `enc` in `atomic.Pointer[encState]` (or guard with `s.mu`) and
load it once per operation.
**Test:** Concurrent `Range`/`Insert` vs `ReEncryptSeries`/`RotateAll` under
`-race`.

## T2. [High] `ReEncryptSeries` leaves a store unreadable after restart
**Location:** `ts/rotate.go` `ReEncryptSeries`, marker rewrite, keychain.
**Problem:** The marker is re-sealed with the new key while other series remain
under the old key. Within the process the fallback keychain can read them; after
reopening with a single `Config.Secret`, the new key opens the marker but cannot
decrypt old-key series, and the old key fails marker verification.
**Fix:** Keep the marker under the old key until `RotateAll` finishes, carry
multiple canaries/keys, or document/enforce that `ReEncryptSeries` must be
followed by `RotateAll` before closing.
**Test:** Reopen after a partial rotation and read an un-rotated series.

## T3. [Medium] `RotateAll` is not atomic
**Location:** `ts/rotate.go`.
**Problem:** Each series rewrite commits the new-key marker in its own
transaction; a crash/cancel after the first series leaves a mixed-key store that
no single key can open. `withKey` drops old fallback keys for the running store.
**Fix:** Write the marker only after all series succeed, or stage the whole
rotation in one transaction.
**Test:** Cancel mid-rotation; the old key must still open/read everything.

## T4. [Medium] Retention prunes scan the whole series on every insert
**Location:** `ts/retention.go` `pruneBucket`, `ts/block.go` `pruneBlocks`,
called from `ts/series.go` `InsertMany`.
**Problem:** The zero-policy fast path runs after a full cursor scan (and block
decryption), so every insert is O(n); block mode re-unseals every block.
**Fix:** Return immediately when `MaxAge<=0 && MaxPoints<=0` before scanning;
compute totals lazily in block mode.
**Test:** A scan-counter/benchmark asserting a no-policy insert does not read
existing points.

## T5. [Medium] Database-root stores cannot detect or enumerate existing series
**Location:** `ts/aead.go` `hasData`, `ts/rotate.go` `allSeriesNames`.
**Problem:** At `len(Root)==0`, `hasData` never inspects DB-root series buckets
and `allSeriesNames` only looks at known buckets/cache. Enabling encryption over
an existing plaintext DB-root store is not rejected, and `RotateAll`/`VerifyAll`
miss uncached series.
**Fix:** Enumerate DB-root series buckets (skipping known metadata) when
`len(root)==0`, or require a non-empty Root for encrypted/rotating stores.
**Test:** Nil-root encryption on existing plaintext data; nil-root RotateAll and
reopen.

## T6. [Medium] `DeleteSeries` can delete an unrelated top-level bucket
**Location:** `ts/store.go` `DeleteSeries`.
**Problem:** `parent == nil` is used both for “no root” and “configured root
missing”, so a configured-but-missing root falls back to the database root and
can delete a same-named top-level series bucket.
**Fix:** Branch on `len(s.root)>0`; treat a missing configured root as not found
and never fall back.
**Test:** Configured missing root + colliding top-level bucket.

## T7. [Medium] Empty `Config.Root` segments panic on first insert
**Location:** `ts/store.go` `createRootBucket`/`rootBucket`/
`createSeriesBucket`.
**Problem:** `createRootBucket` skips empty segments and can return nil while
`len(s.root)>0`; `createSeriesBucket` dereferences nil. `Open` does not validate
Root.
**Fix:** Normalize/filter empty segments in `Open` or reject them with an error.
**Test:** `Root:[][]byte{{}}` must not panic (DB root or error).

## T8. [Medium] `EncryptValues` does not control value sealing
**Location:** `ts/encode.go` `sealValue`/`decodePoint` vs
`encryptsValues()`/marker.
**Problem:** Any enabled encryption feature (e.g. names-only or block-only) sets
`enc != nil`, so point values are sealed even when `EncryptValues` is false and
the marker says `encValues=false`.
**Fix:** Gate non-block sealing on `encryptsValues()` (block sealing follows
`blockDur>0`), or update the Config contract and tests.
**Test:** Names-only config: values should be 8 bytes if the documented contract
is plaintext.

## T9. [Low] Dead code: `Store.openValue`
**Location:** `ts/encode.go`.
**Problem:** No callers; `decodePoint` reimplements its decrypt/length logic, so
the two can drift.
**Fix:** Delete it or make `decodePoint` delegate to it.

## T10. [Low] `Config.Root` doc and `Name` mismatch
**Location:** `ts/aead.go`, `ts/store.go` `Name`.
**Problem:** Doc says empty Root uses a `"ts"` bucket; actual empty Root is the
DB root. `Name` is only used by the root `storm` package.
**Fix:** Correct the doc or default to `"ts"` consistently; update the tests.

## T11. [Low] Block-mode retention/prune paths ignore context
**Location:** `ts/retention.go` `pruneSeriesBucket`/`pruneBucket`, `ts/block.go`
`pruneBlocks`.
**Problem:** A cancelled context cannot stop a large block unseal/rewrite.
**Fix:** Thread `ctx` through the prune functions and check it in loops.

## T12. [Low] Missing tests
Add tests for: concurrent rotation vs read/insert (T1), reopen mid-rotation (T2),
aborted RotateAll (T3), no-policy insert does not scan (T4), nil-root hasData/
RotateAll (T5), missing-root DeleteSeries collision (T6), empty Root segments
(T7), and the EncryptValues-off layout (T8).

---

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
