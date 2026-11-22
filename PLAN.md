# Storm — Release Readiness Review and Remediation Plan

**Reviewed revision:** `c039d2e` (branch `master`, no tags)
**Review date:** 2026-11-22
**Scope:** `storm`, `db`, `db/index`, `db/q`, `fs`, `fs/s3fs`, `internal/absfs`, `ts`, `vec`, `vec/hnsw`, `internal/crypto`, `internal/slip21`, module/CI/release files.
**Method:** full source read per area; baseline build/vet/gofmt/test/race; staticcheck and govulncheck; targeted reproductions in a throwaway module under `$TMPDIR` with a `replace` to this tree. No repository files were modified by the review.

---

## Implementation status (2026-11-22)

The remediation was implemented on branch `master` with one or more commits per
area:

- **P0-1 … P0-12:** fixed, each with regression tests. Details are in the
  commit history (`git log --oneline`) and in [POST.md](POST.md).
- **P1:** S3 SigV4 copy signing, chunked-write offset commit, corrupt-metadata
  guards, format-version checks, S3 `ReadDir` contract, root `Close` after
  `Vec().Close()`, the vec failed-flush/delete race and `EncryptPointIDs`
  value encryption are fixed. Quantizer-setting persistence and sub-store
  rotation ergonomics are called out as pre-tag verification items in
  [POST.md](POST.md).
- **P2/P3:** `Limit(0)` semantics, the pre-auth KDF ceiling, reserved payload
  keys, S3 XML error handling, the MIT license, `SECURITY.md`,
  `CONTRIBUTING.md` and `CHANGELOG.md` are done. The remaining P2/P3 items are
  listed as follow-ups in [POST.md](POST.md).
- **Verification:** `gofmt`, `go build`, `go vet`, `go test ./...`,
  `staticcheck@v0.8.2 ./...` and `govulncheck@v1.10.0 ./...` are green; the Go
  floor is 1.27.4 and the lint tools are pinned.

---

## Verdict

**Not ready for a first public release.** Everything builds, the test suite and `-race` pass, and the cryptographic core is sound. However, the review found **twelve release blockers** that can silently corrupt, truncate, hide or strand user data, plus a deadlock in a documented API and a permanently red CI pipeline.

The blockers are concentrated in four areas that must be fixed before tagging:

1. Record decoding does not reset destinations (`db`).
2. String list-index keys are ambiguous and not value-ordered (`db`).
3. Snapshot and external-content write ordering (`fs`).
4. Metadata counters and rotation atomicity (`ts`), plus a public crash/OOM in `vec`.

A detailed remediation sequence is at the end of this document.

---

## Release blockers (P0)

### P0-1 — Root `LICENSE` is missing
- **Status:** verified.
- **Location:** repository root; `README.md` License section; `fs/README.md:6,187`; `fs/s3fs/README.md:6,117`.
- **Problem:** `LICENSE` was deleted in `dbb64b2`. The README and both sub-package READMEs still advertise MIT and link to a `LICENSE` file that does not exist. This is a fork of `asdine/storm`; the MIT terms require preserving the upstream copyright notice.
- **Impact:** Legal blocker for a public release; broken license badges/links.
- **Fix:** Restore the MIT `LICENSE` with the upstream copyright lines (Asdine El Hrychy 2017, Anderson Bargas 2020) plus the current maintainer, add the missing credit, and fix/point the sub-package links at this repository.

### P0-2 — `ReIndex` silently corrupts records with omitted fields
- **Status:** verified with a probe.
- **Location:** `db/type_store.go:195` (`root.unmarshal(raw, data)`), persisted by `db/type_store.go:386-398`.
- **Problem:** `ReIndex` decodes every record into the **same** `data` pointer. `encoding/json` does not reset fields absent from the input (e.g. `json:"...,omitempty"`), so a record inherits values from the previously decoded record and is then re-marshalled and written back.
- **Evidence:**
  ```
  type riRec struct { ID int `db:"id"`; Opt string `json:"opt,omitempty"` }
  saved {1,"AAA"}, {2,""}
  before ReIndex: [{1 AAA} {2 }]
  after  ReIndex: [{1 AAA} {2 AAA}]     // record 2 corrupted on disk
  ```
- **Impact:** Persistent data corruption from the operation meant to repair indexes; the caller's `data` argument is also overwritten with the last record. If a stale field is indexed, the rebuilt index encodes the wrong value too.
- **Fix:** Zero the decode destination before each unmarshal, or allocate a fresh `reflect.New(reflect.TypeOf(data).Elem())` per record. Centralizing “zero before decode” in `node.unmarshal` would also fix P0-3.
- **Tests:** ReIndex round-trip with `omitempty` fields, zero-valued records and indexed zero fields.

### P0-3 — Sorted and filtered scans return stale field values
- **Status:** verified with a probe.
- **Location:** reusable decode destinations: `db/sink.go:492-502` (`listSink.elem` scratch) and the pre-sized slot reused for rejected records (`db/sink.go:459-483`, `db/query.go:325-333`).
- **Problem:** `OrderBy` decodes every record into one reused `scratch`; filtered/skipped scans decode into the same pre-sized slot until a record matches. Because decoding does not zero the destination, omitted fields retain values from a previously decoded record.
- **Evidence:**
  ```
  records: {1, Ord:1, Tag:"skip", Opt:"AAA"}, {2, Ord:2, Tag:"keep", Opt:""}
  OrderBy("Ord")              -> {2, Opt:"AAA"}   // should be ""
  Select(Eq("Tag","keep"))    -> {2, Opt:"AAA"}   // should be ""
  ```
- **Impact:** Silent wrong data on any `OrderBy` or filtered scan for structs/codecs that do not fully overwrite the destination; ordering can also use the stale value.
- **Fix:** Zero the destination before each `Unmarshal` (ideally in `node.unmarshal`), or allocate a fresh value for every record.
- **Tests:** `OrderBy` + `omitempty`; filtered scan where the first record does not match; `Skip` with omitted fields.

### P0-4 — String list indexes are ambiguous for exact lookups, ordering and `Range`
- **Status:** verified with probes (two related defects).
- **Location:** `db/index/list.go:544-551` (`generatePrefix` = `value + "__"`), used by `HasValueID` (`:212`), `Get` (`:272`), `All` (`:303`), `AllRecords` (`:371`), `Range` (`:413`).
- **Problem A — exact match is prefix match:** keys are `value ++ "__" ++ id ++ len`, and hits are accepted with `bytes.HasPrefix(key, value+"__")` without decoding/comparing the value component. A value that begins with `<other>__` is matched by the shorter value. `HasValueID` consequently reports an index entry as current when it belongs to another value, so `Update` can leave the index stale.
  ```
  records: {1,"a"}, {2,"a__b"}
  Find("Tag","a")  -> [{2,a__b} {1,a}]   // should be only 1
  One("Tag","a")   -> {2,a__b}           // wrong record
  update {5,"a__b"} -> "a": index keeps "a__b";
    Find("Tag","a")   includes 5
    Find("Tag","a__b") includes 5 whose stored Tag is "a"
  ```
- **Problem B — ordering/range is raw-key order:** `"_"` (0x5f) sorts after many printable bytes, so `Range` seeks/continues incorrectly and `AllRecords` is not value-ordered. Values `"a","a!","a0","b"` yield order `"a!","a0","a","b"`, and `Range("a","a")` returns not found.
- **Impact:** Wrong exact-match reads (`Find`/`One`), dropped records from valid ranges, wrong index order, and persistent record/index divergence for any string or `[]byte` list-index value containing `__` or prefixing another value (e.g. compound slugs, `user__admin`).
- **Fix (before v1):** Make value matching length-aware: decode the value component with `ListKeyValueLen`, require exact equality for Get/All/HasValueID/Remove, and make `Range`/ordering compare decoded values (or adopt an order-preserving, length-prefixed key format while there is no public compatibility contract). If the format changes, add a version marker/migration.
- **Tests:** values with `__`, prefix siblings, `Range` equality and ordering, update from one colliding value to another, reverse traversal.

### P0-5 — `Update` on an inline embedded struct zeroes sibling fields
- **Status:** verified with a probe.
- **Location:** `db/type_store.go:402-426` (top-level field merge, `cf.Set(f)` at `:417`); inline flattening `db/extract.go:271-284`.
- **Problem:** Partial-update semantics are applied per top-level field. An `inline` embedded struct is one top-level field, so a partially populated embedded value is copied wholesale over the stored one, zeroing the fields that were not supplied.
- **Evidence:**
  ```
  type Base struct { A string `db:"index"`; B string `db:"index"` }
  type Rec struct { ID int `db:"id"`; Base Base `db:"inline"` }
  saved {A:"aa",B:"bb"}; Update(&Rec{ID:1, Base:Base{A:"aa2"}})
  stored -> {A:"aa2", B:""}       // B silently lost
  ```
- **Impact:** Data loss on the common “patch one inline field” pattern; indexed siblings also diverge.
- **Fix:** Merge inline-promoted fields individually (use the flattened `cfg.Fields` paths), or zero-then-copy only the supplied subfields.
- **Tests:** partial update of each inline subfield, indexed inline fields, mixed named/embedded inlines.

### P0-6 — Snapshot read transaction deadlocks against writes on the same DB (`CopyToFS` is unusable)
- **Status:** verified (existing test is skipped for this exact reason; reproduced with a normal-size DB).
- **Location:** `fs/snapshot.go:37-57` (long-lived read tx), `fs/snapshot.go:253-307` (`CopyToFS` writes via `MkdirAll`/`OpenFile`/`Chtimes` while `s.tx` is open); `fs/snapshot_test.go:586-587` documents it.
- **Problem:** bbolt read transactions hold `db.mmaplock.RLock()`. A write that must grow/remap the mmap takes the write lock. `CopyToFS` performs the write on the same goroutine that holds the snapshot read transaction, so once the copy needs more space than the current mmap it waits forever on a reader it will not release until `CopyToFS` returns. A plain write while a snapshot is open has the same hazard.
  ```
  default bolt options, 2 MiB source, CreateSnapshot -> CopyToFS -> HANG (>8 s)
  ```
- **Impact:** Snapshot export/restore and any write concurrent with an open snapshot can wedge the whole filesystem; the API cannot meet its intended point-in-time/backup use case on a normally opened DB.
- **Fix:** Do not write to the same DB while holding the snapshot read tx. Materialize the snapshot into a private buffer/temporary destination first, release the tx, then write; or use a separate DB handle. At minimum, serialize writes with snapshot lifetime and fail fast instead of hanging. Add a regression test with default `bolt.Options` (no `InitialMmapSize`).
- **Related docs:** `fs/README.md:74,91` calls snapshots “backup-2024”/“daily-backup”; snapshots are in-memory, non-durable, and pin the bbolt MVCC view. Reword.

### P0-7 — S3 multipart upload failure silently truncates the object
- **Status:** verified with a failing `UploadPart` client.
- **Location:** `fs/s3fs/s3file.go:165-207` (buffer advanced before upload), `:214-226` (`abortMultipartLocked` does not restore `buffer`/`flushed`), `:265-326` (`Close` falls back to a single `PutObject` of the remaining buffer).
- **Problem:** A failed part upload drops the already-sliced prefix from `buffer` and forgets it; `Close` then writes only the remainder as the whole object and returns success.
- **Evidence:**
  ```
  Write 6 MiB, first UploadPart fails:
  n=0 err="upload part failed"; Close()=nil; stored object = 1,048,576 bytes
  ```
- **Impact:** Silent data truncation on a transient S3 error; retries are inconsistent.
- **Fix:** Restore the buffer/flushed state on failure and keep the multipart for retry, or mark the file failed so `Close` returns the error and never uploads a partial buffer. Add an error-injection test.
- **Related:** P1-1 (S3 copy signing), P2-1 (S3 ReadDir contract).

### P0-8 — External-content writes commit the inode before the content
- **Status:** verified with a probe.
- **Location:** `fs/file.go:219-226` (contentFS branch: `saveInode` then `saveData`); read-path self-heal at `fs/file.go:78-85`.
- **Problem:** For `Store.OpenWithContentFS`, a write commits the new `Size` first and writes external content second. If the content write fails, metadata advertises a longer file than the external object. A later `Read` sees the mismatch, truncates `node.Size` down to the actual content length, and persists that, silently adopting the failed write as a shorter file.
  ```
  write 3 bytes; close content store; append 2 bytes:
  Write n=2 err="failed to create directory: mkdir /: file does not exist"
  mainFS.Stat("/f").Size() == 5          // content is still 3 bytes
  ```
- **Impact:** Silent, irreversible truncation/data loss for the headline contentFS/S3 mode on any external-store failure.
- **Fix:** Write content first, then commit the size-bearing inode; or use one unit of work with recoverable ordering. Do not “heal” a size mismatch by shrinking the logical file when content cannot be verified — return the inconsistency.
- **Related:** P2-2 (`resizeContent` contentFS truncate ordering).

### P0-9 — `ts` point counts drift after `DeleteRange`, breaking `Count()` and retention
- **Status:** verified with a probe.
- **Location:** `ts/series.go:716-731` (block path), `:762-770` (timestamp path); `ts/retention.go:23-25` (`satisfied` uses `meta.Count`).
- **Problem:** A series with points but no metadata record (the common case, because `Insert` does not create metadata when there are no labels/retention) reads metadata as zero. `DeleteRange` then subtracts from zero and **unconditionally writes** a metadata record with `Count == 0` while the series still holds points. Subsequent inserts count from zero. A `DeleteRange` that deletes nothing also creates that bad record.
  ```
  10 points, no metadata; DeleteRange(empty) -> 0; Insert 1
  Count() = 1, actual points = 11
  ```
- **Impact:** Wrong user-visible counts; `MaxAge`/`MaxPoints` satisfaction is decided from the low count, so retention is skipped, allowing unbounded growth/stale data. Reproduces in block mode too.
- **Fix:** If there was no metadata record (or `Count == 0` while the bucket is non-empty), seed the count from data (`seriesCount`: `bucket.Stats().KeyN` / `countBlocks`) before writing; do not persist when nothing was deleted and no metadata existed.
- **Tests:** DeleteRange on a metadata-less series (empty and non-empty ranges), block mode, retention after deletion.

### P0-10 — `ts.RotateAll` can strand series created concurrently
- **Status:** verified (code + concurrent probe reported by the review reviewer).
- **Location:** `ts/rotate.go:288-334`; names are collected in a separate `View` (`:302-308`), then rewritten in a later `Update` (`:315-333`); `withKey` (`:46-58`) uses `Keychain.WithKey` (`internal/crypto/keychain.go:108-113`), which **drops every old key**.
- **Problem:** A writer that creates a series between the name snapshot and the rewrite is not in `names`; its points stay sealed under the old key, then the new keychain has no fallback. The series becomes permanently undecryptable.
  ```
  4 concurrent inserters + RotateAll -> VerifyAll fails with authentication failed
  ```
- **Impact:** Permanent data loss under normal concurrent writes during rotation. `vec.RotateAll` avoids this by snapshotting inside the write lock; `ts` does not.
- **Fix:** Enumerate series **inside** the same `Update` transaction that rewrites and publishes the marker (bbolt's single writer makes it atomic against writers), or add a store-wide rotation RWMutex like `vec.Store.rotMu`. Keep old keys as fallbacks until the rewrite commits.
- **Tests:** concurrent writer + RotateAll + VerifyAll; block and non-block series.

### P0-11 — CI lint job is permanently red
- **Status:** verified by running the CI commands.
- **Location:** `.github/workflows/ci.yml` `lint` job; `go.mod:3`.
- **Evidence:**
  - `staticcheck@latest ./...` exits 1 with **54 findings**, including 8 non-test findings: `fs/fastwalk.go:21` ST1012, `fs/fs.go:24` U1000, `fs/inode.go:282,308` S1009, `internal/absfs/filemode.go:70` ST1005, `vec/rotate.go:539` and `vec/store.go:957,961` SA1019. The remaining 46 are in test files, which staticcheck also analyzes.
  - `govulncheck@latest ./...` exits 3: **GO-2026-6859** (net/http MaxBytesReader race) in `go1.27.3`, fixed in `go1.27.4`; `go.mod` pins `go 1.27.3` and CI installs `go-version-file: go.mod`.
- **Impact:** A public release whose own README says CI runs on every push has a red pipeline; the stdlib advisory affects the S3 HTTP client.
- **Fix:** Raise the Go floor to 1.27.4+, fix/allowlist the 8 non-test staticcheck findings (and clean or explicitly suppress test-only findings), pin staticcheck/govulncheck/benchstat versions for reproducibility, and re-run the exact CI commands.

### P0-12 — `FlatIndex.Search` panics or exhausts memory on a large `topK`
- **Status:** verified with a probe.
- **Location:** `vec/flat_index.go:56-68` (`make([]flatCandidate, 0, topK)` before any clamp); public path `vec/collection.go:244-258` only rejects `topK <= 0`.
- **Problem:** `topK` is caller-controlled. `Search(..., math.MaxInt)` panics with `makeslice: cap out of range`; a large representable value (e.g. 1e9) attempts a multi-GB allocation.
- **Impact:** Remote/caller-driven process crash or OOM; the HNSW index already clamps, the Flat index does not.
- **Fix:** `if topK > len(f.points) { topK = len(f.points) }` before allocating; add a regression test for `MaxInt`, large values and an empty index.

---

## High priority (P1)

### P1-1 — S3 `CopyObject` omits `x-amz-copy-source` from SigV4
- **Status:** suspected (code inspection; no live S3/MinIO available).
- **Location:** `fs/s3fs/http_client.go:130-148` (signed headers only `host`, `x-amz-content-sha256`, `x-amz-date`, optional security token) vs `:233-241` (sets `X-Amz-Copy-Source`).
- **Problem:** AWS SigV4 requires every sent `x-amz-*` header to be in `SignedHeaders`; real S3 and current MinIO reject an unsigned copy source. `Rename`/directory moves go through this path.
- **Impact:** Single-file and directory rename fail entirely against real backends; the mock verifier only re-computes the client's declared signature, so tests cannot catch it.
- **Fix:** Include `x-amz-copy-source` in canonical/signed headers; add an integration test against MinIO or a verifier that enforces all `x-amz-*` headers.

### P1-2 — Chunked `Write` advances the offset on failure
- **Status:** verified with a probe.
- **Location:** `fs/file.go:191-199`.
- **Problem:** `f.offset += len(p)` and `f.node.Size` are updated before `writeRangeAndInode`; on error the method returns `(0, err)` but the handle has advanced.
  ```
  offset 3; Write("X") fails "database not open"; Seek(Current) -> 4
  ```
- **Impact:** A retry writes at the wrong offset, leaving a zero-filled hole; violates `io.Writer` semantics.
- **Fix:** Compute the target offset locally and only commit it (and `Size`) after the transaction succeeds.

### P1-3 — Corrupt persisted data can panic the process (`b2i` on short values)
- **Status:** code-verified.
- **Location:** `fs/dirents.go:104` (`b2i(value)` with no length check), `fs/fs.go:144` (root inode value), `fs/chunk.go:195` (chunk key slicing).
- **Problem:** `binary.BigEndian.Uint64` panics on values shorter than 8 bytes. A crash mid-write, bad disk, or tampered DB turns a directory read/open into a process crash instead of a recoverable error.
- **Fix:** Validate lengths before `b2i` and before chunk key slicing; return a corrupt-data error. Add fuzz/regression tests with truncated values.

### P1-4 — `db.checkVersion` accepts newer on-disk formats
- **Status:** verified with a probe.
- **Location:** `db/store.go:130-158`.
- **Problem:** It reads the version record but only writes it when missing/empty; any non-empty value (including `9.9.9`) returns nil. FS/TS have no version marker; vec writes `FormatVersion` but never validates it on read.
- **Impact:** A first release cannot safely evolve formats: an older binary will silently open and misinterpret newer data. No documented compatibility contract exists.
- **Fix:** Reject unknown/newer versions with a typed sentinel; define v1 as this release and document forward-incompatibility; add version markers to fs/ts and validate vec’s `FormatVersion`.

### P1-5 — S3 `ReadDir` returns success/empty for missing paths and files
- **Status:** verified with a probe.
- **Location:** `fs/s3fs/s3fs.go:667-743`.
- **Problem:** It lists the prefix without checking existence/type; missing keys and regular files both yield `([], nil)` instead of `ErrNotExist`/`ENOTDIR`.
  ```
  ReadDir("missing") -> 0 entries, nil
  ReadDir("file")    -> 0 entries, nil
  ```
- **Impact:** Violates the `fs.ReadDir` contract; `Walk`, `RemoveAll` and sync tools can mask real errors or treat files as empty directories.
- **Fix:** `HeadObject` the path (and trailing-slash marker) before listing; return `ErrNotExist`/`ENOTDIR`.

### P1-6 — Vector quantization settings are not persisted; reopening can make data inaccessible or silently misread
- **Status:** verified for `UseQuant`; suspected for custom `Quantizer` identity.
- **Location:** `storm.go:226-234` (uses per-open `options.UseQuant`), contract at `storm.go:156` (“Later Opens use the stored settings”); enforcement `vec/store.go:701-702`; `CollectionMeta` stores only a boolean.
- **Problem:** A store created with `UseQuant:true` and reopened with default options (`false`) opens, but `NewCollection`/`LoadCollection` fail with “different quantization setting” and zero points. A custom quantizer is not identified at all; reopening with the default SQ8 can dequantize with the wrong algorithm and return silently wrong vectors.
  ```
  reopen default UseQuant -> LoadCollection err: ... different quantization setting; points 0
  ```
- **Impact:** On-disk vector data appears lost on a normal reopen; custom-quantizer mismatch can yield wrong search results without an error.
- **Fix:** Persist quantization/quantizer identity in `storm.Metadata` (as encryption flags are), or persist a quantizer schema/version and refuse mismatches; document that options are required until then.

### P1-7 — Sub-store rotation through the root handle bricks the store
- **Status:** verified with a probe.
- **Location:** `storm.go:449-466` exposes `DB()/FS()/TS()/Vec()`; sub-stores expose `RotateAll`/`ReEncrypt`.
- **Problem:** Root keys are re-derived from the password on every `storm.Open`. Rotating a sub-store’s key directly succeeds (`DB().RotateAll(...) -> nil`) but the next `storm.Open` derives the old key and fails authentication; the data is unreachable through the root API.
  ```
  DB().RotateAll -> nil; Close -> nil; storm.Open(password) -> authentication failed
  ```
- **Impact:** A user following the README rotation section on a sub-store silently makes that component’s data unreachable. Root and sub-store rotation contracts are incompatible.
- **Fix:** Do not expose raw rotation through root-owned sub-stores, or add root-level password re-derivation/re-sealing; at minimum document loudly that sub-store rotation is unsupported on a `storm` store and fail fast.

### P1-8 — `Store.Vec().Close()` poisons `Store.Close()` and leaks the bbolt handle
- **Status:** verified with a probe.
- **Location:** `storm.go:691-715`; `vec/store.go:519-533` (vec `Close` sets `closed=true` even when `ownsDB=false`).
- **Problem:** After closing the vec sub-store, root `Close` calls `s.vec.Flush()`, which returns “storage is closed”; root `Close` returns before `s.bs.Close()`, so the handle/file lock is never released and every retry fails.
  ```
  Vec().Close() -> nil; Store.Close() -> "flush vec store: storage is closed"
  ```
- **Impact:** Resource/handle leak and permanently unclosable store.
- **Fix:** Close the bbolt handle regardless of flush outcome (returning the flush error alongside), and/or treat an already-closed, non-owning vec store as a successful flush. Do not let vec `Close` permanently poison the root path.

### P1-9 — Per-series `ts.EnforceRetention` prunes but never persists the new count
- **Status:** verified with a probe.
- **Location:** `ts/retention.go:104-139`.
- **Problem:** It reads metadata, prunes, and returns `removed`; unlike the store-wide variant (`:193-203`), it never decrements/writes `meta.Count`, so `Count()` keeps over-reporting and every later insert re-prunes.
  ```
  10 points, labels seed Count=10, MaxPoints=3; EnforceRetention removed=8, actual=3
  Count() = 1?  (in probe: Count=1 vs actual 3; generally stale)
  ```
- **Impact:** Persistent count inflation after manual retention; retention re-scans on every write; `Count()` cannot be trusted.
- **Fix:** After pruning, `meta.Count -= removed` (clamped at 0) and `writeMeta` in the same transaction, mirroring `Store.EnforceRetention`.

### P1-10 — `vec` flush failure + concurrent `Delete` can resurrect points
- **Status:** suspected (code-window analysis; transient failure required).
- **Location:** `vec/collection.go:192-204` (error path re-buffers after releasing `flushMu`), `:311-318` (Delete serializes on `flushMu`).
- **Problem:** Upsert detaches `flush`, releases `c.mu`, commits under `flushMu`; on failure it releases `flushMu` and re-acquires `c.mu` to prepend the batch. A concurrent Delete can run after `flushMu` release and before the re-lock, deleting IDs from storage/index; the detached batch is not visible to its `writeBuf` filter, so the batch is re-buffered and a later Flush resurrects deleted points.
- **Impact:** Storage/index divergence and resurrection of deleted points after a transient write error.
- **Fix:** Keep `flushMu` held across the re-buffer decision, or track pending deletes under `c.mu` and filter the re-buffered batch (Delete already serializes on `flushMu`).

### P1-11 — `EncryptPointIDs` alone does not hide point IDs
- **Status:** code-verified.
- **Location:** `vec/store.go:86-91` (`toStoredPoint` stores `ID: p.ID` plaintext), `vec/point.go` (StoredPoint); option wiring in `vec/aead.go`/`store.go`.
- **Problem:** With `EncryptPointIDs` enabled but `EncryptPoints` disabled, only the bbolt key is SIV-encrypted; the value is unencrypted and contains the ID in plaintext. The option name promises more than it delivers.
- **Impact:** Security/compliance expectation gap for users who enable only ID encryption.
- **Fix:** Encrypt the ID in the value when ID encryption is on, or make `EncryptPointIDs` imply point encryption and document it; add a test that inspects stored bytes.

---

## Medium priority (P2)

### P2-1 — `resizeContent` truncate-to-0 on contentFS deletes content before persisting the inode
- **Location:** `fs/fs.go:725-734`.
- **Problem:** `contentFS.Remove` runs before `saveInode`; if inode persistence fails, content is gone while metadata still advertises the old size. Related to P0-8.
- **Fix:** Persist the size-0 inode first, or use an ordering that never deletes the only copy before metadata commits.

### P2-2 — `Copy` can orphan a copied subtree; `CopyToFS` ignores its `Close` error
- **Location:** `fs/fs.go:983-999` (`copyNode` commits new inodes/data before the final link/persist); `fs/snapshot.go:296` (`defer file.Close()` discards the result).
- **Problem:** A late failure in `Copy` leaves unreferenced inodes/data; `Close` errors from an external/S3 content store are swallowed during snapshot restore.
- **Fix:** Delete the copied subtree on late failure; capture and return the `Close` error.

### P2-3 — KDF parameters allow a ~4 GiB pre-auth allocation on `Open`
- **Location:** `meta.go:49` (`maxKDFMemory = 4*1024*1024` KiB), `meta.go:88-102`; applied before canary verification.
- **Problem:** An attacker with write access to unauthenticated metadata can force a 4 GiB Argon2id allocation before authentication fails (local DoS). The same attacker can corrupt the file, so severity is limited.
- **Fix:** Lower the pre-auth ceiling or enforce a sane opening limit; consider an integrity tag over KDF parameters.

### P2-4 — Reserved vector payload keys can clobber user data
- **Location:** `vec/store.go:84` (`payloadJSONKey = "\x00__storm_json_payload__"`), `:143-147`; `"__quantized_vector"` at `:649`.
- **Problem:** A user payload containing the reserved keys is overwritten/consumed, and quantized collections consume `__quantized_vector`. Probability is low (NUL-prefixed key; reserved quantized name) but the API does not reject them.
- **Fix:** Reject/namespace reserved keys at the API boundary, or store them out-of-band from the payload map.

### P2-5 — `Limit(0)` returns `ErrNotFound` instead of an empty success
- **Location:** `db/query.go:306-311`.
- **Problem:** `Limit(0)` short-circuits to `sink.flush`, which returns `ErrNotFound` for empty results; `Delete(...,Limit(0))` therefore reports not found rather than “deleted nothing”.
- **Fix:** Return an empty result slice for reads and treat the empty delete as success.

### P2-6 — Snapshot API/documentation invites unsafe use
- **Location:** `fs/snapshot.go:37-57,310-336`; `fs/README.md:74,90-92`.
- **Problem:** Snapshots are in-memory, non-durable, hold a long-lived read tx (pinning the MVCC view/file growth), and the README names them “backup-2024”/“daily-backup”. Combined with P0-6, users can mistake them for durable backups and wedge writes.
- **Fix:** Document non-durability/growth/hold-time; rename examples away from “backup”; recommend prompt `Release`; add a real backup recipe (`bdb.View` + `WriteTo`).

### P2-7 — `fs/s3fs` ignores XML unmarshal errors on success bodies
- **Location:** `fs/s3fs/http_client.go:262,421` (`_ = xml.Unmarshal(...)`).
- **Problem:** A malformed success body yields an empty ETag/fields returned as success, which can break multipart completion or copy semantics.
- **Fix:** Return the unmarshal error (or explicitly handle it), and add parser tests/fuzzing.

### P2-8 — README claims that do not match the code
- **Verified claims to correct:**
  - `fs/s3fs/README.md:8` says “using the AWS SDK v2”; the package hand-rolls SigV4 (`go.mod` has no AWS SDK).
  - `README.md:93` says `Options{Plaintext:true}` opts out of encryption entirely, but `storm.go:163-165` still requires a non-empty `Password` (canary is password-derived).
  - `storm.go:156` promises “Later Opens use the stored settings”, but `UseQuant`/`Quantizer`/`Codec` are per-open options (P1-6).
  - `README.md:717` mentions vec buffering without the crash-loss window (points below `DefaultFlushThreshold` are lost on crash until Flush/Close).
  - `fs/README.md:74,90-92` calls snapshots backups (P2-6).
  - Sub-package README license/badge links point at `absfs/s3fs` / missing files.

### P2-9 — Missing release/security process files
- **Location:** repository root.
- **Problem:** No `SECURITY.md`, `CONTRIBUTING.md`, `CHANGELOG`, or release tags (`git tag` empty; master is 55 commits ahead of `origin/master`). For a crypto/storage library this leaves no disclosure channel or release artifacts.
- **Fix:** Add a security policy with private reporting, contributor guide, changelog, and tag `v0.1.0`/`v1.0.0` after blockers are fixed.

### P2-10 — No fuzz/property coverage for on-disk and network parsers
- **Location:** `internal/crypto/marker.go`, `meta.go`, `fs/inode.go` GobDecode/decodeNode, `ts/series.go` decodeMeta, `ts/encode.go`, `vec/binary.go`, `fs/s3fs/http_client.go` XML/SigV4.
- **Problem:** Only `internal/absfs` has fuzz tests. Corrupt/tampered bytes are exactly what the AEAD markers are meant to detect; panics (P1-3) and silent empty results (P2-7) are untested.
- **Fix:** Add `FuzzDecodeMarker`, `FuzzDecodeMetadata`, `FuzzGobInode`, `FuzzTSBlock/Point`, `FuzzStoredPoint`, and S3 response/SigV4 fuzz targets with seeds from real records.

---

## Low priority / polish (P3)

- **Dead code:** `vec/hnsw/heap` is not imported by non-test code (the graph uses its own `candidateSet`/`resultSet`); `fs/walkChildren` has an unreachable fallback because `absfs.Filer` always provides `ReadDir`; `internal/absfs` has an unreachable `ExtendSymlinkFiler` branch (structural typing). Remove or gate to reduce public surface/confusion.
- **Error-contract consistency:** only `db` implements the documented `storm <operation>: %w` wrapper; `fs`/`ts`/`vec` use ad-hoc prefixes. Scope the README guarantee or normalize.
- **Public API surface:** `vec` exports generated codec handles (`StoredPointBinco`, `ValueBinco`, `ValueKindBinco`, schema fingerprints); `vec/hnsw` panics on dimension/empty-heap misuse. Document as unstable or move to `internal/`; replace reachable panics with errors where practical.
- **Staticcheck test findings:** 46 warnings are in tests (nil contexts, unused values, deprecated `io/ioutil`, redundant code). CI needs a policy (clean or allowlist) or it will stay red.
- **`vec/gen`:** `main` writes into the working directory and `run` failure branches are barely tested; add a temp-dir invocation in CI.
- **Dependency/toolchain:** the external code generator and its runtime dependency were removed in favour of a hand-written vec codec; `go 1.27.3` is a very new floor (now 1.27.4). Consider a lower language floor for adopters (policy decision).

---

## Areas reviewed and found sound

- **db rotation/locking:** `rotMu` RWMutex ordering, bbolt single-writer transactions, atomic enc pointer, re-encrypt/verify round-trip on values+metadata+keys, list/unique index re-writing and KV; no mixed-key reads or rollback gaps found.
- **db key encoding:** int/uint sign-flip and float bit transforms are order-preserving; `ListKeyValueLen` bounds-checked; corrupt markers rejected without panics (except the separate list-index semantics findings).
- **db context/transaction behavior:** cancellation checks before publish/commit; bbolt rollback on error/panic; no partial-result publication found.
- **fs dirents format/migration:** atomic write of entry keys + childless blob, legacy fallback and full-sync migration, deletion cleanup; sound apart from the `b2i` robustness item.
- **fs inode cache:** shared read nodes, copy-on-write children cloning on mutation, deep-copy on Put, no reuse of inode numbers; sound.
- **fs symlinks/paths:** depth-bounded resolution (`ELOOP`), relative targets, subtree confinement, `fs.ValidPath`/`sanitizePath` traversal defenses; sound.
- **fs chunk transactionality:** chunk+inode updates in one transaction, sparse zero-fill, truncate/delete chunk cleanup; sound (apart from offset advancement on failure).
- **ts block merge/range/encoding:** sorted merge, last-write-wins duplicates, negative timestamps, inclusive bounds, uint32 overflow guard, marker/mode checks; sound apart from count/rotation findings.
- **vec HNSW invariants:** randomized 5,000-operation add/update/delete probe kept `Len` consistent, search returned known IDs, replacement kept dimensions/levels; candidate/result heaps and SQ8 quantization behave correctly.
- **encryption/SLIP-0021:** RFC 5297 KATs, CMAC/CTR/S2V, HKDF domain separation, GCM nonce layout/empty-plaintext sealing, marker authentication, SLIP-0021 vectors, `Destroy` wiping, key copying; sound. (KDF ceiling is P2-3.)
- **Top-level lifecycle (except noted):** wrong password/wrong mode/closed/double-close, metadata corruption, canary/flag binding, KV encryption, sub-store sharing (`SetOwnsDB(false)`), vec flush-before-close; sound apart from vec-close and rotation traps.

---

## Recommended release sequence

### Phase 1 — Stop data loss (P0-1 … P0-10, P0-12)
1. Add failing regression tests for each P0 using the reproduction snippets in this document.
2. Fix decoding destinations (zero-before-unmarshal) and re-run ReIndex/OrderBy/filter tests.
3. Redesign/validate string list-index matching and ordering; include a migration/version decision before the first public format is frozen.
4. Fix inline partial updates.
5. Fix snapshot write ordering/deadlock and S3 multipart recovery; fix contentFS write ordering and chunked offset commit.
6. Fix `ts.DeleteRange`/`EnforceRetention` count seeding/persistence and make `RotateAll` enumerate + rewrite + publish in one writer-locked unit.
7. Clamp `vec` topK and add the regression test.

### Phase 2 — Make the project shippable (P0-11, P1)
1. Restore `LICENSE`; add `SECURITY.md`/`CONTRIBUTING.md`; fix README claims and sub-package links.
2. Raise Go to 1.27.4+, fix/allowlist staticcheck findings, pin CI tool versions, and make the exact CI commands green.
3. Fix S3 copy signing (with a real MinIO/Verifier test), vector quantization persistence, sub-store rotation exposure, root close-after-vec-close, `b2i` length guards, S3 `ReadDir` contract, version guards, `EncryptPointIDs`.
4. Add debuggability/robustness: fuzz seeds for all on-disk/network parsers.

### Phase 3 — Verify before tagging
Run and attach results:
- `go build ./...`, `go vet ./...`, `gofmt -l .`
- `go test -count=1 -timeout 300s ./...`
- `go test -race -count=1 -timeout 300s ./...`
- `go test -run '^$' -bench . -benchmem ./...`
- `staticcheck@<pinned> ./...`
- `govulncheck@<pinned> ./...`
- Integration: real S3/MinIO multipart, rename, ranged reads; snapshot copy on a default-size DB; concurrent ts `RotateAll`.
- Tag `v0.1.0` (or `v1.0.0`) only after all P0 items are closed and P1 items are either fixed or explicitly accepted in the release notes.

---

## Appendix A — Reproduction highlights

All probes ran in a throwaway module under `$TMPDIR` with `replace github.com/malivvan/storm => /home/malivvan/projects/storm`; repository untouched.

```
REINDEX before=[{ID:1 Opt:AAA} {ID:2 Opt:}] after=[{ID:1 Opt:AAA} {ID:2 Opt:AAA}]
ORDERBY=[{ID:1 Ord:1 Tag:skip Opt:AAA} {ID:2 Ord:2 Tag:keep Opt:AAA}]
FILTER=[{ID:2 Ord:2 Tag:keep Opt:AAA}]
INLINE_UPDATE={ID:1 Base:{A:aa2 B:}}
FIND_a=[{ID:2 Tag:a__b} {ID:1 Tag:a}]
ONE_a={ID:2 Tag:a__b}
RANGE_a_a=[] err=storm range: not found
ALLBYINDEX=[{ID:3 Tag:a!} {ID:4 Tag:a0} {ID:2 Tag:a__b} {ID:1 Tag:a}]
UPDATE_DIVERGENCE find_a=[{ID:2 Tag:a__b} {ID:5 Tag:a} {ID:1 Tag:a}]
                   find_a__b=[{ID:2 Tag:a__b} {ID:5 Tag:a}] stored={ID:5 Tag:a}
LIMIT0 err=storm query find: not found
TS_DELETERANGE count=1 actual=11
TS_ENFORCE removed=8 count=1 actual=3
VEC_TOPK_PANIC=runtime error: makeslice: cap out of range
QUANT_LOAD err=invalid encryption config: collection "c" was created with a different quantization setting
STORM_CLOSE_AFTER_VEC_CLOSE err=storm: flush vec store: storage is closed
STORM_SUBSTORE_ROTATE rot=<nil> close=<nil> reopen=storm: open db store: ... authentication failed
S3_MULTIPART n=0 werr=write obj: upload part failed cerr=<nil> rerr=<nil> stored=1048576
FS_CHUNK_OFFSET n=0 err=database not open pos=4
FS_CONTENT_WRITE n=2 err=failed to create directory: mkdir /: file does not exist statSize=5
S3_READDIR missing=0/<nil> file=0/<nil>
DB_FUTURE_VERSION err=<nil>
```

## Appendix B — Baseline and CI evidence

- `go build ./...` — pass.
- `go vet ./...` — pass.
- `gofmt -l .` — clean.
- `go test ./...` — pass (16 packages).
- `go test -race ./...` — pass.
- `go mod tidy -diff` — clean; `go mod verify` — all modules verified.
- `staticcheck@latest ./...` — exit 1, 54 findings (8 non-test).
- `govulncheck@latest ./...` — exit 3, GO-2026-6859 (net/http), fixed in Go 1.27.4; module/CI use 1.27.3.
- No `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md`, `CHANGELOG`; no git tags; `fs/snapshot_test.go:586-587` explicitly skips `CopyToFS` due to the read-tx/write deadlock.
