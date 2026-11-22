# TODO — Performance improvements

Performance review of `github.com/malivvan/storm` at commit **`b5fa831`** (working tree
clean). The repository already received a round of performance work immediately before this
review (commits `c70de72` "linear ReIndex / O(1) index membership / cheaper fs opens",
`460513e` "share vec batches / bound regex caches", `1a9868d` "persist ts counts / skip
satisfied retention"), so this file lists what is **still** slow at this commit, not what was
already fixed.

Scope: `db` (+`db/index`, `db/q`), `fs` (+`internal/absfs`, `fs/inode`, `fs/s3fs`), `ts`,
`vec` (+`vec/hnsw`), the top-level `storm` package and the shared `internal/encryption`
primitives. `go build ./...`, `go vet ./...` and `go test ./...` are green; this is purely a
performance pass.

Method: hot paths were read directly, and three parallel deep reviews ran with real
benchmarks/profiles (`go test -bench . -benchmem ./db/...`, `./fs/...`, and an external
`/tmp` module for `vec`, which has no in-repo benchmarks). Numbers quoted below are from those
runs on an i7-8550U, Go 1.27.3; treat them as order-of-magnitude, not a contract.

Each item lists **Location**, **Problem** (what costs time/allocations and its big-O),
**Fix** (code-level sketch), **Impact** and **Risk**. Items are ordered by impact inside each
package. `vec/vek` (the old SIMD subtree) no longer exists — all distance math is scalar Go,
so "defeating SIMD" is not a concern here.

---

## 1. `db` (highest-impact first)

### 1.1 `Count()` and `Select().Delete()` JSON-decode every record just to count/delete keys — HIGH
> **Status:** ✅ FIXED — sinks declare `needsValue()`; `countSink`, `deleteSink` and bare
> `rawSink` skip unmarshalling when there is no matcher or ordering. Regression tests in
> `db/perf_regression_test.go`.

- **Location:** `db/sink.go:686` (`countSink.elem`), `db/sink.go:587` (`deleteSink.elem`),
  `db/sink.go:54-67` (`sorter.filter`), `db/query.go:310-343`, `db/finder.go:759-764`.
- **Problem:** `countSink`/`deleteSink` implement `elem()`, so `sorter.filter` treats them as
  `reflectSink` and runs `n.unmarshal(v, …)` — a full `encoding/json` decode — for **every**
  record in the bucket, although `countSink.add` only increments a counter and `deleteSink`
  only needs the raw key. `Count` and `Delete` are therefore O(n) JSON decodes instead of
  O(n) cursor steps. JSON decode measured at 63% of the no-index scan cost.
- **Fix:** add `needsValue() bool` to the `sink` interface; return `false` for `countSink`,
  `deleteSink` and `rawSink`. In `filter`, skip `unmarshal`/`tree.Match` when
  `tree == nil && len(orderBy) == 0 && !needsValue()`.
- **Impact:** High — removes nearly all decode cost from `Count` and filtered `Delete`.
- **Risk:** Low — matcher/ordering paths unchanged.

### 1.2 No-index scans allocate a `reflect.New` + grow the result slice per record — HIGH
- **Location:** `db/sink.go:448-453` (`listSink.elem`), `db/sink.go:459-483` (`add`), and the
  string compare in `db/q/compare.go:85-88` (`constant.MakeString`).
- **Problem:** `listSink.results` starts at len 0, so `elem()` always returns a new
  `reflect.New`, and `add` `reflect.Append`s — one `reflect.unsafe_New` + one `growslice`
  reallocation per record (profiled 27% growslice + 23% unsafe_New of alloc space). Separately
  `q.compare` compares strings with `constant.Compare(constant.MakeString(a), …)`, allocating
  both sides on every field compare (18% of alloc space in `FindWithoutIndex`).
- **Fix:** (a) pre-size the sink from `bucket.Stats().KeyN` and decode straight into
  `results.Index(i)` (the indexed path already does this); (b) add a direct `string`/`string`
  fast path in `compare` (token switch on `==`, `<`, …) and keep `go/constant` only for
  cross-kind compares.
- **Impact:** High — removes the dominant allocation from every unindexed scan.
- **Risk:** Low for the string fast path; small for pre-sizing (skip `v == nil` nested buckets,
  which the cursor already excludes).

### 1.3 Struct metadata is re-parsed from tags on every operation; `isZero` uses `reflect.DeepEqual` — HIGH
- **Location:** `db/extract.go:82-133` (`extract`), `db/extract.go:135-218` (`extractField`),
  `db/extract.go:256-260` (`isZero`); callers `db/type_store.go:400-406,464`,
  `db/finder.go:88,208,506,642`.
- **Problem:** Every `Save`/`Update`/`Find`/`One`/`Range`/`Prefix`/`DeleteStruct`/`Init`/
  `ReIndex` re-walks all exported fields, `strings.Split`s the `db` tag, allocates a
  `map[string]*fieldConfig`, and calls `isZero` = `reflect.Zero(...).Interface()` +
  `reflect.DeepEqual` per field. Profiled on `BenchmarkSave`: `extract` = 9.2% CPU and ~11%
  alloc.
- **Fix:** cache the static per-type metadata (bucket name, field names, index kinds,
  IsID/Increment) in a `sync.Map[reflect.Type]*structMeta`; bind the per-call
  `Value *reflect.Value` afterwards. Replace every `isZero` with `v.IsZero()` (allocation-free).
- **Impact:** High — every CRUD op pays this.
- **Risk:** Medium — rebind dynamic `Value` pointers per call; keep inline structs out of the
  static cache.

### 1.4 `getIndex` rebuilds index wrappers (and re-opens buckets) per field per operation — HIGH
- **Location:** `db/extract.go:238-254` (`getIndex`), `db/index/list.go:13-38`
  (`NewListIndex` opens two buckets), called per indexed field in `db/type_store.go:295` and at
  `db/finder.go:271,399,575,708`.
- **Problem:** Each `getIndex` does `parent.Bucket(name)` (twice for list indexes:
  `storm__ids`), each hitting bbolt's bucket-open path. Profiled on `BenchmarkSave`: `getIndex`
  = **23.6% of alloc space (122 MB)** and ~12% CPU.
- **Fix:** build the `index.Index` handles once per `(bucket, field)` inside a transaction and
  reuse them; skip opening `storm__ids` for read-only `Get`/`All` on list indexes.
- **Impact:** High — dominant allocation in `Save`, also helps indexed `Find`/`Range`.
- **Risk:** Low — indexes are stateless handles over a bucket.

### 1.5 Ordered queries materialize everything, then sort with per-swap `reflect.Swapper` and per-compare `FieldByName` — HIGH
- **Location:** `db/query.go:310-349`, `db/sink.go:220-249` (`less` → `FieldByName`),
  `db/sink.go:251-263` (`sort.Sort`), `db/sink.go:361-367` (`Swap` builds `reflect.Swapper`
  per call), `db/sink.go:143-218` (`compareValue`, may JSON-encode via `keyBytes`).
- **Problem:** With `OrderBy`, every match is decoded and retained, then sorted with O(n log n)
  comparisons that each do a linear `FieldByName` scan; every `Swap` allocates a new swapper
  closure (O(n log n) allocations). `compareValue`'s fallback JSON-encodes both operands per
  comparison. `Limit` does not help because all records are decoded and sorted before
  truncation.
- **Fix:** resolve each `orderBy` name to a field index once (`Type.FieldByName` → int) and use
  `Field(i)`; create one swapper / use `sort.SliceStable`; decorate-sort-undecorate (precompute
  keys once); avoid `keyBytes`/JSON in `compareValue` for comparable kinds.
- **Impact:** High for any `OrderBy`/`Reverse` query.
- **Risk:** Low-Medium — preserve existing ordering semantics.

### 1.6 Primary key tagged `db:"increment"` (or bare `ID`) misses the O(1) key path — HIGH
- **Location:** `db/extract.go:196-215` (fallback sets `m.ID = f` but leaves `f.IsID=false`,
  `f.Index=""`), `db/finder.go:93-95,222-240` (fast path guard `!field.IsID && field.Index==""`).
- **Problem:** For `type T struct { ID int \`db:"increment"\` }`, `m.ID` is set but `IsID`/`Index`
  are not, so `One("ID", v)` falls back to a strict query = **full bucket scan**. Measured:
  `BenchmarkOneByID` 286–351 µs vs `BenchmarkOneWithIndex` 7.9 µs (~44×) on the same 100-record
  setup.
- **Fix:** in the `field.Name == "ID"` fallback, when `f` already exists, also set
  `f.IsID = true` and `f.Index = tagID` so the guard routes to the single `bucket.Get`.
- **Impact:** High — turns an O(n) scan into O(1).
- **Risk:** Low-Medium — confirm save/delete still key records correctly and existing
  scan-semantics tests.

### 1.7 Numeric comparisons box through `go/constant` on every field eval — MEDIUM-HIGH
- **Location:** `db/q/compare.go:22-84`.
- **Problem:** All numeric compares call `constant.MakeInt64/MakeUint64/MakeFloat64` + `Compare`,
  boxing on every record for `Gt`/`Lte`/`In`/… .
- **Fix:** fast-path same-kind comparisons directly (`vala.Int()` vs `valb.Int()` with a token
  switch); keep `go/constant` only for mixed signed/unsigned/float/string kinds.
- **Impact:** Medium-High — every unindexed numeric matcher.
- **Risk:** Low — retain the constant path for cross-kind correctness.

### 1.8 `sorter.filter` heap-allocates an `*item` per record — MEDIUM
- **Location:** `db/sink.go:44-107` (`itm := &item{…}`).
- **Problem:** Even on the pre-sized indexed path, one `&item` heap allocation per record just
  to ferry `bucket/k/v/value` into `add` (`sorter.filter` ≈ 23% of alloc in `FindWithIndex`).
- **Fix:** pass `item` by value to `add`, or have `filter` write directly into the sink.
- **Impact:** Medium.
- **Risk:** Low.

### 1.9 AES-SIV `generateSubkeys` is recomputed on every CMAC call — MEDIUM (only with `EncryptKeys`)
- **Location:** `internal/encryption/siv.go:140-173` (`cmac` → `generateSubkeys` at 142),
  `s2v` `:113-137`, `generateSubkeys` `:239-247`.
- **Problem:** `s2v` calls `cmac` twice and each `cmac` recomputes `generateSubkeys` (one AES
  block + two `dbl`) and allocates several 16-byte temporaries — on every `SealName`/`OpenName`,
  i.e. every deterministic key encryption.
- **Fix:** compute `k1, k2` once in `NewSIVEngine`, store them on the engine, reuse in `cmac`.
- **Impact:** Medium for encrypted-key stores.
- **Risk:** Low (pure refactor; SIV test vectors pin correctness).

### 1.10 AES-GCM draws a fresh nonce via `crypto/rand` per value; `Open` trial-decrypts each key — MEDIUM
- **Location:** `internal/encryption/keychain.go:62-80` (`Seal` → `rand.Read`), `:85-104`
  (`Open` loops engines).
- **Problem:** Each encrypted value write performs a `getrandom`/urandom syscall for a 12-byte
  nonce; bulk saves serialize on it. (Rotation fallbacks make reads O(#keys), which is fine.)
- **Fix:** batch nonces from a buffered CSPRNG reader, slicing unique 12-byte values per seal.
  This is only safe with a guarantee of uniqueness across process restarts — keep per-value
  authentication unchanged.
- **Impact:** Medium for encrypted bulk writes.
- **Risk:** Medium-High — nonce reuse is catastrophic; only do this behind a tested, persisted
  counter/random scheme.

### 1.11 `ReIndex` decodes and re-reads each record ~2-3× — MEDIUM
- **Location:** `db/type_store.go:187-201` → `root.Update` → `update` (`db/type_store.go:479-505`)
  does a `One` (decode #2) + `extract` again before `save`.
- **Problem:** A full reindex costs ~2-3 decodes and 2 `extract`s per record.
- **Fix:** call `save` directly on the already-decoded record during reindex; reuse the `cfg`
  extracted at `db/type_store.go:121`.
- **Impact:** Medium — matters for large migrations.
- **Risk:** Low.

### 1.12 `meta.increment` uses `bytes.Buffer` + `binary.Write` — LOW-MEDIUM
- **Location:** `db/metadata.go:51-86`, `db/store.go:276-293` (`numbertob`/`numberfromb`).
- **Problem:** Every auto-increment save allocates a `bytes.Buffer` and uses reflection-based
  `binary.Write`/`Read` for an int64.
- **Fix:** `var b [8]byte; binary.BigEndian.PutUint64(b[:], uint64(n))` and
  `binary.BigEndian.Uint64(raw)`; validate `len(raw) == 8`. Byte layout is identical, so no
  migration needed.
- **Impact:** Low-Medium (hot write path).
- **Risk:** Low.

### 1.13 Matcher field lookup re-resolves `FieldByName` per record — MEDIUM
- **Location:** `db/q/fieldmatcher.go:34-47,64-77`.
- **Problem:** `fieldMatcherDelegate.MatchValue`/`field2fieldMatcherDelegate.MatchValue` call
  `rec.FieldByName(f.Field)` for every record (linear field scan + `reflect.Value` build);
  ~12.6% CPU in `FindWithoutIndex`.
- **Fix:** resolve the field index once per matcher (or cache `reflect.Type → int` in a
  `sync.Map`) and use `rec.Field(idx)`.
- **Impact:** Medium (unindexed scans).
- **Risk:** Low.

### 1.14 Index result slices grow by `append` without capacity — MEDIUM
- **Location:** `db/index/list.go:252-317` (`All`/`AllRecords`/`Range`/`Prefix`),
  `db/index/unique.go:146-181`.
- **Problem:** `[][]byte` results are built with repeated `append`; `ListIndex.All` is ~18% of
  alloc in `FindWithIndex`.
- **Fix:** pre-size from `IndexBucket.Stats().KeyN` (an over-estimate is fine).
- **Impact:** Medium.
- **Risk:** Low.

### 1.15 Bucket-path helpers allocate name slices / `[]byte` conversions per call — LOW-MEDIUM
- **Location:** `db/bucket.go:7-44` (`getBucket`), `db/bucket.go:47-72`
  (`createBucketIfNotExists`).
- **Problem:** Each call builds `make([][]byte, …)` and string→bytes conversions to walk the
  root path; called per operation and inside scans.
- **Fix:** precompute the `[][]byte` path on the node and reuse; special-case the single-segment
  root.
- **Impact:** Low-Medium (pervasive constant factor).
- **Risk:** Low.

### 1.16 `From` deep-clones the bucket path per scan result — LOW
- **Location:** `db/node.go:64-72`, used per result in `db/scan.go:78,152`.
- **Problem:** `PrefixScan`/`RangeScan` call `From(...)` per matched child and `From` copies
  every path element; `withTransaction` copies the node per call.
- **Fix:** share the immutable parent path and append only the new segment; clone only on
  mutation/escape.
- **Impact:** Low (scan-heavy admin ops).
- **Risk:** Low-Medium (path-immutability assumptions).

### 1.17 `checkContext` is called ~6× per record inside `filter` — LOW
- **Location:** `db/sink.go:44-141`.
- **Problem:** Cancellation is checked before/after unmarshal, match and add for every record.
- **Fix:** check once per record (or per K records); the query-level check still guards
  publication.
- **Impact:** Low.
- **Risk:** Low (slightly slower cancellation).

---

## 2. `fs` (highest-impact first)

> Default (`non-chunked`, `EncryptContent` on) mode is the expensive one: every read/write
> decrypts/encrypts the whole file. The chunked path (`chunks` bucket) already does range I/O.

### 2.1 Non-chunked `Write` reloads, re-grows, re-encrypts and re-saves the whole file — HIGH
- **Location:** `fs/file.go:178-204` (+ `fs/fs.go:560-618` `loadData`,
  `fs/fs.go:504-557` `saveData`).
- **Problem:** Each `Write` calls `loadData` (decrypt entire file), possibly allocates a larger
  buffer and copies, then `saveData` seals the whole buffer and `Put`s it. O(file size)
  allocation + AES-GCM per call; writing a file in k appends is O(n²).
- **Fix:** keep a per-`File` write buffer and flush once on `Close`/`Sync`; or always route
  writes through the chunk path (`writeRange`) so only affected bytes are touched. Minimal:
  cache the loaded buffer between writes and re-seal only the dirty region.
- **Impact:** High — `BenchmarkWrite_*` ≈ 8 µs/op for a 1 KB file and grows linearly with size.
- **Risk:** Medium — buffering changes durability (flush on `Close`/`Sync`; handle concurrent
  visibility).

### 2.2 Non-chunked `Read` decrypts and copies the whole file on every call — HIGH
- **Location:** `fs/file.go:65-93`, `fs/fs.go:560-618`.
- **Problem:** `Read` calls `loadData`, which `Get`s, decrypts and copies the entire file, then
  copies out only `p`. Reading 1 byte is O(n) + O(n) crypto + O(n) alloc; `io.Copy` over many
  reads is O(n²). (`readChunked` is the fast path.)
- **Fix:** cache the decrypted buffer on the open `File` (invalidate on Write), or store a small
  header enabling partial decrypt, or default to a chunk size for "whole file" mode.
- **Impact:** High — default read path; `BenchmarkRead` ≈ 123–218 µs for a **1 KB** file.
- **Risk:** Medium — memory and staleness; chunking is the structural fix.

### 2.3 Directory listing opens one bbolt read transaction per child inode — HIGH
- **Location:** `fs/file.go:333-340` (`Readdir`), `fs/file.go:462-472` (`ReadDir`),
  `fs/fs.go:331-341` (`readDir`) → `fs/fs.go:487-502` (`loadInode`, a fresh `db.View`).
- **Problem:** Listing N entries performs N `db.View` calls (each splitting the bucket path and
  doing ~5 `Bucket()` lookups). `BenchmarkReadDir_NoCache` = 4.8 ms for 50 files (~96 µs/child),
  still 2.8 ms with the inode cache.
- **Fix:** batch all `GetInode`s inside a single `View` (reuse one `openFsBucketWithCache`
  result).
- **Impact:** High — affects `ReadDir`, `Readdir`, `Walk`, `FastWalk`, `RemoveAll`.
- **Risk:** Low (read-only batching; keep the tx short).

### 2.4 Two-or-more write transactions (each an fsync) per logical write — HIGH
- **Location:** `fs/file.go:164-176` (chunked: `writeRange` then `saveInode`),
  `fs/file.go:194-202` (non-chunked: `saveInode` then `saveData`), plus `fs/fs.go:1145`
  `saveParentChild` on create.
- **Problem:** Every `Write` commits two `db.Update` transactions (inode + content) and `Create`
  adds a third; each `Update` is an fsync. `BenchmarkWrite` ≈ 8.1 ms/op for 1 KB.
- **Fix:** merge inode + content persistence into one `Update`
  (`saveInodeAndData(ino, node, data)`), and skip the inode write when only the data changed.
- **Impact:** High — dominant cost for small writes.
- **Risk:** Low (behavior-preserving; slightly wider critical section).

### 2.5 `truncate` / `O_TRUNC` loads the whole file just to discard it — HIGH
- **Location:** `fs/fs.go:658-665` (`resizeContent` default branch), `fs/fs.go:647-652`
  (contentFS branch), reached from `fs/fs.go:1178-1183` (`openFile` `O_TRUNC`) and
  `fs/file.go:414`.
- **Problem:** Truncating to size `s` does `loadData` (full read/decrypt) then
  `saveData(resizeBytes(...))` (full re-encrypt/write). Truncate-to-0 — the common
  `Create`-on-existing case — is O(n) to produce an empty value.
- **Fix:** if `size == 0 && contentFS == nil && !chunked()`, just `data.Delete(key)`; on the
  chunked/contentFS paths use `truncateData`/recreate the object. For shrink-while-keeping,
  still rewrite but skip the grow-copy.
- **Impact:** High for create-heavy workloads.
- **Risk:** Low.

### 2.6 `loadData` makes a redundant full-file copy on the encrypted bbolt path — MEDIUM
- **Location:** `fs/fs.go:609-610` (contrast the contentFS branch `:583-587`).
- **Problem:** `openContent` already returns a fresh buffer (AES-GCM `Open(nil, …)`), but the
  code does `data = make(...); copy(data, plain)` — a second full allocation/copy per read. The
  copy is only needed for the plaintext passthrough (where `d` aliases bbolt mmap memory).
- **Fix:** return `plain` directly when encryption is on; copy only in the passthrough case.
- **Impact:** Medium — halves read-path allocation/copy for encrypted non-chunked files.
- **Risk:** Low.

### 2.7 Chunk writes read-modify-write full 64 KiB chunks and pad the tail — MEDIUM
- **Location:** `fs/chunk.go:134-160` (`writeDataRange`, grow at 139-143), `fs/chunk.go:256-265`
  (`writeAllChunks`), `fs/chunk.go:75` (`writeChunk`).
- **Problem:** Appending 1 byte re-reads/decrypts the affected chunk, allocates a full 64 KiB
  zero-padded buffer and re-encrypts the whole chunk; every partial last chunk is stored padded
  to `chunkSize` (wasted space + crypto).
- **Fix:** only grow when writing past the current chunk end; keep a logical last-chunk length
  so the tail is not padded (touches on-disk framing).
- **Impact:** Medium (chunked write path).
- **Risk:** Medium (framing/`readDataRange` bounds).

### 2.8 Inode cache `Get`/`Put` deep-copy the whole inode including all `Children` — MEDIUM
- **Location:** `fs/cache.go:62` (`Get`), `fs/cache.go:81` (`Put`), `fs/inode.go:179-201`
  (`copyInode`).
- **Problem:** Every cache hit allocates a new inode and entries slice and copies every child;
  `resolve` hits this per path component. `BenchmarkResolve_DeepPath_WithCache` ≈ 8.5 µs,
  59 allocs.
- **Fix:** treat cached inodes as immutable and return the shared pointer (callers already copy
  the struct header); copy-on-write only for mutation.
- **Impact:** Medium (all reads; scales with fan-out).
- **Risk:** Medium — audit that no caller mutates a returned inode in place.

### 2.9 Snapshot `Open`/`ReadFile` eagerly load the whole file and copy it twice — MEDIUM
- **Location:** `fs/snapshot.go:397-415` (`Open`), `fs/snapshot.go:184-193` (`ReadFile`),
  `fs/snapshot.go:197-216` (`readContent`).
- **Problem:** Opening a snapshot file reads/decrypts the entire file into memory and copies it
  again; `ReadFile` adds another `make`+`copy`. For encrypted content the extra copies are pure
  waste.
- **Fix:** stream from the snapshot's read tx on demand (`data.Get`/`readDataRange`), and skip
  the defensive copy when `readContent` already returned a private buffer.
- **Impact:** Medium (snapshot reads / `CopyToFS` of large files).
- **Risk:** Low-Medium (respect tx lifetime).

### 2.10 Every directory mutation rewrites the entire children blob — MEDIUM
- **Location:** `fs/inode.go:206-225` (`Link`), `fs/inode.go:227-240` (`Unlink`),
  `fs/inode.go:276-292` (`encodeNode`, gob of all children), `fs/tx.go:176-196` (`PutInode`).
- **Problem:** A directory inode is one value holding all children; each create/remove/rename
  gob-encodes (and optionally re-seals) the whole list, O(children) per mutation, plus an O(n)
  slice shift on insert/delete.
- **Fix:** store directory entries as separate keys (`dir/<ino>/<name>` → child ino) so a
  mutation touches O(1) keys; keep the sorted slice only as an in-memory cache.
- **Impact:** Medium (large directories).
- **Risk:** High (on-disk format change + migration).

### 2.11 `resolve`/`resolveFollow` re-split paths and re-open bucket chains per call — LOW-MEDIUM
- **Location:** `fs/fs.go:1065`, `fs/fs.go:1045-1088`, `fs/tx.go:140-156` (`openBucket`),
  `fs/tx.go:40-62` (`newFsBucketWithCache`, ~5 `Bucket()` lookups).
- **Problem:** Each resolution allocates a `[]string` of components and every transaction
  re-derives the bucket from a slash path; multiplied by the N+1 transactions above.
- **Fix:** cache the resolved `*bolt.Bucket` chain per `FS` (stable for the handle) and
  pre-split virtual paths once.
- **Impact:** Low-Medium (constant factor everywhere).
- **Risk:** Low.

### 2.12 `Walk` allocates heavily — LOW-MEDIUM
- **Location:** `fs/walk.go:73-85`.
- **Problem:** Per directory it materializes all `FileInfo` (triggering 2.3), wraps them in a
  reverse-sortable slice, and re-`path.Join`s per entry. `BenchmarkWalk_NoCache` ≈ 2.7 ms,
  788 KB, 17.8 K allocs.
- **Fix:** use the already-sorted `Children`/`ReadDir` order, and prefer `ReadDir` (`DirEntry`,
  no inode loads) when only names/types are needed.
- **Impact:** Low-Medium (walk-heavy workloads).
- **Risk:** Low.

### 2.13 Encryption seal allocates a nonce + copies per seal — LOW-MEDIUM
- **Location:** `internal/encryption/keychain.go:62-80`, `internal/encryption/siv.go:77-85`.
- **Problem:** Each seal allocates the nonce, then allocates+appends `nonce||ciphertext`; for
  SIV an extra buffer. Repeats per 64 KiB chunk / per `Write`.
- **Fix:** seal into one pre-sized buffer (`dst := make([]byte, 0, nonceSize+overhead+len(p))`);
  reuse a pooled nonce buffer that is refilled from `rand` before each seal.
- **Impact:** Low-Medium (encrypted workloads).
- **Risk:** Low (nonce uniqueness preserved by refilling).

### 2.14 `chunkKey` escapes to the heap on every chunk access — LOW
- **Location:** `fs/chunk.go:14-19`.
- **Problem:** returns `key[:]` of a stack array, so it escapes → one 16-byte alloc per
  `readChunk`/`writeChunk`.
- **Fix:** take a caller-owned `[16]byte` (or a scratch array on `fsBucket`) and pass `key[:]`.
- **Impact:** Low.
- **Risk:** Low.

### 2.15 Repeated bucket-path parsing and `chunks` lookup per transaction — LOW
- **Location:** `fs/tx.go:140-156`, `fs/tx.go:40-62`, `fs/fs.go:173-193`.
- **Problem:** Every `View`/`Update` re-runs `strings.Split(path.Clean(...))` and up to 6
  `Bucket()` lookups — including the `chunks` bucket even for non-chunked stores.
- **Fix:** cache the nested bucket chain; skip the `chunks` lookup when `!chunked()`.
- **Impact:** Low (constant factor, but frequent).
- **Risk:** Low.

---

## 3. `vec` (highest-impact first)

**Measured baseline (5000 points × 128 dims, topK=10):** flat with deep payload **12.10 ms /
4.47 MB / 30,019 allocs**; flat with empty payload **2.31 ms / 745 KB / 19 allocs**; HNSW
no-filter **0.358 ms / 48 KB / 433 allocs**; HNSW filtered **2.65 ms / 3,413 allocs**; HNSW
build of 2000 points **1.20 s / 157 MB / 904,035 allocs**.

### 3.1 `FlatIndex.Search` deep-clones the payload of every matching point, then discards all but topK — HIGH
- **Location:** `vec/flat_index.go:64-90` (loop at `:65`, `Payload: clonePayload(...)` at `:75`).
- **Problem:** For each of n points it computes the distance **and** deep-copies the payload map
  (incl. nested maps/slices); only `topK` survive the truncation, so ~n−topK deep copies are
  wasted. O(n · payloadSize) allocations per query. Measured: deep vs empty payload =
  12.10 ms vs 2.31 ms (~9.8 ms / 3.7 MB / ~30 k allocs are pure cloning of discarded results).
- **Fix:** collect only `{id, version, score}` during the scan; after selecting topK, clone
  payloads for those K only.
- **Impact:** High.
- **Risk:** Low (pure result-assembly refactor).

### 3.2 `FlatIndex.Search` materializes a size-n result slice and sorts all of it regardless of topK — HIGH
- **Location:** `vec/flat_index.go:64-90` (`append` at `:71`, `sort.Slice` at `:79`).
- **Problem:** Every query allocates/grows an n-element `[]ScoredPoint` and does O(n log n) sort
  to keep `topK`. Should be O(n log topK) with O(topK) memory. Measured: topK=1 and topK=10
  allocate the same 745 KB / 19 allocs (2.01 vs 2.31 ms), confirming topK does not reduce work.
- **Fix:** bounded max-heap of size `topK` over `{id, version, dist}` (reject in O(1) against
  the current worst), then sort only the K survivors (`slices.SortFunc`). Keep the
  Euclid-ascending / Cosine-Dot-descending ordering.
- **Impact:** High.
- **Risk:** Low-Medium (ordering semantics).

### 3.3 HNSW `search()` uses O(n) linear scans (`Max`, `PopLast`) inside the per-neighbor inner loop — HIGH
- **Location:** `vec/hnsw/graph.go:110-146` (`result.Max()` at `:114` and `:134`,
  `result.PopLast()` at `:136`), backed by `heap.Max`/`maxIndex` O(n) scans at
  `vec/hnsw/heap/heap.go:73-113`, and `candidates.PopLast()` at `:143`.
- **Problem:** `Max()` and `PopLast()` are O(k); `candidates.PopLast()` is O(efSearch) and fires
  for **every** neighbor once the candidate list is full (default `efSearch` is large), so
  insert cost balloons. Measured build: 1.20 s / 904 k allocs for 2000 points (~452 allocs per
  insert).
- **Fix:** back `candidates` with a true max-heap so `PopLast` is O(log n); track the current
  worst of `result` in a variable (or keep `result` as a max-heap) instead of scanning for max.
- **Impact:** High.
- **Risk:** Medium (preserve eviction/ordering invariants; graph tests cover correctness).

### 3.4 HNSW `search()` allocates + sorts `mapsKeys` per popped candidate — HIGH/MEDIUM
- **Location:** `vec/hnsw/graph.go:122-123` (`neighborKeys := mapsKeys(...); slices.Sort(...)`),
  `mapsKeys` in `vec/hnsw/hnsw.go`.
- **Problem:** One slice allocation + O(deg log deg) sort per popped node, on the hot path of
  both search and construction — purely for deterministic test ordering.
- **Fix:** iterate `for id, n := range current.neighbors` directly; if deterministic order is
  needed for tests, gate the sort behind an unexported flag set only by tests.
- **Impact:** High-Medium (contributes to the 433 allocs/search and ~452 allocs/insert).
- **Risk:** Low.

### 3.5 Generic heap boxes every element through `interface{}` — MEDIUM
- **Location:** `vec/hnsw/heap/heap.go:29-38` (`Push(x interface{})`, `Pop() interface{}`) used
  via `container/heap`.
- **Problem:** Each push/pop of a `searchCandidate` boxes into an interface and type-asserts on
  pop — during every search and every construction add.
- **Fix:** drop `container/heap`; implement `siftUp`/`siftDown` on `[]T` for the generic
  `T Lessable[T]`. `Min`/`Max` become O(1)/tracked.
- **Impact:** Medium.
- **Risk:** Low.

### 3.6 `Collection.Upsert` holds the collection write lock across a synchronous bbolt fsync — MEDIUM-HIGH
- **Location:** `vec/collection.go:146-191` (`c.mu.Lock` at `:147`, `flushUnsafe` at `:187`),
  `vec/collection.go:201-210` (`flushUnsafe` → `storage.UpsertPoints`).
- **Problem:** Every `flushThreshold` (64) points the write lock is held while the batch is
  committed and fsynced, blocking all concurrent `Search`/`Count` (RLock) and writers. Flush
  measured at 1.4–3.7 ms per 500 points → a 64-point flush stalls the collection for hundreds
  of µs.
- **Fix:** under `c.mu`, snapshot `buf := c.writeBuf; c.writeBuf = nil`; release; commit the
  batch outside `c.mu` (serialize commits with `flushMu` only). Keep index updates under
  `c.mu`.
- **Impact:** Medium-High (concurrency/throughput).
- **Risk:** Medium (preserve Delete-vs-flush ordering guarantees).

### 3.7 Storage JSON-encodes every complex payload value per point on flush and load — MEDIUM
- **Location:** `vec/store.go:117-127` (`toStoredPoint`, `json.Marshal` per non-scalar value),
  decode at `vec/store.go:672-675` + `decodeJSONPayload`/`normalizeJSONValue`
  (`vec/store.go:159-205`).
- **Problem:** Each nested map/slice is JSON-encoded separately on write and re-decoded (with
  normalization) on read; dominates load allocations. Measured: flush 1.42 ms/5,774 allocs
  (scalar) vs 3.72 ms/12,311 (nested); load JSON 4.38 ms/23,268 allocs (~46/point).
- **Fix:** encode the payload once at upsert and persist the encoded blob; defer decoding of
  `KindJSON` values until a filter/result needs them; or use binco instead of `encoding/json`.
- **Impact:** Medium.
- **Risk:** Medium (wire-format / `SchemaFingerprint` migration).

### 3.8 `binco.Encode` allocates a fresh buffer per point per flush — MEDIUM
- **Location:** `vec/store.go:619`.
- **Problem:** One full-size allocation per point per flush (vector `D·4` + payload), before
  `b.Put` copies into the page.
- **Fix:** reuse a growable scratch buffer with `binco.AppendEncode(buf[:0], ...)` then
  `b.Put(key, buf)`; only for the plaintext path (sealed path already returns a new slice).
- **Impact:** Medium.
- **Risk:** Low.

### 3.9 HNSW wrapper recomputes distances `Graph.Search` already computed; filtered search double-scans — MEDIUM
- **Location:** `vec/hnsw_index.go:271-273` and `:300-303`
  (`Score: CalculateDistance(...)`), `:241-256` (`Search` over-fetches `fetchK = topK*10` then
  falls back to a full `scan` when matches < topK).
- **Problem:** `Graph.Search` already computed each neighbor distance (`searchCandidate.dist`)
  but returns only nodes, so the wrapper recomputes O(fetchK·D). Filtered search may then run a
  second O(n·D) exact scan. Measured: HNSW filtered 2.65 ms vs unfiltered 0.358 ms.
- **Fix:** add `Graph.SearchWithDistances` returning `{node, dist}` and use it; pre-filter
  in-graph (collect enough candidates) instead of re-scanning everything.
- **Impact:** Medium.
- **Risk:** Low.

### 3.10 Distance dispatch is an indirect call per comparison; Euclid does `sqrt` each time; no norm precompute — MEDIUM
- **Location:** `vec/hnsw_index.go:93-118` (per-metric closures assigned to `g.Distance`),
  `vec/math.go:43-52` (`switch metric` per distance), `vec/hnsw/distance.go`
  (`CosineDistance` recomputes both norms).
- **Problem:** `Graph.Distance` is a func field → indirect, non-inlinable call per neighbor;
  Euclid computes `math.Sqrt` per comparison; cosine recomputes the stored vector's norm every
  time. Measured ~462 ns per 128-dim cosine distance.
- **Fix:** select a concrete metric implementation once at construction so the inner loop is
  direct/inlinable; precompute/store unit norms (or normalize on upsert) so cosine is a single
  dot product; rank Euclid by squared distance and `sqrt` only the final topK (re-apply at
  result time to keep the public score).
- **Impact:** Medium.
- **Risk:** Medium (metric/score semantics).

### 3.11 `addNeighbor` recomputes the distance to every existing neighbor on each overflow — MEDIUM
- **Location:** `vec/hnsw/graph.go:53-61`.
- **Problem:** When a node exceeds M neighbors it scans all M recomputing distances — O(M·D) per
  add; during construction each insert triggers this for ~M neighbors.
- **Fix:** cache the per-edge distance (store `dist` alongside the neighbor), or keep the worst
  edge known so pruning is O(1)/O(log M).
- **Impact:** Medium.
- **Risk:** Medium (edge bookkeeping on delete/replenish).

### 3.12 `replenish` allocates a candidate slice + `seen` map and does O(M²) work + sort per eviction — MEDIUM
- **Location:** `vec/hnsw/graph.go:167-213` (`candidates := make(...)` `:179`, `seen :=
  make(map[...]bool)` `:180`, `slices.SortStableFunc` `:199`).
- **Problem:** Called on every worst-eviction and on delete (`isolate`), i.e. frequently during
  build: two fresh allocations + a sort + a map even for tiny degrees.
- **Fix:** reuse scratch buffers (per node/layer or `sync.Pool`); replace `seen` with a linear
  scan for small M; use partial selection instead of a full sort.
- **Impact:** Medium.
- **Risk:** Low-Medium.

### 3.13 Filter evaluation uses `reflect.TypeOf`×2 + `DeepEqual` fallback; naive `containsString` — LOW-MEDIUM
- **Location:** `vec/models.go:129`, `vec/models.go:170-183`, `vec/models.go:360-366`
  (`containsString` char-by-char).
- **Problem:** Opaque payload types hit reflection (allocation-prone) per point; `containsString`
  reimplements substring search instead of `strings.Contains`.
- **Fix:** use `strings.Contains`; fast-path `string==string` before the reflect fallback;
  precompile `Condition`s (resolve numeric target / regex once per query).
- **Impact:** Low-Medium.
- **Risk:** Low.

### 3.14 Regex cache does an O(n) size scan per new pattern — LOW
- **Location:** `vec/models.go:374-403` (`regexCacheSize()` ranges the map at `:397`).
- **Problem:** Each distinct-pattern insert that misses ranges up to 1024 entries; every match
  does a `sync.Map.Load`.
- **Fix:** track the count in an atomic; compile the regex once per query rather than per point.
- **Impact:** Low.
- **Risk:** Low.

### 3.15 Quantization clones the payload and double-scans the vector per flush — LOW-MEDIUM
- **Location:** `vec/store.go:597-612` (`clonePayload` `:606`, `Quantize` `:610`),
  `vec/quant.go:38-107` (two passes: min/max then encode).
- **Problem:** With `UseQuant`, every flush deep-copies the payload and rescans the vector; a
  second full payload copy on top of the JSON/binco cost.
- **Fix:** quantize once at upsert and store the quantized blob; build a small fresh map instead
  of `clonePayload`.
- **Impact:** Low-Medium.
- **Risk:** Low.

### 3.16 Bulk filter reads/deletes lack preallocation; HNSW bulk delete re-searches per id — LOW
- **Location:** `vec/flat_index.go:114-158`, `vec/hnsw_index.go:335-393`.
- **Problem:** Result slices grow by doubling; `DeleteByFilter`/`Delete` call `graph.Delete(id)`
  per point (each doing layer scans + `isolate`+`replenish`).
- **Fix:** pre-size result slices from `len(points)`; batch HNSW deletions in one graph pass.
- **Impact:** Low.
- **Risk:** Low.

---

## 4. `ts` (highest-impact first)

The big `ts` wins (skip satisfied retention on insert, stream block aggregation, atomic
`SetNow`, persisted `meta.Count`) already landed in `1a9868d`. Remaining:

### 4.1 `insertBlocks`/`mergePoints` rebuild a map and re-sort the whole block on every insert — MEDIUM-HIGH
- **Location:** `ts/block.go:60-76` (`mergePoints`), `ts/block.go:115-145` (`insertBlocks`).
- **Problem:** Each insert touching a time block creates a `map[int64]float64` of
  `len(existing)+len(incoming)`, copies both lists in, allocates a new slice and sorts the whole
  block. In block mode a hot block receiving points one at a time is therefore O(blockSize log
  blockSize) per point → quadratic over the block, plus heavy per-insert allocation.
- **Fix:** add fast paths: if `incoming` is already strictly after the last existing timestamp
  (the common append case), `append(existing, incoming...)` without a map; if both lists are
  sorted, do a linear merge that overwrites equal timestamps. Fall back to the map only for
  arbitrary interleaving with duplicates.
- **Impact:** Medium-High — the dominant block-mode insert cost.
- **Risk:** Low (the merge contract "last value for a timestamp wins" must be preserved).

### 4.2 Non-block `InsertMany` does an extra `Get` per point and re-reads/re-writes metadata JSON each call — MEDIUM
- **Location:** `ts/series.go:206-260` (`bucket.Get(key)` before each `Put` at `:223`; `readMeta`
  + `writeMeta` later).
- **Problem:** `bucket.Get(key[:])` adds a second B-tree lookup per point purely to compute
  `added`; and every call decodes and re-encodes (and optionally re-seals) the series metadata,
  even when labels/retention are unchanged.
- **Fix:** detect new keys with a single cursor pass, or derive `added` from a bulk insert;
  only write metadata when `Count` (or labels/retention) actually changes.
- **Impact:** Medium (high-frequency single-point inserts).
- **Risk:** Low.

### 4.3 Encrypted non-block series pay a full AEAD open per point on every range read — MEDIUM (design/guidance)
- **Location:** `ts/encode.go:86-106` (`decodePoint`), `ts/series.go:526-547` (`Range` scan),
  `ts/aggregate.go:149-166`.
- **Problem:** With `EncryptValues` and no `BlockDuration`, each stored point is independently
  sealed (nonce + 16-byte tag) and independently opened. Scanning m points costs m AEAD opens +
  m allocations. Block mode amortizes this over one seal/open per block.
- **Fix:** document/enable block mode (`ts.Config.BlockDuration`) as the recommended path for
  encrypted high-volume series, or auto-suggest it; optionally add a batched read that opens
  blocks. Not a code defect — a guidance/perf-default item.
- **Impact:** Medium for encrypted unblocked series.
- **Risk:** Low (config/guidance change).

### 4.4 `Series.Insert` allocates a one-element slice per call — LOW
- **Location:** `ts/series.go:174-177` (`InsertMany(ctx, []Point{p})`).
- **Problem:** Every single insert allocates `[]Point{p}` and runs the batch path (map merge in
  block mode, loop in non-block).
- **Fix:** add a single-point fast path that avoids the slice (and in block mode can use the
  append fast path from 4.1).
- **Impact:** Low-Medium (hot path for streaming writers).
- **Risk:** Low.

### 4.5 Block-mode `Count` still decrypts every block when `meta.Count == 0` — LOW
- **Location:** `ts/series.go:571-590`.
- **Problem:** `Count` returns persisted `meta.Count` when > 0, but falls back to
  `countBlocks` (decrypt + unpack every block) when it is 0 — including for a genuinely empty
  series and for data written before count tracking existed.
- **Fix:** treat a missing/legacy count as "scan once and persist", or keep `Count` maintained
  on every write path (already partly done) and only scan for legacy stores.
- **Impact:** Low.
- **Risk:** Low.

### 4.6 `Range`/`RangeEach` allocate the result slice without capacity — LOW
- **Location:** `ts/series.go:395-460`.
- **Problem:** `points := make([]Point, 0)` grows by doubling for large ranges.
- **Fix:** pre-size from `meta.Count` (an over-estimate is fine) or `bucket.Stats().KeyN` in
  non-block mode.
- **Impact:** Low.
- **Risk:** Low.

---

## 5. Top-level `storm` package

### 5.1 Every KV operation acquires the store `RWMutex` twice — LOW-MEDIUM
- **Location:** `storm.go:481-506` (`withDB`/`checkOpen`), used by `lookup`, `scanKeys`,
  `Get`/`Set`/`Delete`/`Has`/`Keys`/`Len` (`storm.go:533-664`).
- **Problem:** `Get` calls `checkOpen()` (RLock/RUnlock) and then `lookup` → `withDB`
  (RLock/RUnlock): two lock cycles per operation, plus a closure allocation for `withDB`.
- **Fix:** acquire the read lock once per public method (or replace the closed flag with an
  `atomic.Bool`/`atomic.Pointer` so the hot path needs no lock), and avoid the generic
  closure indirection in `withDB`.
- **Impact:** Low-Medium (KV micro-ops).
- **Risk:** Low.

### 5.2 `Len` and `Keys` scan every value instead of using bucket stats — LOW
- **Location:** `storm.go:556-574` (`scanKeys`), `storm.go:652-664`.
- **Problem:** `Len` walks the whole cursor to count; `Keys` appends without capacity. The KV
  bucket never contains nested buckets (nested entries are only the sub-store roots under other
  buckets), so `bucket.Stats().KeyN` is an O(1) valid count and a capacity hint.
- **Fix:** `Len` → `b.Stats().KeyN`; `Keys` → preallocate `make([]string, 0, b.Stats().KeyN)`.
- **Impact:** Low.
- **Risk:** Low (confirm no nested buckets can be created under `kvBucket`).

> Argon2id key derivation at `Open` is deliberately expensive for security — do **not**
> "optimize" it. The shared SIV/GCM allocation items are in §1.9–§1.10.

---

## 6. Cross-cutting: measurement and regression protection

### 6.1 There are no `vec` benchmarks and few `fs`/`ts` benchmarks — MEDIUM (process)
- **Problem:** `vec` (the most allocation-sensitive package) has no in-repo benchmarks, so the
  wins above cannot be measured or protected against regression. `ts` has none either.
- **Fix:** add `BenchmarkFlatSearch`, `BenchmarkHNSWSearch`, `BenchmarkHNSWBuild`,
  `BenchmarkStoreFlush`/`Load` in `vec`; `BenchmarkInsertBlockMode`/`BenchmarkRange` in `ts`;
  run with `-benchmem` and keep CPU/alloc profiles for the hot paths.
- **Impact:** Medium (enables all of §3).
- **Risk:** Low.

### 6.2 Add benchmarks/`benchstat` and a race job to CI — LOW-MEDIUM
- **Problem:** No perf regression gate.
- **Fix:** run `go test -race ./...` and `go test -bench . -benchmem ./...` in CI, compare with
  `benchstat` on PRs that touch hot paths.
- **Impact:** Low-Medium.
- **Risk:** Low.

### 6.3 The default `db` codec is `encoding/json` — MEDIUM (documentation/API)
- **Problem:** JSON decode is the single biggest cost in scans (see §1.1–§1.3). The codec is
  already pluggable (`db.Config.Codec`), but the docs/examples default to JSON with no note on
  the cost.
- **Fix:** document the JSON cost and point at the `Codec` option (and, for the top-level
  package, `Options.Codec`) for throughput-sensitive workloads; consider shipping an optional
  faster binary codec.
- **Impact:** Medium (guidance; no behavior change).
- **Risk:** Low.

### 6.4 bbolt `fsync` dominates small writes — note, not a defect
- **Problem:** `db.Save`/`fs.Write`/`ts.Insert` each commit a bbolt transaction (an fsync). This
  is inherent, and batching is the intended remedy.
- **Fix (guidance):** document batching via `db.WriteTransaction` and the `vec`/`ts` batch APIs;
  avoid per-record `Save` in loops. No code change required.
- **Impact:** Informational.
- **Risk:** None.

---

## Quickest wins (highest gain per unit of risk)

1. `vec` §3.1 + §3.2 — flat search: clone payloads only for topK, bounded max-heap. Turns a
   ~12 ms deep-payload query into roughly the ~2.3 ms empty-payload cost.
2. `fs` §2.1 + §2.2 + §2.3 + §2.4 + §2.5 — stop whole-file read/write in non-chunked mode,
   batch directory listing into one transaction, merge write transactions, special-case
   truncate-to-0. These account for most of the observed small-write and listing costs.
3. `db` §1.1 + §1.2 + §1.7 — stop decoding in `Count`/`Delete`, pre-size scan sinks, add the
   direct string/number fast path.
4. `db` §1.3 + §1.4 — cache struct metadata and index handles (pervasive CRUD win).
5. `db` §1.6 — restore the O(1) primary-key fast path for `db:"increment"` IDs.
6. `vec` §3.3 + §3.4 + §3.5 — replace O(n) heap scans, drop the per-pop `mapsKeys` sort, and
   remove `interface{}` boxing.
7. `ts` §4.1 — append/linear-merge fast path in `insertBlocks` (block-mode streaming inserts).
