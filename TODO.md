# TODO — Storm codebase review (correctness, integrity, efficiency)

This file is the output of a full review of the current working tree, with a
heavy focus on correctness, data integrity and efficiency. It is a list of
defects and unpolished areas to solve. Line numbers are from the reviewed
revision and may shift; use the named symbols and files to locate the code.

## Verification snapshot

- `go build ./...` — passes.
- `go vet ./...` — passes.
- `gofmt -l .` — clean.
- `go test ./...` — passes (no test currently exercises the paths below).
- `go test -race ./...` — passes, but **targeted race probes fail**:
  - rotating a `db` store while `Save` runs reports `WARNING: DATA RACE`
    (`db.(*Store).adoptEnc` writes `Store.enc`, `db.encFromNode` reads it);
  - rotating a `vec` store while `Collection.Flush` runs reports a race
    (`vec.(*Store).adoptEnc` vs `vec.(*Store).bucketKey`).
- Repository-health items R1–R3 are resolved in the first fix commit.

## Scope

- root package: `storm.go`, `meta.go`, `meta_test.go`, `storm_test.go`
- `db` (including `db/index`, `db/q`)
- `fs` (including `internal/absfs`, `fs/s3fs`)
- `ts`
- `vec` core (store, collections, indexes, filters, quantization, persistence,
  rotation, encryption)
- `vec/hnsw`, `vec/hnsw/heap`, `internal/encryption`, `internal/slip21`
- repository health: README, go.mod, generated code, formatting, tests

## Severity legend

- **H** — high: data races, data loss, silent wrong results, security weakness.
- **M** — medium: correctness or contract bugs with bounded blast radius.
- **L** — low: robustness, API polish, docs, edge cases.
- **E** — efficiency.
- **T** — missing tests.

---

# 1. Concurrency and rotation integrity — RESOLVED

- **H1/H2/H3** — `db`, `vec` and `fs.Store` now hold the encryption state in an
  `atomic.Pointer[encState]`; all readers use `Load()` and rotation uses
  `Store()`. `db` and `vec` additionally serialize top-level transactions with
  `rotMu` (`RLock` for operations, `Lock` for rotation).
- **H4** — the rotation adoption window is closed for writes:
  - `db`/`vec` hold `rotMu.Lock` across the rewrite and adoption, and keys are
    computed inside the top-level transaction (KV, finder, type-store and
    delete paths were moved inside the callbacks).
  - `ts` adopts the new state inside the rotation transaction (restoring the
    old state if the transaction fails).
  - `fs` blocks writes on every tracked file system while `RotateAll` runs, and
    `ReEncryptFS` holds the target file system's lock across rewrite + adopt.
- **H5** — `fs.RotateAll` rewrites every file system in a single `bolt.Update`
  and adopts the new state only after the transaction commits.
- **H6** — the partial-rotation contract is documented on `fs.ReEncryptFS`,
  `ts.ReEncryptSeries`, `vec.ReEncryptCollection` and in the README.
- Regression tests: `db/rotate_concurrency_test.go`,
  `vec/rotate_concurrency_test.go`, `fs/rotate_concurrency_test.go`,
  `ts/rotate_concurrency_test.go`; all pass under `-race`.

# 2. Silent wrong results

## C1. [High] `db` index keys are not order-preserving for negative integers
**Location:** `db/store.go:200-227` (`toBytes`/`numbertob`), used by all index
lookups and `db/index/*`.
**Problem:** Signed integers are encoded as big-endian two's complement, so
negative values sort *after* positive values. `Range`, `Prefix`, and
`AllByIndex` therefore return wrong rows/order. Reproduced with `Age int
`db:"index"` values `-5, 1, 10`:
```
Range(Age,-10,0) -> 0 rows (expected -5)
AllByIndex(Age)  -> order 1, 10, -5 (expected -5, 1, 10)
```
**Fix:** Encode signed integers with the sign bit flipped (`uint64(v) ^
1<<63`, like `ts/encode.go:encodeKey`) and keep a fixed width per type. Add
regression tests for negative ranges and ordering.

## C2. [High] `db` float index keys are JSON text, so ranges are lexicographic
**Location:** `db/store.go:200-218` (`toBytes` default branch →
`codec.Marshal`).
**Problem:** `float32`/`float64` fields fall through to JSON encoding
(`"2"`, `"10"`, `"3"`), and index cursors compare bytes. Reproduced with
`Dist float64 `db:"index"` values `2, 10, 3`:
```
Range(Dist,1,4) -> 3 rows including 10 (expected 2 and 3 only)
```
**Fix:** Add order-preserving float encoding (IEEE-754 bits with sign handling)
or disallow non-integer indexed fields with a clear error. Add tests.

## C3. [High] List-index composite keys are ambiguous when the value or ID contains `__`
**Location:** `db/index/list.go:348-367` (`Range` CompareFn uses
`bytes.LastIndex(val, "__")`), key construction in `Add`
(`db/index/list.go:95-99`).
**Problem:** Keys are `value ++ "__" ++ id` with no escaping. `Range` strips
the ID with `LastIndex("__")`, so an ID containing `__` makes it compare the
wrong prefix and `val[:pos]` can even panic when no separator exists.
Reproduced with IDs `plain1, plain2, a__b, a__c` all having `Tag="x"`:
```
Range(Tag,x,x) -> ErrNotFound (0 rows; One finds a__b)
```
**Fix:** Length-prefix or escape one of the components (or use a separate
mapping bucket), and make the CompareFn refuse malformed keys instead of
slicing at `-1`. Add tests for `__` in values and IDs.

## C4. [High] `UpdateField` on the primary key duplicates the record
**Location:** `db/type_store.go:422-452`, `update` at
`db/type_store.go:455-500`, `save` ID use at `db/type_store.go:270-285`.
**Problem:** `UpdateField(ctx, data, "ID", newID)` mutates `current.ID`, which
also updates `cfg.ID.Value` (both point at the same struct), so `save` writes a
new record under `newID` while the old record remains under the old key.
Reproduced:
```
UpdateField ID=2 -> nil
All -> [{1 alice} {2 alice}]
```
**Fix:** Reject updates to the primary key (return `ErrIncompatibleValue` or a
dedicated error), or implement a real move: delete the old record and its index
entries and write the new one in the same transaction. Document the chosen
contract and add tests.

## C5. [Medium] Quantization mode is not persisted; loading with the wrong mode yields nil vectors
**Location:** `vec/store.go:519-575` (`UpsertPoints` quantizes only when
`useQuant`), `vec/store.go:581-632` (`LoadCollection` dequantizes only when
`useQuant`), `vec/models.go:76-90` (`CollectionMeta` has no quant field).
**Problem:** A collection written with `UseQuant=true` contains `Vector=nil` and
a `__quantized_vector` payload entry. Reopening the store without `UseQuant`
makes `LoadCollection` return points with nil vectors and the reserved key
leaked, and `NewCollection` then fails with a misleading dimension error.
Reproduced:
```
LoadCollection without UseQuant: err=nil, vector=[], payload has __quantized_vector
NewCollection existing quantized -> loaded point has invalid dimension 0
```
**Fix:** Persist the quantization mode (and quantizer identity) in
`CollectionMeta`, validate it when opening a store/collection, and reject a
mismatch with `ErrInvalidConfig`.

## C6. [Medium] `ts.Store.RotateAll`/`VerifyAll` fail on cached-but-never-written series
**Location:** `ts/store.go:87-105` (`Series` caches), `ts/rotate.go:381-430`
(`allSeriesNames` includes `s.order`), `ts/rotate.go:140-150`
(`reEncryptSeries` returns `ErrNotFound` when the bucket/meta is absent).
**Problem:** Merely calling `store.Series("x")` (without inserting) puts "x" in
the cache. `RotateAll` then includes it and fails:
```
RotateAll with cached-only series: ts: not found
```
**Fix:** Filter cached names through an on-disk existence check, or have
`reEncryptSeries` skip a series with neither a bucket nor metadata.

## C7. [Medium] `vec` stores opened at the database root treat unrelated buckets as collections
**Location:** `vec/store.go:634-680` (`ListCollections` uses `tx.ForEach` at the
DB root), `vec/aead.go:269-309` (`hasData` treats every top-level bucket as
data), `vec/rotate.go:231-272` (`collectionNames`).
**Problem:** At `len(root)==0`, every top-level bucket except
`__collections_meta__` is considered a collection. On a shared Storm database
(`db`, `fs`, `ts`, `0x00`, `storm.Metadata`), `ListCollections`, `RotateAll`
and `VerifyAll` will enumerate and try to rewrite foreign buckets. `ts`
already refuses encryption without a non-empty `Root` (`ts/aead.go:265-270`);
`vec` does not.
**Fix:** Require a non-empty `Root` for encrypted/rotating `vec` stores (like
`ts`), or maintain a dedicated collections registry and skip unknown buckets.
Add a regression test on a shared database.

## C8. [Medium] `db` lookups encode the dynamic value type, not the field type
**Location:** `db/aead.go:438-444` (`keyBytes`), `db/store.go:200-218`
(`toBytes`).
**Problem:** An `int32` field saved as a 4-byte key cannot be found with an
`int64` value (8 bytes); different integer widths and float JSON formatting
produce different keys. On 64-bit, `int`/`int64` happen to align, masking the
bug.
**Fix:** Encode the lookup value using the field's declared type (and reject
incompatible types), or normalise all integer index keys to one width and add
tests for cross-type lookups.

## C9. [Low] `Query.Raw`/`RawEach` with a nil sample silently ignore matchers/ordering
**Location:** `db/query.go:160-200`, `db/sink.go:897-915` (`newRawSink`),
`db/sink.go:40-100` (`sorter.filter`).
**Problem:** With `kind == nil` the sink is a `rawSink`, which has no `elem()`;
`sorter.filter` therefore skips `tree.Match` and never populates `s.list`, so a
matcher is ignored and an `OrderBy` yields nothing. Reproduced: with a node
root the query returns the root bucket's raw values rather than the record
bucket's rows. The doc only warns; it does not reject.
**Fix:** Reject `Raw`/`RawEach` with a nil `kind` when the query has matchers
or `orderBy` (return a sentinel error), or decode into a provided type.

## C10. [Low] Empty result sets have inconsistent error semantics
**Location:** `db/finder.go` `find`/`rnge`/`prefix` vs `db/finder.go:472-490`
(`All`).
**Problem:** `All` turns `ErrNotFound` into an empty slice, while `Find`,
`Range` and `Prefix` return `ErrNotFound` when nothing matches. Callers must
special-case which finder they use.
**Fix:** Document the contract explicitly, or make all finders return an empty
result with a nil error.

---

# 3. Data-loss / integrity

## I1. [High] Empty ciphertext bypasses authentication and silently truncates data
**Location:** `internal/encryption/keychain.go:61-100` (`Seal`/`Open` treat
length 0 as a passthrough), callers such as `fs/fs.go:555-600` (`loadData`),
`fs/file.go:40-70` (`Read`).
**Problem:** An attacker (or corruption) that replaces a sealed value with the
empty byte string is not detected: `Open` returns an empty plaintext. For fs
content this silently turns the file into an empty file with a `nil` error.
Reproduced: writing 9 bytes with `EncryptContent`, then setting the `data`
bucket value to `[]byte{}`, reopens and reads `""` with `err=nil` (inode size
still 9). Similar zero-fill happens when a chunk is deleted
(`fs/chunk.go:readDataRange` skips missing chunks).
**Fix:** Distinguish "absent" (bbolt `Get` returns nil) from "present but
empty at rest"; stores should treat a present zero-length sealed payload as
`ErrAuthFailed`, except for entities explicitly allowed to be empty at rest.
Alternatively bind the plaintext length into the AEAD associated data.

## I2. [Medium] Sub-store markers do not authenticate their flags/extra fields
**Location:** `db/aead.go:177-215`, `fs/aead.go:119-145`, `ts/aead.go:131-160`,
`vec/aead.go:172-210`. Each `encodeMarker` seals a constant canary; `matches`
compares decoded flags to the configured state.
**Problem:** The root `storm` canary was fixed to bind the flags
(`storm.go:336-366`), but the four sub-store markers still seal a constant.
An attacker who flips a marker flag/extra (e.g., `markerFlagValues`, ts block
duration, fs chunk size) can open the store with a config matching the flipped
value: `matches` succeeds and the canary still verifies, so ciphertext is
misread as plaintext (or vice versa). When the caller's config is fixed the
tamper only causes `ErrWrongMode` (a denial of service), but direct sub-store
users who load config from disk are exposed to silent misreads.
**Fix:** Bind the marker flags/extra into the canary plaintext (as the root
package does), or pass them as GCM associated data. Add a tamper test for each
sub-store.

## I3. [High] `fs.saveData` ignores the external `contentFS` `Close` error
**Location:** `fs/fs.go:508-548`, especially `defer f.Close()` at line 528.
**Problem:** When `contentFS` is an upload-on-close filesystem (s3fs is), the
actual write happens in `Close`. `saveData` never checks that error, so a
failed upload is reported as success and the inode points at missing content.
The same pattern exists in `fs/snapshot.go:278` (`CopyToFS`).
**Fix:** Call `Close` explicitly and propagate its error (and use a
temp-key-then-rename/commit protocol where the backend allows it). Add a test
with a contentFS whose `Close` fails.

## I4. [High] `s3fs.File.Truncate` discards already-uploaded multipart content
**Location:** `fs/s3fs/s3file.go:351-390`.
**Problem:** For a large file with an in-progress multipart upload, `Truncate`
aborts the upload (discarding uploaded parts) and sets `flushed = 0`, keeping
only the unflushed buffer. Truncating such a file therefore loses its prefix
instead of resizing it.
**Fix:** Do not support truncating a file after parts have been flushed; return
an error, or re-download the object, apply the truncation and rewrite it.
Document the limitation and add a test.

## I5. [Medium] `fs.RemoveAll`/`saveParentChild`/`copyNode` are not atomic
**Location:** `fs/fs.go:1460-1620` (`RemoveAll`: content removal, inode
deletion and parent update in separate transactions), `fs/fs.go:1273-1315`
(`saveParentChild`), `fs/fs.go:900-950` (`copyNode`).
**Problem:** A failure between transactions leaves dangling children, an
orphaned inode, a stale parent listing or leaked copied inodes.
**Fix:** Perform the inode/data/parent mutations of a removal (and the
child+parent creation of a link) in a single `bolt.Update`; for `copyNode`,
stage the subtree or add cleanup on failure. Add failure-injection tests.

## I6. [Medium] `fs` `O_APPEND` writes use the open-time size, not the current end
**Location:** `fs/fs.go:1150-1160` (`openFile` sets `f.offset = child.Size`),
`fs/file.go:125-180` (`Write`).
**Problem:** With concurrent appenders, one handle overwrites the bytes another
appended after the first handle was opened; `O_APPEND` is not atomic.
**Fix:** For `O_APPEND`, reload the inode size and seek to the end inside the
`fs.mu`-protected write, or document that concurrent appends are unsupported.

## I7. [Medium] `fs.Snapshot` is not point-in-time for external `contentFS`
**Location:** `fs/snapshot.go:191-211` (`readContent` calls
`fs.loadData(node.Ino)` for an external contentFS).
**Problem:** bbolt-based content is read from the snapshot's read transaction,
but external content is read live; concurrent writes/deletes change what the
snapshot returns.
**Fix:** Either snapshot external content (copy-on-create/versioning) or
document that snapshots only isolate bbolt metadata/content and add a test that
pins the behavior.

## I8. [Medium] `fs` chunk reads can silently shift data when a chunk is missing
**Location:** `fs/chunk.go:224-245` (`readAllChunks` concatenates chunks without
respecting indexes) and `fs/chunk.go:78-120` (`readDataRange` zero-fills gaps).
`loadChunkedData` is used by `loadData` (e.g., `copyNode`).
**Problem:** A deleted/missing middle chunk makes `readAllChunks` concatenate
the next chunk at the wrong offset, silently changing file content; the
range-based reader instead zero-fills, so the two paths disagree.
**Fix:** Index chunks explicitly and reject gaps (or zero-fill consistently),
and add a test with a missing chunk.

## I9. [Low] `vec.Collection` writes can be missed by a concurrent `Store.Flush`/`Close`
**Location:** `vec/store.go:479-500` (`flushCollections` copies the collection
list and flushes), `vec/collection.go:150-170` (`Upsert` buffers after the
index update).
**Problem:** An `Upsert` that appends to `writeBuf` after its collection was
flushed but before `closed` is set can be lost; the in-memory index shows the
point but storage does not.
**Fix:** Serialize `Store.Flush`/`Close` with `Collection.Upsert` (a store-wide
lock or a per-collection closed flag checked under `c.mu`), or document that
concurrent writers must stop before closing.

## I10. [Low] `db` `newMeta` ignores the codec-record write error
**Location:** `db/metadata.go:13-40`, `m.Put([]byte(metaCodec), ...)`.
**Problem:** A failed `Put` (e.g., read-only transaction/disk error) leaves the
metadata bucket without a codec while the caller proceeds.
**Fix:** Check and return the error.

## I11. [Medium] `fs` read paths are not atomic across transactions
**Location:** `fs/fs.go:350-372` (`ReadFile`: `resolve` then
`readRange`/`loadData` in separate transactions), `fs/fs.go:321-348`
(`ReadDir`: resolve then per-child `loadInode`), `fs/fs.go:150-170`
(`OpenFile`/`Mkdir`/`Remove`/`Rename` hold `fs.mu`, but `ReadFile`/`ReadDir`
do not).
**Problem:** A concurrent rename/remove/truncate between the inode resolution
and the data read can yield a torn result (a stale inode pointing at deleted
content, an empty file for a just-removed path, or a directory listing whose
children vanish mid-loop). The same applies to `fs.Sub`/`Snapshot` paths that
call back into `loadData`.
**Fix:** Perform each high-level read inside a single bbolt read transaction
(resolve + read), and take `fs.mu.RLock` around the multi-step operations that
must not interleave with writers.

---

# 4. Panics, crashes and unbounded resource use

## P1. [High] `fs.File.Write` panics on a large offset
**Location:** `fs/file.go:125-170`, especially `size := len(p) + int(f.offset)`
and `data[int(f.offset):]`.
**Problem:** `Seek(math.MaxInt64)` followed by `Write` overflows/panics.
Reproduced:
```
PANIC: runtime error: slice bounds out of range [9223372036854775807:0]
```
There is no file-size limit, so even non-overflowing huge offsets attempt
multi-gigabyte allocations.
**Fix:** Reject offsets/sizes above a configurable maximum (return `EFBIG`),
check integer overflow before allocating, and add tests for `MaxInt64`,
`MaxInt32`, and a large-but-valid sparse write policy.

## P2. [High] `fs.Truncate`/`resizeBytes` panics or OOMs on a huge size
**Location:** `fs/fs.go:620-645` (`resizeBytes`), `fs/fs.go:1203-1230`
(`Truncate`), `fs/fs.go:646-676` (`resizeContent`).
**Problem:** `Truncate(path, math.MaxInt64)` panics:
```
PANIC: runtime error: makeslice: len out of range
```
**Fix:** Bound the size and return `EFBIG`/`EINVAL`; enforce the same bound in
`File.Write`, chunked writes and the external contentFS path.

## P3. [High] `fs.File` methods panic after `Close`
**Location:** `fs/file.go:193-215` (`Close` sets `f.node = nil`), then
`Read`/`Write`/`Seek`/`Readdir`/`ReadDir`/`Truncate` dereference `f.node`.
**Problem:** Reproduced: `Read` and `Seek` after `Close` panic with a nil
pointer dereference; `Stat` after `Close` returns `nil, nil` and its
`FileInfo` methods panic later.
**Fix:** Track a closed flag and return `os.ErrClosed` from every method; make
`Stat` return an error. Add tests for every `File` method after `Close`.

## P4. [High] HNSW search panics on a huge `topK`
**Location:** `vec/hnsw_index.go:182-240`, `vec/hnsw/graph.go:105`
(`make([]searchCandidate[K], 0, k)`).
**Problem:** `Collection.Search` only checks `topK > 0`; `Search(query, nil,
math.MaxInt)` panics:
```
PANIC: runtime error: makeslice: cap out of range
```
**Fix:** Clamp `topK` to the index count (or a sane maximum) before calling the
graph, and add a test with `math.MaxInt`.

## P5. [Medium] `vec` panics when `Config.Root` contains only empty segments
**Location:** `vec/store.go:332-350` (`createRootBucket` skips empty segments
and returns nil), `vec/store.go:384-400`
(`createCollectionBucket` dereferences it).
**Problem:** `vec.Open(..., Config{Root: [][]byte{{}}})` succeeds; the first
`NewCollection` panics:
```
PANIC: invalid memory address or nil pointer dereference
... bbolt.(*Bucket).CreateBucketIfNotExists
```
`ts` filters empty segments (`ts/aead.go:212-222`) and guards a nil root;
`vec` does neither. `metaBucket` has the same nil-return pattern.
**Fix:** Filter empty segments in `Open` (as `ts` does), validate/reject an
all-empty root, and guard every `createRootBucket` caller.

## P6. [Medium] `s3fs` truncate panics on negative sizes and OOMs on huge ones
**Location:** `fs/s3fs/s3fs.go:575-615` (`FileSystem.Truncate`),
`fs/s3fs/s3file.go:351-373` (`File.Truncate`).
**Problem:** `FileSystem.Truncate(-1)` reaches `content[:size]` (slice bounds
panic); a size larger than the object triggers `make([]byte, size)` (OOM).
`File.Truncate` has the same `make([]byte, size)` growth path.
**Fix:** Reject negative sizes with `os.ErrInvalid`; bound positive growth
(e.g., S3 part limits or a configurable maximum) and add tests.

## P7. [Medium] `s3fs.File.Write` can overflow `int` for large offsets
**Location:** `fs/s3fs/s3file.go:127-160`, `end := int(rel) + len(b)`.
**Problem:** A large `Seek` offset plus a write overflows `int`, producing a
negative/absurd length and a panic or OOM in `make`.
**Fix:** Validate `rel` and `len(b)` against `maxInt` and a maximum object size
before allocating.

## P8. [Medium] `db` query matchers panic on unexported fields or non-struct values
**Location:** `db/q/fieldmatcher.go:33-56`.
**Problem:** `field.Interface()` panics for an unexported field (``cannot
return value obtained from unexported field``), and `v.FieldByName` panics when
the record is not a struct. A caller-supplied field name can therefore crash a
query.
**Fix:** Check `field.IsValid()`/`field.CanInterface()` and the record kind,
returning `ErrUnknownField` instead of panicking.

## P9. [Medium] `internal/absfs.ParseFileMode` panics on an over-long mode string
**Location:** `internal/absfs/filemode.go:11-75`.
**Problem:** Only `len(input) < 10` is rejected; a longer string iterates past
the 9-entry `permissions` array (`permissions[i]` → index out of range).
**Fix:** Require exactly 10 characters (or iterate only the first 9) and add a
fuzz test.

## P10. [Medium] HNSW import can allocate multi-gigabyte structures
**Location:** `vec/hnsw/encode.go:20-35` (`maxEncodedLength = 1<<30`),
`binaryRead` for strings/slices, `Import` for layers/nodes/neighbors.
**Problem:** A malicious length prefix of `1<<30` passes the cap; for
`[]float32` that is a 4 GiB allocation, and `make([]K, nNeighbors)` can be just
as large. `M`/`Ml`/`EfSearch` from the stream are not validated either.
**Fix:** Bound lengths by the remaining input size, use a much smaller
element cap, and validate parameters (`M > 0`, `0 < Ml < 1`) on import.

## P11. [Medium] Invalid HNSW `Ml` causes panics or enormous layer allocation
**Location:** `vec/hnsw/graph.go:321-350` (`maxLevel`, `randomLevel`).
**Problem:** `Ml == 1` divides by `log(1/1) == 0`; the resulting infinite level
is converted to an `int` and `Add` appends layers in a loop, likely OOM.
`Ml > 1` yields a negative denominator. `Add` also panics on a dimension
mismatch and a nil `Distance`.
**Fix:** Validate `Ml` in `0 < Ml < 1` (and `M > 0`) when setting it, return
errors instead of panicking where possible, and add guard tests.

## P12. [High] Unauthenticated KDF parameters can exhaust memory/CPU on `Open`
**Location:** `storm.go:246-267` (`loadOrCreateState` derives keys before
verifying the canary), `meta.go:79-90` (`validateKDFParams` only checks
positivity and `memory >= 8*threads`).
**Problem:** A store's salt/KDF parameters are unauthenticated by necessity,
but a tampered/corrupt metadata record can set `Memory` up to ~4 TiB and
`Time`/`Threads` arbitrarily high. `Open` runs Argon2id with those parameters
*before* the canary check, so an attacker with write access (or a corrupt file)
causes OOM or a CPU hang.
**Fix:** Cap accepted parameters (e.g., memory ≤ 4 GiB, time ≤ 100, threads ≤
64), and reject out-of-range metadata as `ErrCorruptStore` before deriving.

## P13. [Medium] `internal/absfs.extendedFS.Truncate` ignores the requested size
**Location:** `internal/absfs/filesystem.go:434-450`.
**Problem:** The fallback opens the file with `O_WRONLY|O_TRUNC`, closes it and
returns — `size` is never used, so `Truncate(path, 5)` zeroes the file. The
existing test only checks that the call returns nil.
**Fix:** Implement the resize (read, resize, write) or return
`ErrNotImplemented`; add a test asserting the resulting length.

## P14. [Medium] `internal/absfs.extendedFS.ReadFile` silently truncates a growing file and can OOM
**Location:** `internal/absfs/filesystem.go:478-515`.
**Problem:** It pre-allocates `make([]byte, size)` from `Stat` and
`io.ReadFull`s exactly `size` bytes; if the file grows in between, the tail is
silently dropped. A huge reported size causes a huge allocation/panic.
**Fix:** Use `io.ReadAll` (optionally seeded by the size hint) and cap the
pre-allocation.

## P15. [Low] `internal/absfs.extendedFS.RemoveAll` returns an error for a missing path
**Location:** `internal/absfs/filesystem.go:362-432`.
**Problem:** `RemoveAll` opens the path first and returns the `Open` error;
`os.RemoveAll` and `fs.FS.RemoveAll` return nil for a missing path.
**Fix:** Treat `os.ErrNotExist` as success, and close each file once.

---

# 5. Efficiency

## E1. [High] Non-chunked `fs.File.Write` rewrites the whole file on every write
**Location:** `fs/file.go:125-180`; `loadData` (`fs/fs.go:555-600`) decrypts
the entire file, then `Write` re-encrypts and rewrites it.
**Problem:** Streaming writes are O(n²) in total bytes and hold `fs.mu` for
the whole rewrite, serialising all filesystem operations.
**Fix:** Enable chunked storage by default for large files, or implement an
in-place append/overwrite path that only touches the affected range.

## E2. [Medium] Every directory mutation rewrites the whole directory inode
**Location:** `fs/inode.go:66-110` (`GobEncode` includes `Children`),
`fs/fs.go:1273-1315` (`saveParentChild`), `fs/fs.go` `Remove`/`Rename`.
**Problem:** A directory with many children is re-encoded and written on every
create/delete/rename, making directory operations O(children).
**Fix:** Store children in a per-directory bucket (keyed by name) instead of a
single gob blob, or shard the directory; at minimum document the limit.

## E3. [Medium] `db.ReIndex` is O(n²)
**Location:** `db/type_store.go:135-205`.
**Problem:** For each of `total` records it runs
`Select(q.True()).Skip(i).First(...)`, which rescans the bucket from the start
every time (and `Count` also scans). Reindexing 100k records performs ~5×10⁹
row visits.
**Fix:** Iterate the bucket cursor once, decode each record and re-add its
index entries; keep it in the same transaction.

## E4. [Medium] Index maintenance does O(n) scans per save/delete
**Location:** `db/index/unique.go:87-105` (`UniqueIndex.RemoveID` scans the
whole index), `db/type_store.go:330-360` (`save` calls `idx.All(value)` and
walks every ID to test membership), `vec/collection.go:250-280` (Delete gets
the IDs by filter and then calls `DeleteByFilter`, scanning twice).
**Problem:** Deleting one record from a unique index scans every entry (O(n)
per delete). Saving a record into a list index whose value is shared by k
records scans all k IDs and allocates a k-element slice; a bulk import is
O(n·k). Filtered vector deletes scan the index twice.
**Fix:** Keep a reverse ID→value bucket for unique/list indexes (the list index
already has `IDs`), use `IDs.Get(id)` for O(1) membership, and pass the IDs
from `GetIDsByFilter` into the index delete instead of re-scanning.

## E5. [Medium] Retention with a policy rescans/decrypts the series on every insert
**Location:** `ts/series.go:223-229`, `ts/retention.go:211-280`,
`ts/block.go:277-337`.
**Problem:** The zero-policy fast path is fixed, but any non-zero policy makes
`pruneBucket` count/scan the whole series and `pruneBlocks` unseal every block
on every `InsertMany`. Insert cost becomes O(n) (O(n) decryption in block
mode).
**Fix:** Track counts/oldest keys in metadata, prune only when a threshold is
exceeded, and make block pruning touch only the affected blocks. Add a
benchmark asserting that an insert with a satisfied policy does not scan.

## E6. [Medium] `ts.Series.Count` decrypts every block in block mode
**Location:** `ts/series.go:517-549`, `ts/block.go:countBlocks`.
**Problem:** Counting decrypts and unpacks all blocks; O(n) per call.
**Fix:** Persist a point count in series metadata (updated on insert/delete)
or maintain per-block counts in the block header.

## E7. [Medium] `vec` copies each point multiple times on upsert
**Location:** `vec/collection.go:150-195` (`clonePoint` into `batch`),
`vec/flat_index.go:30-45` / `vec/hnsw_index.go:130-175` (clone again), then
`writeBuf` holds the first copy.
**Problem:** Each upsert copies the vector/payload at least twice (three times
for HNSW) and retains a duplicate in `writeBuf`.
**Fix:** Establish a single ownership boundary (e.g., the collection owns the
batch, the index takes references to the batch's immutable points), or make
`clonePoint` optional for internal callers.

## E8. [Low] Unbounded per-process caches
**Location:** `vec/models.go:366` (`regexCache`), `db/q/regexp.go:12`
(`regexpCache`), `ts/store.go:87-105` (`series` map).
**Problem:** Patterns and series names are never evicted; a service that
accepts arbitrary filter patterns/series names grows memory without bound.
**Fix:** Use a bounded LRU or document a persistence/lifecycle requirement.

## E9. [Low] Per-open and per-read traversals/writes in `fs`
**Location:** `fs/tx.go:60-75` (`InodeInit` uses `inodes.Stats()`, which
traverses pages on every open), `fs/fs.go:52-150` (`newFS` rewrites the
encryption marker, and re-encodes the system inodes when `!hadMarker`, on every
open), `fs/fs.go:1000-1020` (`openFile` calls `fs.Umask()`, a separate DB View,
on every create).
**Problem:** Open cost is linear in the number of stored keys, and an
otherwise-read-only open performs writes (so a read-only bbolt handle fails).
Every file creation performs an extra read transaction for the umask.
**Fix:** Persist an "initialized" flag instead of calling `Stats`, write the
marker only when it is missing or the key changed, and cache the umask in the
`FS` (invalidated by `SetUmask`).

---

# 6. API, contracts and documentation

## A1. [High] Moving `absfs` to `internal/absfs` breaks the public import path and leaks internal types into exported APIs
**Location:** `internal/absfs/*`, `fs/walk.go:21` (`Walk(filer absfs.Filer,
...)`), `fs/fastwalk.go:46/51`, `fs/file.go` (methods return `absfs.File`),
`fs/store.go` (`OpenWithContentFS(name string, contentFS absfs.FileSystem)`).
**Problem:** External code importing `github.com/malivvan/storm/fs/absfs` no
longer compiles, and the exported `fs` signatures now name an internal type.
Values can still be passed structurally, but callers cannot declare the types
in their own signatures and pkg.go.dev cannot link them.
**Fix:** Either keep a public `fs/absfs` compatibility package that aliases the
internal types, or move the interfaces into the public `fs` package and keep
one canonical path. Document the breaking change in the README.

## A2. [High] `vec.Store.Close` closes the caller's shared bbolt handle
**Location:** `vec/store.go:426-465`.
**Problem:** In an embedded `storm.Store`, `s.Vec()` returns this store; a user
calling `s.Vec().Close()` closes the handle shared with `db`, `fs` and `ts`,
breaking every sibling. The root `storm.Store.Close` avoids it by calling
`Flush`, but the accessor still exposes the footgun. The doc calls it a
"convenience" while also saying the caller owns the handle.
**Fix:** Make `vec.Store.Close` flush only (like `db`/`fs`/`ts`), or add an
`ownsDB` flag set only by a standalone constructor. Document the ownership
model and add a test that `s.Vec().Close()` does not break `s.DB()`.

## A3. [Medium] `fs.Truncate` creates missing files and handles symlinks inconsistently
**Location:** `fs/fs.go:1203-1230` (creates on `os.ErrNotExist`, uses
`resolve` which does not follow symlinks), `fs/fs.go:350-372` (`ReadFile` does
not follow), `fs/fs.go:1688-1710` (`Chmod` does not follow),
`fs/fs.go:1170-1199` (`Stat` does follow), `fs/file.go` (`openFile` follows).
**Problem:** `os.Truncate` returns `ENOENT` for a missing file and follows
symlinks; here it creates the file and truncates the symlink inode.
`ReadFile`/`ReadDir`/`Chmod`/`Chtimes` operate on the link, while
`Stat`/`OpenFile`/`Chown` follow it.
**Fix:** Define a consistent policy: follow links for `Truncate`, `ReadFile`,
`ReadDir`, `Chmod`, `Chtimes` (as POSIX does), use `Lstat`/`Lchown` for the
no-follow variants, and return `ENOENT` from `Truncate` on a missing path.
Add tests for each API on symlinks.

## A4. [Medium] `fs.MkdirAll` succeeds when the path is an existing non-directory
**Location:** `fs/fs.go:1382-1420`.
**Problem:** `mkdir` returns `os.ErrExist` for any existing child; `mkdirAll`
treats `ErrExist` as success. Reproduced:
```
MkdirAll(/file) where /file is a regular file -> err=nil
```
**Fix:** On `ErrExist`, stat the path and return `ENOTDIR` if it is not a
directory; add a test.

## A5. [Medium] `fs.File.ReadDir(0)` at EOF returns `io.EOF`, contradicting its doc and `Readdir`
**Location:** `fs/file.go:381-420` vs `fs/file.go:270-320`.
**Problem:** `ReadDir` checks `f.diroffset >= len(children)` before `n < 1` and
returns `io.EOF`; `Readdir` returns an empty slice and nil for `n <= 0`.
Reproduced:
```
ReadDir(-1): n=2 err=nil
ReadDir(0) at EOF: n=0 err=EOF
```
**Fix:** Apply the `n <= 0` rule before the EOF check; add a test.

## A6. [Medium] `fs.ReadAt`/`WriteAt` mutate the sequential offset
**Location:** `fs/file.go:114-123` (`ReadAt` sets `f.offset = off` then calls
`Read`), `fs/file.go:182-190` (`WriteAt`).
**Problem:** `io.ReaderAt`/`io.WriterAt` must not change the file offset.
Reproduced: after `ReadAt(buf, 5)`, the offset is 8 and the next `Read` returns
`"89"` instead of continuing from the previous position. Concurrent `ReadAt`
calls also race on `f.offset` (it is written outside `fs.mu`). The generic
`internal/absfs.fileadapter` fallbacks (`fileadapter.go:50-80`) have the same
defect.
**Fix:** Save/restore the offset under the lock (or use a local offset for the
read/write path); add tests that assert the offset is unchanged.

## A7. [Medium] `s3fs.Mkdir` does not reject an existing file, and `File.Close` is not idempotent
**Location:** `fs/s3fs/s3fs.go:292-330` (checks only the `name/` marker),
`fs/s3fs/s3file.go:264-319` (does not reset `writing`/`multipartID`/`parts`).
**Problem:** A file at `name` and a directory marker `name/` can coexist; a
second `Close` of a multipart file re-completes an already completed upload
(or re-uploads a simple object).
**Fix:** `Mkdir` should `HeadObject(name)` as well and return `ErrExist`;
`Close` should be idempotent and clear its state. Add tests.

## A8. [Low] `db` `index` returns slices that alias the bbolt transaction
**Location:** `db/index/unique.go:Get/All`, `db/index/id.go:Get/All`,
`db/index/list.go:All/AllRecords/Range/Prefix` (append cursor values).
**Problem:** The index package is public; outside the owning transaction those
slices are invalid. Internal callers use them within the transaction, but the
API gives no warning.
**Fix:** Document the lifetime, or return copies from public methods (with a
fast internal variant for hot paths).

## A9. [Low] `index.Options` is mutated in place
**Location:** `db/index/*.go` (`opts.Skip--`, `opts.Limit--`).
**Problem:** Reusing an options value across calls silently changes the
pagination.
**Fix:** Copy the options at the start of each operation, or document the
mutation.

## A10. [Low] Error/contract inconsistencies and stale docs
- `vec.NewCollection` accepts `vectorLen <= 0`; unknown `Distance` values are
  silently treated as Cosine (`vec/math.go:CalculateDistance`,
  `vec/hnsw_index.go:NewHNSWIndexWithParams`).
- `vec.matchRegex` silently treats an invalid pattern as "no match"
  (`vec/models.go:matchRegex`); `db/q.Re` returns the error.
- `db.Query.Raw`/`RawEach` nil-kind contract is only a comment (see C9).
- Stale "Copy" artifacts in docs: `internal/absfs/flags.go:58`,
  `internal/absfs/filesystem.go:15/131`, `vec/hnsw/graph.go:229/258`.
- `internal/absfs/flags.go:51` documents `ParseFlags` as returning an
  "`Encrypt` value".
- `vec/quant.go:Quantize` still comments "then shift to [-128, 127]" while the
  code writes `[0, 255]`.
- `fs/file.go:208-220` `Seek` silently ignores an unknown `whence` and clamps
  negative offsets instead of returning `EINVAL`.
- `fs/fs.go:974-990` `Chdir` accepts a file path.
- `db.Find`/`Range`/`Prefix` empty-result semantics differ from `All` (C10).

## A11. [Low] Dead/unused state
- `fs/inodeCache` dirty tracking (`MarkDirty`/`MarkClean`/`GetDirty`/
  `IsDirty`) is only used by tests.
- `db/index.IDIndex.AllRecords` always returns an empty list.
- `vec/hnsw/encode.go` has a TODO about the oversized byte count returned for
  `*int`; the count is not used for seeking, but the contract is confusing.

---

# 7. Repository health

## R1–R3. [Resolved] Repository health

- **R1** — `vec/hnsw/LICENSE` (CC0 1.0) restored; the adapted HNSW code keeps
  its upstream license.
- **R2** — `go mod tidy` applied (`golang.org/x/sys` is now indirect).
- **R3** — CI gained `staticcheck` and `govulncheck` jobs, and the README has a
  "Concurrency and rotation" section documenting the rotation contract.

---

# 8. Missing tests

- **T1 (H1-H5):** concurrent rotation vs. `Save`/`Insert`/`Upsert`/`Flush` for
  `db`, `fs`, `ts`, `vec` under `-race`, plus a cancelled/failed `RotateAll`
  for every store.
- **T2 (C1/C2):** index `Range`/`Prefix`/`AllByIndex` with negative integers,
  floats, and mixed numeric types.
- **T3 (C3):** list-index values and IDs containing `__` (Range, Find, One,
  DeleteStruct).
- **T4 (C4):** `UpdateField` on the primary key.
- **T5 (C5):** quantized collection reopened with and without `UseQuant`.
- **T6 (I1):** encrypted fs/ts/vec/db values truncated to zero bytes; missing
  chunks; tampered markers per sub-store.
- **T7 (I3/I4):** contentFS whose `Close` fails; s3fs `Truncate` on a
  multipart file.
- **T8 (P1-P7):** `MaxInt64`/`MaxInt32` offsets and sizes, `File` after
  `Close`, HNSW `topK = MaxInt`, all-empty `Config.Root`, `s3fs.Truncate(-1)`.
- **T9 (A3/A4/A5/A6):** symlink behavior for `Truncate`/`Chmod`/`ReadFile`,
  `MkdirAll` over a file, `ReadDir(0)` at EOF, `ReadAt`/`WriteAt` offset
  preservation (fs and absfs).
- **T10 (E3/E5):** benchmarks asserting `ReIndex` is linear and a satisfied
  retention policy does not rescan on insert.
- **T11 (ts C6):** `RotateAll` with a cached-only series.
- **T12 (meta P12):** metadata with out-of-range Argon2 parameters must be
  rejected before key derivation.

---

# Suggested fix order

1. **Integrity/races:** H1–H5 (rotation races, adoption window, fs atomic
   rotation). These can silently lose or lock data.
2. **Silent wrong results:** C1–C5 (index ordering, `__` keys, UpdateField ID,
   quantization mode).
3. **Data loss/panics:** I1, I3, I4, I11, P1–P7, P12 (empty ciphertext,
   contentFS close errors, s3fs truncate, torn reads, size panics, vec root
   panic, KDF DoS).
4. **Remaining correctness:** C6–C10, I5–I10, A3–A7.
5. **Efficiency:** E1–E9.
6. **API/docs/repo:** A1, A2, A8–A11, R1–R3, then the missing tests T1–T12.
