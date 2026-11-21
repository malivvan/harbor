# TODO — Storm codebase review

This file is the output of a full-codebase review of `github.com/malivvan/storm` (a
BoltDB-based toolkit with `db` ORM, `fs` filesystem, `ts` time-series and `vec`
vector-search modules plus a shared `internal/encryption` package and a thin top-level
`storm` wrapper).

Scope and method:

- Read directly: `storm.go`, `storm_test.go`, `internal/encryption/*`, `db/{db,store,aead}.go`,
  and the entire `ts` package.
- Reviewed via parallel sub-agents (and then spot-verified against the source for the
  highest-severity claims): `db` (incl. `db/index`, `db/q`), `fs` (incl. `absfs`, `inode`,
  `s3fs`), and `vec` (incl. `hnsw`, `vek`).

Verification status: `go build ./...`, `go vet ./...` and `go test ./...` are all green.
The issues below are therefore defects that the current test suite does **not** exercise;
each entry lists a concrete location and a suggested fix. Severity is High / Medium / Low.

---

## 1. Package `storm` (repository root)

### High

- **Encryption master key is hard-coded, identical for every store**
  — `storm.go:73`
  Problem: `Open` builds `secret := [32]byte{9, 9, 9, …}` and passes the same 32-byte key
  to `db`, `fs`, `ts` and `vec`. Every database ever opened with this package uses the same
  encryption key, so the AEAD/SIV layers provide no confidentiality at all (anyone with the
  library can decrypt any store). This looks like a placeholder.
  Fix: accept a caller-supplied key (e.g. a `Key []byte`/`Secret` parameter on `Open`, or a
  `Config` struct), reject lengths other than 32, and fail loud if encryption is requested
  without a key. Remove the hard-coded constant.
  **Status: FIXED** — `Open` now takes `Config{Secret []byte}`, rejects any length other than
  `db.KeySize`, and the hard-coded constant is gone.

- **Top-level `Get`/`Set` target a bucket that is never created**
  — `storm.go:30-33, 44-71, 132-169`
  Problem: `Open` creates/reads the store-ID bucket under key `[]byte{0}` (`_zero`), but
  `Get` and `Set` look up `tx.Bucket(_null)` where `_null` is `nil`. A nil bucket name is
  never created anywhere, so `Get` always fails with "null bucket not found" and `Set`
  always fails the same way. The plain key/value API is therefore dead on arrival.
  Fix: make `Get`/`Set` use the same bucket key that `Open` creates (e.g. `_zero`), or
  explicitly create a dedicated KV bucket and route both through it. Add a test for
  `Set`→`Get` round-trip.
  **Status: FIXED** — both now address `_zero`, the bucket `Open` creates; round-trip test added.

### Medium

- **`Store.Get` panics on error instead of returning one**
  — `storm.go:132-152`
  Problem: `Get` calls `panic(err)` on *any* `View` error, including the not-found case
  (it returns a sentinel "key not found" error which then panics). Library code should
  never panic for a normal miss, and callers cannot `errors.Is` a panic.
  Fix: change the signature to `Get(key string) (string, error)` (or add a `bool` for
  found) and return errors, or at minimum return `("", false)` for not-found.

- **`Open` leaks the BoltDB handle when a sub-store fails to open**
  — `storm.go:75-107`
  Problem: if `db.Open`/`fs.Open`/`ts.Open`/`vec.Open` returns an error, `Open` returns
  `nil, err` without calling `s.b.Close()`. The caller has no way to close the already-opened
  database.
  Fix: on any sub-store error, `s.b.Close()` before returning.

### Low

- **Dead/confusing package-level bucket vars and unused `Store` fields**
  — `storm.go:20-33`
  Problem: `_null`/`_zero`/`_one`/`_two` are defined but `_one` and `_two` are never used,
  and the sub-stores are opened with inline literals (`[]byte{1}` … `[]byte{4}`) rather than
  these names, so the mapping is inconsistent. `Store.mu` (`sync.RWMutex`) is never used,
  and `Store.ctx` is stored but only read via the sub-store opens.
  Fix: either delete the unused vars/fields or use them consistently; document the bucket-key
  layout in one place.

- **`storm_test.go` is a throwaway demo, not a test**
  — `storm_test.go`
  Problem: it opens `storm.db` in the repository root and never removes it (leaks a binary
  artifact on every run); it uses `log.Fatalf`/`fmt.Println` inside the test; it ignores the
  error from `NewCollection` (`col, _ := …`); and it `defer f.Close()` **and** explicitly
  `f.Close()`s the same file (double close).
  Fix: use `t.TempDir()` + `t.Cleanup`, assert all errors, remove the double close, and drop
  the `Println`/`Fatalf` noise.

---

## 2. `internal/encryption`

The shared cipher primitives (AES-256-GCM `Keychain`, the AES-SIV name cipher, the marker
framing and the rotation helpers) were read in full and found sound: `S2V`/`CMAC`/`dbl` match
RFC 5297, keys are HKDF-domain-separated (each store/domain passes its own `info`), the
canary/marker scheme correctly distinguishes wrong key (`ErrAuthFailed`) from wrong mode
(`ErrWrongMode`), and `Keychain.Keys()`/`NewKeychain` correctly copy key material so a caller's
key slice is never retained or aliased. No correctness findings to fix here; the actionable
issues in this area are the hard-coded key in `storm.go` (section 1) and the per-store races on
the encryption state (sections 4 and 6).

---

## 3. Package `db` (incl. `db/index`, `db/q`)

### High

- **`Query.Raw` / `RawEach` silently ignore all matchers**
  — `db/sink.go:54-56` (raw sink is not a `reflectSink`), surfaced via `db/query.go:162-190`
  Problem: `sorter.filter` only evaluates `q.tree.Match` when the sink implements
  `reflectSink`. The raw sink does not, so `filter` takes the `!ok` branch and adds *every*
  record. `s.Select(q.Eq("Age", 2)).Bucket("Rec").Raw(ctx)` returns all records while the
  equivalent `Find` returns one — matchers (and therefore `Query.Raw`'s documented contract)
  are silently dropped.
  Fix: give `Raw`/`RawEach` a sample/kind parameter so records can be decoded and the matcher
  tree evaluated, or explicitly redesign them as unfiltered bucket dumps and document that.

- **`Query.Delete` deletes keys while iterating the same bbolt cursor**
  — `db/query.go:298-315` (cursor loop) and `db/sink.go:643` (`i.bucket.Delete(i.k)`)
  Problem: the loop walks `bucket.Cursor()` and deletes the current key as it goes. bbolt
  documents that mutating a bucket while traversing its cursor can invalidate the cursor and
  return unexpected keys; the same repository stages deletions before applying them in
  `internal/encryption/rotation.go` precisely because a cursor "must not have entries removed
  while it is being walked".
  Fix: collect matching keys during the scan and delete them after the loop (or use
  `Cursor.Delete` where available and re-verify behavior).

### Medium

- **`Get` panics on a typed-nil pointer destination**
  — `db/kv.go:168` and `db/kv.go:196`
  Problem: `Get` validates only `ref.IsValid()` and `Kind() == Ptr`. For
  `var p *T; Get(ctx, bucket, key, p)`, `ref.Elem()` is the zero `reflect.Value`, so
  `reflect.New(ref.Elem().Type())` panics. `One`/`Find` correctly return
  `ErrStructPtrNeeded` in this case.
  Fix: add `ref.IsNil()` to the validation and return `ErrPtrNeeded`.

- **`BucketNamer` implemented on a pointer receiver is ignored by Save/Init/ReIndex**
  — `db/extract.go:53-63` (`bucketNameFromValue`), also `db/sink.go:402-411`
  Problem: `extract` dereferences the pointer before calling `bucketNameFromValue`, so the
  value passed to the `BucketNamer` assertion is the struct value, not the pointer.
  `func (t *T) StormBucketName()` is never seen, and the static type name is used, while
  `deleteSink`/`countSink` (which use the original pointer) honor the custom name — an
  inconsistency that breaks lookups for runtime-generated/anonymous types.
  Fix: in `bucketNameFromValue`, try both `v.Interface().(BucketNamer)` and, when addressable,
  `v.Addr().Interface().(BucketNamer)`.

- **`IDIndex.Range`/`Prefix` treat nested sub-bucket names as IDs**
  — `db/index/id.go:117` and `db/index/id.go:159`
  Problem: `IDIndex.IndexBucket` is the record bucket, whose cursor also yields `__metadata`
  and `__storm_index_*` entries with `value == nil`. Both loops ignore the value and append
  the key, so broad ranges/prefixes on an ID field return bucket names as IDs; the finder then
  does `bucket.Get(name)` → nil → `ErrNotFound` (reproduced with a string-ID `Range`).
  Fix: capture the value and `continue` when `value == nil` (as `ListIndex` already does).

- **`ListIndex.Add` reuses a bbolt-returned byte slice as a scratch buffer**
  — `db/index/list.go:90-96`
  Problem: `key` comes from `idx.IDs.Get(ctx, targetID)` and may point into bbolt-owned memory
  (mmap or an inline-page clone). The code does `key = key[:0]` and appends the new composite
  key, writing into memory bbolt owns (SIGSEGV reproduced with an equivalent non-inline
  bucket). The current `save` call order hides it, but `ListIndex` is public.
  Fix: never mutate the returned slice; build the new key into a freshly allocated buffer.

- **`ListIndex.Remove` leaves orphaned entries in the nested `storm__ids` bucket**
  — `db/index/list.go:126-157` (line 153)
  Problem: `Remove` deletes the `value + "__" + id` entries from `IndexBucket` but then calls
  `idx.IDs.RemoveID(ctx, value)`. The `IDs` bucket is keyed by target ID (its value is the full
  composite key), so `RemoveID(ctx, value)` compares against the bare field value and removes
  nothing — the reverse mapping is left stale.
  Fix: while collecting keys in the cursor loop, extract `id := k[len(prefix):]` per entry and
  remove by id, instead of `idx.IDs.RemoveID(ctx, value)`.

- **`q.compare` nil-comparison condition has a typo and is asymmetric**
  — `db/q/compare.go:18-21`
  Problem: the second parenthesized condition uses `ak == reflect.Slice` and
  `bk == reflect.Interface` where `bk == reflect.Slice` and `ak == reflect.Interface` were
  intended. Result: `compare(nilPtr, nilSlice)` is true but `compare(nilSlice, nilPtr)` is
  false — `q.Eq` gives inconsistent results depending on argument order.
  Fix: correct to `(bk == reflect.Ptr || bk == reflect.Slice || ak == reflect.Interface || bk == reflect.Invalid)`.

- **`q.compare` mixed signed/unsigned integer comparisons overflow**
  — `db/q/compare.go:27-28` and `db/q/compare.go:48-49`
  Problem: `int64(valb.Uint())` wraps values above `MaxInt64`, and `uint64(valb.Int())` wraps
  negative values to huge unsigned numbers, so `compare(uint(1), int(-1), GTR)` compares
  `1 > MaxUint64` and returns false instead of true.
  Fix: promote both operands to a common signed representation (or `math/big`/`go/constant`)
  with correct sign handling instead of casting through fixed-width types.

- **`Prefix` fallback interpolates the prefix into a regex without escaping**
  — `db/finder.go:675`
  Problem: for unindexed fields `Prefix` builds `q.Re(field, fmt.Sprintf("^%s", prefix))`.
  Regex metacharacters in the prefix change the match, and some prefixes produce invalid
  regexes that error. Users expect literal prefix semantics.
  Fix: use `regexp.QuoteMeta(prefix)` or a dedicated literal prefix matcher.

- **Reverse range scan uses `bytes.HasPrefix` instead of an upper-bound comparison**
  — `internal/internal.go:50-52` (reached via `db/index/list.go:328` and `db/index/unique.go:183`)
  Problem: `RangeCursor.First` in reverse mode steps back only when `!bytes.HasPrefix(k, c.Max)`.
  If `max` is a prefix of the next stored key (max `"abc"`, next `"abcd"`) it wrongly treats
  `"abcd"` as in-range. For list indexes it is worse: when the exact value exists, `Seek("abc")`
  lands on the first `abc__id` entry, so reverse `Range` over multiple IDs for the max value
  skips all but the first.
  Fix: compare with the same `CompareFn` used by `Continue`, e.g.
  `if k != nil && CompareFn(k, c.Max) > 0 { k, v = c.C.Prev() }`.

- **`sorter.compareValue` returns `1` for equal `time.Time` values, breaking reverse `OrderBy`**
  — `db/sink.go:184-193`
  Problem: the `time.Time` branch returns `-1` only when `lt.Before(rt)` and `1` in every
  other case including equality. In a reverse sort this makes both `Less(i,j)` and `Less(j,i)`
  true for equal timestamps, violating the strict weak ordering `sort.Sort` requires.
  Fix: add `if lt.Equal(rt) { return 0 }` before the `Before` check, then return `1` only for
  strictly greater.

### Low

- **`KeyExists` returns `ErrNotFound` when the bucket is absent instead of `(false, nil)`**
  — `db/kv.go:294-297`
  Problem: a boolean "does this key exist" probe naturally reads `false` when the bucket is
  missing; returning `ErrNotFound` forces callers to special-case normal absence.
  Fix: return `(false, nil)` when `getBucket` returns nil.

- **`q.compare` treats nil `*time.Time` as true for every comparison token**
  — `db/q/compare.go:91-103`
  Problem: when either operand is a nil `*time.Time`, the function returns `true`
  unconditionally, so `Gt`/`Lt`/`Gte`/`Lte` all match nil times.
  Fix: handle nil `*time.Time` separately; only equal/not-equal semantics should consider two
  nils equal.

---

## 4. Package `fs` (incl. `absfs`, `inode`, `s3fs`)

### High

- **`File.Truncate` zeroes existing data when growing a file**
  — `fs/file.go:356-362` (the `copy(data, d)` at line 360)
  Problem: when the new size is larger than the current size, the code allocates a zero buffer
  `d` and calls `copy(data, d)` — this copies zeros **into** `data` (the old content) and then
  assigns `data = d`. The result is an all-zero file of the requested size; the original bytes
  are destroyed. Reversed-argument bug (verified in source).
  Fix: `copy(d, data)` (copy old content into the front of the new zero buffer), then `data = d`.

- **`OpenFile` follows symlinks with no cycle/depth limit**
  — `fs/fs.go:883-902` (recursive call at 902)
  Problem: a symlink that points to itself (or a cycle `a -> b`, `b -> a`) recurses without
  bound and panics with a stack overflow. `statWithDepth` guards symlink resolution with
  `maxSymlinkDepth`, but `OpenFile` has no equivalent guard.
  Fix: thread a depth counter through the recursive `OpenFile` call and return `syscall.ELOOP`
  past `maxSymlinkDepth`.

- **`O_TRUNC` does not truncate chunked content**
  — `fs/fs.go:913-925`
  Problem: the truncate branch only writes `b.data.Put(i2b(child.Ino), []byte{})`. Chunked
  files are read via `readChunked`/`readRange`, which use `node.Size` and the `chunks` bucket;
  neither is touched, so opening a chunked file with `O_TRUNC` leaves the old chunks and
  `node.Size`, and reads return the stale pre-truncate content.
  Fix: in the `O_TRUNC` branch, when `fs.chunked() && fs.contentFS == nil`, truncate the chunk
  range (delete all chunk keys) and set `child.Size = 0` before saving the inode.

- **`FileSystem.Truncate` corrupts encrypted (non-chunked) files**
  — `fs/fs.go:1027-1035`
  Problem: `b.data.Get(key)` returns the **sealed ciphertext** when content encryption is on.
  The code copies those encrypted bytes into a `size`-length buffer and puts them back without
  decrypting or re-sealing. The next read will try `openContent` on the truncated ciphertext and
  fail authentication.
  Fix: read via `loadData` (decrypt), resize in plaintext, then write via `saveData`
  (re-encrypt), or decrypt/re-encrypt inside the transaction.

- **`Copy` copies only the inode, not the file's data**
  — `fs/fs.go:726-751`
  Problem: `Copy` creates `copyInode(srcChild)` and links it, but never copies the data bucket,
  chunk keys, symlink target, or external contentFS file. The destination exists with the
  source's `Size` but its content reads empty/zero. For directories it shallow-copies
  `Children` pointing at the same inodes without bumping their link counts, so removing a child
  from the copy can delete data still referenced by the source.
  Fix: after allocating the new inode, copy `data`/`chunks`/`symlinks`/contentFS content from
  `srcChild.Ino` to the new ino; for directories, recursively copy and bump `Nlink` of the
  copied children.

- **Deleting an inode leaks chunk and symlink data**
  — `fs/fs.go:1122-1136` (`deleteInode`) and `fs/fs.go:1334-1356` (`RemoveAll`)
  Problem: `deleteInode` deletes only the `inodes` key and (when `contentFS == nil`) the `data`
  key; it never deletes keys from the `chunks` or `symlinks` buckets. `RemoveAll` has the same
  omission. On a chunked filesystem every removed file leaves its chunks behind permanently;
  removed symlinks leave their target strings.
  Fix: in both paths, delete the inode's chunk keys (iterate the `chunkKey(ino,0)` prefix) and
  `symlinks.Delete(i2b(ino))`.

- **Snapshots return empty files when an external contentFS is configured**
  — `fs/snapshot.go:178-196` (`ReadFile`) and `fs/snapshot.go:378-397` (`Open`)
  Problem: these always read `s.bucket.data.Get(i2b(node.Ino))` and ignore `fs.contentFS`. When
  content is stored externally the bbolt `data` bucket is empty, so every snapshot read yields
  `[]byte{}`.
  Fix: route snapshot content reads through the same `loadData`-style logic that consults
  `fs.contentFS` (and chunked storage) before falling back to the data bucket.

- **s3fs `Seek` has no effect on subsequent `Read`/`Write`**
  — `fs/s3fs/s3file.go:109-126` and `fs/s3fs/s3file.go:280-293`
  Problem: `Read` streams `f.body` from the beginning and never uses `f.offset`; `Write` always
  appends to `f.buffer` and ignores `f.offset`. A `Seek(0, io.SeekStart)` followed by `Write`
  appends at EOF instead of overwriting at offset 0, corrupting the file, and makes the
  `O_APPEND` distinction meaningless.
  Fix: implement `Write`/`Read` against `f.offset` (write into `f.buffer[offset:]` with
  zero-fill extension; read from the cached buffer at `f.offset`); set offset from the end only
  when `append` is true.

### Medium

- **`O_TRUNC` and `FileSystem.Truncate` ignore external contentFS storage**
  — `fs/fs.go:913-925` and `fs/fs.go:1013-1036`
  Problem: with `contentFS != nil`, file bytes live in `inoToPath(ino)` files, not the bbolt
  `data` bucket. Both truncate paths only touch the `data` bucket (and inode size), so a
  shorter subsequent write leaves trailing stale bytes and `Truncate` has no effect on the real
  content.
  Fix: when `contentFS != nil`, perform the truncate against the contentFS object
  (rewrite/delete as appropriate).

- **`Chmod` replaces the whole mode, including type bits**
  — `fs/fs.go:1457` (`node.Mode = mode`)
  Problem: assigning the caller-provided mode verbatim can clear `os.ModeDir` (and other type
  bits); e.g. `Chmod("/", 0644)` turns the root directory into a regular file.
  Fix: `node.Mode = (node.Mode & os.ModeType) | (mode &^ os.ModeType)`.

- **`Chown` resolves relative symlinks against the wrong base path**
  — `fs/fs.go:1442-1443` (`link = path.Join(name, link)`)
  Problem: for a relative symlink target the base should be the directory containing the
  symlink (`path.Dir(name)`), as `statWithDepth` does. Joining with the full `name` yields
  paths like `/d/link/x` instead of `/d/x`, so `Chown` on a relative symlink usually fails
  with ENOENT.
  Fix: `link = path.Join(path.Dir(name), link)`.

- **`Chown` masks save errors with a nil inner error**
  — `fs/fs.go:1421-1423`
  Problem: on `saveInode` failure the code sets `pathErr.Err = nil` and returns `pathErr`, so
  the caller gets a non-nil `*os.PathError` whose `Err` is nil (misleading empty message,
  `errors.Is` impossible).
  Fix: `pathErr.Err = err`.

- **`O_APPEND` is silently ignored by the bbolt filesystem**
  — `fs/file.go:117-163` and `fs/fs.go:846`
  Problem: `OpenFile` accepts the flag but neither initializes `f.offset` to the current size
  nor makes `Write` seek to EOF, so writes on an `O_APPEND` handle overwrite from offset 0.
  Fix: in `OpenFile`, when `O_APPEND` is set, set `File.offset = child.Size`; and/or honor the
  flag in `Write`.

- **`RemoveAll` returns an error for a non-existent path**
  — `fs/fs.go:1251-1273`
  Problem: the documented contract (and `os.RemoveAll`) is to return `nil` when the path does
  not exist; here `RemoveAll("/nonexistent")` returns the ENOENT from `Lstat`.
  Fix: detect `os.IsNotExist` from `Lstat` and return `nil`.

- **Data race on `fs.enc` during key rotation**
  — `fs/store.go:318-325` (`adoptEnc` writes `filesystem.enc = next`) vs reads in
  `fs/chunk.go:22-24`, `fs/fs.go:527`, etc.
  Problem: `adoptEnc` mutates each live `FileSystem.enc` under `Store.mu`, but `FileSystem`
  methods read `fs.enc` with no synchronization. Concurrent `ReEncryptFS`/`RotateAll` with
  file I/O is a data race and can observe a half-switched encryption state.
  Fix: store `enc` in an `atomic.Pointer[encState]` (or protect it with the same mutex) and
  load it once per operation.

- **Inode read-modify-write has no synchronization; concurrent operations lose updates**
  — `fs/fs.go:1042-1110` (`loadParentChild`+`saveParentChild`) and `fs/fs.go:850-936` (`OpenFile`)
  Problem: public operations load inodes in one transaction, mutate the in-memory copy, and save
  it in a second transaction with no lock around the cycle. Two concurrent `Mkdir`/`Create` in
  the same directory each save their own copy of the parent, so the second save silently drops
  the first child. The same applies to concurrent `Write` on the same inode via separate handles.
  Fix: add per-filesystem (or per-inode) locking around load-modify-save, or use a
  transaction-level compare-and-swap.

- **Negative `size` panics in `FileSystem.Truncate`**
  — `fs/fs.go:1030` (`make([]byte, int(size))`) and `fs/file.go:356-362`
  Problem: `make`/slice expressions panic on negative length; `Truncate` should reject
  `size < 0` with an error instead.
  Fix: return `os.ErrInvalid`/`syscall.EINVAL` for negative sizes before allocating.

- **Negative offsets panic in `File.ReadAt`/`File.WriteAt`**
  — `fs/file.go:109-114` and `fs/file.go:168-171`
  Problem: both methods assign `f.offset = off` then call `Read`/`Write`, which slice
  `data[f.offset:]`. A negative `off` produces a negative slice index and a runtime panic.
  Fix: validate `off >= 0` and return `os.ErrInvalid` before touching the offset.

- **`inode.Inode.Rename` links a potentially stale source node**
  — `fs/inode/inode.go:441-501`
  Problem: `Rename` resolves `srcNode` before acquiring the directory locks; another goroutine
  can replace/remove the source entry in between. After locking, the code only re-checks that
  an entry with `srcName` exists and then links the **old** `srcNode` while decrementing the
  current entry's inode — linking an inode that is no longer referenced and decrementing the
  wrong link count.
  Fix: after locking, use `srcEntry.Inode` as the node to link (not the pre-lock `srcNode`).

- **`inode.Inode.UnlinkAll` leaks link counts for `..`**
  — `fs/inode/inode.go:359-381` (explicit `continue` at 371-372)
  Problem: `UnlinkAll` clears the directory but skips the `..` entry without ever calling
  `countDown` on its target. Since `NewDir` `countUp`s both `.` and `..`, clearing a directory
  leaves the `..` target's `Nlink` permanently inflated by one per directory.
  Fix: decrement the `..` target's link count too (unless it is a self-reference already
  handled by `.`), or stop managing `.`/`..` as counted entries.

- **s3fs `WriteAt`/`Truncate` panic on negative offsets/sizes**
  — `fs/s3fs/s3file.go:201-210` (`WriteAt`) and `fs/s3fs/s3file.go:320-327` (`Truncate`)
  Problem: negative `off`/`size` reach negative slice indices / negative `make` length and
  panic.
  Fix: validate `off >= 0` and `size >= 0` at the top of each method.

### Low

- **`inode.Walk` visits `.` and `..` entries and has no cycle guard**
  — `fs/inode/testutils.go:10-40`
  Problem: the callback is invoked for `/a/.` and `/a/..` before the suffix check returns, and
  there is no visited-set/ancestor check, so a directory entry pointing at an ancestor recurses
  without bound.
  Fix: skip `.`/`..` before calling `fn`, and add a depth or visited-set guard.

- **s3fs `fileInfo.Mode` reports `0644` even for directories**
  — `fs/s3fs/s3fs.go:522`
  Problem: directory entries from `Stat`/`ReadDir` have `IsDir() == true` but `Mode()` returns
  `0644` (no `os.ModeDir`), so `info.Mode()&os.ModeDir` misclassifies them.
  Fix: return `os.ModeDir | 0755` when `fi.isDir`.

- **s3fs `sanitizePath` traversal rejection is ineffective**
  — `fs/s3fs/s3fs.go:101-121`
  Problem: because `path.Clean("/" + p)` always begins with `/`, the
  `strings.HasPrefix(clean, "..")` check can never fire; `"../../secret"` is silently collapsed
  to `"secret"` instead of being rejected — the traversal detection is dead code.
  Fix: validate the original input for `..` components before cleaning, or compare the cleaned
  result against the intended root using `path.Rel`.

- **`FastWalkConfig.FollowSymlinks` is ignored**
  — `fs/fastwalk.go:29-33` and `fs/fastwalk.go:221-224`
  Problem: the exported `FollowSymlinks` option is never read; only the `TraverseLink` sentinel
  is honored, so users who set the option get no traversal.
  Fix: in `readDir`, treat `config.FollowSymlinks` as true when deciding whether to enqueue
  symlink entries as directories.

---

## 5. Package `ts`

### Medium

- **`pruneBucket` deletes keys while iterating the same cursor**
  — `ts/retention.go:248-256`
  Problem: the retention pass walks `b.Cursor()` and calls `b.Delete(key)` for each pruned key
  inside the loop, the same mutate-while-traversing pattern flagged for `db.Query.Delete` above.
  It happens to work for a forward scan today but is fragile and inconsistent with the
  stage-then-apply approach the repo uses elsewhere (`encryption.ApplyEntries`, `DeleteRange`).
  Fix: collect the keys to delete first, then delete them after the cursor loop.

- **`Store.SetNow` / `Store.now` is unsynchronized**
  — `ts/store.go:16, 187-195` and `ts/retention.go:197-202`
  Problem: `SetNow` writes `s.now` while `nowNanos()` (invoked from `InsertMany` → retention
  pruning) reads it with no synchronization. The doc says "configure before concurrent use",
  but a mis-timed `SetNow` is an unsynchronized data race.
  Fix: guard `now` with an `atomic.Pointer`/`atomic.Value`, or document and enforce that it must
  be set once before any concurrent use.

### Low

- **`unpackBlock` has a dead `count < 0` check**
  — `ts/block.go:41-43`
  Problem: `count` is `int(binary.LittleEndian.Uint32(...))`; on 64-bit targets it can never be
  negative, so the `count < 0` clause is dead code (it only matters on 32-bit, and even then the
  size check covers it).
  Fix: drop the redundant clause or use an explicit `uint32`→`int` overflow guard.

- **`Aggregate` (block mode) materializes the whole range before aggregating**
  — `ts/aggregate.go:131-137`
  Problem: when block mode is on, `Aggregate` calls `se.Range` (collecting every point into a
  slice) and then aggregates, rather than streaming. For large windows this doubles memory and
  defeats the point of the block encoding.
  Fix: stream the aggregation over `scanBlocks` as the non-block path already does.

---

## 6. Package `vec` (incl. `hnsw`, `vek`)

### High

- **`heap.Max`/`heap.PopLast` remove an arbitrary leaf, not the maximum**
  — `vec/hnsw/heap/heap.go:73-75` and `vec/hnsw/heap/heap.go:88-91`
  Problem: the heap is a min-heap (smallest distance at index 0), but `Max()` returns
  `data[len-1]` and `PopLast()` removes that last element. In a min-heap the last element is
  not the largest, so when `layerNode.search` evicts the "worst" candidate or trims the candidate
  list (`vec/hnsw/graph.go:128-131`, `134-137`) it evicts an arbitrary leaf instead of the
  farthest node. The retained k-nearest set is therefore wrong (verified with a small
  `container/heap` reproduction).
  Fix: implement `Max()`/`PopLast()` by scanning for the true maximum (or maintain a max-heap /
  use `container/heap` against negated distances).

- **HNSW search returns results in heap-array order, not sorted by distance**
  — `vec/hnsw/graph.go:147` and `vec/hnsw/graph.go:467-474`
  Problem: `search()` returns `result.Slice()`, the underlying min-heap array, which is not
  sorted (only index 0 is the minimum). `HNSWIndex.Search` then truncates in that arbitrary
  order (`vec/hnsw_index.go:178-181`), so the returned top-K are not the true top-K and their
  ordering is wrong.
  Fix: sort `result.Slice()` by `dist` before returning (or drain the heap into a sorted slice).

- **Replacing a node can drop the newly computed higher layers**
  — `vec/hnsw/graph.go:400-404`
  Problem: in `Graph.Add`, when a key already exists in the current layer the code calls
  `g.Delete(key)`, which removes that key from *every* layer. If the new node was already
  inserted into higher layers (when `insertLevel` exceeds the old node's max level), this wipes
  those just-inserted high-layer nodes and only re-inserts the current and lower layers, losing
  layers above the old height and breaking edges the high-level insert had just created.
  Fix: delete the old node from each layer once before the insertion loop (or remove only the
  old node in the current layer), then insert at all levels `0..insertLevel` without re-deleting.

- **Range filter silently matches payload values it does not understand**
  — `vec/models.go:137-194` (`matchRange`)
  Problem: `matchRange` only handles `float64` and `int`; for any other payload type (`string`,
  `int64`, `float32`, `bool`, …) the switch falls through to `return true`. A range condition on
  a string, or on a persisted `int64` (which `fromStoredPoint` returns as `int64`), therefore
  matches every point that has the key, producing wrong filtered search/delete results
  (verified in source).
  Fix: add `default: return false` (and add `int64`/`float32` cases), or normalize payload
  numbers to `float64` before comparison.

- **`replenish` always uses cosine distance regardless of the graph metric**
  — `vec/hnsw/graph.go:167`
  Problem: `layerNode.replenish` hard-codes `CosineDistance` when choosing replacement
  neighbors. For Euclidean and Dot graphs, neighbor eviction/delete repair (`addNeighbor`
  line 66, `isolate` line 180) selects edges with the wrong metric, corrupting graph quality
  (and for Dot/Euclid potentially ranking edges backward).
  Fix: pass the graph's `Distance` function into `replenish`/`addNeighbor`/`isolate` instead of
  hard-coding `CosineDistance`.

### Medium

- **Zero vectors produce NaN cosine distances in the HNSW graph**
  — `vec/hnsw/distance.go:15`
  Problem: `CosineDistance` is `1 - vek32.CosineSimilarity(a,b)` and
  `vek32.CosineSimilarity` computes `dot / sqrt(normA*normB)` with no zero-norm guard, yielding
  NaN for zero vectors. NaN breaks worst-neighbor selection and violates the heap `Less`
  ordering, so graphs containing zero embeddings get arbitrary neighbor sets and results. The
  flat path guards zero norms (`vec/math.go:60-62`), the HNSW path does not.
  Fix: make `hnsw.CosineDistance` return a defined value (e.g. 1.0) when either norm is zero,
  or fix `vek32.CosineSimilarity` to guard zero norms.

- **`topK <= 0` is unvalidated: negative topK panics and topK 0 returns a result in HNSW**
  — `vec/collection.go:182-190`, `vec/flat_index.go:69-73`, `vec/hnsw_index.go:143-150`
  Problem: `FlatIndex.Search` does `results[:topK]` (negative panics); HNSW passes `topK` into
  `layerNode.search`, where `make([]searchCandidate, 0, k)` panics for negative `k` and for
  `k == 0` still seeds one result and returns it. Nothing in `Collection.Search` validates
  `topK`.
  Fix: reject `topK <= 0` in `Collection.Search` (or clamp) before dispatching to the index.

- **Exact filter matching is type-fragile for persisted numeric payloads**
  — `vec/models.go:116-117` with `vec/store.go:99-101`
  Problem: `matchCondition` uses Go interface equality (`val == cond.Match.Value`).
  `toStoredPoint` stores `int`/`int64` as `KindInt` and `fromStoredPoint` returns them as
  `int64`, so a filter built with `MatchValue{Value: 5}` (`int`) matches in memory but stops
  matching after a restart because `int64(5) != int(5)`.
  Fix: compare numerics by converting to a common numeric type (e.g. `float64`/`int64`), or
  normalize payload numbers consistently at ingestion.

- **Stale empty layers cause nodes to be inserted above their assigned level**
  — `vec/hnsw/graph.go:363-367`
  Problem: the empty-layer branch inserts the new node unconditionally, without checking
  `insertLevel >= i`. After `Delete` has emptied some layers (`Delete` never truncates
  `g.layers`), the next `Add` places a node into those empty top layers even when its
  `randomLevel` is lower, violating the level distribution and making searches start from
  artificially high layers.
  Fix: guard the empty-layer insertion with `insertLevel >= i` (or prune empty layers in
  `Delete`).

- **`Open(ctx, nil, nil, …)` returns a Store whose methods panic with nil DB**
  — `vec/store.go:199-206` and `vec/store.go:357-364`
  Problem: `Open` returns a usable-looking store when `bdb == nil`, but `EnsureCollection`
  (and `Close`, `UpsertPoints`, `LoadCollection`, …) call `s.db.Update`/`s.db.Close` with no nil
  check, so `NewCollectionWithParams` → `EnsureCollection` panics.
  Fix: either reject nil DB in `Open`, or guard every DB access (return "storage not open").

- **`fromStoredPoint` JSON-decodes raw byte payloads, changing their type**
  — `vec/store.go:106-115`
  Problem: any `KindBytes` value is first tried with `json.Unmarshal`, so a stored
  `[]byte("123")` comes back as `float64(123)` and `[]byte("\"x\"")` as `string`.
  `toStoredPoint` also routes all complex types (maps/slices) through JSON into `KindBytes`, so
  nested integers become `float64` on reload. Payload type round-trips are not preserved, which
  breaks exact/range filters after a restart.
  Fix: add a distinct kind for JSON vs raw bytes (or wrap raw bytes), and preserve integer types
  for JSON-serialized complex values.

- **Import fills missing neighbor references with nil, enabling later nil-pointer panics**
  — `vec/hnsw/encode.go:247-255`
  Problem: after reading each layer, `node.neighbors[key] = nodes[key]` blindly assigns the map
  lookup. If the encoded file references a neighbor key not present in that layer, the neighbor
  becomes `nil`; later `addNeighbor`/`search`/`isolate` dereference `neighbor.neighbors`/
  `neighbor.Value` and panic. The import path returns no error for this corrupt/mismatched input.
  Fix: after filling, verify every neighbor pointer is non-nil and return an error otherwise.

- **`Graph.Dims` panics on an empty graph with leftover layers**
  — `vec/hnsw/graph.go:320-325`
  Problem: `Dims()` does `len(g.layers[0].entry().Value)` without checking that `entry()` is
  non-nil. After all nodes are deleted (layers remain), or after importing empty layers,
  `entry()` returns nil and `Dims` nil-dereferences. `assertDims` handles this; `Dims` does not.
  Fix: return 0 when `g.layers[0].entry() == nil`.

- **`PointStruct.Version` is never persisted and never returned in search results**
  — `vec/point.go:74-78`, `vec/store.go:35-39`, `vec/flat_index.go:55-59`, `vec/hnsw_index.go:172-176`
  Problem: `StoredPoint` has no `Version` field and `toStoredPoint` drops it, so versions set by
  `Collection.Upsert` are lost on reload. Both index `Search` implementations also leave
  `ScoredPoint.Version` at 0.
  Fix: add `Version` to `StoredPoint` (and regenerate `binco.gen.go`), copy it in
  `toStoredPoint`/`fromStoredPoint`, and set it in search results.

- **`NewCollectionWithParams` overwrites collection metadata before validating loaded points**
  — `vec/collection.go:70-100`
  Problem: `SaveCollectionMeta` is called before `LoadCollection`/dimension validation. If a
  caller opens an existing collection with the wrong `vectorLen` (or loading otherwise fails),
  the on-disk metadata has already been replaced with the incorrect configuration.
  Fix: load and validate points first, then persist metadata (or persist metadata only when the
  collection is newly created).

- **`EfConstruction` and `K` HNSW params are silently ignored**
  — `vec/hnsw_index.go:92-99`
  Problem: `NewHNSWIndexWithParams` applies only `M` and `EfSearch`.
  `params.EfConstruction` is never used (construction uses `g.EfSearch` in `Graph.Add`), and
  `params.K` is never used, so callers tuning construction get different behavior than requested.
  Fix: add a construction `ef` field to the graph and wire `EfConstruction` into `Add`; validate
  or document `K`.

- **HNSW upsert mutates the points map before the graph add can fail**
  — `vec/hnsw_index.go:119-126`
  Problem: `h.points[p.ID] = p` is set for all points before `h.graph.Add(nodes...)`. If
  `graph.Add` panics (dimension mismatch via `assertDims`, or a nil/panic in search), the points
  map contains IDs not in the graph, so later `Search` silently skips them and `Count` disagrees.
  Fix: build nodes first, add them to the graph, and only update `h.points` after insertion
  succeeds (or validate dimensions in `Upsert` and return an error instead of panicking).

- **`Collection.Delete` drops buffered points before the storage delete succeeds**
  — `vec/collection.go:243-259`
  Problem: `writeBuf` is filtered first, then `storage.DeletePoints` runs. If the storage delete
  fails, the function returns an error but the buffer no longer contains those points while the
  memory index still does; a later flush won't rewrite them, so disk and memory diverge after a
  transient bbolt error.
  Fix: delete from storage and index first; filter `writeBuf` last (or only after all deletions
  succeed).

### Low

- **Unsynchronized `Store.closed` flag**
  — `vec/store.go:27` and `vec/store.go:343-353`
  Problem: `closed` is read in every Store method and written in `Close` with no mutex/atomic.
  Concurrent `Close` with `UpsertPoints`/`EnsureCollection`/`LoadCollection` is a data race
  (bbolt itself is safe, the flag is not).
  Fix: guard with `sync.Mutex`/`atomic.Bool`.

- **SQ8 quantizer mangles non-finite values**
  — `vec/quant.go:50-72`
  Problem: for vectors containing `+Inf`/`-Inf`, `range_` becomes `+Inf` and `scale` becomes 0,
  so `(v-min)*scale` yields NaN for finite elements; for NaN vectors every byte becomes 0.
  Dequantization then returns NaN for all components, silently destroying the vector.
  Fix: reject or sanitize non-finite values (or define fallback scale/min) before quantizing.

- **HNSW import panics on negative encoded lengths**
  — `vec/hnsw/encode.go:41-42` and `vec/hnsw/encode.go:53-54`
  Problem: `binaryRead` decodes a varint length and immediately does `make([]byte, ln)` /
  `make([]float32, ln)` without checking `ln >= 0`; a malformed file triggers a runtime panic
  instead of an error.
  Fix: validate `ln` (e.g. `ln >= 0` and a sane cap) and return an error.

- **`GetPointsByFilter` returns shallow copies (shared vectors/payload maps)**
  — `vec/flat_index.go:115-121`, `vec/hnsw_index.go:229-235`
  Problem: both index implementations copy the `PointStruct` struct but not its `Vector` slice
  or `Payload` map, so callers mutating a returned point silently mutate the index.
  Fix: deep-copy `Vector` and `Payload` (or document that callers must not mutate).

- **`DeleteByFilter(nil)` deletes every point in the index implementations**
  — `vec/flat_index.go:125-136`, `vec/hnsw_index.go:240-257`
  Problem: `MatchFilter` treats a nil filter as "match all", so calling the index-level
  `DeleteByFilter` directly with nil removes the whole index. `Collection.Delete` guards against
  this, but the index-level API does not.
  Fix: reject nil filters in `DeleteByFilter` (return an error) to match the destructive
  semantics.

- **Distance functions iterate only over `a`, panicking on length mismatch**
  — `vec/math.go:42-47`, `vec/math.go:53-59`, `vec/math.go:69-77`
  Problem: `dotProduct`/`cosineSimilarity`/`euclideanDistance` all use `for i := range a` and
  index `b[i]`. `CalculateDistance` is public with no length check, so mismatched slices panic.
  Fix: check `len(a) == len(b)` in `CalculateDistance` and return an error/defined value.

- **`hnsw.RegisterDistanceFunc` / `distanceFuncs` map has no synchronization**
  — `vec/hnsw/distance.go:29-49`
  Problem: `RegisterDistanceFunc` writes to the global `distanceFuncs` map while
  `distanceFuncToName`/`Import` read it; concurrent registration and export/import is a data
  race and can panic.
  Fix: guard the map with a `sync.RWMutex`.

---

## 7. Cross-cutting / maintainability

### Medium

- **Large amounts of vendored/copied third-party and stdlib code live under `internal/`**
  — `internal/math32/*` (~60 files, a full float32 port of Go's `math`), `internal/partial/*`
  (a copied third-party top-k package with its own LICENSE/README), `internal/rand/*` (a
  `math/rand` clone), `internal/slices/*` and `internal/constraints/*` (Go stdlib backports).
  Problem: this is ~90 files of duplicated code that must be maintained by hand and will drift
  from upstream; on a Go 1.27 toolchain `slices`/`constraints` (and most of `rand`) already
  exist in the standard library.
  Fix: replace `internal/slices`/`internal/constraints`/`internal/rand` with stdlib equivalents,
  and evaluate whether `internal/math32` should move to a maintained dependency or be dropped in
  favor of the `vek`/`vek32` path that the vector store actually uses.

### Low

- **No `go test -race` coverage in the default loop, and the suite does not exercise the buggy paths**
  — (repository-wide)
  Problem: `go test ./...` is green while the file lists multiple data races and correctness
  bugs (HNSW ordering, `matchRange`, `Truncate`, `q.compare`, `Query.Raw`), because the tests
  check round-trips and counts but not ordering, type round-trips, negative inputs, symlink
  cycles, or concurrency.
  Fix: add targeted tests alongside each fix, and run `go test -race ./...` (at least for
  `db`, `fs`, `vec`, `ts`) in CI so the races flagged above are caught.

---

## Suggested fix order

1. `storm.go` hard-coded key and broken `Get`/`Set` (security + dead API).
2. `fs`: `File.Truncate` reversed `copy`, chunked/external `O_TRUNC`/`Truncate`, symlink-cycle
   guard, `Copy`/`deleteInode`/`RemoveAll` data leaks (data corruption/loss).
3. `vec`: heap `Max`/`PopLast` and unsorted search results, `matchRange` default-`true`,
   hard-coded cosine in `replenish` (wrong search results).
4. `db`: `Query.Raw` matcher skip and `Query.Delete` cursor deletion (silently wrong results),
   then the `index`/`q.compare` correctness bugs.
5. The `ts` retention cursor-mutation and `SetNow` race.
6. The Medium/Low robustness items (nil/negative/type checks) and vendored-code consolidation.
