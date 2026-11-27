# PLAN — Zstd compression & deduplication for tempest

This document is a complete, executable implementation plan. It is written for a
coding agent (e.g. Reasonix + deepseek-flash) to follow step by step. Each phase
names files, function signatures, on-disk invariants, and the tests that prove
it. Verify with the commands in [Verification](#verification) before marking any
phase done.

**Algorithm constraint:** compression is built entirely on **Zstandard**
(`github.com/klauspost/compress/zstd`). No gzip/lz4/brotli/snappy. The
`tmp/` reference implementation (`absfs/compressfs`) is only a design reference
for the *files* store; it is not imported, not vendored, and must not be a
dependency. It will be deleted in the next step.

---

## Contents

1. [Scope and goals](#scope-and-goals)
2. [Design decisions and invariants](#design-decisions-and-invariants)
3. [Phase 0 — `internal/compress` (Zstd codec + envelope)](#phase-0--internalcompress-zstd-codec--envelope)
4. [Phase 1 — record store compression](#phase-1--record-store-compression)
5. [Phase 2 — fstree compression](#phase-2--fstree-compression)
6. [Phase 3 — series compression](#phase-3--series-compression)
7. [Phase 4 — vector compression](#phase-4--vector-compression)
8. [Phase 5 — search compression](#phase-5--search-compression)
9. [Phase 6 — graphs compression](#phase-6--graphs-compression)
10. [Phase 7 — object compression](#phase-7--object-compression)
11. [Phase 8 — fstree content-addressed chunk deduplication](#phase-8--fstree-content-addressed-chunk-deduplication)
12. [Phase 9 — object blob deduplication](#phase-9--object-blob-deduplication)
13. [Phase 10 — root package wiring (`tempest.Options`)](#phase-10--root-package-wiring-tempestoptions)
14. [Documentation updates](#documentation-updates)
15. [Verification](#verification)
16. [Risks, trade-offs, open decisions](#risks-trade-offs-open-decisions)

---

## Progress

- [x] Phase 0 — internal/compress (Zstd codec + envelope)
- [x] Phase 1 — record store compression
- [x] Phase 2 — fstree compression
- [x] Phase 3 — series compression
- [x] Phase 4 — vector compression
- [x] Phase 5 — search compression
- [x] Phase 6 — graphs compression
- [ ] Phase 7 — object compression
- [ ] Phase 8 — fstree content-addressed chunk deduplication
- [ ] Phase 9 — object blob deduplication
- [ ] Phase 10 — root package wiring
- [ ] Documentation updates

---

## Scope and goals

Add **Zstd compression** and **deduplication** to tempest, applied to every
store, without breaking existing on-disk data.

- **Compression** applies to every sub-store (`record`, `fstree`, `series`,
  `vector`, `search`, `graphs`, `object`). It is transparent and self-describing
  (see below), so existing stores open unchanged and no on-disk format version
  is bumped.
- **Deduplication** applies where content is large and repeated: the **files**
  store (content-addressed chunks) and the **object** store (content-addressed
  blobs). It is opt-in and, because it changes key layout, it is recorded in the
  store's marker and fails closed on mismatch — exactly like `fstree.ChunkSize`
  already works today.
- **Records, series, vector, search, graphs** get compression only; their values
  are small/heterogeneous and content-addressed dedup would add refcounting
  overhead for negligible benefit. This is a deliberate scope boundary (see
  [decisions](#design-decisions-and-invariants)).

---

## Design decisions and invariants

These are fixed up front so the implementing agent does not re-derive them.

1. **One shared codec package.** `internal/compress` (new) owns the Zstd
   encoder/decoder pooling and the on-disk envelope. Every store links against
   it; no store implements its own compression framing.
2. **Compression is self-describing, not mode-gated.** Compressed values carry a
   versioned envelope (`ZCMP` magic + version + mode + original length + zstd
   frame). The read path *always* decodes the envelope when the magic is
   present and passes raw bytes through otherwise. Consequences:
   - No marker/format change, no version bump, no "wrong mode" error: a store
     written compressed is readable by a build opened with compression disabled,
     and vice versa.
   - `Config.Compress` controls the **write** path only (whether new values are
     emitted as envelopes). It is a write-time preference, not a persisted
     requirement. Users should keep it consistent across opens for predictable
     space savings, but correctness never depends on it.
   - The envelope is **only emitted when it actually shrinks the value**
     (compressed size + header < plaintext size). Small or incompressible
     values are stored as raw plaintext with no envelope, so there is **no
     space regression** — critical for series 8-byte points and small records.
3. **Compression happens on the plaintext side of encryption.** The pipeline is
   `marshal → compress → seal` on write and `open → decompress → unmarshal` on
   read. Compression is therefore invisible to key rotation, `VerifyAll`, the
   encryption canary, and every marker: those layers continue to operate on
   sealed ciphertext and never need to know about compression. **Do not
   compress ciphertext** (compress-then-encrypt, never encrypt-then-compress).
4. **Deterministic-encrypted names are never compressed.** Record primary keys,
   index values, series names, vector point IDs / collection names, and search
   term keys are AES-SIV encrypted for exact lookup; compressing them would
   break lookups and add nothing. Only *value* payloads are compressed.
5. **Dedup is content-addressed on plaintext.** The dedup key is the SHA-256 of
   the *plaintext* chunk/blob, so identical plaintext maps to one stored value
   regardless of encryption key, compression level, or nonce. The stored value
   is `seal(compress(plain))` (a random-nonce seal, so equal plaintext does not
   produce equal ciphertext). Refcounts track live references; all refcount and
   blob mutations happen in one bbolt transaction so no orphaned blobs can
   accumulate.
6. **Dedup changes key layout → marker flag, fail closed.** Dedup is recorded as
   a flag in the store's existing marker (like `fstree`'s `markerFlagChunked`
   already records the data-vs-chunks layout) and `Open` fails closed on
   mismatch. Existing stores have the flag clear and are unaffected.
7. **Dedup requires chunked content for fstree.** `Config.Dedup` implies
   `ChunkSize > 0` (defaulting to `DefaultChunkSize`) and therefore
   `EncryptContent`, matching the existing chunked-mode contract. Object dedup
   is independent of encryption.
8. **Threat model note (must be documented).** Encrypted dedup leaks
   plaintext-equality: an observer who can count dedup entries learns which
   chunks/blobs are identical. This is inherent to dedup and must be stated in
   `README.md#threat-model`, not papered over.
9. **New dependency.** Add `github.com/klauspost/compress` (pure Go, CGO-free,
   with optional amd64 assembly and pure-Go fallbacks, so it satisfies
   `CGO_ENABLED=0` and the 386/arm/qemu compat matrix). No other compression
   library.

---

## Phase 0 — `internal/compress` (Zstd codec + envelope)

New package: `internal/compress/compress.go` (+ `compress_test.go`). It is
internal, so `store/*` and the root package may import it; external callers
cannot.

### 0.1 Dependency

```sh
go get github.com/klauspost/compress@latest
```

Pin the version in `go.mod`/`go.sum`. Verify `CGO_ENABLED=0 go build ./...`
still passes.

### 0.2 Envelope format

Compressed values are emitted **only when smaller**; otherwise raw bytes are
stored with no envelope.

```text
envelope (16-byte header + zstd frame):
  [0:4]   magic     = 0x5A 0x43 0x4D 0x50  ("ZCMP")
  [4]     version   = 1
  [5]     mode      = 0x01 (zstd)   — other values reserved, fail on decode
  [6:8]   reserved  = 0
  [8:16]  original  = plaintext length, big-endian uint64
  [16:]   zstd frame
```

Constants:

```go
const (
    Magic   = "ZCMP"
    Version = 1
    ModeZstd = 0x01
    HeaderSize = 16
    DefaultLevel = 3
)
```

### 0.3 API

```go
package compress

// Config selects the zstd level and the minimum plaintext size worth trying to
// compress. A zero Config selects DefaultLevel and MinSize 0.
type Config struct {
    Level   int // zstd level; 0 selects DefaultLevel
    MinSize int // skip values smaller than this many bytes; 0 tries every value
}

// Codec is a pooled Zstd codec safe for concurrent use. Stores hold one Codec
// per open and call Encode/Decode on every sealed value.
type Codec struct { /* pooled *zstd.Encoder / *zstd.Decoder */ }

func NewCodec(cfg Config) (*Codec, error)

// Encode returns dst[:n]. When the compressed value is not smaller than src
// (or len(src) < cfg.MinSize), it returns a copy of src with no envelope.
func (c *Codec) Encode(dst, src []byte) ([]byte, error)

// Decode returns dst[:n]. If src has no ZCMP envelope it returns src unchanged
// (aliasing src is permitted). It validates version/mode and that the decoded
// length equals the stored original length; any mismatch returns
// ErrCorrupt/ErrUnsupportedVersion.
func (c *Codec) Decode(dst, src []byte) ([]byte, error)
```

Implementation notes:
- Use `zstd.Encoder.EncodeAll(src, dst)` and `zstd.Decoder.DecodeAll(src, dst)`
  for one-shot, allocation-friendly code; pool one encoder and one decoder per
  `Codec` (both are safe for concurrent use).
- `Encode` must compute the compressed bytes, compare `len(compressed)+HeaderSize`
  against `len(src)`, and either write the envelope or return `src` unchanged.
  Never emit an envelope that is not strictly smaller.
- `Decode` must be a fast no-op when `len(src) < HeaderSize || string(src[:4]) !=
  Magic`.

Errors (package-level): `ErrCorrupt = errors.New("compress: corrupt envelope")`,
`ErrUnsupportedVersion = errors.New("compress: unsupported envelope version")`.

### 0.4 Tests

File: `internal/compress/compress_test.go`
- `TestEncodeDecodeRoundTrip`: values of various sizes/skew round-trip exactly.
- `TestEncodeNeverGrows`: for random + incompressible inputs, the encoded output
  is never larger than the input (no-envelope path).
- `TestDecodePlainPassthrough`: non-envelope input is returned unchanged.
- `TestDecodeRejectsBadVersion` / `TestDecodeRejectsTruncated` /
  `TestDecodeRejectsLengthMismatch`.
- `TestConcurrentCodec`: hammer one `Codec` from many goroutines under `-race`.

---

## Phase 1 — record store compression

Compress **record values and raw key/value values** only. Keys, index values,
and auto-increment metadata counters are deterministic (SIV) or tiny and are
**not** compressed.

### 1.1 Config

File: `store/record/aead.go` — extend `Config`:

```go
// Compress Zstd-compresses record values and key/value values before they are
// sealed. It is a write-time preference; reads auto-detect compressed values.
Compress bool
// CompressLevel selects the zstd level (0 = default).
CompressLevel int
// MinCompress skips compressing values smaller than this many bytes.
MinCompress int
```

`Config.Validate`/`Normalize` need no change (compression has no key
requirement); `resolveConfig` builds a `*compress.Codec` when `Compress` is
set and stores it in `encState`.

### 1.2 encState

`store/record/aead.go`:

```go
type encState struct {
    kc          *crypto.Keychain
    encValues   bool
    encMetadata bool
    encKeys     bool
    comp        *compress.Codec // nil = compression off
}
```

Wrap the value path only:

```go
func (e *encState) sealValues(plain []byte) ([]byte, error) {
    if e == nil || !e.encValues { return plain, nil }
    if e.comp != nil { plain, _ = e.comp.Encode(nil, plain) } // envelope or raw
    return e.seal(plain)
}
func (e *encState) openValues(raw []byte) ([]byte, error) {
    if e == nil || !e.encValues { return raw, nil }
    plain, err := e.open(raw)
    if err != nil { return nil, err }
    if e.comp != nil { return e.comp.Decode(nil, plain) }
    return plain, nil
}
```

Leave `sealKey`/`openKey`, `sealMetadata`/`openMetadata` untouched. The
compression is applied inside the existing seal/open boundary, so `RotateAll`,
`ReEncrypt` and `VerifyAll` (which call `openValues` then `sealValues`, or just
`openValues`) are unaffected.

### 1.3 Tests

File: `store/record/rotate_test.go` (or new `compress_test.go`)
- `TestRecordCompressRoundTrip`: save structs and KV entries with compression,
  read them back unchanged, and assert the on-disk values are smaller than the
  JSON for a highly repetitive document.
- `TestRecordCompressMixedOpen`: write with `Compress: true`, reopen with
  `Compress: false`, read back unchanged (transparent decode); then write again
  with `false` and read back (mixed store stays readable).
- `TestRecordCompressRotation`: compress + `RotateAll` + reopen under new key,
  data intact.

---

## Phase 2 — fstree compression

This is the store the `tmp/compressfs` reference maps onto. Compress **file
content** (and, optionally, symlink targets — see below) on the plaintext side
of `sealContent`/`openContent`. Works for both whole-blob (`data/`) and chunked
(`chunks/`) modes.

### 2.1 Config

File: `store/fstree/aead.go`:

```go
// Compress Zstd-compresses file content (and symlink targets) before sealing.
// It is a write-time preference; reads auto-detect compressed content.
Compress bool
// CompressLevel selects the zstd level (0 = default).
CompressLevel int
// MinCompress skips compressing content smaller than this many bytes.
MinCompress int
```

`resolveConfig` builds a `*compress.Codec` into `encState` when `Compress` is
set. `Config.Validate`/`Normalize` unchanged. No marker bit (envelope is
self-describing).

### 2.2 encState

File: `store/fstree/aead.go`:

```go
type encState struct {
    kc          *crypto.Keychain
    encContent  bool
    encMetadata bool
    chunkSize   int
    comp        *compress.Codec
}

func (e *encState) sealContent(plain []byte) ([]byte, error) {
    if e == nil || !e.encContent { return plain, nil }
    if e.comp != nil { plain, _ = e.comp.Encode(nil, plain) }
    return e.seal(plain)
}
func (e *encState) openContent(raw []byte) ([]byte, error) {
    if e == nil || !e.encContent { return raw, nil }
    plain, err := e.open(raw)
    if err != nil { return nil, err }
    if e.comp != nil { return e.comp.Decode(nil, plain) }
    return plain, nil
}
```

- `sealContent`/`openContent` are the single chokepoint already used by both the
  `data` blob path and the chunk path (`chunk.go` calls `b.enc.openContent` /
  `sealContent`), so this one change covers whole-blob and chunked content.
- Symlink targets use the same `sealContent` path if they are sealed today; if
  symlinks are sealed with a different helper, apply the same wrap there. Verify
  by reading `file.go`/`inode.go` for the symlink write path.
- `sealMetadata` (inodes) is **not** compressed — inodes are small, fixed-ish,
  and compression would only add envelope overhead.

### 2.3 Interaction with chunked read paths

`chunk.go` reads a chunk then slices it (`chunk[from-start:to-start]`). Because
`openContent` already decompresses the full chunk before the slice, partial
reads keep working unchanged. `readDataRange`'s zero-fill/gap logic is
unaffected.

### 2.4 Tests

File: `store/fstree/compress_test.go` (new)
- `TestFSCompressRoundTrip`: whole-blob and chunked modes; read back identical
  bytes; assert the `data`/`chunks` bucket values are smaller than the raw
  content for repetitive input.
- `TestFSCompressTransparentOpen`: write compressed, reopen with `Compress:
  false`, read back identical (and vice versa).
- `TestFSCompressPartialWriteChunked`: partial overwrite + truncate on a
  compressed chunked file preserves bytes and touches only affected chunks.
- `TestFSCompressSnapshot`: `Snapshot`/`OpenReadOnly`/`Walk` over compressed
  content.
- `TestFSCompressRotateAll`: compression + `RotateAll` + reopen under new key.

---

## Phase 3 — series compression

Compress **series point values and block payloads** (the `seal`/`open` value
path). Series names (SIV) are not compressed. Non-blocked single points are
8 bytes and will be stored raw by the envelope (no regression).

### 3.1 Config

File: `store/series/aead.go`:

```go
// Compress Zstd-compresses point values and block payloads before sealing.
Compress bool
CompressLevel int
MinCompress int
```

`resolveConfig` builds a `*compress.Codec` into `encState`. `encState` gains a
`comp *compress.Codec` field. Wrap `seal`/`open` in `aead.go` (the existing
generic `e.seal`/`e.open` methods used for values):

```go
func (e *encState) seal(plain []byte) ([]byte, error) {
    if e == nil { return plain, nil }
    if e.comp != nil { plain, _ = e.comp.Encode(nil, plain) }
    return e.kc.Seal(plain)
}
func (e *encState) open(raw []byte) ([]byte, error) {
    if e == nil { return raw, nil }
    plain, err := e.kc.Open(raw)
    if err != nil { return nil, err }
    if e.comp != nil { return e.comp.Decode(nil, plain) }
    return plain, nil
}
```

- Block payloads (`BlockDuration` path) are sealed once as a whole; wrapping
  `seal`/`open` compresses the whole block in one shot, which is where the real
  win is.
- Series names (`sealName`/`openName`) are separate and untouched.
- `RotateAll`/`VerifyAll` use `open`/`seal` and are unaffected.

### 3.2 Tests

File: `store/series/compress_test.go` (new)
- `TestSeriesCompressBlock`: many points with `BlockDuration`, compressed; read
  `RangeEach`/`Get` back identical; assert smaller stored blocks.
- `TestSeriesCompressSinglePointNoGrow`: non-blocked single points store raw (no
  envelope) — assert on-disk size does not grow vs uncompressed.
- `TestSeriesCompressRotation`: compress + `RotateAll` + reopen, data intact.

---

## Phase 4 — vector compression

Compress **point blobs and collection metadata** (the `sealPoints`/`openPoints`
and metadata value paths). Point IDs and collection names (SIV) are not
compressed. Quantization still applies first (quantized vectors are stored in
the payload); compression adds a further reduction on the JSON payload and
metadata.

### 4.1 Config

File: `store/vector/aead.go`:

```go
// Compress Zstd-compresses point blobs and collection metadata before sealing.
Compress bool
CompressLevel int
MinCompress int
```

Add `comp *compress.Codec` to `encState`. Wrap the value paths:

```go
func (e *encState) sealPoints(plain []byte) ([]byte, error) {
    if e == nil || !e.encPoints { return plain, nil }
    if e.comp != nil { plain, _ = e.comp.Encode(nil, plain) }
    return e.seal(plain)
}
func (e *encState) openPoints(raw []byte) ([]byte, error) {
    if e == nil || !e.encPoints { return raw, nil }
    plain, err := e.open(raw)
    if err != nil { return nil, err }
    if e.comp != nil { return e.comp.Decode(nil, plain) }
    return plain, nil
}
```

- Collection metadata is sealed via the same `seal`/`open` path (`sealBytes` in
  `store.go`/`aead.go`); ensure that path also goes through `comp`. If
  `SaveCollectionMeta` uses `s.sealBytes`, wrap `sealBytes`/`openBytes` too.
- `sealName`/`openName` (IDs, collection names) untouched.
- `LoadCollection` (which decodes point values) is unaffected because
  `openPoints` already decompresses.

### 4.2 Tests

File: `store/vector/compress_test.go` (new)
- `TestVectorCompressRoundTrip`: upsert points with large JSON payloads,
  compressed; `LoadCollection`/search returns identical points; stored blobs
  smaller.
- `TestVectorCompressQuantized`: `UseQuant` + compression; load/dequantize
  round-trips.
- `TestVectorCompressRotation`: compress + `RotateAll` + reopen, data intact.

---

## Phase 5 — search compression

Compress **document records, posting lists, and index metadata** (the `seal`/
`openValue` value path). Term keys (SIV) are not compressed.

### 5.1 Config

File: `store/search/search.go`:

```go
// Compress Zstd-compresses documents, posting lists and metadata before
// sealing. Term keys are never compressed.
Compress bool
CompressLevel int
MinCompress int
```

Add `comp *compress.Codec` to `encState`; build it in `resolveEnc` when
`Compress` is set. Wrap `seal`/`openValue`:

```go
func (s *Store) seal(plain []byte) ([]byte, error) {
    enc := s.encState()
    if enc == nil || !enc.encValues { return plain, nil }
    if enc.comp != nil { plain, _ = enc.comp.Encode(nil, plain) }
    return enc.kc.Seal(plain)
}
func (s *Store) openValue(raw []byte) ([]byte, error) {
    enc := s.encState()
    if enc == nil || !enc.encValues { return raw, nil }
    plain, err := enc.kc.Open(raw)
    if err != nil { return nil, err }
    if enc.comp != nil { return enc.comp.Decode(nil, plain) }
    return plain, nil
}
```

- `termKey` (SIV) untouched.
- Search rotation/`VerifyAll` (added in a separate gap-closure plan) operate on
  `openValue`/`seal` and are unaffected.

### 5.2 Tests

File: `store/search/compress_test.go` (new)
- `TestSearchCompressRoundTrip`: index several documents, compressed; `Search`
  and `Count` unchanged; posting/doc values smaller.
- `TestSearchCompressOpenPlain`: compressed index reopened with `Compress:
  false` still searches correctly.

---

## Phase 6 — graphs compression

Compress **node/edge/version records** (the `seal`/`openValue` path). Keys are
plaintext.

### 6.1 Config

File: `store/graphs/graphs.go`:

```go
// Compress Zstd-compresses graph records before sealing.
Compress bool
CompressLevel int
MinCompress int
```

Add `comp *compress.Codec` to `encState`; wrap `seal`/`openValue` in
`graphs.go`:

```go
func (s *Store) seal(plain []byte) ([]byte, error) {
    enc := s.encState()
    if enc == nil { return plain, nil }
    if enc.comp != nil { plain, _ = enc.comp.Encode(nil, plain) }
    return enc.kc.Seal(plain)
}
func (s *Store) openValue(raw []byte) ([]byte, error) {
    enc := s.encState()
    if enc == nil { return raw, nil }
    plain, err := enc.kc.Open(raw)
    if err != nil { return nil, err }
    if enc.comp != nil { return enc.comp.Decode(nil, plain) }
    return plain, nil
}
```

- The canary in `checkMeta` is sealed with the same `kc` but must **not** be
  wrapped in the envelope (it is opened directly, not via `openValue`); verify
  that `checkMeta` calls `enc.kc.Seal`/`Open` directly and not `s.seal`/
  `s.openValue`. If it routes through the generic helpers, add a dedicated
  uncompressed seal/open for the canary.
- `MigrateFromRecord`/`Copy` write through the same storage layer and are
  unaffected.

### 6.2 Tests

File: `store/graphs/compress_test.go` (new)
- `TestGraphsCompressRoundTrip`: nodes/edges with rich properties, compressed;
  `GetAllNodes`/`GetAllEdges`/`Statistics` unchanged.
- `TestGraphsCompressCanaryUnaffected`: open, verify canary path still works
  (no double envelope).

---

## Phase 7 — object compression

Compress **object content** (the `Data` field, i.e. the whole JSON record) on
the `seal`/`openValue` path. Keys/buckets are plaintext.

### 7.1 Config

File: `store/object/object.go`:

```go
// Compress Zstd-compresses object records before sealing.
Compress bool
CompressLevel int
MinCompress int
```

Add `comp *compress.Codec` to `encState`; wrap `seal`/`openValue` in
`object.go` exactly as in Phase 6. Same canary caveat: `checkMeta` must seal/open
the canary directly with `enc.kc`, not through the compressed helpers.

### 7.2 Tests

File: `store/object/compress_test.go` (new)
- `TestObjectCompressRoundTrip`: `Put`/`Get`/`ReadAll`/`List`/`Stat` round-trip;
  stored records smaller for repetitive content.
- `TestObjectCompressMigrate`: `MigrateFromFile` into a compressed store, read
  back identical.

---

## Phase 8 — fstree content-addressed chunk deduplication

Opt-in via `Config.Dedup`. Identical chunks across a file, across files, and
across file systems under one store are stored once.

### 8.1 Config and marker

File: `store/fstree/aead.go`:

```go
// Dedup stores chunk content by its plaintext SHA-256, so identical chunks are
// written once and reference-counted. It implies ChunkSize (default
// DefaultChunkSize) and therefore content encryption. It is recorded in the
// filesystem marker and fails closed on mismatch.
Dedup bool
```

- Add `markerFlagDedup = 1 << 4` (record/fstree marker flags currently use bits
  0–2; append bit 4). Add `dedup bool` to `marker`, `encState`, and to
  `flagsByte`/`matches`/`encodeMarker`/`decodeMarker`.
- `resolveConfig`: when `cfg.Dedup`, set `chunkSize = DefaultChunkSize` if
  `cfg.ChunkSize == 0`, and `dedup = true`. `encryptionEnabled()` must return
  true when `Dedup` is set. `Validate` unchanged (Dedup implies a key via the
  chunked rule).
- `Open` validates the persisted `dedup` bit against `cfg.Dedup` and returns
  `ErrInvalidConfig`/`ErrWrongMode` on mismatch (fail closed).

### 8.2 On-disk layout change (dedup mode only)

Replace the `ino||idx` chunk keying with content addressing. Inside each
filesystem's bucket:

```text
chunks/    key = sha256(plaintext chunk)   value = sealContent(envelope(chunk))
chunkmap/  key = inode (8-byte BE)          value = ordered hash list (sealed when EncryptMetadata)
chunkrefs/ key = sha256(plaintext chunk)    value = uint64 refcount (BE)
```

- `chunkmap` value layout: `uint64 count` followed by `count × 32-byte` hashes
  (big-endian order = file order). This replaces the implicit `ino||idx`
  ordering; the inode's `Size` still determines the logical length, and a
  missing/trailing hash means zero-fill (preserving current sparse semantics).
- `chunkrefs` is plaintext (a count), keyed by hash.
- All three mutations (put/delete chunk, update hash list, bump/bump-down
  refcount) happen in the same bbolt transaction as the inode update, so a
  crash cannot orphan a blob or leak a refcount.

### 8.3 Rewrite `chunk.go`

Add helpers and rework the existing functions:

```go
func chunkHash(dst *[32]byte, chunk []byte) []byte      // sha256
func (b *fsBucket) readChunkList(ino uint64) ([][32]byte, error)  // from chunkmap
func (b *fsBucket) writeChunkList(ino uint64, hashes [][32]byte) error
func (b *fsBucket) acquireChunk(hash [32]byte, data []byte) error  // put if absent; ref++
func (b *fsBucket) releaseChunk(hash [32]byte) error                // ref--; delete at 0
```

Rework:
- `readChunk(ino, idx)` → resolve `hash := list[idx]`, read `chunks[hash]`.
- `writeChunk(ino, idx, data)` → compute hash, `acquireChunk`, set `list[idx]`,
  release the old hash if replaced.
- `readDataRange` / `writeDataRange` / `truncateData` / `readAllChunks` /
  `writeAllChunks` / `deleteAllChunks` iterate the hash list instead of the
  `ino||idx` cursor, preserving the same single-cursor seek behavior where
  feasible.
- `deleteAllChunks` releases every referenced hash (refcount down) and deletes
  the chunkmap entry.

### 8.4 Non-dedup path unchanged

Keep the existing `chunkKey` (ino||idx) code path intact for `dedup == false`.
Gate the new content-addressed path on `e.dedup`. This keeps all current tests
green and makes dedup purely additive.

### 8.5 Tests

File: `store/fstree/dedup_test.go` (new)
- `TestDedupAcrossFiles`: two files sharing large identical regions store one
  chunk per unique content (`chunkrefs` count reflects sharing); read both back
  identical.
- `TestDedupRefcountOverwrite`: overwrite one file's chunk → refcount adjusts,
  shared chunk retained, replaced chunk freed at 0.
- `TestDedupRefcountTruncateAndRemove`: truncate/remove releases refs and
  deletes unreferenced chunks.
- `TestDedupMarkerFailsClosed`: open a dedup store with `Dedup: false` → error;
  and an undedup store with `Dedup: true` → error.
- `TestDedupRotateAllAndSnapshot`: `RotateAll`, `Snapshot`, `OpenReadOnly`,
  `VerifyAll` all work on a dedup store.

---

## Phase 9 — object blob deduplication

Opt-in via `Config.Dedup`. Identical objects (same content) across keys and
buckets are stored once.

### 9.1 Config and mode marker

File: `store/object/object.go`:

```go
// Dedup stores object content by its SHA-256, so identical objects are stored
// once and reference-counted. It is recorded in the store's mode byte and
// fails closed on mismatch.
Dedup bool
```

The object store's mode byte currently is `0` (plaintext) or `1` (encrypted),
written unconditionally in `checkMeta`. Add `modeDedup byte = 1 << 1` (value 2);
`want = (encrypted?1:0) | (dedup?2:0)`. Validate on open (fail closed).

### 9.2 On-disk layout change (dedup mode only)

```text
__blobs__/       key = sha256(plaintext content)   value = seal(envelope(content))
__blobs_refs__/  key = sha256                       value = uint64 refcount (BE)
<bucket>/<key>    value = sealed record{SHA256, Size}   (Data removed)
```

- Object `record` struct: when `Dedup`, the persisted record stores only
  `SHA256` and `Size` (add `Size int64` to the record; keep `Data []byte` for
  the non-dedup path or make it conditional).
- `Put`: read all → sha256 → `acquireBlob(hash, content)` (put if absent,
  ref++) → write record, all in one transaction.
- `Get`/`ReadAll`: read record → read `__blobs__[hash]` → open/decompress.
- `Delete`: `releaseBlob(hash)` (ref--, delete at 0) + delete record, one
  transaction.
- `Move`: moves the record reference only; no blob change.
- `Stat`/`List` use `record.Size` and `record.SHA256` (already present).
- `MigrateFromFile` goes through `Put`, so it benefits automatically.

### 9.3 Tests

File: `store/object/dedup_test.go` (new)
- `TestObjectDedupAcrossKeys`: put identical content under two keys/buckets;
  one `__blobs__` entry; both read back identical.
- `TestObjectDedupRefcount`: delete one reference → blob retained; delete last →
  blob removed.
- `TestObjectDedupMove`: `Move` keeps the same blob and refcount.
- `TestObjectDedupFailsClosed`: mode-mismatch on open errors.

---

## Phase 10 — root package wiring (`tempest.Options`)

Expose one compression knob and dedup knobs at the root, and pass them to every
sub-store in `Open`.

### 10.1 New types

File: `db.go`:

```go
// Compression selects Zstd compression across sub-stores. It is a write-time
// preference; reads auto-detect compressed values, and existing stores are
// unaffected.
type Compression struct {
    Enable  bool
    Level   int // zstd level; 0 selects the default
    MinSize int // skip values smaller than this many bytes
}

// Dedup selects content-addressed deduplication for the stores that support it.
type Dedup struct {
    Files   bool // fstree chunk dedup (implies chunked content)
    Objects bool // object-store blob dedup
}
```

Add `Options.Compression Compression` and `Options.Dedup Dedup`.

### 10.2 Propagation in `Open`

In `Open`, thread the settings into each sub-store `Config`:

- `record.Config{..., Compress: o.Compression.Enable, CompressLevel: ..., MinCompress: ...}`
- `fstree.Config{..., Compress: ..., Dedup: o.Dedup.Files}`
- `series.Config{..., Compress: ...}`
- `vector.Config{..., Compress: ...}`
- `search.Config{..., Compress: ...}`
- `graphs.Config{..., Compress: ...}`
- `object.Config{..., Compress: ..., Dedup: o.Dedup.Objects}`

Because compression is transparent (no marker), reopening a store does not
require the caller to re-supply `Compression`; dedup, however, is persisted, so
reopening a dedup store requires `Options.Dedup.Files`/`.Objects` to match (the
sub-store fails closed otherwise).

### 10.3 Tests

File: `db_test.go` (or new `compress_test.go` at root)
- `TestOpenWithCompression`: open a store with `Options.Compression{Enable:
  true}`, exercise every accessor, close, reopen with the same and with
  `Enable: false`, assert data intact across stores.
- `TestOpenWithDedup`: `Options.Dedup{Files: true, Objects: true}`, write
  duplicate content, assert dedup occurred and reopen with matching options
  succeeds; mismatched options fail closed.

---

## Documentation updates

Follow `AGENTS.md` documentation conventions; update in the same change.

1. **README.md**
   - New "Compression & deduplication" section: Zstd, per-store coverage,
     transparent-read semantics, dedup scope (files + objects), and usage
     (`Options.Compression`, `Options.Dedup`).
   - Threat model: add the encrypted-dedup plaintext-equality leakage note.
   - Performance section: note chunked+compressed behavior and that compression
     is write-time, decompression is read-time.
2. **AGENTS.md**
   - Repository map / architecture: document the new `internal/compress`
     package, the `ZCMP` envelope, the compress-before-seal invariant, and the
     dedup key layout for fstree/object.
   - Add the "compression is transparent, dedup is marker-gated" rule under
     Encryption internals.
3. **Package guides** (`store/fstree/README.md`, `store/object/README.md`,
   `store/record/README.md`, `store/series/README.md`, `store/vector/README.md`,
   `store/search/README.md`, `store/graphs/README.md`): document the new
   `Compress`/`CompressLevel`/`MinCompress` (and `Dedup`) fields.
4. `go.mod`/`go.sum`: record the `klauspost/compress` dependency.

---

## Verification

Run after each phase, and once at the end (from `AGENTS.md`):

```sh
test -z "$(gofmt -l .)"
go vet ./...
go build ./...
CGO_ENABLED=0 go build ./...
go test -count=1 -timeout 300s ./...
CGO_ENABLED=0 go test -count=1 -timeout 300s ./...
go test -race -count=1 -timeout 600s ./...        # Phases 0, 8, 9 (concurrency)
CGO_ENABLED=0 GOARCH=386 go vet ./...
CGO_ENABLED=0 GOARCH=386 go test -count=1 -timeout 600s ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...
go mod tidy -diff
```

Benchmark check (compression must not regress the FS/TS hot paths):

```sh
go test -run '^$' -bench . -benchmem -benchtime=100ms ./store/fstree ./store/series ./store/object
```

---

## Risks, trade-offs, open decisions

1. **Compression is write-time and transparent (no marker).** This preserves
   every existing on-disk format and needs no version bump, but it means a store
   can end up with a mix of compressed and uncompressed values if the caller
   toggles `Compress` between opens. Correctness never depends on it; space
   savings assume the setting is kept consistent. **[DECISION]** Default: keep
   transparent (no persisted compression flag); document the consistency
   expectation.
2. **Dedup leaks plaintext equality under encryption.** Accepting dedup on an
   encrypted store means an observer who can read refcounts/hash keys learns
   which chunks are identical. This is inherent and must be in the threat model.
   A future option could disable dedup when encryption is on; not required now.
3. **fstree dedup requires chunked content mode** (and therefore content
   encryption), matching the existing `ChunkSize` contract. Whole-file
   (non-chunked) dedup is out of scope. **[DECISION]** Default: dedup implies
   chunked mode.
4. **Envelope header overhead.** The `ZCMP` header is 16 bytes; the "emit only
   when smaller" rule means no value ever grows. MinSize is a CPU optimization
   only.
5. **Dedup changes key layout**, which is why it is the only part of this plan
   that touches a marker flag (fail-closed), mirroring how `ChunkSize` already
   records its layout change in the FENC marker.
6. **Dependency risk.** `klauspost/compress` must build under `CGO_ENABLED=0`
   and the 386/arm/qemu compat matrix; it does (pure-Go fallbacks), but the
   first step of Phase 0 must confirm the full cross-build before proceeding.

### Open decisions for the user (defaults chosen, confirm if different) (user decision behind "->" arrow):

- **Compression persistence**: transparent/no-marker (recommended) vs. a
  persisted per-store flag that fails closed on mismatch. -> **transparent/no-marker** 
- **Dedup scope**: files + objects only (recommended). Extending dedup to
  records/series/vector/search/graphs is possible but not recommended (small,
  heterogeneous values; refcounting overhead). -> **files + objects only**
- **fstree dedup requires chunking + encryption** (recommended) vs. allowing
  plaintext dedup. -> **chunking + encryption**
