# Plan: Implement optional encryption for the `fs` store + merge `fs/aeadfs` into it

Implements the recommended proposal of `FS.md`: native, **opt-in** AEAD
encryption for the bbolt-backed `fs` package, reusing `internal/encryption`
(exactly as `ts` already does), and folding the `fs/aeadfs` wrapper into `fs` in
the process.

## Hard requirement: encryption is optional

Encryption is strictly opt-in and must cause **zero behavioral change** when not
configured:

- The existing `Open(ctx, db, root)` constructor keeps its exact signature and
  semantics — a plaintext store, byte-for-byte identical to today.
- Encryption is only active when an explicit `Config` is passed to a new
  `OpenWithConfig` constructor **and** that `Config` enables at least one
  encryption feature. No implicit encryption, no global default.
- A `Store`/`FileSystem` opened without encryption has all encryption state
  nil/zero, and every code path falls through to the current plaintext
  encode/decode (raw gob inodes, raw content, raw symlink paths).
- The on-disk format of a plaintext store is unchanged — no marker is written,
  and a regression test asserts the raw bytes are identical.

Every change below is guarded so that, when encryption is disabled, the code
performs exactly what it does today.

## Background and reuse

- `internal/encryption` provides `CipherSuite` (`CipherAuto`→`CipherAES256GCM`,
  `CipherXChaCha20Poly1305`), `CipherEngine`, `NewCipherEngine`,
  `GenerateNonce`, `Resolve`, `KeySize=32`, `SIVEngine`, `ErrAuthFailed`,
  `ErrUnsupportedCipher`. `fs` imports this directly (module-internal path).
- `fs` does **not** need SIV: file/dir names and metadata live *inside* the
  gob-encoded inode blob, and lookups are by numeric inode number (`i2b(ino)`),
  so random-nonce AEAD hides names + metadata with no deterministic cipher.
- `ts` (`ts/aead.go`, `ts/store.go`, `ts/rotate.go`, `ts/block.go`) is the
  reference implementation whose `Config`/`OpenWithConfig`/marker/canary/
  rotation machinery is mirrored here.

## Proposed API (new file `fs/aead.go`)

```go
type Config struct {
    Cipher CipherSuite   // default CipherAuto → AES-256-GCM
    Key    []byte        // exactly KeySize (32); OR KeyProvider
    KeyProvider KeyProvider

    EncryptContent  bool // seal data-bucket file contents + symlink paths
    EncryptMetadata bool // seal inode blobs (hides names, mode, size, times)
    ChunkSize       int  // Tier 2: >0 enables chunked content encryption
}

type KeyProvider interface { Key() ([]byte, error) }
// RawKeyProvider, EnvKeyProvider, MultiKeyProvider (mirror ts).

func OpenWithConfig(ctx context.Context, db *bolt.DB, root []byte, cfg *Config) (*Store, error)
```

`EncryptFSNames` (SIV on `Store.Open` paths) is deferred — FS.md recommends
leaving filesystem names clear since file/dir names are already hidden by inode
encryption.

## Shared encryption state

Introduce one shared, nil-when-disabled struct referenced by `Store`,
`FileSystem`, and `fsBucket`:

```go
type encState struct {
    engine      CipherEngine   // primary engine for writes
    engines     []CipherEngine // primary first, rotation fallbacks after
    cipher      CipherSuite
    encContent  bool
    encMetadata bool
    chunkSize   int
}
```

`Store` holds `enc *encState` (nil = plaintext) and hands it to every
`FileSystem` it opens; the `FileSystem` hands it to each `fsBucket`. When `enc`
is nil, all encryption helpers are no-ops returning the plaintext bytes.

## Storage formats

Sealed records (fresh random nonce each write):

```
sealed content = nonce || engine.Encrypt(nonce, fileContent)       // data bucket
sealed inode   = nonce || engine.Encrypt(nonce, gob(iNode))        // inodes bucket
sealed symlink = nonce || engine.Encrypt(nonce, targetPath)        // symlinks bucket
sealed chunk   = nonce || engine.Encrypt(nonce, chunkBytes)        // Tier 2 chunks
```

Marker + canary (unencrypted, in each filesystem's `state` bucket under the
reserved key `"\x00__fs_enc__"`):

```
magic "FENC"(4) | version(1) | cipher(1) | flags(1: content<<0|metadata<<1|chunked<<2) | chunkSize(8 LE) | sealed canary
canary = nonce || seal("storm/fs/aead/v1")
```

The marker is per-filesystem (each filesystem has its own `state` bucket), while
the key is per-`Store` (all its filesystems share it).

## Seams to thread encryption through (exact, from current code)

| File | Current function | Change |
|------|------------------|--------|
| `fs/tx.go` | `fsBucket.PutInode` → `encodeNode` | seal gob before `inodes.Put` |
| `fs/tx.go` | `fsBucket.GetInode` → `decodeNode` | unseal after `inodes.Get` |
| `fs/fs.go` | `FileSystem.loadInode` → `decodeNode` | route through the sealed decode |
| `fs/inode.go` | `encodeNode`/`decodeNode` (free funcs) | become `fsBucket` methods (or accept `*encState`) |
| `fs/fs.go` | `FileSystem.saveData`/`loadData` (bolt `data` branch) | seal/unseal content |
| `fs/tx.go` | `fsBucket.Symlink`/`Readlink` | seal/unseal path |
| `fs/snapshot.go` | snapshot's `fsBucket` | must inherit `enc` so snapshot reads decrypt |

`fsBucket` gains an `enc *encState` field, set by `FileSystem.openFsBucket`/
`openFsBucketWithCache` from the `FileSystem`. The in-memory inode cache is
unaffected (it already holds plaintext nodes).

## Mode detection and key verification

`Store.Open(name)` performs, before mutating anything:

1. Read the filesystem's `state` marker at `bucketPath(name)` (read-only tx).
2. Marker present + `enc == nil` → `ErrWrongMode` (encrypted fs opened without a
   key).
3. Marker present + `enc != nil` → verify the marker matches `enc` settings, and
   decrypt the canary (wrong key → `ErrAuthFailed`).
4. Marker absent + `enc != nil` → if the filesystem already has user data
   (inode keys beyond `i2b(0)`/`i2b(1)`, or non-empty `data`/`symlinks`), return
   `ErrWrongMode` (never silently mix); otherwise `newFS` writes the marker
   during init.
5. Marker absent + `enc == nil` → plaintext (current behavior).

`newFS` gains an `enc *encState` parameter and writes the marker + canary when
`enc != nil`.

## Tier 1 — `EncryptContent` + `EncryptMetadata` (the `db/codec/aead` / ts analog)

File-by-file:

| File | Change |
|------|--------|
| `fs/aead.go` (new) | `encState`, `Config`, `KeyProvider`, `RawKeyProvider`, `EnvKeyProvider`, cipher aliases, `resolveConfig`, marker encode/decode, `ensureMarker`, `readMarker`, `verifyCanary`, `seal`/`open` byte helpers. |
| `fs/errors.go` (new) | `ErrAuthFailed`, `ErrInvalidKey`, `ErrUnsupportedCipher`, `ErrInvalidConfig`, `ErrWrongMode` (plus existing `ErrNilContext`/`ErrNilParam`). |
| `fs/store.go` | `Store` gains `enc *encState`; add `OpenWithConfig`; `Open` delegates to `OpenWithConfig(nil)`; `Open`/`OpenWithContentFS` perform mode detection and pass `enc` to `newFS`. |
| `fs/fs.go` | `FileSystem` gains `enc *encState`; `newFS` gains the parameter and writes the marker; `saveData`/`loadData` (bolt branch) seal/unseal; `loadInode` uses the sealed decode; `openFsBucket*` sets `fsBucket.enc`. |
| `fs/tx.go` | `fsBucket` gains `enc`; `PutInode`/`GetInode`/`Symlink`/`Readlink` seal/unseal. |
| `fs/inode.go` | `encodeNode`/`decodeNode` become `fsBucket` methods (gob serialize/deserialize + optional seal/unseal). |
| `fs/snapshot.go` | snapshot `fsBucket` inherits `enc` so reads decrypt. |
| `fs/aead_test.go` (new) | See test plan. |

Notes:

- `i2b(ino)` keys and the `state` keys stay clear (numeric/structural).
- External `contentFS` (`SetContentFS`) is left plaintext and documented as such;
  content encryption applies to the bolt `data` bucket only.
- Inode `Size` remains the plaintext size (recovered from the decrypted inode),
  so `File`/`Stat` behavior is unchanged.

## Tier 2 — chunked content + rotation (the `aeadfs` merge)

| File | Change |
|------|--------|
| `fs/chunk.go` (new) | Chunked data layer (port of `aeadfs/chunk_format.go`): `readRange(ino, off, n)`, `writeRange(ino, off, data)`, `truncateRange(ino, size)`; chunks keyed by `i2b(ino) || chunkIndex` (new `chunks` bucket) or a reserved composite key; each chunk `= nonce || seal(chunk)`. |
| `fs/file.go` | `Read`/`Write`/`ReadAt`/`WriteAt`/`Truncate` route through the range layer when `enc != nil && chunkSize > 0`; whole-blob `loadData`/`saveData` path unchanged otherwise. |
| `fs/rotate.go` (new) | `MultiKeyProvider`, `ReEncryptFS(ctx, name, newKey)`, `RotateAll(ctx, newKey)`, `VerifyAll(ctx)` (mirror `ts/rotate.go`): re-seal every inode/content/chunk/symlink under the new key, then adopt it and rewrite the marker canary. |

Chunk semantics to get right: seek/read/write/truncate touch only affected
chunks; fresh nonce per chunk on every write; `node.Size` remains plaintext size
so `Stat`/`Seek(SeekEnd)` are unaffected; `Count`/directory operations are
unchanged (they read inodes, not content).

## Merge `fs/aeadfs` into `fs`

- Lift into `fs`: the `Config`/`KeyProvider` surface, the chunked format, and
  key rotation (done above). SIV is already in `internal/encryption`.
- Delete `fs/aeadfs/` (source + tests + README). Verified no other package in the
  module imports `github.com/malivvan/storm/fs/aeadfs` (only its own README).
- Consequence (accepted): the generic "encrypt any `absfs.FileSystem`" wrapper is
  removed; `fs` now encrypts at rest in bbolt natively. External
  `contentFS`/non-bolt filesystems are not covered (documented).

## Error handling and nonce discipline

- Decrypt failures → `ErrAuthFailed` on every read path; never reported as
  "not found", never a panic.
- Invalid config (bad key length, unknown cipher, both/neither `Key` and
  `KeyProvider`, negative `ChunkSize`) → `ErrInvalidConfig`.
- Mode/key mismatch → `ErrWrongMode` / `ErrAuthFailed` (canary), never silent
  downgrade or plaintext fallback.
- Fresh random nonce per sealed record (content, inode, symlink, chunk) via
  `encryption.GenerateNonce`; re-encryption always uses new nonces.

## Backward compatibility and optionality guarantees

- `Open(ctx, db, root)` is unchanged and plaintext-only.
- `OpenWithConfig` with a nil `Config` (or none enabled) is equivalent to `Open`.
- Unencrypted stores write no marker and their bytes are byte-identical
  (regression test asserts raw gob inodes and raw content).
- An encrypted filesystem opened without a key (or with the wrong key) fails
  loudly (`ErrWrongMode`/`ErrAuthFailed`), never returns garbage.
- Opening a filesystem that already has plaintext user data with encryption
  requested returns `ErrWrongMode`; plaintext→encrypted migration is a separate
  follow-on (out of scope for this core).

## Test plan

- **Optionality**: nil `Config` ⇒ raw gob inode bytes, raw content bytes, no
  `FENC` marker; byte-identical to the current implementation.
- **Round-trip**: every public op (`OpenFile`/`ReadFile`/`Read`/`Write`/
  `ReadAt`/`WriteAt`/`Seek`/`Truncate`/`Mkdir`/`Remove`/`Rename`/`ReadDir`/
  `Stat`/`Symlink`/`Readlink`/snapshots) matches a plaintext control store.
- **Auth**: wrong key → `ErrAuthFailed` (canary); tampered inode/content/symlink
  → `ErrAuthFailed`; truncated ciphertext → error, no panic.
- **Nonce**: two writes of the same content produce different ciphertext.
- **Tier 2**: chunk boundary reads/writes, seek across chunks, partial-chunk
  write, truncate; size consistency.
- **Rotation**: `ReEncryptFS`/`RotateAll`/`VerifyAll`; old key decrypts until
  rotation, then fails; new key works after.
- **Mode mismatch**: plaintext open of encrypted fs / encrypted open of plaintext
  fs → `ErrWrongMode`.
- `go vet ./fs/...`, `go test -race ./fs/...`, `go test ./...`.

## Verification checklist

```sh
cd /home/malivvan/projects/storm
# before deleting aeadfs: confirm no importers
grep -r "storm/fs/aeadfs" --include=*.go . || true
gofmt -l fs internal/encryption        # expect empty
go vet ./fs/... ./internal/encryption/...
go build ./fs/...
go test -count=1 ./fs/...
go test -race -count=1 ./fs/...
go test -count=1 ./...                 # no regressions elsewhere
```

Acceptance: build/vet/test pass; plaintext behavior provably unchanged; the
optional encryption path matches `ts`/`aeadfs` security properties (confidential
content + metadata/names, authenticated, fresh nonces, key verification +
rotation); `fs/aeadfs` is removed.

## Ordered task list

1. Add `fs/errors.go` (new sentinels) and `fs/aead.go` (`encState`, `Config`,
   `KeyProvider`, providers, cipher aliases, `resolveConfig`, marker/canary
   encode/decode + `seal`/`open` helpers).
2. Add `enc *encState` to `Store`; add `OpenWithConfig`; make `Open` delegate;
   add mode-detection in `Store.Open`/`OpenWithContentFS` and pass `enc` to
   `newFS`.
3. Thread `enc` through `FileSystem` and `fsBucket`; convert
   `encodeNode`/`decodeNode` to sealed `fsBucket` methods; seal/unseal inodes,
   content (`saveData`/`loadData` bolt branch), and symlinks; make
   `loadInode`/snapshot reads enc-aware; write the marker in `newFS`.
4. Add Tier 1 tests (round-trip, wrong key, tamper, nonce, optionality,
   mode mismatch).
5. Add `fs/chunk.go` (chunked data layer) and route `File.Read/Write/ReadAt/
   WriteAt/Truncate` through it when `chunkSize > 0` (Tier 2).
6. Add `fs/rotate.go` (`MultiKeyProvider`, `ReEncryptFS`, `RotateAll`,
   `VerifyAll`).
7. Add Tier 2 tests (chunks, rotation).
8. Delete `fs/aeadfs/`; confirm no importers (step in checklist).
9. Run the full verification checklist; fix any gofmt/vet/test issues.
