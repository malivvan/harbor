# Encrypting the `fs` (bbolt file system) store — research findings

This documents how the bbolt-backed `fs` package can be encrypted in the same
native, optional way as `ts` (and, conceptually, `db/codec/aead`), and how
`fs/aeadfs` is merged into `fs` in the process. It is research only; no code is
changed.

## 1. The references

- **`ts`** (`ts/aead.go`, `ts/store.go`, `ts/siv.go`, `ts/block.go`,
  `ts/rotate.go`) — the pattern to mirror. A bbolt store gains optional
  encryption through `Config` + `OpenWithConfig`, reusing
  `internal/encryption`. `Open` stays plaintext; a `Store` holds `engine`,
  `engines` (rotation fallbacks), `cipher`, `encValues`, `encNames`, `blockDur`,
  `siv`. A marker + sealed canary in the metadata bucket detects mode/key
  mismatches. SIV (in `internal/encryption/siv.go`, key derived with HKDF)
  encrypts series names because names are bbolt bucket keys.
- **`db/codec/aead`** (`db/codec/aead`) — the "wrap a value" model: `Marshal` =
  `nonce || seal(plaintext)`, `Unmarshal` splits the nonce and decrypts; auth
  failures → `ErrAuthFailed`.
- **`fs/aeadfs`** (`fs/aeadfs`) — a *wrapper* around any `absfs.FileSystem`
  with `Config{Cipher, KeyProvider, FilenameEncryption, ChunkSize, ...}`,
  `RawKeyProvider`/`EnvKeyProvider`/`MultiKeyProvider`, AES-SIV filename
  encryption, a chunked file format for random access, and key rotation
  (`ReEncrypt`/`RotateAllKeys`/`VerifyAllEncryption`).

Shared primitive: `internal/encryption` (now including `SIVEngine`).

## 2. What `fs` actually stores (the encryption seam)

`fs` is a bbolt-backed `absfs.FileSystem`. Each named filesystem (`Store.Open`)
lives under a bucket path and contains four buckets (`fs/tx.go`):

| Bucket | Key | Value | Sensitivity |
|--------|-----|-------|-------------|
| `state` | string keys (`umask`, `rootIno`) | small blobs | structural (leave clear, or minimal) |
| `inodes` | `i2b(ino)` (8-byte big-endian inode number) | **gob-encoded `iNode`** — mode, size, times, uid/gid, **and `Children` (name → ino)** | **metadata + all file/dir names** |
| `data` | `i2b(ino)` | **raw file content bytes** (whole blob) | **file contents** |
| `symlinks` | `i2b(ino)` | symlink target path | path (moderate) |

The exact write/read seams are small and centralized:

- **Inodes**: `fsBucket.PutInode`/`GetInode` (`fs/tx.go`) → `encodeNode`/
  `decodeNode` (`fs/inode.go`); plus a direct `decodeNode` call in
  `FileSystem.loadInode` (`fs/fs.go`). `FileSystem.saveInode` funnels through
  `fsBucket.PutInode`.
- **Content**: `FileSystem.saveData`/`loadData` (`fs/fs.go`), which `File.Read`/
  `Write`/`ReadAt`/`WriteAt`/`Truncate` already call exclusively.
- **Symlinks**: `fsBucket.Symlink`/`Readlink`.

The in-memory inode cache (`fs/cache.go`) holds plaintext `*iNode` values, so
encryption is purely at-rest; the cache needs no change.

## 3. Key insight: `fs` needs **no SIV**

In `ts`, series *names* are the bucket keys, so deterministic AES-SIV was needed
to keep lookups working. In `fs` the lookup key is a **numeric inode number**
(`i2b(ino)`), and names are ordinary data *inside* the gob-encoded inode blob.
Encrypting that blob with random-nonce AEAD therefore hides names and metadata
together, with no deterministic cipher needed. This is both simpler than `ts`
and stronger than `aeadfs`'s filename-only SIV mode.

The only place a name becomes a key is `Store.Open(name)` (the filesystem's
bucket path). Hiding *filesystem names* would need SIV (optional; see
`EncryptFSNames` below), but hiding file/directory names inside a filesystem
does not.

## 4. Design (mirrors `ts`, absorbs `aeadfs`)

```go
// fs/store.go — new file fs/aead.go
type Config struct {
    Cipher CipherSuite   // default CipherAuto → AES-256-GCM
    Key    []byte        // exactly KeySize (32); OR KeyProvider
    KeyProvider KeyProvider

    EncryptContent  bool // seal data-bucket file contents (and symlink paths)
    EncryptMetadata bool // seal inode blobs (hides names, mode, size, times)

    // aeadfs merge
    ChunkSize    int   // >0 enables chunked content encryption (random access)
    EncryptFSNames bool // optional: SIV-encrypt the Store.Open(name) bucket path
}

type KeyProvider interface { Key() ([]byte, error) }
// RawKeyProvider, EnvKeyProvider, MultiKeyProvider (mirror ts/aeadfs).

func OpenWithConfig(ctx context.Context, db *bolt.DB, root []byte, cfg *Config) (*Store, error)
```

- `Open(ctx, db, root)` remains the plaintext constructor (`OpenWithConfig(nil)`).
- `Store` gains the same optional state as `ts.Store`: `engine`, `engines`
  (fallbacks), `cipher`, `encContent`, `encMetadata`, `chunkSize`, `siv`.
- Each `FileSystem` opened via `Store.Open` inherits the store's engine; the
  encryption seam (inodes/content/symlinks) consults it.

**Hard requirement — encryption is optional:** with a nil `Config` (or none
enabled) every code path must be byte-identical to today; no marker is written
and plaintext gob/data bytes are unchanged. This is the same invariant `ts`
enforces and must be regression-tested the same way.

## 5. Tiers

### Tier 1 — `EncryptContent` + `EncryptMetadata` (the `db/codec/aead` / ts `EncryptValues` analog)

- `saveData` writes `nonce || seal(content)`; `loadData` splits nonce, decrypts.
- `fsBucket.PutInode`/`GetInode` and `FileSystem.loadInode` seal/unseal the
  gob-encoded inode (`nonce || seal(gob(iNode))`).
- `fsBucket.Symlink`/`Readlink` seal/unseal the target path.
- Inode numbers, `state` keys, and bucket paths stay clear (numeric/structural).
- Fresh random nonce per write (value, inode, symlink). `i2b(ino)` keys are
  unaffected, so inode random access and iteration keep working.

Sealed record formats:

```
sealed value = nonce || engine.Encrypt(nonce, plaintext)
```

### Tier 2 — chunked content + rotation (the `aeadfs` analog)

- **Chunked content** (`ChunkSize`): port `aeadfs`'s chunked file format
  (`chunk_format.go`/`chunked_file.go`) so `ReadAt`/`WriteAt`/`Seek`/`Truncate`
  on large files touch only the affected chunk(s) instead of re-encrypting the
  whole blob. This requires refactoring `File.Read`/`Write`/`ReadAt`/`WriteAt`/
  `Truncate` (which today do whole-blob `loadData`/`saveData`) to a range-based
  data layer keyed by `(ino, chunkIndex)`. Each chunk = `nonce || seal(chunk)`.
- **Rotation** (`ReEncrypt`/`RotateAll`/`VerifyAll`): mirror `ts/rotate.go` /
  `aeadfs/key_rotation.go` — decrypt every inode/content/symlink record with the
  current (or fallback) key and re-seal under the new key, then adopt the new
  key and rewrite the marker canary.

## 6. Mapping `ts` → `fs`

| `ts` concept | `fs` equivalent | Notes |
|---|---|---|
| point value | file content (`data` bucket) | `saveData`/`loadData` |
| series metadata (JSON) | inode (gob, incl. `Children` names) | `PutInode`/`GetInode`/`loadInode` |
| series name (bucket key) | *inside* the inode; not a key | no SIV needed |
| timestamp key (order-preserving, clear) | inode number key (`i2b`, clear) | same "numeric key stays clear" trade-off |
| `EncryptValues` | `EncryptContent` (+ `EncryptMetadata`) | |
| `EncryptSeriesNames` (SIV) | optional `EncryptFSNames` (SIV on Store.Open path) | only if filesystem names must be hidden |
| `BlockDuration` (blocks) | `ChunkSize` (chunks) | both give random access |
| marker + canary | marker + canary in `state` bucket | same design |
| `MultiKeyProvider`/`ReEncrypt`/`RotateAll`/`VerifyAll` | identical, at inode+content level | |

## 7. Merging `fs/aeadfs` into `fs`

Bring over (lift, not copy):

- `Config` shape, `KeyProvider` + `RawKeyProvider`/`EnvKeyProvider`/
  `MultiKeyProvider` — into `fs` (mirroring `ts`). Reuse `internal/encryption`
  for engines and SIV (already shared).
- The chunked file format (`chunk_format.go`) and its LRU chunk cache — into a
  new `fs/chunk.go` (Tier 2).
- Key-rotation helpers (`key_rotation.go`) — into `fs/rotate.go` (Tier 2).
- SIV engine already lives in `internal/encryption`; only needed if
  `EncryptFSNames` is wanted.

Drop (what the wrapper did that native `fs` does not):

- The generic `absfs.FileSystem` **wrapper** (`AEADFS`): native `fs` encrypts at
  rest in bolt instead of wrapping. This removes the ability to transparently
  encrypt *other* filesystems (osfs/memfs/s3fs) with one wrapper.
- `FilenameEncryptionRandom` + metadata database: not needed — inode encryption
  already hides names without a mapping database.

Consequence to decide: `FileSystem.SetContentFS` (external content filesystem)
stores content outside bolt. Native content encryption applies to the bolt
`data` bucket; if external content storage must also be encrypted, either keep a
thin `aeadfs` shim for that path or wrap the contentFS — flag as an open
decision.

## 8. Marker, canary, mode detection

Mirror `ts` exactly, but store the marker in the filesystem's `state` bucket
(reserved key `"\x00__fs_enc__"`):

```
magic "FENC"(4) | version(1) | cipher(1) | flags(1: content<<0|metadata<<1|chunked<<2) | chunkSize(8 LE) | sealed canary
canary = nonce || seal("storm/fs/aead/v1")
```

- Wrong key → `ErrAuthFailed` at open (canary).
- Plaintext open of an encrypted store, or encrypted open of a store that
  already has plaintext data, → `ErrWrongMode` (never silent mixing).
- The marker is written on the first encrypted write; a plaintext store has
  none.

## 9. Error handling, nonce discipline, backward compatibility

- Add `fs.ErrAuthFailed`/`ErrInvalidKey`/`ErrUnsupportedCipher`/
  `ErrInvalidConfig`/`ErrWrongMode`; decrypt failures surface as `ErrAuthFailed`
  (never as "not found", never a panic).
- Fresh random nonce per sealed record (value, inode, symlink, chunk) via
  `encryption.GenerateNonce`; re-encryption uses new nonces.
- `Open` is unchanged and plaintext-only; unencrypted on-disk bytes are
  byte-identical (regression test asserts this, incl. no marker).
- Inode `Size` remains the plaintext size (it lives inside the encrypted inode
  and is recovered on decrypt), so `File`/`Stat` behavior is unchanged.

## 10. What leaks (per option)

| | plaintext | `EncryptMetadata` | `EncryptContent` | both |
|---|---|---|---|---|
| file contents | visible | visible | **confidential** | **confidential** |
| file/dir names | visible | **confidential** | visible | **confidential** |
| inode metadata (mode/size/times/uid/gid) | visible | **confidential** | visible | **confidential** |
| symlink targets | visible | visible | **confidential** | **confidential** |

Always visible: number of inodes/records, record sizes, access patterns, bucket
structure, inode numbers, and (unless `EncryptFSNames`) the filesystem names
passed to `Store.Open`.

## 11. Open decisions

1. **Scope**: Tier 1 (`EncryptContent`+`EncryptMetadata`) vs Tier 2 (chunked +
   rotation). Recommend Tier 1 first, matching the ts rollout.
2. **External contentFS**: encrypt the bolt `data` bucket only, or also support
   encrypted external content (wrap contentFS / keep an aeadfs shim)?
3. **`fs/aeadfs` removal**: fully delete the package (dropping its
   "encrypt any absfs.FileSystem" capability), or keep a thin shim for non-bolt
   filesystems? The task says merge; recommendation is to delete and fold the
   crypto into `fs`.
4. **`EncryptFSNames`**: hide filesystem names (`Store.Open` paths) with SIV, or
   leave them clear (file/dir names are already hidden by inode encryption)?
5. **Key scope**: one key per `Store` (all its filesystems share it) vs per-
   filesystem keys. Recommend per-Store (matches `ts`).

## 12. Files affected (proposed)

```
internal/encryption/       (existing, reused: CipherSuite, CipherEngine, SIVEngine)
fs/
├── store.go               add engine/engines/cipher/enc flags; OpenWithConfig
├── aead.go (new)          Config, KeyProvider, Raw/Env/MultiKeyProvider, marker/canary
├── tx.go                  fsBucket gains engine; PutInode/GetInode/Symlink/Readlink seal/unseal
├── inode.go               encodeNode/decodeNode become seal/unseal-aware (or move behind fsBucket)
├── fs.go                  saveData/loadData seal/unseal; loadInode routes via sealed decode
├── file.go                (Tier 2 only) range/chunk-based Read/Write/ReadAt/WriteAt/Truncate
├── chunk.go (new)         chunked content format + LRU cache (port of aeadfs/chunk_format.go)
├── rotate.go (new)        MultiKeyProvider, ReEncrypt, RotateAll, VerifyAll
├── aead_test.go (new)     round-trip, wrong-key, tamper, nonce, optionality, chunk, rotation
└── (delete) aeadfs/       folded into fs
```

## 13. Test plan sketch

- **Optionality**: nil `Config` ⇒ on-disk inode bytes are raw gob, content bytes
  are raw, no `FENC` marker; byte-identical to the current implementation.
- **Round-trip**: every public op (`OpenFile`/`ReadFile`/`Read`/`Write`/
  `ReadAt`/`WriteAt`/`Seek`/`Truncate`/`Mkdir`/`Remove`/`Rename`/`ReadDir`/
  `Stat`/`Symlink`/`Readlink`/snapshots) matches a plaintext control store.
- **Auth**: wrong key → `ErrAuthFailed` (canary); tampered inode/content →
  `ErrAuthFailed`; truncated ciphertext → error, no panic.
- **Nonce**: two writes of the same content produce different ciphertext.
- **Tier 2**: chunk boundary writes/reads, seek across chunks, partial-chunk
  write, truncate; `Count`/size consistency.
- **Rotation**: `ReEncrypt`/`RotateAll`/`VerifyAll`; old key decrypts until
  rotation, then fails; new key works after.
- **Mode mismatch**: plaintext open of encrypted store / encrypted open of
  plaintext store → `ErrWrongMode`.
- `go vet ./fs/...`, `go test -race ./fs/...`, `go test ./...`.

## 14. Verification checklist

```sh
cd /home/malivvan/projects/storm
gofmt -l fs internal/encryption        # expect empty
go vet ./fs/... ./internal/encryption/...
go build ./fs/...
go test -count=1 ./fs/...
go test -race -count=1 ./fs/...
go test -count=1 ./...                 # no regressions elsewhere
```
