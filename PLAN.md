# Plan: Implement optional encryption for the `ts` time series store

Implements the recommended proposal from `TS.md`: an **opt-in, tiered** AEAD
encryption layer for `ts`, reusing the shared `internal/encryption` package that
already backs `db/codec/aead` and `fs/aeadfs`.

## Hard requirement: encryption is optional

Encryption is **strictly opt-in** and must cause **zero behavioral change** when
not configured:

- The existing `Open(ctx, db, root)` constructor keeps its exact signature and
  semantics — a plaintext store, byte-for-byte identical to today.
- Encryption is only ever active when an explicit `Config` is passed to a new
  `OpenWithConfig` constructor **and** that `Config` enables at least one
  encryption feature. There is no implicit encryption and no global default.
- A `Store` opened without encryption has all encryption state nil/zero, and
  every code path must fall through to the current plaintext encode/decode
  (`encodeValue`/`decodeValue`/`decodePoint` and JSON metadata).
- The on-disk format of a plaintext store does not change in any way.

Every change below is guarded so that when the store is unencrypted, the code
performs exactly what it does today.

## Background and reuse

- `internal/encryption` (`internal/encryption/encryption.go`) already provides
  `CipherSuite` (`CipherAuto`→`CipherAES256GCM`, `CipherXChaCha20Poly1305`),
  `CipherEngine` (`Encrypt`/`Decrypt`/`NonceSize`/`Overhead`),
  `NewAESGCMEngine`/`NewXChaCha20Poly1305Engine`/`NewCipherEngine`,
  `GenerateNonce`, `Resolve`, `KeySize=32`, `ErrAuthFailed`,
  `ErrUnsupportedCipher`. `ts` imports this package directly (it is within the
  module, so the `internal` path is allowed).
- `ts` does **not** depend on `db/codec/aead` (that `Codec` wraps a
  `codec.MarshalUnmarshaler`, which `ts` does not use); it uses the raw engine
  like `fs/aeadfs` does.
- AES-SIV (needed only for Tier 2 series-name encryption) is added as a new
  `SIVEngine` in `internal/encryption` (additive; `fs/aeadfs`'s private copy in
  `fs/aeadfs/siv.go` is left untouched). See Tier 2.

## Proposed API

New file `ts/aead.go`:

```go
type Config struct {
    Cipher CipherSuite   // default CipherAuto (resolves to AES-256-GCM)
    Key    []byte        // exactly encryption.KeySize (32) bytes; OR KeyProvider
    KeyProvider KeyProvider

    EncryptValues      bool  // Tier 1: seal point values + series metadata
    EncryptSeriesNames bool  // Tier 2: AES-SIV deterministic series-name encryption
    BlockDuration      int64 // Tier 2: >0 groups points into time blocks (nanoseconds)
}

type KeyProvider interface { Key() ([]byte, error) }

// RawKeyProvider, EnvKeyProvider, MultiKeyProvider mirror fs/aeadfs.

func OpenWithConfig(ctx context.Context, db *bolt.DB, root []byte, cfg *Config) (*Store, error)
```

- `Open` remains unchanged and is the plaintext path. (Internally it may share
  initialization with `OpenWithConfig(nil)`.)
- `storm.go` is **not** modified: the top-level `Store.TS()` stays a plaintext
  `ts.Store`. Wiring an encrypted store into `storm.Store` would require a key
  source and is explicitly out of scope.

`Store` gains optional fields (all zero/nil when disabled):

```go
engine    CipherEngine // nil when encryption disabled
cipher    CipherSuite
encValues bool
encNames  bool
blockDur  int64
siv       *SIVEngine   // nil when !encNames
```

## Storage formats

### Sealed value (Tier 1, `EncryptValues`)

```
record = nonce || engine.Encrypt(nonce, encodeValue(v))     // fresh random nonce per point
```

Plaintext record stays `encodeValue(v)` (8 bytes). Timestamp keys stay in the
clear in Tier 1 (order-preserving `encodeKey` untouched), so `Seek`/`Cursor`/
`KeyN` keep working.

### Sealed metadata (Tier 1)

`__ts_meta__` records (`seriesMeta` JSON) are sealed the same way
(`nonce || seal(json)`) when `EncryptValues` is on, plain JSON otherwise. The
metadata bucket is keyed by the physical series key (see Tier 2).

### Store marker + key-verification canary

One reserved, **unencrypted** record in `__ts_meta__` (key `"\x00__ts_enc__"`):

```
magic "TENC"(4) | version(1) | cipher suite(1) | flags(1: encValues<<0|encNames<<1) | blockDur(8 LE) | sealed canary
canary = nonce || seal("storm/ts/aead/v1")
```

- The marker records the store's encryption mode and lets `OpenWithConfig`
  detect mismatches (e.g. opening an encrypted store without a key, or a
  plaintext store with encryption requested while data already exists).
- The sealed canary verifies the key at open time: wrong key → `ErrAuthFailed`
  immediately (same "wrong key fails authentication" behavior as the references).
- The marker is created on the first encrypted write; a plaintext store never has
  one.

### Series-name encryption (Tier 2, `EncryptSeriesNames`)

Physical bucket/meta key = `base64url(SIV(name))` (deterministic, so lookups
work). Implemented by a `(s *Store) bucketKey(name string) []byte` helper used by
`seriesBucket`/`createSeriesBucket`/`DeleteSeries` and the meta-key helpers.
`SeriesNames()` returns logical names from the in-memory cache (already does).

### Time blocks (Tier 2, `BlockDuration`)

```
block key   = encodeKey(blockStart)                    // still order-preserving
block value = nonce || engine.Encrypt(nonce, packBlock(points))
packBlock   = uint32 count | (int64 ts | float64 val)*count   // little-endian
blockStart(ts) = ts - floorMod(ts, blockDur)          // like bucketStart
```

Points inside a block are encrypted together, hiding individual timestamps and
values; only coarse block boundaries leak. `Range`/`RangeEach`/`Aggregate`
seek to the first block and decrypt in order; `Count`/`DeleteRange`/retention do
read-modify-write on blocks.

## File-by-file changes (ordered: Tier 1, then Tier 2)

### Tier 1 — `EncryptValues` (the `db/codec/aead` analog)

| File | Change |
|------|--------|
| `ts/aead.go` (new) | `Config`, `KeyProvider`, `RawKeyProvider`, `EnvKeyProvider`, cipher aliases, config validation, marker/canary encode+decode. |
| `ts/errors.go` | Add `ErrAuthFailed` (alias of `encryption.ErrAuthFailed`), `ErrInvalidKey`, `ErrUnsupportedCipher`, `ErrInvalidConfig`, `ErrWrongMode`. |
| `ts/store.go` | Add optional engine/cipher/encValues fields; add `OpenWithConfig`; refactor init shared with `Open`; add `sealValue`/`openValue` helpers; marker read/write on open + first write. |
| `ts/encode.go` | Add `(s *Store) sealValue(v float64) ([]byte, error)` and `openValue(raw []byte) (float64, error)` (nil engine → plaintext). Keep `encodeKey`/`decodeKey` unchanged. |
| `ts/series.go` | `InsertMany` uses `sealValue`; `Get`/`First`/`Last`/`Range`/`RangeEach`/`Aggregate` use `openValue` via a `decodePoint` that is now a `Series`/`Store` method; `readMeta`/`writeMeta` seal/unseal JSON when `encValues`. |
| `ts/retention.go` | No change (prunes by timestamp keys, still clear). |
| `ts/store_test.go`, `ts/aead_test.go` (new) | See test plan. |

Notes for Tier 1:

- `decodePoint(key, value)` becomes a method (or takes a value decoder) because
  decryption needs the store's engine. Its current `len(value) != recordSize`
  check becomes: plaintext → must be 8; sealed → must be ≥ `NonceSize()+Overhead()`.
- `Count` is unchanged (`KeyN` still = number of points when not blocked).

### Tier 2 — `EncryptSeriesNames` + `BlockDuration` + rotation (the `fs/aeadfs` analog)

| File | Change |
|------|--------|
| `internal/encryption/siv.go` (new) | `SIVEngine` (RFC 5297 S2V + AES-CTR), 64-byte key, `Encrypt`/`Decrypt` with `ErrAuthFailed`. Additive only. |
| `ts/siv.go` (new) | Wraps SIV for series names: HKDF-SHA256(masterKey, "storm/ts/siv/v1") → 64-byte SIV key; `encryptName`/`decryptName` with base64url. |
| `ts/store.go` | Add `encNames`, `blockDur`, `siv`; add `bucketKey(name)` translation; thread it through `seriesBucket`/`createSeriesBucket`/`DeleteSeries`/meta helpers. |
| `ts/block.go` (new) | `packBlock`/`unpackBlock`, `blockStart`, block-key helpers; block-level insert-merge, range scan, count, delete, prune. |
| `ts/series.go` | Route `InsertMany`, `Get`, `First`, `Last`, `Range`, `RangeEach`, `Aggregate`, `Count`, `DeleteRange` through the block codec when `blockDur > 0`. |
| `ts/retention.go` | `pruneBucket` gains a block-aware path (read-modify-write over blocks) when blocked. |
| `ts/rotate.go` (new) | `MultiKeyProvider`, `Store.ReEncryptSeries`, `Store.RotateAll`, `Store.VerifyAll` (mirrors `fs/aeadfs/key_rotation.go`). |

Block-mode semantics to get right:

- `InsertMany`: group incoming points by block; for each block read the existing
  block (decrypt, unpack), merge (last-write-wins per timestamp), pack, seal,
  write under `blockKey(blockStart)`.
- `Get(ts)`: seek to the block containing `ts`, unpack, exact lookup.
- `Range`/`RangeEach`/`Aggregate`: iterate blocks overlapping `[start,end]`,
  decrypt each, filter/aggregate points within the window.
- `Count`: sum unpacked point counts (no longer `KeyN`).
- `DeleteRange`/retention: for each overlapping block, decrypt, drop the target
  points, re-encrypt; delete the block when empty.
- Fresh nonce per block on every write (re-encryption included).

## Key management and rotation

Mirrors `fs/aeadfs`:

- `RawKeyProvider`, `EnvKeyProvider` (ports of `fs/aeadfs/key_provider.go`).
- `MultiKeyProvider` — primary key for writes, fallbacks tried in order for
  decrypt during rotation.
- `ReEncryptSeries(ctx, name, newKey)` — decrypt every record/block with the
  current (or fallback) key and re-seal under the new key, updating the marker
  canary.
- `RotateAll(ctx, newKey)` — walk `__ts_meta__`/series buckets.
- `VerifyAll(ctx)` — re-read everything to confirm decryptability before the old
  key is retired.

## Error handling and nonce discipline

- Map decrypt failures to `ts.ErrAuthFailed` on all read paths. A tampered point
  is an integrity error, never reported as "not found".
- Reject invalid config (`Key` length ≠ 32, unknown `CipherSuite`, `BlockDuration`
  < 0, both `Key` and `KeyProvider` set, or neither when any encryption flag is
  on) with `ErrInvalidConfig`.
- Fresh random nonce per point (Tier 1) or per block (Tier 2) via
  `encryption.GenerateNonce`; never reuse a nonce under the same key.
- Mode mismatch (marker vs config) → `ErrWrongMode`, never silent downgrade/upgrade.

## Backward compatibility and optionality guarantees

- `Open(ctx, db, root)` is untouched and plaintext-only.
- `OpenWithConfig` with `cfg == nil` or all flags false is equivalent to `Open`.
- Unencrypted stores write no marker, and their bytes are byte-identical to the
  current implementation (a regression test asserts this directly).
- An encrypted store opened without a key (or with the wrong key) fails loudly
  (`ErrAuthFailed` via the canary / `ErrWrongMode`), never returns garbage.
- Opening a store that already contains plaintext series with encryption
  requested returns `ErrWrongMode` rather than silently mixing plaintext and
  ciphertext; plaintext→encrypted migration is a separate `ts.Migrate` helper
  noted as a follow-on (out of scope for this plan's core).

## Test plan

- **Optionality**: with no `Config`, assert on-disk point values are exactly 8
  bytes (raw `encodeValue`), no `__ts_enc__` marker exists, and every public op
  matches a plaintext control store byte-for-byte.
- **Round-trip**: with `EncryptValues`, all of `Insert/InsertMany/Get/First/Last/
  Range/RangeEach/Aggregate/Count/DeleteRange/Drop/SetRetention/EnforceRetention`
  equal the plaintext store's results.
- **Key/auth**: wrong key → `ErrAuthFailed` at open (canary); tampered value →
  `ErrAuthFailed`; truncated ciphertext → error, never panic.
- **Nonce**: two `Insert` of the same value produce different ciphertext.
- **Tier 2**: SIV determinism (same name → same bucket key, distinct names
  differ, base64url round-trip); block mode range across blocks, partial-block
  delete/retention, `Count` correctness.
- **Rotation**: `ReEncryptSeries`/`RotateAll`/`VerifyAll`; old key decrypts until
  re-encryption, then fails; new key works after.
- **Mode mismatch**: plaintext open of a marked store, and encrypted open of a
  store with existing plaintext data, both return `ErrWrongMode`.
- `go vet ./ts/...`, `go test -race ./ts/...`, and `go test ./...` (no regressions).

## Verification checklist

```sh
cd /home/malivvan/projects/storm
gofmt -l ts internal/encryption        # expect empty
go vet ./ts/... ./internal/encryption/...
go build ./ts/...
go test -count=1 ./ts/...
go test -race -count=1 ./ts/...
go test -count=1 ./...                 # no regressions elsewhere
```

Acceptance: build/vet/test pass; plaintext behavior is provably unchanged; the
optional encryption path matches the reference packages' security properties
(confidentiality + authenticity, deterministic SIV names, fresh nonces, key
verification + rotation).

## Ordered task list

1. Add `ts/aead.go` — `Config`, `KeyProvider`, `RawKeyProvider`/`EnvKeyProvider`,
   cipher aliases, config validation, marker/canary encode+decode.
2. Add `ErrAuthFailed`/`ErrInvalidKey`/`ErrUnsupportedCipher`/`ErrInvalidConfig`/
   `ErrWrongMode` to `ts/errors.go`.
3. Add optional engine/cipher/encValues state + `OpenWithConfig` to `ts/store.go`;
   add `sealValue`/`openValue`.
4. Thread value sealing through `ts/series.go` and `ts/encode.go` (Tier 1);
   seal `readMeta`/`writeMeta`.
5. Add Tier 1 tests (round-trip, wrong key, tamper, nonce, optionality).
6. Add `internal/encryption/siv.go` (`SIVEngine`) and `ts/siv.go` (HKDF + base64url
   name encryption); add `encNames` + `bucketKey` translation (Tier 2).
7. Add `ts/block.go` and route reads/writes/count/delete/retention through blocks
   when `BlockDuration > 0` (Tier 2).
8. Add `ts/rotate.go` — `MultiKeyProvider`, `ReEncryptSeries`, `RotateAll`,
   `VerifyAll`.
9. Add Tier 2 tests (SIV, blocks, rotation, mode mismatch).
10. Run the full verification checklist; fix any gofmt/vet/test issues.
