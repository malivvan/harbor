# Plan: Implement optional, native AEAD encryption for the `vec` store

Implements the recommended proposal of `VEC.md`: native, opt-in AEAD encryption
built directly into `vec`, reusing `internal/encryption` exactly as `ts`, `fs`,
and `db` already do.

## Hard requirement: encryption is optional

Encryption is strictly opt-in and must cause **zero behavioral change** when not
configured:

- The existing `Open(ctx, bdb, root, useQuant, quantizer)` constructor keeps its
  exact signature and semantics — a plaintext store, byte-for-byte identical to
  today.
- Encryption is only active when an explicit `Config` is passed to a new
  `OpenWithConfig` constructor **and** that `Config` enables at least one
  encryption feature. No implicit encryption, no global default.
- A `Store` opened without encryption has `enc == nil`, and every helper
  (`sealBytes`/`openBytes`/`keyBytes`/`bucketKey`) is an exact passthrough to the
  current `binco.Encode`/`binco.DecodeExact`/`json.Marshal`/`[]byte(id)` paths.
- The on-disk format of a plaintext store is unchanged — no marker is written,
  and a regression test asserts raw binco/JSON bytes and raw ID/name keys.

## Background and reuse

- `internal/encryption` provides `CipherSuite` (`CipherAuto`→`CipherAES256GCM`,
  `CipherXChaCha20Poly1305`), `CipherEngine`, `NewCipherEngine`,
  `GenerateNonce`, `Resolve`, `KeySize=32`, `SIVEngine`, `ErrAuthFailed`,
  `ErrUnsupportedCipher`. `vec` imports it directly.
- `ts` is the closest sibling: `ts/aead.go` (Config, marker/canary),
  `ts/siv.go` (HKDF-derived SIV names), `ts/rotate.go` (rotation) are the
  template. `vec` differs only in being *simpler* — no on-disk ordering.

## Proposed API (new file `vec/aead.go`)

```go
type Config struct {
    Cipher CipherSuite   // default CipherAuto → AES-256-GCM
    Key    []byte        // exactly KeySize (32); OR KeyProvider
    KeyProvider KeyProvider

    EncryptPoints          bool // seal point blobs + collection metadata
    EncryptPointIDs        bool // SIV-encrypt point IDs (exact delete/lookup)
    EncryptCollectionNames bool // SIV-encrypt collection bucket + meta names
}

type KeyProvider interface { Key() ([]byte, error) }
// RawKeyProvider, EnvKeyProvider, MultiKeyProvider (mirror ts).

func OpenWithConfig(ctx context.Context, bdb *bbolt.DB, root []byte, useQuant bool, quantizer Quantizer, cfg *Config) (*Store, error)
```

## Shared encryption state and helpers

```go
// vec/aead.go
type encState struct {
    engine      CipherEngine   // primary engine for writes
    engines     []CipherEngine // primary first, rotation fallbacks after
    cipher      CipherSuite
    encPoints   bool
    encPointIDs bool
    encNames    bool
    siv         *SIVEngine     // nil unless EncryptPointIDs or EncryptCollectionNames
}
```

`Store` gains `enc *encState` (nil when disabled). Helpers (nil `enc` ⇒ exact
passthrough):

```go
func (s *Store) sealBytes(p []byte) ([]byte, error)   // nonce || engine.Encrypt(nonce, p); nil enc → p
func (s *Store) openBytes(c []byte) ([]byte, error)   // inverse; auth failure → ErrAuthFailed
func (s *Store) keyBytes(id string) []byte            // SIV(domain "id") when EncryptPointIDs; else []byte(id)
func (s *Store) bucketKey(name string) []byte         // SIV(domain "name") when EncryptCollectionNames; else []byte(name)
func (s *Store) logicalName(physical []byte) (string, error) // decrypt SIV name (for listing)
```

SIV keys are domain-separated via HKDF (`"storm/vec/keys/id/v1"`,
`"storm/vec/keys/name/v1"`), mirroring `ts/siv.go`.

## Seams to change (exact, `vec/store.go` + new files)

| Function | Current | Change |
|----------|---------|--------|
| `UpsertPoints` | `data := binco.Encode(StoredPointBinco, …); b.Put([]byte(p.ID), data)` | `data := sealBytes(encode)`; `b.Put(keyBytes(p.ID), data)` |
| `LoadCollection` | `binco.DecodeExact(StoredPointBinco, v)` | `openBytes(v)` then decode |
| `SaveCollectionMeta` | `json.Marshal(meta)` → `b.Put([]byte(name), data)` | `sealBytes(marshal)`; `b.Put(bucketKey(name), …)` |
| `LoadCollectionMeta` | `json.Unmarshal(b.Get([]byte(name)))` | `openBytes(b.Get(bucketKey(name)))` then unmarshal |
| `ListCollectionMetas` | `json.Unmarshal(v)` | `openBytes(v)` then unmarshal |
| `DeletePoints` | `b.Delete([]byte(id))` | `b.Delete(keyBytes(id))` |
| `collectionBucket` / `createCollectionBucket` / `DropCollection` | `[]byte(name)` | `bucketKey(name)` |
| `ListCollections` | iterate raw bucket names | `logicalName(key)` to return plaintext names |

`binco.gen.go`, `point.go`, `models.go`, `collection.go`, `index.go`,
`flat_index.go`, `hnsw_index.go`, `hnsw/`, `vek/`, `quant.go` are **untouched** —
they operate on plaintext in-memory data.

## Tier 1 — `EncryptPoints` (values + metadata)

File-by-file:

| File | Change |
|------|--------|
| `vec/errors.go` (new) | `ErrAuthFailed`, `ErrInvalidKey`, `ErrUnsupportedCipher`, `ErrInvalidConfig`, `ErrWrongMode`. |
| `vec/aead.go` (new) | `encState`, `Config`, `KeyProvider` + providers, cipher aliases, `resolveConfig`, `sealBytes`/`openBytes`/`keyBytes`/`bucketKey`/`logicalName`, marker/canary encode/decode. |
| `vec/store.go` | `Store.enc`; `OpenWithConfig`; `Open` delegates; seal/open point blobs + metadata in `UpsertPoints`/`LoadCollection`/`SaveCollectionMeta`/`LoadCollectionMeta`/`ListCollectionMetas`. |
| `vec/aead_test.go` (new) | See test plan. |

Notes:

- Quantization stays inside the point payload and is therefore covered by the
  sealed blob; dequantize happens after `openBytes` in `LoadCollection`.
- Search/filter is in-memory, so the query surface is fully intact.
- Point IDs and collection names stay clear in Tier 1.

## Tier 2 — `EncryptPointIDs` + `EncryptCollectionNames` (SIV)

- `keyBytes` SIV-encrypts point IDs (domain `"id"`), so `DeletePoints` and
  dedup exact lookups keep working.
- `bucketKey` SIV-encrypts collection names (domain `"name"`); `ListCollections`
  and `ListCollectionMetas` decrypt names via `logicalName` so they return the
  original names.
- Leak: value equality (same ID/name → same ciphertext); otherwise IDs and names
  are confidential.

## Marker, canary, mode detection

Marker + canary (unencrypted) in the `__collections_meta__` bucket under the
reserved key `"\x00__vec_enc__"`:

```
magic "VENC"(4) | version(1) | cipher(1) | flags(1: points<<0|ids<<1|names<<2) | sealed canary
canary = nonce || seal("storm/vec/aead/v1")
```

`OpenWithConfig` performs:

1. Read the marker (read-only tx).
2. Marker present + `enc == nil` → `ErrWrongMode` (encrypted store, no key).
3. Marker present + `enc != nil` → verify the marker matches settings and decrypt
   the canary (wrong key → `ErrAuthFailed`).
4. Marker absent + `enc != nil` → if the store already has data (any collection
   bucket, or meta entries beyond the marker) → `ErrWrongMode`; otherwise write
   the marker + canary during init.
5. Marker absent + `enc == nil` → plaintext (current behavior).

## Rotation (`vec/rotate.go`)

Mirror `ts/rotate.go`:

- `MultiKeyProvider` (primary + fallbacks).
- `Store.ReEncryptCollection(ctx, name, newKey)` / `Store.RotateAll(ctx, newKey)`
  — walk all collections, decrypt every point blob and metadata with the current
  (or fallback) key and re-seal under the new key; SIV keys are re-derived under
  the new key (full rewrite); rewrite the marker canary; then adopt the new key.
- `Store.VerifyAll(ctx)` — re-read every point + metadata + the canary to confirm
  decryptability before the old key is retired.

## Error handling and nonce discipline

- Decrypt failures → `ErrAuthFailed` on every read; never "not found", never a
  panic.
- Invalid config (bad key length, unknown cipher, both/neither `Key` and
  `KeyProvider`) → `ErrInvalidConfig`.
- Mode/key mismatch → `ErrWrongMode` / `ErrAuthFailed` (canary).
- Fresh random nonce per sealed point/metadata via `encryption.GenerateNonce`;
  re-encryption always uses new nonces.

## Backward compatibility and optionality guarantees

- `Open` is unchanged and plaintext-only.
- `OpenWithConfig` with a nil `Config` (or none enabled) is equivalent to `Open`.
- Unencrypted stores write no marker and their bytes are byte-identical
  (regression test asserts raw binco/JSON bytes + raw keys + no `VENC` marker).
- `NewStorage`/`NewStorageWithQuantization` keep their signatures and remain
  plaintext-only (the borrowed-DB `OpenWithConfig` is the encryption entry
  point); an encrypted owned-DB variant is a noted follow-on.
- Opening a store that already contains plaintext collections with encryption
  requested returns `ErrWrongMode`.

## Test plan

- **Optionality**: nil `Config` ⇒ raw binco point bytes, raw JSON metadata, raw
  ID/name keys, no `VENC` marker; byte-identical to today.
- **Round-trip** (Tier 1): `UpsertPoints`/`LoadCollection`/`SaveCollectionMeta`/
  `LoadCollectionMeta`/`ListCollectionMetas`/`DeletePoints`/`DropCollection`/
  `NewCollection`+`Search` match a plaintext control store.
- **Auth**: wrong key → `ErrAuthFailed` (canary); tampered point/meta →
  `ErrAuthFailed`; truncated ciphertext → error, no panic.
- **Nonce**: two `UpsertPoints` of equal data produce different ciphertext.
- **Quantization**: quantized vectors round-trip through encryption.
- **Tier 2**: SIV determinism (same ID/name → same key, distinct differ, domain
  separation); `DeletePoints` exact lookup works; `ListCollections`/
  `ListCollectionMetas` return decrypted names.
- **Rotation**: `ReEncryptCollection`/`RotateAll`/`VerifyAll`; old key decrypts
  until rotation, then fails; new key works after.
- **Mode mismatch**: plaintext open of encrypted store / encrypted open of
  plaintext store → `ErrWrongMode`.
- `go vet ./vec/...`, `go test -race ./vec/...`, `go test ./...`.

## Verification checklist

```sh
cd /home/malivvan/projects/storm
gofmt -l vec internal/encryption        # expect empty
go vet ./vec/... ./internal/encryption/...
go build ./vec/...
go test -count=1 ./vec/...
go test -race -count=1 ./vec/...
go test -count=1 ./...                 # no regressions elsewhere
```

Acceptance: build/vet/test pass; plaintext behavior provably unchanged; the
optional encryption path matches `ts`/`fs`/`db` security properties (confidential
vectors + payloads + metadata, authenticated, fresh nonces, SIV keys, key
verification + rotation).

## Ordered task list

1. Add `vec/errors.go` sentinels (`ErrAuthFailed`, `ErrInvalidKey`,
   `ErrUnsupportedCipher`, `ErrInvalidConfig`, `ErrWrongMode`).
2. Add `vec/aead.go` — `encState`, `Config`, `KeyProvider` + providers, cipher
   aliases, `resolveConfig`, `sealBytes`/`openBytes`/`keyBytes`/`bucketKey`/
   `logicalName`, marker/canary encode/decode.
3. Add `Store.enc`; add `OpenWithConfig`; make `Open` delegate; add
   marker/canary mode detection.
4. Thread seal/open through `UpsertPoints`, `LoadCollection`,
   `SaveCollectionMeta`, `LoadCollectionMeta`, `ListCollectionMetas` (Tier 1).
5. Add Tier 1 tests (round-trip, wrong key, tamper, nonce, optionality,
   mode mismatch, quantization).
6. Implement `EncryptPointIDs` + `EncryptCollectionNames` SIV in
   `keyBytes`/`bucketKey`/`logicalName`, and route `DeletePoints`,
   `collectionBucket`/`createCollectionBucket`/`DropCollection`/`ListCollections`
   through them (Tier 2).
7. Add `vec/rotate.go` — `MultiKeyProvider`, `ReEncryptCollection`, `RotateAll`,
   `VerifyAll`.
8. Add Tier 2 + rotation tests.
9. Run the full verification checklist; fix any gofmt/vet/test issues.

## Progress log

- [x] **Phase 1 — Tier 1 (`EncryptPoints`)** — `vec/errors.go` sentinels,
  `vec/aead.go` (Config, key providers, `encState`, seal/open + SIV helpers,
  `VENC` marker + canary, mode-detection helpers), `Store.enc` +
  `OpenWithConfig` (with `Open` delegating), and the Tier 1 seams:
  `UpsertPoints` seals the binco blob at `keyBytes(ID)`, `LoadCollection` opens
  the blob and keys points by their real ID, `SaveCollectionMeta`/
  `LoadCollectionMeta`/`ListCollectionMetas` seal and open the JSON (listing
  skips the marker), and the bucket helpers use `bucketKey`/`logicalName`.
  `vec/aead_test.go` covers optionality/byte-identity (bare binco blobs decoded
  directly, no marker), plaintext-vs-encrypted round-trip including
  `NewCollection`+`Search`, confidentiality, wrong key, tampering, fresh nonces,
  quantization, mode mismatch, empty-store adoption and invalid config.
  `go vet ./vec/...` and `go test ./vec/...` pass.
- [x] **Phase 2 — Tier 2 (`EncryptPointIDs` + `EncryptCollectionNames`)** —
  `keyBytes` seals point IDs (domain `id`), `bucketKey` seals collection bucket
  and metadata names (domain `name`), and `logicalName` decrypts names for
  `ListCollections`, so listing still returns the caller's names. Exact deletes,
  reloads and searches keep working through the deterministic SIV keys.
  `vec/aead_names_test.go` covers SIV determinism and domain separation, hidden
  IDs/names/metadata keys, the full round trip under encrypted names (including
  root-scoped stores and reopen), and the rejection of multi-key providers when
  names are encrypted.
- [x] **Phase 3 — rotation** — `vec/rotate.go` adds `withKey`/`adoptEnc`,
  `ReEncryptCollection`, `RotateAll` and `VerifyAll`. A rotation runs in one
  bbolt write transaction: collection buckets are renamed to the new SIV name
  (create-copy-delete, since bbolt cannot rename), point IDs are re-derived,
  point blobs and metadata are re-sealed, and the marker canary is rewritten;
  the store adopts the new key after the commit. `ReEncryptCollection` is
  documented as rotating a single collection only (the others then still need
  the old key, so finished with `RotateAll`). `vec/rotate_test.go` covers a full
  rotation (names, keys and blobs all change; old key rejected; new key
  verifies), value-only rotation (names and IDs untouched), single-collection
  rotation plus the documented `MultiKeyProvider` finish, preconditions, tamper
  detection (point blob, metadata, encrypted ID, marker canary) and the
  multi-key fallback path.
- [x] **Phase 4 — verification and docs** — `gofmt -l vec internal/encryption` is
  empty (one pre-existing indentation issue in `vec/hnsw/graph.go` was fixed:
  whitespace only), `go vet ./vec/... ./internal/encryption/...`,
  `go build ./vec/...`, `go test -count=1 ./vec/...` and
  `go test -race -count=1 ./vec/...` all pass, and the full repository suite
  (`go test -count=1 ./...`) shows no regressions. `POST.md` records the design
  decisions and the follow-ups (owned-DB encrypted constructors, plaintext
  migration, chunked/online rotation, multi-key with encrypted names, leak
  surface, docs, extra tests).
