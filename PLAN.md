# Plan: Implement optional, native AEAD encryption for the `db` store

Implements the recommended proposal of `DB.md`: native, opt-in AEAD encryption
built **directly into `db`** (no encrypting codec), reusing `internal/encryption`
exactly as `ts` and `fs` already do.

## Hard requirement: encryption is optional

Encryption is strictly opt-in and must cause **zero behavioral change** when not
configured:

- The existing `Open(ctx, db, root, c)` constructor keeps its exact signature and
  semantics — a plaintext store, byte-for-byte identical to today.
- Encryption is only active when an explicit `Config` is passed to a new
  `OpenWithConfig` constructor **and** that `Config` enables at least one
  encryption feature. No implicit encryption, no global default.
- A `Store`/`node` opened without encryption has `enc == nil`, and every helper
  (`marshal`/`unmarshal`/`keyBytes`) is an exact passthrough to
  `codec.Marshal`/`codec.Unmarshal`/`toBytes`.
- The on-disk format of a plaintext store is unchanged — no marker is written,
  and a regression test asserts raw codec bytes and raw `toBytes` keys.

## Background and reuse

- `internal/encryption` provides `CipherSuite` (`CipherAuto`→`CipherAES256GCM`,
  `CipherXChaCha20Poly1305`), `CipherEngine`, `NewCipherEngine`,
  `GenerateNonce`, `Resolve`, `KeySize=32`, `SIVEngine`, `ErrAuthFailed`,
  `ErrUnsupportedCipher`. `db` imports it directly.
- **No encrypting codec**: encryption is applied at the Marshal/Unmarshal and
  `toBytes` call sites via internal `*node` helpers — never by wrapping
  `codec.MarshalUnmarshaler`. (`db/codec/aead` was already removed; it is not
  re-created.)
- `ts/aead.go` + `ts/rotate.go` and `fs/aead.go` + `fs/rotate.go` are the
  reference implementations whose `Config`/`encState`/marker/canary/rotation
  machinery is mirrored.

## Proposed API (new file `db/aead.go`)

```go
type Config struct {
    Cipher CipherSuite   // default CipherAuto → AES-256-GCM
    Key    []byte        // exactly KeySize (32); OR KeyProvider
    KeyProvider KeyProvider

    EncryptValues   bool // seal record + KV values (and complex-typed keys)
    EncryptMetadata bool // seal auto-increment counters (codec name stays clear)
    EncryptKeys     bool // SIV-encrypt primary keys + index values (exact lookups keep working)
}

type KeyProvider interface { Key() ([]byte, error) }
// RawKeyProvider, EnvKeyProvider, MultiKeyProvider (mirror ts/fs).

func OpenWithConfig(ctx context.Context, db *bolt.DB, root []byte, c codec.MarshalUnmarshaler, cfg *Config) (*Store, error)
```

## Shared encryption state and node helpers

```go
// db/aead.go
type encState struct {
    engine      CipherEngine   // primary engine for writes
    engines     []CipherEngine // primary first, rotation fallbacks after
    cipher      CipherSuite
    encValues   bool
    encMetadata bool
    encKeys     bool
    siv         *SIVEngine     // nil unless EncryptKeys (HKDF-derived per domain)
}
```

`Store` gains `enc *encState`; every `node` reaches it through its existing
`s *Store` pointer (copied `node`s in `From`/`WithCodec`/`withTransaction` keep
the same `s`, so `enc` is preserved).

Internal helpers on `*node` (nil `enc` ⇒ exact passthrough):

```go
func (n *node) marshal(v any) ([]byte, error)      // codec.Marshal, then seal (if EncryptValues)
func (n *node) unmarshal(raw []byte, v any) error  // open (if EncryptValues), then codec.Unmarshal
func (n *node) keyBytes(k any, domain string) ([]byte, error) // toBytes(k, codec), then SIV(domain, …) (if EncryptKeys)
func (n *node) seal(p []byte) ([]byte, error)      // nonce || engine.Encrypt(nonce, p); nil enc → p
func (n *node) open(c []byte) ([]byte, error)      // inverse; auth failure → ErrAuthFailed
```

`keyBytes` domain strings: `"id"`, `"index"`, `"kv"` (domain-separated SIV keys
via HKDF so the same value in different contexts yields different ciphertext).

## Seams to change (exact, from current code)

| File:line | Current | Change |
|-----------|---------|--------|
| `db/store.go:377` | `n.codec.Marshal(data)` (record value) | `n.marshal(data)` |
| `db/kv.go:200` | `n.codec.Marshal(value)` (KV value) | `n.marshal(value)` |
| `db/kv.go:175` | `n.codec.Unmarshal(raw, …)` (KV get) | `n.unmarshal(raw, …)` |
| `db/finder.go:181,432` | `n.codec.Unmarshal(raw, …)` (reads) | `n.unmarshal(raw, …)` |
| `db/sink.go:64` | `s.node.Codec().Unmarshal(v, …)` | `s.node.unmarshal(v, …)` |
| `db/db.go:128` | `s.Codec().Unmarshal(raw, &v)` (version) | route through `n.unmarshal` |
| `db/store.go:271,573` | `toBytes(cfg.ID.Value, codec)` (PK) | `n.keyBytes(…, "id")` |
| `db/store.go:322` | `toBytes(fieldValue, codec)` (index value) | `n.keyBytes(…, "index")` |
| `db/finder.go:105,243,536,545,679` | `toBytes(value/min/max/prefix, codec)` | `n.keyBytes(…, "index")` |
| `db/kv.go:32,98,150,218,257` | `toBytes(key, codec)` (KV key) | `n.keyBytes(…, "kv")` |
| `db/sink.go:196,201` | `toBytes(…, s.node.Codec())` (query compare) | `s.node.keyBytes(…, "index")` |
| `db/metadata.go` | `meta.increment` stores `numbertob` counters | seal/unseal counter when `EncryptMetadata` |

`db/index/*` and `db/q/*` are untouched — they only receive already-encoded
bytes produced by `keyBytes`.

`meta` (`db/metadata.go`) gains `enc *encState` (set in `newMeta` from the
`*node`), so `increment` can seal/unseal the counter; the `codec` name stays
plain (needed by the `ErrDifferentCodec` check).

## Tier 1 — `EncryptValues` + `EncryptMetadata`

File-by-file:

| File | Change |
|------|--------|
| `db/aead.go` (new) | `encState`, `Config`, `KeyProvider`, `RawKeyProvider`, `EnvKeyProvider`, cipher aliases, `resolveConfig`, `seal`/`open`/SIV helpers, marker/canary encode/decode. |
| `db/errors.go` | add `ErrAuthFailed`, `ErrInvalidKey`, `ErrUnsupportedCipher`, `ErrInvalidConfig`, `ErrWrongMode`, `ErrEncryptedOrdering`. |
| `db/db.go` | `Store.enc`; `OpenWithConfig`; `Open` delegates; `checkVersion` reads the version through `unmarshal`; `toBytes` stays as the plaintext base helper. |
| `db/store.go` | `node.save` uses `n.marshal`; `n.keyBytes` for PK/index values. |
| `db/finder.go`, `db/query.go`, `db/scan.go`, `db/sink.go` | `n.codec.Unmarshal` → `n.unmarshal`. |
| `db/kv.go` | `Set`/`Get` use `n.marshal`/`n.unmarshal`; KV keys via `n.keyBytes`. |
| `db/metadata.go` | seal/unseal counters when `EncryptMetadata`. |
| `db/aead_test.go` (new) | See test plan. |

Notes:

- With `EncryptValues`, the `version` record (written through `Set`) and
  complex-typed keys (the `toBytes` default branch → `codec.Marshal`) are sealed
  as values; the `keyBytes` SIV path only applies when `EncryptKeys` is on.
- Query surface fully intact in Tier 1 — indexes and all finders keep working.

## Tier 2 — `EncryptKeys` (SIV) + ordered-query guard

- `n.keyBytes` SIV-encrypts the `toBytes` output (base64url), domain-separated by
  `"id"`/`"index"`/`"kv"` via HKDF (`"storm/db/keys/<domain>/v1"`).
- Exact lookups keep working: `Save`/`One`/`Get`/`Find`/`Update`/`DeleteStruct`,
  unique + list index exact lookups, `Set`/`Get`/`SetBytes`/`GetBytes`.
- **Ordered/prefix queries return `ErrEncryptedOrdering`** when `encKeys` is on
  (adopted recommendation — fail loud, no silent perf cliff):
  `Range`, `Prefix`, `AllByIndex`, and `Reverse`/`OrderBy` over index scans.
  `All`/`Each`/`AllRecords` continue to work but return records in ciphertext
  order (documented; not sorted).
- Guard is centralized in `finder.go`/`query.go` (`if n.s.enc != nil &&
  n.s.enc.encKeys { return ErrEncryptedOrdering }`).

## Marker, canary, mode detection

Marker + canary (unencrypted) in the `__database` bucket under the reserved key
`"\x00__db_enc__"`:

```
magic "DENC"(4) | version(1) | cipher(1) | flags(1: values<<0|metadata<<1|keys<<2) | sealed canary
canary = nonce || seal("storm/db/aead/v1")
```

`OpenWithConfig` performs:

1. Read the marker (read-only tx on `__database`).
2. Marker present + `enc == nil` → `ErrWrongMode` (encrypted store, no key).
3. Marker present + `enc != nil` → verify marker matches settings, decrypt canary
   (wrong key → `ErrAuthFailed`).
4. Marker absent + `enc != nil` → if the store already has user data (any record
   bucket other than `__database`, or non-empty `__database` beyond the reserved
   keys) → `ErrWrongMode`; otherwise write the marker + canary during init.
5. Marker absent + `enc == nil` → plaintext (current behavior).

## Rotation (`db/rotate.go`)

Mirror `ts/rotate.go`:

- `MultiKeyProvider` (primary + fallbacks).
- `Store.ReEncrypt(ctx, newKey)` / `Store.RotateAll(ctx, newKey)` — walk all
  record buckets, decrypt every value/metadata with the current (or fallback)
  key and re-seal under the new key; keys are re-derived (SIV) when `EncryptKeys`
  is on; rewrite the marker canary; then adopt the new key.
- `Store.VerifyAll(ctx)` — re-read every record + the canary to confirm
  decryptability before the old key is retired.

## Error handling and nonce discipline

- Decrypt failures → `ErrAuthFailed` on every read; never "not found", never a
  panic.
- Invalid config (bad key length, unknown cipher, both/neither `Key` and
  `KeyProvider`) → `ErrInvalidConfig`.
- Mode/key mismatch → `ErrWrongMode` / `ErrAuthFailed` (canary).
- `EncryptKeys` ordered/prefix queries → `ErrEncryptedOrdering`.
- Fresh random nonce per sealed value/counter via `encryption.GenerateNonce`;
  re-encryption always uses new nonces.

## Backward compatibility and optionality guarantees

- `Open(ctx, db, root, c)` is unchanged and plaintext-only.
- `OpenWithConfig` with a nil `Config` (or none enabled) is equivalent to `Open`.
- Unencrypted stores write no marker and their bytes are byte-identical
  (regression test asserts raw codec output + raw keys + no `DENC` marker).
- Codec-name compatibility (`ErrDifferentCodec`) is unaffected.
- Opening a store that already has plaintext records with encryption requested
  returns `ErrWrongMode`; plaintext→encrypted migration is a separate follow-on.

## Test plan

- **Optionality**: nil `Config` ⇒ raw codec bytes, raw `toBytes` keys, no `DENC`
  marker; byte-identical to today.
- **Round-trip** (Tier 1): `Save`/`One`/`Find`/`All`/`Range`/`Prefix`/`Update`/
  `DeleteStruct`/`ReIndex`/`Set`/`Get`/`SetBytes`/`GetBytes`/`Count`/`Drop` match
  a plaintext control store.
- **Auth**: wrong key → `ErrAuthFailed` (canary); tampered value/counter →
  `ErrAuthFailed`; truncated ciphertext → error, no panic.
- **Nonce**: two `Save` of equal data produce different ciphertext.
- **Tier 2**: SIV determinism (same value → same key; distinct values differ;
  domain separation across id/index/kv); exact lookups work; `Range`/`Prefix`/
  `AllByIndex` → `ErrEncryptedOrdering`.
- **Rotation**: `ReEncrypt`/`RotateAll`/`VerifyAll`; old key decrypts until
  rotation, then fails; new key works after.
- **Mode mismatch**: plaintext open of encrypted store / encrypted open of
  plaintext store → `ErrWrongMode`.
- `go vet ./db/...`, `go test -race ./db/...`, `go test ./...`.

## Verification checklist

```sh
cd /home/malivvan/projects/storm
gofmt -l db internal/encryption        # expect empty
go vet ./db/... ./internal/encryption/...
go build ./db/...
go test -count=1 ./db/...
go test -race -count=1 ./db/...
go test -count=1 ./...                 # no regressions elsewhere
```

Acceptance: build/vet/test pass; plaintext behavior provably unchanged; the
optional encryption path matches `ts`/`fs` security properties (confidential
records + indexed values via SIV, authenticated, fresh nonces, key verification
+ rotation).

## Ordered task list

1. Add `db/errors.go` sentinels (`ErrAuthFailed`, `ErrInvalidKey`,
   `ErrUnsupportedCipher`, `ErrInvalidConfig`, `ErrWrongMode`,
   `ErrEncryptedOrdering`).
2. Add `db/aead.go` — `encState`, `Config`, `KeyProvider` + providers, cipher
   aliases, `resolveConfig`, `seal`/`open`/SIV helpers, marker/canary
   encode/decode.
3. Add `Store.enc`; add `OpenWithConfig`; make `Open` delegate; route
   `checkVersion` through `unmarshal`; add marker/canary mode detection.
4. Add `*node` helpers (`marshal`/`unmarshal`/`keyBytes`) and convert the seam
   call sites in `store.go`, `kv.go`, `finder.go`, `query.go`, `scan.go`,
   `sink.go` (Tier 1 values + keys).
5. Seal/unseal auto-increment counters in `metadata.go` (`EncryptMetadata`).
6. Add Tier 1 tests (round-trip, wrong key, tamper, nonce, optionality,
   mode mismatch).
7. Implement `EncryptKeys` SIV in `keyBytes` + `ErrEncryptedOrdering` guard in
   `finder.go`/`query.go` (Tier 2).
8. Add `db/rotate.go` — `MultiKeyProvider`, `ReEncrypt`, `RotateAll`, `VerifyAll`.
9. Add Tier 2 + rotation tests.
10. Run the full verification checklist; fix any gofmt/vet/test issues.

## Progress log

- [x] **Phase 1 — Tier 1 (`EncryptValues` + `EncryptMetadata`)** — `db/errors.go`
  sentinels, `db/aead.go` (Config, key providers, `encState`, seal/open/SIV
  helpers, marker + canary, mode detection helpers), `Store.enc` +
  `OpenWithConfig` (with `Open` delegating), `*node` helpers
  `marshal`/`unmarshal`/`keyBytes`, all Tier 1 seam call sites converted
  (`store.go`, `kv.go`, `finder.go`, `sink.go`), sealed auto-increment counters
  in `metadata.go`, and `db/aead_test.go` (optionality/byte-identity,
  plaintext-vs-encrypted round-trip, confidentiality, wrong key, tampering,
  fresh nonces, mode mismatch, empty plaintext store adoption, invalid config).
  `go vet ./db/` and `go test ./db/` pass.
- [x] **Phase 2 — Tier 2 (`EncryptKeys`)** — `keyBytes` seals keys with
  HKDF-derived per-domain AES-SIV engines (`id`, `index`, `kv`), and
  `errEncryptedOrdering` guards the index-backed `Range`, `Prefix`,
  `AllByIndex` and `Find(..., Reverse())` paths with `ErrEncryptedOrdering`.
  Scan-based lookups (`All`, `Each`, `Count`, `Select`) and lookups on
  non-indexed fields keep working, because they decode records instead of
  trusting key order. `db/aead_keys_test.go` covers SIV determinism and domain
  separation, hidden ids/keys, exact lookups, ordered-query rejection, reopen
  stability and tampered-key authentication.
