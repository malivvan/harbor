# TODO — encryption implementation review (fs, db, vec, ts)

Review of the optional native-AEAD encryption in `fs`, `db`, `vec`, and `ts`
against the shared `internal/encryption` package. Three categories:

- **Dedup** — code duplicated across packages that should move into
  `internal/encryption` (the explicit goal of this task).
- **Bugs** — correctness / security defects.
- **Issues** — inconsistencies and fragile design choices.

## 1. Deduplication → move to `internal/encryption`

All four packages re-implement the same machinery. Extract it once.

| Shared piece | Where it lives | Proposed home in `internal/encryption` |
|---|---|---|
| `CipherSuite`/`CipherEngine` aliases + `CipherAuto`/`CipherAES256GCM`/`CipherXChaCha20Poly1305`/`KeySize` | `fs/aead.go`, `db/aead.go`, `vec/aead.go`, `ts/aead.go` | already aliases — keep the per-package re-exports, but the block is copy-paste boilerplate |
| `KeyProvider` interface | same 4 files | `encryption.KeyProvider` |
| `RawKeyProvider` + `NewRawKeyProvider` + `Key` | same 4 | `encryption.RawKeyProvider` / `NewRawKeyProvider` |
| `EnvKeyProvider` + `NewEnvKeyProvider` + `Key` | same 4 | `encryption.EnvKeyProvider` / `NewEnvKeyProvider` |
| `MultiKeyProvider` + `NewMultiKeyProvider` + `Key` + `TryKeys` | `fs/rotate.go`, `db/aead.go`, `vec/aead.go`, `ts/rotate.go` | `encryption.MultiKeyProvider` |
| `resolveKeys(cfg)` (validate Key vs KeyProvider, expand MultiKeyProvider, copy + length-check) | same 4 | `encryption.ResolveKeys(key []byte, provider KeyProvider) ([][]byte, error)` |
| `seal` / `open` (fresh nonce `nonce‖seal`; try-each-engine fallback decrypt) | `fs/db/vec` `encState.seal/open`; `ts` `Store.sealBytes/openBytes` | a `encryption.Keychain` type: `NewKeychain(suite, keys)` with `Seal`/`Open` (single source for the fallback loop) |
| `deriveSIV` (HKDF-SHA256 → 64-byte key → `NewSIVEngine`) | `db/aead.go`, `vec/aead.go`, `ts/siv.go` | `encryption.DeriveSIV(masterKey []byte, info string) (*SIVEngine, error)` |
| base64url + SIV name/key seal/open (`sealName`/`openName`, `sealKey`/`openKey`, `EncryptName`/`DecryptName`) | `vec/aead.go`, `db/aead.go`, `ts/siv.go` | `encryption.SIVNameCipher` (`SealName`/`OpenName`) — also gives a single place to fix the error-swallowing (see B2) |
| `verifyCanary` (decrypt + compare constant) | `fs/aead.go`, `db/aead.go`, `vec/aead.go`, `ts/aead.go` | `encryption.VerifyCanary(keychain, sealed, want)` |
| marker encode/decode/`matches` (magic + version + cipher + flags + optional extra field + sealed canary) | same 4 (fs/ts have an 8-byte extra field; db/vec do not) | **done** — `encryption.MarkerSpec`/`Marker` + `EncodeMarker`/`DecodeMarker` own the framing, version and error messages; each store keeps only its `MarkerSpec` (magic + extra size), its flag mapping and `matches` |
| `withKey` / `adoptEnc` (build a same-flags/new-key state; swap it in) | `fs/rotate.go`, `db/rotate.go`, `vec/rotate.go`, `ts/rotate.go` | fold into the `Keychain` rebuild (`Keychain.WithKey`) |
| `pair` + `applyEntries` (rotation rewrite helper) | `db/rotate.go`, `vec/rotate.go` | shared `internal/encryption` (or a small `internal/rotation`) helper |
| `bucketKeys` (copy a bucket's keys) | `fs/rotate.go` | shared helper |
| `ErrInvalidKey` / `ErrInvalidConfig` / `ErrWrongMode` / `ErrEncryptedOrdering` | `fs/errors.go`, `db/errors.go`, `vec/errors.go`, `ts/errors.go` | move to `internal/encryption` as shared sentinels (re-exported per package, like `ErrAuthFailed`/`ErrUnsupportedCipher` already are). Note `ErrEncryptedOrdering` is currently db-only |

The four marker formats already share the same layout but different magic
(`TENC`/`FENC`/`DENC`/`VENC`) and fixed sizes (fs=15, ts=15, db=7, vec=7). A
generic codec would remove four near-identical `encodeMarker`/`decodeMarker`/
`marker.matches`/`verifyCanary` implementations.

## 2. Bugs

### B1 — `ts` does not reject `EncryptSeriesNames` + multiple keys

`db` (`db/aead.go:536-538`) and `vec` (`vec/aead.go:524-526`) reject
`EncryptKeys`/`EncryptNames` combined with `MultiKeyProvider`, because the
deterministic SIV name is derived from a *single* master key and a fallback key
could decrypt values but never reproduce the physical name (silent lookup
misses). `ts.resolveConfig` (`ts/aead.go`) has **no such check** and derives the
SIV key only from `settings.keys[0]` in `OpenWithConfig`.

- Impact: opening a `ts` store with `EncryptSeriesNames` and a
  `MultiKeyProvider` will derive names from the primary key only; after a
  rotation that keeps a fallback key, series-name lookups silently fail.
- Fix: add the same `len(keys) > 1 && encNames → ErrInvalidConfig` guard as
  `db`/`vec` (and ideally centralize it in the shared `resolveKeys`/keychain).

### B2 — SIV name/key helpers swallow errors and return nil keys

- `ts/siv.go` `EncryptName` does `if err != nil { return nil }`.
- `vec/aead.go` `keyBytes`/`bucketKey` discard the error from
  `sealName` and return `nil`.

The "AES-SIV cannot fail with a valid key" comment is true for the cipher
itself, but `sealName`/`sealKey` also fail for other reasons (unknown/missing
domain in the `sivs` map, e.g. a future config mismatch). A returned `nil` is
then used as a bbolt key: `bucket.Put(nil, …)` → `ErrKeyRequired` (misleading),
or `tx.Bucket(nil)` → nil bucket → "not found" (silent).

- `db` handles this correctly: `sealKey`/`openKey` return `(error)`.
- Fix: propagate errors from `sealName`/`EncryptName`/`keyBytes`/`bucketKey`
  (change signatures to return `error`, or assert/panic on the unreachable
  path), and centralize in `encryption.SIVNameCipher`.

### B3 — `db` list-index `"__"` separator collides with base64url output

`db` SIV-encrypts index keys and base64url-encodes them (`db/aead.go` `sealKey`/
`openKey`). base64url's alphabet includes `_`, so an encrypted value or ID can
itself contain `__`. `db/index/list.go` `ListIndex.Range`'s `CompareFn` splits
entries with `bytes.LastIndex(val, []byte("__"))`; a `__` inside the encrypted
component makes that split land in the wrong place.

- Currently latent: `Range`/`Prefix`/`AllByIndex` are gated by
  `errEncryptedOrdering()` (`db/finder.go:272,383,574,712`) when `encKeys` is
  on. The rotation walk (`db/rotate.go` `listIndex`) is safe because it uses
  exact `bytes.HasSuffix(k, "__"+id)`.
- Fix (hardening): use a key encoding whose alphabet excludes the separator
  (hex, or a separator byte outside the base64url alphabet such as `\x00`/
  `!`), or length-prefix each component, so the format is self-delimiting and
  does not depend on the `ErrEncryptedOrdering` gate being airtight.

## 3. Issues / inconsistencies

### I1 — structural divergence: `encState` vs `encSettings` vs store fields

`fs`/`db`/`vec` put encryption state in a shared `encState` struct; `ts` puts it
in `encSettings` (in `aead.go`) and then copies the fields onto `Store` itself,
with `Store.sealBytes/openBytes/sealValue/openValue` instead of
`encState.seal/open`. The rotation `withKey` therefore returns `*encState`
(fs/db/vec) vs `*Store` (ts). Unify on one pattern (recommend the `encState`/
`Keychain` shape) so the shared helpers from section 1 drop in everywhere.

### I2 — per-entity rotation adopts the new key immediately, orphaning the rest

`ReEncryptFS` (fs), `ReEncryptCollection` (vec), `ReEncryptSeries` (ts), and
`ReEncrypt` (db, whole-store so N/A) each `adoptEnc(next)` — making the new key
the **only** key — after rewriting a *single* entity. The remaining entities are
still under the old key and become unreadable until `RotateAll`. The docs note
this, but the API is footgun-prone.

- Suggest: `ReEncrypt<X>` should keep the old key as a *fallback* (add to
  `engines`) rather than replacing the primary, and only `RotateAll` should
  drop the old key — or `ReEncrypt<X>` should not adopt at all. This also
  aligns with `MultiKeyProvider`'s "fallback for decryption" semantics.

### I3 — empty-plaintext asymmetry in `fs`

`fs` `sealContent` seals an empty payload (producing ~28 bytes), but
`openContent` early-returns on `len(raw)==0`. `db`/`vec` instead skip empty on
*both* seal and open (`len(plain)==0` → passthrough). Make the four consistent
(recommend: skip empty on both, as db/vec do).

### I4 — `resolveKeys` error text is tied to per-package sentinels

`resolveKeys` is byte-identical across the four but formats errors with the
package's own `ErrInvalidKey`/`ErrInvalidConfig`. Moving it into
`internal/encryption` (section 1) requires the shared sentinels to move first;
otherwise the shared function can't produce the same errors. Do both in one
change.

### I5 — inconsistent error wrapping

`db` wraps operation context (`wrapError("save", err)` → `"storm save: …"`);
`fs`/`vec`/`ts` return bare sentinels or `fmt.Errorf("%w: …")` with no operation
label. Not a bug, but the encryption error paths (`ErrAuthFailed`,
`ErrWrongMode`) are surfaced with different context across stores. Consider a
consistent wrapping convention for the shared encryption errors.

### I6 — `ErrEncryptedOrdering` only exists in `db`

Only `db` has key encryption, so this sentinel is legitimately db-specific
today. If the shared keychain/SIV helpers move to `internal/encryption`, define
the sentinel there and re-export from `db` so the "ordered query on encrypted
keys" concept has one home.

## 4. Verification items (confirm during the consolidation)

1. `db` `ErrEncryptedOrdering` gate: the four call sites in `finder.go` cover
   `Range`/`Prefix`/`AllByIndex`/`OrderBy`/`Reverse`; confirm `query.go`,
   `scan.go`, and `sink.go` have no ordered index scan that bypasses the gate.
2. Before moving `KeyProvider`/`MultiKeyProvider`/sentinels to
   `internal/encryption`, grep for external importers of the per-package types
   (`ts.KeyProvider`, `db.RawKeyProvider`, `vec.MultiKeyProvider`, …) — any
   public API change (signature of `keyBytes`, or moved types) is a breaking
   change for those packages' users and should be staged with re-export
   aliases.
3. Run the full suite after each extraction:
   `gofmt -l . && go vet ./... && go test -count=1 ./...` and
   `go test -race -count=1 ./ts/... ./fs/... ./db/... ./vec/...`.

## 5. Progress

Status of each item as the consolidation lands. Commits reference the section.

### Deduplication (section 1)

| Piece | Status |
|---|---|
| `KeyProvider`, `RawKeyProvider`, `EnvKeyProvider`, `MultiKeyProvider` | **done** — `internal/encryption/keys.go`; the four packages re-export type aliases + constructor wrappers, so `fs.NewRawKeyProvider`, `ts.MultiKeyProvider`, … keep working |
| `resolveKeys` | **done** — `encryption.ResolveKeys(key, provider)`; the four copies deleted |
| `seal`/`open` + fallback loop | **done** — `encryption.Keychain.Seal`/`Open` (fresh nonce, try-each-engine fallback); `internal/encryption/keychain.go` + tests |
| `deriveSIV` | **done** — `encryption.DeriveSIV(masterKey, info)`; each package passes its own info string |
| base64 + SIV name/key seal/open | **done** — `encryption.SIVNameCipher` (`SealName`/`OpenName`); `db`, `vec` and `ts` share it, and their local base64/SIV wrappers are gone |
| `verifyCanary` | **done** — `encryption.VerifyCanary(keychain, sealed, want)`; the four packages keep a one-line wrapper |
| marker encode/decode/matches | pending |
| `withKey`/`adoptEnc` | **partly done** — `Keychain.WithKey` / `WithKeyKeepingFallbacks` exist and every package rebuilds its state through them; the per-entity rotation semantics follow in I2 |
| `pair` + `applyEntries`, `bucketKeys` | pending |
| `ErrInvalidKey`/`ErrInvalidConfig`/`ErrWrongMode`/`ErrEncryptedOrdering` | **done** — shared sentinels in `internal/encryption/errors.go`, re-exported by `fs`/`db`/`vec`/`ts` (I6 for the sentinel’s home; the db-only API stays) |

### Bugs (section 2)

- **B1 — done.** `ts.resolveConfig` now calls `encryption.RequireSingleKey` when
  `EncryptSeriesNames` is set, rejecting a `MultiKeyProvider` combination with
  `ErrInvalidConfig` exactly like `db`/`vec` do.
- **B2 — done.** `SIVNameCipher.SealName/OpenName` return errors, and the packages propagate them: `db` already did, `vec.keyBytes`/`bucketKey`/`collectionBucket` and `ts.bucketKey`/`seriesBucket` now return `error` instead of a nil key, and every call site handles it. No key can silently become `nil` any more.
- **B3 — done.** The shared cipher encodes with base64 `RawStdEncoding`, whose alphabet has no `_`, so an encrypted key or name can never contain the `__` separator used by `db` list-index keys. The format change only affects stores written by the pre-consolidation build (the feature is unreleased); plaintext stores are untouched.

### Issues (section 3)

- **I1 — done.** `ts` no longer keeps `engine`/`engines`/`cipher`/`encValues`/`encNames`/`blockDur`/`siv` on `Store`; all four packages now hold an `*encState` wrapping a `*encryption.Keychain`, and `ts` reads its flags through `blockDuration()`/`encryptsValues()` accessors.
- **I2** pending — decision: per-entity rotation keeps the old key as a decryption
  fallback; only `RotateAll` drops it.
- **I3 — done.** `Keychain.Seal`/`Open` pass empty payloads through on both sides, so `fs` now matches `db`/`vec` (an empty file blob stays empty instead of being sealed to a nonce-only payload).
- **I4 — done** together with the shared sentinels.
- **I5** pending (wrapping convention).
- **I6 — done for the sentinel’s home** (`ErrEncryptedOrdering` now lives in
  `internal/encryption` and is re-exported by `db`).

### Verification (section 4)

- Pending; section 4 is re-run at the end.
