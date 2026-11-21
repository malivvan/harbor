# Post-implementation notes — optional `ts` encryption

## Status

The plan in `PLAN.md` is implemented and verified.

| Commit | Scope |
|--------|-------|
| `66a4a48` | Tier 1: `OpenWithConfig`, key providers, marker + canary, value and metadata sealing, shared `internal/encryption` SIV engine |
| `a5de584` | Tier 2a: `EncryptSeriesNames` (HKDF-derived AES-SIV bucket/meta keys), `MultiKeyProvider` |
| `ed9e65d` | Tier 2b: `BlockDuration` encrypted time blocks and block-aware retention |
| `76843de` | Key rotation: `ReEncryptSeries`, `RotateAll`, `VerifyAll` |

Verification performed:

- `go build ./...` — OK
- `go vet ./...` — clean
- `go test ./...` — all packages pass
- `go test -race ./ts/` — pass
- `gofmt -l ts internal/encryption` — empty
- `go mod tidy -diff` — empty (only the existing `golang.org/x/crypto` dependency is used, for HKDF)

Plaintext behaviour is unchanged: `Open(ctx, db, root)` opens a plaintext store,
writes no marker, keeps 8-byte point records and JSON metadata, and
`OpenWithConfig` with a nil/empty config is byte-for-byte equivalent (tested).

## Remaining work and limitations

These are deliberate scope boundaries or follow-ups; none affect the default
plaintext path.

1. **No plaintext → encrypted migration.** Opening a store that already contains
   data with an encryption config returns `ErrWrongMode`; a `ts.Migrate` helper
   is left as a follow-on. The reverse (opening an encrypted store without a
   key) also returns `ErrWrongMode`.

2. **Series discovery at the database root.** A root-scoped store enumerates
   series from its root bucket. With `root == nil`, top-level buckets may belong
   to other stores sharing the same bbolt database, so `RotateAll` and
   `VerifyAll` only discover series known from `__ts_meta__` metadata and the
   in-memory cache. Use a non-nil root for complete rotation/verification
   coverage.

3. **Plaintext-data detection at the database root** has the same limitation:
   with `root == nil` only metadata entries are inspected, so series created
   without labels or retention are not detected when encryption is requested on
   an existing plaintext store.

4. **Rotation concurrency.** Rotation re-encrypts in a transaction and switches
   the store's active key afterwards. Do not run `ReEncryptSeries`/`RotateAll`
   concurrently with other operations on the same `Store`; perform rotation
   during quiescence.

5. **Fallback keys do not cover series names.** `MultiKeyProvider` fallbacks are
   tried for values, blocks, metadata and the marker canary. Series names are
   AES-SIV-encrypted with the primary key only, so a store opened with
   `EncryptSeriesNames` must use the current primary key; rotation re-keys the
   names.

6. **Block mode implies encryption.** Setting `BlockDuration > 0` creates an
   engine and seals blocks even when `EncryptValues` is false; the flag has no
   additional effect in block mode.

7. **Top-level integration.** `storm.Store.TS()` remains a plaintext `ts.Store`,
   as specified in the plan. Wiring an encrypted store into `storm.Store`
   requires a key source and was out of scope.

8. **Mode changes require re-encryption.** Changing the cipher, encryption
   flags, or block duration between opens is rejected with `ErrWrongMode`; there
   is no implicit migration between modes.

## Usage

```go
key := loadKey() // exactly ts.KeySize (32) bytes

store, err := ts.OpenWithConfig(ctx, db, []byte("tenant"), &ts.Config{
    Key:           key,
    EncryptValues: true,              // seal values and metadata
    // EncryptSeriesNames: true,      // deterministic AES-SIV bucket names
    // BlockDuration: 5 * time.Minute, // encrypted time blocks
})
```

Rotation:

```go
// Keep the old key available for reads while rotating, then adopt the new key.
oldKey := loadOldKey()
newKey := loadNewKey()
if err := store.RotateAll(ctx, newKey); err != nil { return err }
if err := store.VerifyAll(ctx); err != nil { return err }
_ = oldKey
```
