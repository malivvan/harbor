# Encrypting the `db` (relational) store — research findings

This documents how the `db` package can be encrypted the same way as `ts` and
`fs` — **native, optional AEAD built directly into `db`**, not via an
encrypting codec. It is research only; no code is changed.

## 1. References and current state

- **`ts`** and **`fs`** already implement the pattern to mirror: an opt-in
  `Config` + `OpenWithConfig`, a shared `encState`, a marker + sealed canary for
  key/mode detection, `CipherEngine` reuse from `internal/encryption`, and
  `MultiKeyProvider` + `ReEncrypt`/`RotateAll`/`VerifyAll` for rotation.
- **`internal/encryption`** provides `CipherSuite`, `CipherEngine`,
  `NewCipherEngine`, `GenerateNonce`, `Resolve`, `KeySize=32`, `SIVEngine`,
  `ErrAuthFailed`, `ErrUnsupportedCipher` — reused by `db` directly.
- **`db/codec/aead` was removed** from `db/codec` (which now only has `json`,
  `binco`, `internal`). This steer confirms the direction: **do not re-create an
  encrypting codec**; implement AEAD support natively inside `db`.

## 2. What `db` stores (the encryption seam)

`db.Open(ctx, db, root, codec)` wraps bbolt; a `Store` embeds a root `node`
(`node{s *Store, codec, rootBucket, tx}`). Storage:

| Data | Where | Encoding | Sensitivity |
|------|-------|----------|-------------|
| Entity records | bucket named by struct type (`cfg.Name`) | key = encoded PK (`id`), value = `codec.Marshal(data)` | **record contents** |
| Index entries | sub-bucket `indexPrefix+fieldName` per indexed field | key = `toBytes(fieldValue) ‖ "__" ‖ id` (list) / `toBytes(fieldValue)` (unique), value = id | **indexed field values** |
| Primary-key "index" | the record bucket itself (`IDIndex`) | key = encoded PK | **PK values** |
| KV store (`Set`/`Get`) | arbitrary bucket | key = `toBytes(key)`, value = `codec.Marshal(v)` | **KV values/keys** |
| Metadata | `__metadata` sub-bucket | `codec` name (plain) + `fieldName+"counter"` auto-inc counters | counters (minor) |
| Version | `__database` bucket | `Set(ctx, "__database", "version", Version)` via codec | minor |

The exact seams (all centralized in `db`, **not** `db/index`):

- **Record values**: `node.save` (`db/store.go`) does
  `raw := n.codec.Marshal(data); bucket.Put(id, raw)`.
- **Record reads**: every `n.codec.Unmarshal(raw, …)` call site
  (`finder.go`, `query.go`, `scan.go`, `sink.go`, `kv.go`).
- **KV values**: `node.Set` (`db/kv.go`) marshals via the codec; `node.Get`
  unmarshals.
- **Keys**: `toBytes(key, n.codec)` (`db/db.go`) — `[]byte`/`string` pass through
  raw, integers go through `numbertob` (big-endian), everything else through
  `codec.Marshal`. This is used for primary keys, index values, and KV keys.
- **Metadata counters**: `newMeta`/`meta.increment` (`db/metadata.go`) store
  `numbertob` counters; the `codec` name stays plain (needed for the
  `ErrDifferentCodec` check).
- `db/index/*` needs **no change**: it only ever receives already-encoded bytes
  from `toBytes`.

## 3. The core tension: `db`'s keys are semantic and queryable

`ts`/`fs` could keep keys clear because their keys are opaque (timestamps /
inode numbers). `db`'s keys are **semantic and queryable**:

1. **Primary keys** are looked up exactly (`One(ctx, "ID", x)` → `toBytes(x)` →
   `bucket.Get(id)`).
2. **Index keys** are the indexed field values, and the query system depends on
   their *order*: `Find`/`One` (exact `Seek`), `Range`, `Prefix`, `AllByIndex`,
   `Reverse`, `OrderBy` (ordered cursor scans via `RangeCursor`/`PrefixCursor`).

AEAD ciphertext is not order-preserving, and even AES-SIV is only deterministic
(equal → equal), not order-preserving. So there is no way to hide keys *and*
keep ordered queries over them — exactly the same boundary `ts`/`fs` hit, but
much more central here because ordering is the whole point of `db`'s indexes.

Consequence: the tiers below trade key confidentiality against which queries
keep working.

## 4. Design (native AEAD in `db`, no encrypting codec)

```go
// db/aead.go (new)
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

- `Open(ctx, db, root, c)` is unchanged and plaintext-only
  (`OpenWithConfig(..., nil)`).
- `Store` gains `enc *encState`; every `node` reaches it via `n.s.enc`.
- `encState` (shared, nil when disabled) holds `engine`, `engines` (rotation
  fallbacks), `cipher`, `encValues`, `encMetadata`, `encKeys`, `siv`.
- **Direct, not a codec**: encryption is applied at the Marshal/Unmarshal and
  `toBytes` call sites via internal helpers, never by wrapping `codec`:

```go
// node helpers (not a codec):
func (n *node) marshal(v any) ([]byte, error)     // codec.Marshal then seal
func (n *node) unmarshal(raw []byte, v any) error // open then codec.Unmarshal
func (n *node) keyBytes(k any) ([]byte, error)    // toBytes then SIV (if EncryptKeys)
```

**Hard requirement — encryption is optional:** nil `Config` (or none enabled) ⇒
identical on-disk bytes (raw codec output, raw `toBytes` keys, no marker); every
seal/open/SIV helper is a no-op when `enc == nil`.

## 5. Tiers

### Tier 1 — `EncryptValues` + `EncryptMetadata` (the `db/codec/aead` analog, done natively)

- Seal record values at `node.save` (`marshal`) and open at every read
  (`unmarshal`); seal KV values in `Set`/open in `Get`.
- Seal auto-increment counters in `metadata.go`; the codec name and bucket names
  stay clear.
- Keys (IDs, index values, KV keys) stay clear.
- **Query surface unchanged** — indexes and all finders keep working.
- **Leak**: indexed field values (they are the index keys), primary keys, bucket/
  type names, KV keys. Un-indexed field values are confidential (inside the
  sealed record).

### Tier 2 — `EncryptKeys` (SIV, deterministic) — *opt-in, with query caveats*

- SIV-encrypt the output of `toBytes` so primary keys and index values are
  deterministic ciphertext. Use domain-separated SIV keys (HKDF info strings,
  e.g. `"storm/db/keys/id/v1"`, `"storm/db/keys/index/v1"`, `"storm/db/keys/kv/v1"`)
  so the same value in different contexts does not produce the same ciphertext.
- **Still works**: `Save`/`One`/`Get`/`Find`/`Update`/`DeleteStruct`, unique-index
  and list-index exact lookups, `Set`/`Get`/`SetBytes`/`GetBytes` (exact key).
- **Breaks / must degrade**: `Range`, `Prefix`, `AllByIndex`, `Reverse`,
  `OrderBy`, and `All`/`Each` ordering — SIV ciphertext is neither ordered nor
  prefix-preserving. These must either return an explicit error
  (`ErrEncryptedOrdering`) or fall back to a full scan + in-memory filter/sort.
- **Leak**: value equality (same value → same ciphertext) and, via scan counts,
  the fact that ordered queries are being run. Not order of values.

### Tier 3 — full key confidentiality with an encrypted/order-revealing index

Not recommended (same conclusion as `ts`/`fs`): requires an out-of-band order
index or hand-rolled OPE/ORE, with high complexity and weaker security.

## 6. Seams to change (exact)

| File | Current | Change |
|------|---------|--------|
| `db/store.go` | `node.save` `raw := n.codec.Marshal(data)` | `raw, err := n.marshal(data)` |
| `db/finder.go`, `db/query.go`, `db/scan.go`, `db/sink.go` | `n.codec.Unmarshal(raw, …)` | `n.unmarshal(raw, …)` |
| `db/kv.go` | `Set` marshals / `Get` unmarshals | `n.marshal` / `n.unmarshal` |
| `db/db.go` | `toBytes(key, codec)` | `n.keyBytes(key)` (SIV when `EncryptKeys`) |
| `db/metadata.go` | `meta.increment` stores `numbertob` counters | seal/unseal counters when `EncryptMetadata` |
| `db/db.go` | `checkVersion` Set/Get of `version` | flows through KV `marshal`/`unmarshal` (already covered) |
| `db/aead.go` (new) | — | `encState`, `Config`, providers, marker/canary, `seal`/`open`/`sivKey` helpers, `resolveConfig` |
| `db/rotate.go` (new) | — | `MultiKeyProvider`, `ReEncrypt`, `RotateAll`, `VerifyAll` |
| `db/errors.go` | — | add `ErrAuthFailed`, `ErrInvalidKey`, `ErrUnsupportedCipher`, `ErrInvalidConfig`, `ErrWrongMode`, `ErrEncryptedOrdering` |

`db/index/*` is untouched — it only sees already-encoded bytes.

## 7. Storage formats

```
sealed value    = nonce || engine.Encrypt(nonce, codecBytes)     // records, KV, complex keys
sealed counter  = nonce || engine.Encrypt(nonce, numbertob(v))   // metadata counters
siv key         = base64url( SIV(domainInfo, keyBytes) )          // IDs / index values / KV keys
```

Marker + canary (unencrypted, in the `__database` bucket under the reserved key
`"\x00__db_enc__"`):

```
magic "DENC"(4) | version(1) | cipher(1) | flags(1: values<<0|metadata<<1|keys<<2) | sealed canary
canary = nonce || seal("storm/db/aead/v1")
```

Wrong key → `ErrAuthFailed` at open (canary). Plaintext open of an encrypted
store, or encrypted open of a store that already contains plaintext records, →
`ErrWrongMode` (never silent mixing). The marker is written on first encrypted
write; a plaintext store has none.

## 8. What leaks per option

| | plaintext | `EncryptValues` | `EncryptValues`+`EncryptMetadata` | `+EncryptKeys` |
|---|---|---|---|---|
| record contents | visible | **confidential** | **confidential** | **confidential** |
| un-indexed field values | visible | **confidential** | **confidential** | **confidential** |
| indexed field values | visible | visible (index keys) | visible (index keys) | **confidential** (equality leaks) |
| primary keys / KV keys | visible | visible | visible | **confidential** (equality leaks) |
| bucket/type names | visible | visible | visible | visible |
| auto-inc counters | visible | visible | **confidential** | **confidential** |

Always visible: bucket/type names, record sizes, key counts, access patterns,
and (with `EncryptKeys`) value-equality + that ordered queries degrade.

## 9. Error handling, nonce discipline, backward compatibility

- Decrypt failures → `ErrAuthFailed` on every read; never "not found", never a
  panic.
- Invalid config → `ErrInvalidConfig`; mode/key mismatch → `ErrWrongMode` /
  `ErrAuthFailed`.
- `EncryptKeys` on ordered/prefix queries → `ErrEncryptedOrdering` (or documented
  full-scan fallback).
- Fresh random nonce per sealed value/counter via `encryption.GenerateNonce`;
  re-encryption uses new nonces.
- `Open` unchanged and plaintext-only; unencrypted bytes byte-identical
  (regression test asserts raw codec output + raw keys + no `DENC` marker).
- Codec-name compatibility (`ErrDifferentCodec`) is unaffected — the codec name
  stays plain in `__metadata`.

## 10. Open decisions

1. **Scope**: Tier 1 (values + metadata) vs Tier 2 (`EncryptKeys` SIV). Recommend
   Tier 1 first (query surface fully intact), Tier 2 as opt-in with the
   ordered-query caveat, matching the `ts`/`fs` rollout.
2. **Ordered queries under `EncryptKeys`**: explicit `ErrEncryptedOrdering` error
   vs transparent full-scan + in-memory filter/sort. Recommend the explicit
   error (fail loud, no silent perf cliff).
3. **KV keys**: include KV keys under `EncryptKeys` (SIV), or leave KV keys clear
   while encrypting KV values? Recommend including them (same `toBytes` seam).
4. **Metadata counters**: worth encrypting (Tier 1 `EncryptMetadata`) or leave
   clear? They are low-sensitivity auto-increment values; recommend encrypting
   for completeness but it is optional.

## 11. Files affected (proposed)

```
db/
├── aead.go (new)       encState, Config, KeyProvider, Raw/Env/MultiKeyProvider,
│                       marker/canary, seal/open/sivKey helpers, resolveConfig
├── errors.go           add the six new sentinels
├── db.go               OpenWithConfig; Store.enc; toBytes→keyBytes; checkVersion via KV
├── store.go            node.save uses n.marshal
├── finder.go/query.go/scan.go/sink.go   n.codec.Unmarshal → n.unmarshal
├── kv.go               Set/Get use n.marshal/n.unmarshal
├── metadata.go         seal/unseal counters when EncryptMetadata
├── rotate.go (new)     MultiKeyProvider, ReEncrypt, RotateAll, VerifyAll
├── aead_test.go (new)  round-trip, wrong-key, tamper, nonce, optionality, SIV, rotation
```

## 12. Test plan sketch

- **Optionality**: nil `Config` ⇒ raw codec bytes, raw `toBytes` keys, no `DENC`
  marker; byte-identical to today.
- **Round-trip** (Tier 1): `Save`/`One`/`Find`/`All`/`Range`/`Prefix`/`Update`/
  `DeleteStruct`/`ReIndex`/`Set`/`Get`/`SetBytes`/`GetBytes`/`Count`/`Drop` all
  match a plaintext control store.
- **Auth**: wrong key → `ErrAuthFailed` (canary); tampered value/counter →
  `ErrAuthFailed`; truncated ciphertext → error, no panic.
- **Nonce**: two `Save` of equal data produce different ciphertext.
- **Tier 2**: SIV determinism (same value → same key, distinct values differ,
  domain separation across id/index/kv); exact lookups work; `Range`/`Prefix`/
  `AllByIndex` return `ErrEncryptedOrdering` (or documented fallback).
- **Rotation**: `ReEncrypt`/`RotateAll`/`VerifyAll`; old key decrypts until
  rotation, then fails; new key works after.
- **Mode mismatch**: plaintext open of encrypted store / encrypted open of
  plaintext store → `ErrWrongMode`.
- `go vet ./db/...`, `go test -race ./db/...`, `go test ./...`.

## 13. Verification checklist

```sh
cd /home/malivvan/projects/storm
gofmt -l db internal/encryption        # expect empty
go vet ./db/... ./internal/encryption/...
go build ./db/...
go test -count=1 ./db/...
go test -race -count=1 ./db/...
go test -count=1 ./...                 # no regressions elsewhere
```
