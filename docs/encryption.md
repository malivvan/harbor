# Encryption

Harbor encrypts data at rest with AES-256-GCM. Encryption is enabled per store
and per component; a store can mix encrypted and plaintext parts. This document
describes how keys are derived, what each flag protects, how rotation works,
and what the design does not protect against.

## Key derivation

A Harbor password never reaches the database. When the root `harbor` package
creates a store it performs this pipeline:

```text
password
   │  Argon2id(salt, time, memory, threads)
   ▼
32-byte seed
   │  SLIP-0021 master node
   ▼
master key
   ├── harbor/canary/v1  → canary key
   ├── harbor/db/v1      → db store key
   ├── harbor/fs/v1      → fs store key
   ├── harbor/ts/v1      → ts store key
   └── harbor/vec/v1     → vec store key
```

- The salt is 16 random bytes (`harbor.SaltSize`) generated for each new store.
- Argon2id produces a 32-byte seed (`harbor.KeySize`). The default parameters
  follow RFC 9106's second recommendation: `t=3`, `m=64 MiB`, `p=4`.
- SLIP-0021 derives independent child keys. Compromise of one sub-store key
  does not expose the others.
- Temporary byte slices are wiped after use. The password is a Go string and
  cannot be reliably wiped from memory; keep it out of long-lived state.

The salt and cost parameters are stored in an unencrypted metadata record, since
they are needed before any key exists. The metadata record is small and has a
fixed layout: salt, KDF parameters, encryption flags, and a sealed canary.

### KDF parameter sets

| Set | Time | Memory | Threads | Use |
|-----|------|--------|---------|-----|
| `FastKDF*` | 1 | 64 MiB | 1 | Tests and short-lived development data only |
| `DefaultKDF*` | 3 | 64 MiB | 4 | New stores |
| `StrongKDF*` | 4 | 256 MiB | 8 | High-value secrets on capable hardware |

Partial KDF settings are rejected. The reader validates the persisted
parameters before deriving a key, and caps them at `t=100`, `m=1 GiB`,
`p=64`, with `m >= 8*p`, so a tampered metadata record cannot force an
unbounded Argon2id allocation.

## The canary

Every store contains a canary: `"harbor/canary/v1/0x<flags>"` sealed with the
canary key. Opening the store decrypts it and compares the result. This proves:

- the password derives the expected key, and
- the persisted encryption flags are the ones the canary was sealed with.

Because the flags are part of the authenticated plaintext, flipping a flag in
the metadata record causes an authentication failure instead of silently
opening the store in a different mode. The canary exists for plaintext stores
too, which is why a password is required even when encryption is disabled.

## Ciphertext format

- Each sealed value is `nonce || ciphertext || tag`, where the nonce is 12
  random bytes and the tag is 16 bytes. The nonce is generated per write.
- Empty plaintext is still sealed. An empty payload at rest is rejected rather
  than treated as an empty plaintext, so truncating a value to zero bytes
  cannot bypass authentication.
- Deterministic AES-SIV is used where a value must be looked up or listed by a
  hidden name: encrypted index keys, primary keys, series names, collection
  names, point IDs, and key/value keys.
- Every store writes an unencrypted marker that records its encryption mode.
  The root store records the flags in the metadata record; the sub-stores write
  their own versioned markers (`FENC` for `fs`).

## Flags

The root package exposes aggregate flags:

| Flag | Bits |
|------|------|
| `EncryptDB` | keys, values, metadata |
| `EncryptFS` | content, metadata |
| `EncryptTS` | values, series names |
| `EncryptVec` | point IDs, collection names, points |
| `EncryptAll` | all of the above (default for a new store) |

The individual bits, and what they protect:

| Flag | Protects | Leaves readable |
|------|----------|-----------------|
| `EncryptDBKeys` | primary keys, index values, kv keys | bucket names, codec name, value sizes |
| `EncryptDBValues` | record and kv values | keys when `EncryptDBKeys` is off |
| `EncryptDBMetadata` | auto-increment counters | — |
| `EncryptFSContent` | file content, symlink targets | inode structure, content lengths unless chunked |
| `EncryptFSMetadata` | inode blobs (names, modes, sizes, times) | inode numbers, bucket structure |
| `EncryptTSValues` | point values, series metadata | timestamps when block mode is off |
| `EncryptTSSeriesNames` | series and metadata names | point counts |
| `EncryptVecPointIDs` | point IDs; also the stored point value | collection names, point count |
| `EncryptVecCollectionNames` | collection names | point count and sizes |
| `EncryptVecPoints` | point blobs and collection metadata | point IDs when `EncryptVecPointIDs` is off |

## Opening modes

- Opening an encrypted store with a plaintext config or a different mode fails
  with `ErrWrongMode`.
- Opening an encrypted store with the wrong key fails with `ErrAuthFailed`.
- Asking for encryption on a store that already holds plaintext data fails with
  `ErrWrongMode`.
- `ErrInvalidKey` reports a missing or malformed key; a key must be exactly 32
  bytes.
- `ErrInvalidConfig` reports an internally inconsistent configuration, such as
  requesting a feature that the selected mode cannot provide.
- The root package reports an empty or wrong password as `ErrInvalidPassword`
  and malformed metadata as `ErrCorruptStore`.

Migration from plaintext to encrypted storage is not supported. Export the data
and import it into a new encrypted store.

## Store-specific behavior

### db

`EncryptDBKeys` makes exact lookups work over ciphertext but destroys meaningful
ordering. Ordered and prefix lookups return `db.ErrEncryptedOrdering` instead of
returning wrong results. Leave `EncryptDBKeys` off if the application relies on
`Range`, `Prefix`, or index ordering.

### fs

`Config.ChunkSize` enables chunked content encryption and implies
`EncryptContent`. A positive chunk size splits content into independently
sealed chunks of that many plaintext bytes, so a write or truncation only
rewrites the affected chunks. Without chunking, the whole file is one sealed
blob and every write re-seals all of it.

### ts

`Config.BlockDuration` groups points into sealed blocks. It enables encryption
on its own (so `Config.Secret` is required) and is strongly recommended for
high-volume encrypted series: a block is sealed and opened once instead of once
per point. A duration of a few seconds to a minute is a reasonable default for
streaming metrics.

`EncryptSeriesNames` encrypts the physical bucket names with AES-SIV. Listing
still returns the original names, but deterministic names cannot be migrated
one series at a time (see rotation below).

### vec

`EncryptPointIDs` encrypts point IDs and the stored point value (which contains
the ID). `EncryptCollectionNames` encrypts collection bucket and metadata names.
Quantized vectors are sealed like any other point data.

## Rotation

Key rotation rewrites existing data under a new raw 32-byte key. It does not
change the password: the root `harbor` package derives its keys from the
password and does not expose a rotation API. Rotate keys only when you open a
sub-store directly with your own key, or export and re-import to change the
password.

### Full rotation

Use the whole-store methods:

| Store | Method |
|-------|--------|
| `db` | `ReEncrypt` or `RotateAll` (aliases) |
| `fs` | `RotateAll` |
| `ts` | `RotateAll` |
| `vec` | `RotateAll` |

A full rotation is atomic within its store: it rewrites data in one transaction
(or a single sealed pass for block-based stores) and adopts the new key when it
completes. Run it with **exclusive access** to the store; do not write from
other goroutines while it runs. After rotation, `VerifyAll` re-reads every
value and confirms it decrypts with the configured keys.

### Partial rotation

The per-entity methods rewrite one entity at a time:

| Store | Method |
|-------|--------|
| `fs` | `ReEncryptFS(ctx, name, newKey)` |
| `ts` | `ReEncryptSeries(ctx, name, newKey)` |
| `vec` | `ReEncryptCollection(ctx, name, newKey)` |

These are partial migrations. The marker stays under the current key and the
previous key remains available for decryption in the running process, so the
entities that have not been rewritten keep working. To finish the migration:

1. Rewrite every entity.
2. Call `RotateAll` on the store.
3. Only then close the store or open it from a fresh process.

Closing a store between a partial rotation and `RotateAll` is unsupported.

Partial rotation cannot run when deterministic names are encrypted:

- `ts` with `EncryptSeriesNames`
- `vec` with `EncryptPointIDs` or `EncryptCollectionNames`

In those modes the physical names are derived from one master key; once the new
key is installed, the remaining entities can neither be found nor decrypted.
The per-entity methods return `ErrInvalidConfig` in these modes instead of
leaving the database unrecoverable. Use `RotateAll`.

## Threat model

Harbor's encryption protects **data at rest** against someone who obtains the
database file without the password or key.

It does **not** protect against:

- a compromised running process, memory disclosure, or debugger access to keys,
- an attacker who can call the application's API with valid credentials,
- traffic analysis of metadata that is intentionally left readable,
- rollback or deletion of whole database pages (values are authenticated, but
  bbolt's page structure is not a globally authenticated log),
- weak passwords. Use a high-entropy password and consider `StrongKDF*` for
  high-value data.

Metadata that remains visible includes the salt, KDF parameters, encryption
flags, the sealed canary, bucket structure, record counts, and the sizes and
timestamps of encrypted values. Use the per-component flags above to reduce
what is exposed. Database file permissions (for example `0600`) are still your
responsibility.

## Operational checklist

- Create the database with restrictive permissions and pass a lock timeout in
  `bolt.Options`.
- Keep the password out of logs, crash reports, and command-line arguments.
- Back up the database file and the password/key separately.
- Test restores and run `VerifyAll` after moving data between environments.
- Do not copy a database out of a running rotation or partial migration.
