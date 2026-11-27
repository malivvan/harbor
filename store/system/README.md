# system

`system` owns eidos-db's store header: the unencrypted metadata record, the
Argon2id settings, the encryption and audit flag vocabulary, the canary, and the
derivation of every per-store key from the password.

The root `eidos` package uses it when opening a store; the sub-store packages
only receive the derived keys through their `Config` values. Keeping this in one
place means the KDF, canary, key labels and flag bits cannot drift between
stores.

## Metadata

The header lives in a single record:

```text
system/Metadata
  salt[16] | time[4] | flags[2] | memory[4] | threads[1] | canaryLen[4] | canary
```

All integers are big-endian. The record is the only thing read before a key
exists; the salt and cost parameters are public, and the canary is ciphertext.

```go
md, creating, err := system.ReadMetadata(bdb)
md, keys, err := system.LoadOrCreate(bdb, system.Config{Password: "..."})
def := keys.Wipe
```

`LoadOrCreate` creates the header for a new store and derives the keys, or
derives from the stored parameters and verifies the canary for an existing one.
`Keys.Wipe` zeroes the byte key material after the sub-stores have copied it.

## Key derivation

```text
password
   │ Argon2id(salt, time, memory, threads)
   ▼
32-byte seed
   │ SLIP-0021 master
   ├── eidos/canary/v1
   ├── eidos/db/v1, fs/v1, ts/v1, vec/v1, fts/v1
   └── eidos/graphs/v1, object/v1
```

The audit signing keys hang off the same seed at SLIP-10 hardened paths
`m/0'`…`m/5'`. Labels and paths are exported constants; existing values must not
change because they determine persisted key material.

## Flags

`Encrypt*` bits select what a new store protects; `Audit*` bits select which
stores append to the audit trail at runtime. New flags are always appended, so
existing bit assignments stay stable. `EncryptAll` includes graph and object
values; `AuditAll` includes their writes.

## Canary

The canary plaintext is `eidos/canary/v1/0x<flags>`. Opening verifies it, which
proves the password derives the expected key and that the persisted encryption
flags are the ones the canary was sealed with. Flipping a flag bit makes the
canary fail to authenticate instead of silently opening the store in a different
mode.

## Errors

`ErrCorruptStore`, `ErrInvalidPassword` and `ErrInvalidOptions` are re-exported
by the root package. KDF parameters are capped before derivation (`MaxKDFTime`,
`MaxKDFMemory`, `MaxKDFThreads`, with `memory >= 8*threads`) so a tampered header
cannot force an unbounded Argon2id allocation.
