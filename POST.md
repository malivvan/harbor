# Post-implementation notes — optional `fs` encryption

## Status

The plan in `PLAN.md` is implemented and verified.

| Commit | Scope |
|--------|-------|
| `b071f05` | Tier 1: `OpenWithConfig`, key providers, per-filesystem marker/canary, sealed inodes/content/symlinks, mode detection, snapshot decryption, empty-plaintext system-inode migration |
| `2da5bc3` | Chunked content (`ChunkSize`) with a bbolt chunk bucket and File/ReadFile/snapshot routing |
| `2ad7036` | Key rotation: `MultiKeyProvider`, `ReEncryptFS`, `RotateAll`, `VerifyAll` |
| final docs commit | `fs/aeadfs` removed; `fs/README.md`, `fs/doc.go`, cross-package docs updated; this file |

Verification performed:

- `go build ./...` — OK
- `go vet ./...` — clean
- `go test ./...` — all packages pass
- `go test -race -count=1 ./fs/` — pass
- `gofmt -l` on the touched files — empty
- `go mod tidy -diff` — empty (no new dependencies)

Plaintext behaviour is unchanged: no marker is written, inodes remain raw gob,
content and symlink paths remain raw bytes, and all new encryption paths are
skipped when `Config` is nil or has no encryption feature enabled.

## Remaining work and limitations

1. **No plaintext → encrypted migration for populated filesystems.** Requesting
   encryption on a filesystem that already contains user data returns
   `ErrWrongMode`. Only an otherwise empty plaintext filesystem (the two system
   inodes) is migrated automatically.

2. **External content filesystems stay plaintext.** Content configured through
   `SetContentFS` is written to that filesystem as-is; `EncryptContent`,
   `ChunkSize`, rotation and `VerifyAll` cover only the bbolt data/chunk
   buckets.

3. **`Readlink` cannot report authentication failures.** The `absfs` signature
   returns `(string, error)`, but the underlying `fsBucket.Readlink` returns a
   plain string; a tampered symlink therefore reads as `""`. `VerifyAll`
   decrypts symlinks explicitly and does report the failure.

4. **Rotation is not concurrency-safe.** Rotation must run while the filesystem
   is quiescent. Existing `FileSystem` handles are updated to the new key, but
   snapshots and files opened before rotation should be recreated.
   `ReEncryptFS` adopts the new key for the whole `Store`, so a store with
   multiple filesystems must use `RotateAll` (or re-encrypt every filesystem
   before retiring the old key).

5. **One key per Store.** Every filesystem managed by a `Store` shares the same
   key and encryption flags; there are no per-filesystem or per-file keys. The
   `FENC` marker is per filesystem and records the mode/cipher/chunk size (not
   data), while the key is per Store.

6. **Strict mode matching.** Changing the cipher, enabling/disabling metadata or
   content encryption, or changing `ChunkSize` between opens is rejected with
   `ErrWrongMode`; there is no implicit upgrade/downgrade. Plaintext `Open` of
   an encrypted filesystem is likewise rejected.

7. **Chunk sizing.** Any positive `ChunkSize` is accepted (no minimum/maximum
   validation). Every chunk except read-time trimming is padded to `ChunkSize`,
   and each read/write decrypts or encrypts whole affected chunks; very large
   chunks increase amplification.

8. **No SIV name encryption.** File and directory names are hidden because they
   live inside the sealed inode blobs; numeric inode keys and bucket names
   remain clear. This is intentional per the plan and avoids deterministic
   ciphers.

9. **Rotation input.** Rotation requires a raw 32-byte key (no KDF and no
   key-escrow); `MultiKeyProvider` provides decryption fallbacks for reading
   data written under older keys.

10. **Marker visibility.** The unencrypted `FENC` marker reveals that a
    filesystem is encrypted and its mode/cipher/chunk size. This is required for
    fail-fast mode detection and matches the `ts` design.
