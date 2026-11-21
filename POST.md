# Post-implementation notes: optional AEAD encryption for the `vec` store

`PLAN.md` is now implemented. This file records what is in place, the decisions
taken while implementing it, and what is still worth doing. (The earlier `db`
notes this file used to hold are preserved in git history, commit `62253fc`.)

## What is in place

- **Optional by construction.** `Open(ctx, bdb, root, useQuant, quantizer)` keeps
  its signature and semantics and always produces a plaintext store; encryption
  only happens through the new
  `OpenWithConfig(ctx, bdb, root, useQuant, quantizer, cfg)` with a `Config` that
  enables at least one feature. With `enc == nil` every helper
  (`sealBytes`/`openBytes`/`keyBytes`/`bucketKey`/`logicalName`) is an exact
  passthrough to today's `binco`/JSON/raw-key paths, and a regression test
  asserts bare binco blobs, raw JSON metadata, raw keys and no `VENC` marker.
- **Tier 1 — `EncryptPoints`.** Point blobs (vector, payload, quantized vector)
  and collection metadata are sealed with a fresh nonce per write. Searching and
  filtering are in-memory, so the whole query surface is unaffected.
- **Tier 2 — `EncryptPointIDs` / `EncryptCollectionNames`.** Point IDs and
  collection bucket/metadata names are AES-SIV encrypted with HKDF-derived,
  domain-separated keys (`id`, `name`). Exact deletes and reloads keep working;
  `ListCollections` and `ListCollectionMetas` decrypt names and return the
  caller's names.
- **Mode detection.** A `VENC` marker plus a sealed canary in
  `__collections_meta__`: plaintext open of an encrypted store and settings
  mismatches give `ErrWrongMode`, a wrong key gives `ErrAuthFailed`, and a store
  that already holds plaintext collections refuses to become encrypted.
- **Rotation.** `ReEncryptCollection` rotates one collection (and its metadata),
  `RotateAll` rotates everything, `VerifyAll` re-reads the marker, every point,
  every point ID and every metadata entry. Rotations run in one bbolt write
  transaction and adopt the new key after the commit.

## Decisions worth knowing

1. **Bucket renaming is create-copy-delete.** bbolt cannot rename a bucket, so a
   rotation that changes an encrypted collection name writes the rotated entries
   into a new bucket and deletes the old one, inside the same transaction.
2. **Multiple keys cannot be combined with encrypted names.** Deterministic name
   encryption is derived from a single master key, so a fallback key could
   decrypt blobs but never reproduce the physical names; lookups would silently
   miss. The combination is rejected with `ErrInvalidConfig`. A `MultiKeyProvider`
   is still useful for `EncryptPoints`-only stores: read old data with the
   fallback, then `RotateAll` to fold everything onto the primary key.
3. **`ReEncryptCollection` adopts the new key**, exactly like the `fs`
   per-filesystem rotation: the store then only holds the new key, so the other
   collections are no longer readable. The method documents this and points at
   `RotateAll`; the test shows the `MultiKeyProvider` sequence that finishes the
   job safely.
4. **Owned-database constructors stay plaintext-only**, as the plan requires:
   `NewStorage`/`NewStorageWithQuantization` have no key parameter. Encryption is
   the borrowed-DB entry point (`OpenWithConfig`).
5. **The root bucket is for collections only.** With `EncryptCollectionNames`,
   `ListCollections` decrypts every bucket name under the root, so an unrelated
   bucket placed there is reported as a decryption error instead of being
   silently listed as a collection.
6. **An emptied blob is a decode error, not an auth error.** A zero-length value
   stays zero-length (so a missing entry keeps its "not found" semantics), which
   makes it fail `binco.DecodeExact` rather than the MAC. Truncated or modified
   ciphertext does fail with `ErrAuthFailed`.

## Follow-ups

1. **Owned-DB encrypted constructors.** `NewStorageEncrypted(path, ...)` (or a
   `Path` + `Config` variant) would let applications use encryption without
   borrowing the bbolt handle.
2. **Plaintext → encrypted migration.** Not implemented: a store with existing
   collections opened with encryption returns `ErrWrongMode`. A migration would
   rewrite every blob, SIV every name and write the marker last.
3. **Chunked / online rotation.** Rotation holds one write transaction for the
   whole store. A per-collection transaction with a progress callback would bound
   latency and memory, and a dual-write window (old and new name entries) would
   allow rotating a live store.
4. **Multi-key with encrypted names.** Storing a key identifier next to each
   encrypted name (key-checked SIV) would allow trying several keys on lookup
   instead of rejecting the combination.
5. **Rotation versus the collection write buffer.** `Collection` buffers writes
   and flushes on its own schedule; a rotation must not run while a collection
   holds buffered points (flush first). This is documented behavior, but a guard
   or a `Flush`-first helper would make it hard to get wrong.
6. **What still leaks.** Bucket count, number of points per collection, blob
   sizes, the marker header (cipher + flags), and equality: the same ID or name
   always maps to the same ciphertext, and equal point blobs written twice would
   look different only because of the nonce.
7. **Docs.** `README.md` has no `vec` section today, so nothing referenced the
   removed design doc (`VEC.md` does not exist in the repository). A short
   "Encryption" section with a runnable `OpenWithConfig` example would help, and
   the design notes currently live only in `PLAN.md` and this file.
8. **Test ideas not yet covered.** Fuzzing `openName`/`openBytes` with random
   ciphertext, a rotation of a store combining root scope + encrypted names +
   quantized points, and a benchmark comparing encrypted and plaintext
   `UpsertPoints`/`LoadCollection` to quantify overhead.
9. **`vec/hnsw/graph.go` formatting.** One pre-existing gofmt issue (a comment
   indentation inside a parameter list) was fixed while running the plan's
   verification checklist; it is whitespace only.
