# Post-implementation notes: optional AEAD encryption for the `db` store

`PLAN.md` is now implemented. This file records what is done, what was decided
along the way, and what is still worth doing.

## What is in place

- **Optional by construction.** `Open(ctx, db, root, c)` keeps its signature and
  semantics and always produces a plaintext store; encryption only happens
  through `OpenWithConfig(ctx, db, root, c, cfg)` with a `Config` that enables
  at least one feature. `enc == nil` makes every helper an exact passthrough to
  `codec.Marshal`/`codec.Unmarshal`/`toBytes`, and a regression test asserts raw
  codec bytes, raw keys and the absence of a marker.
- **Tier 1 — `EncryptValues` / `EncryptMetadata`.** Record and KV values are
  sealed with AES-256-GCM (or XChaCha20-Poly1305) using a fresh nonce per write;
  auto-increment counters are sealed too. Queries, indexes and orderings behave
  exactly as before.
- **Tier 2 — `EncryptKeys`.** Primary keys, index values and KV keys are
  AES-SIV encrypted with HKDF-derived, domain-separated keys (`id`, `index`,
  `kv`). Exact lookups keep working; ordered/prefix lookups on the index paths
  return `ErrEncryptedOrdering`.
- **Mode detection.** A `DENC` marker plus a sealed canary lives unencrypted in
  `__database`: plaintext open of an encrypted store and settings mismatches give
  `ErrWrongMode`, a wrong key gives `ErrAuthFailed`, and a store with plaintext
  data refuses to become encrypted.
- **Rotation.** `Store.ReEncrypt` / `Store.RotateAll` rewrite the whole database
  in one bbolt write transaction (keys re-derived per domain, values re-sealed,
  index entries rebuilt, counters re-sealed, marker canary rewritten) and adopt
  the new key after the commit. `Store.VerifyAll` re-reads everything.

## Decisions worth knowing

1. **`SetBytes` seals and `GetBytes` opens.** The plan's seam table treated
   `SetBytes` as a raw write. Storing values unauthenticated left two classes of
   value in one bucket ("sealed" and "raw") that a rotation cannot tell apart,
   which makes `VerifyAll` unable to detect a tampered value. Sealing at the
   `SetBytes` layer keeps the public round trip identical (a `SetBytes`/
   `GetBytes` pair still returns the bytes that were written) while every stored
   value stays authenticated. `Set` therefore marshals and delegates, and only
   one seal happens. An empty value still stays empty, so `Set(ctx, b, k, nil)`
   keeps reading back as `ErrNotFound`.
2. **Multiple keys cannot be combined with `EncryptKeys`.** Deterministic key
   encryption is derived from a single master key, so a fallback key can decrypt
   values but cannot reproduce the physical keys; lookups would silently miss.
   The combination is rejected with `ErrInvalidConfig` and the message points at
   `ReEncrypt`. `MultiKeyProvider` remains useful for value/metadata-only stores
   (read old data with the fallback, write with the primary, then rotate).
3. **Ordered-query guard is on index paths.** `Range`, `Prefix`, `AllByIndex`
   and `Find(..., Reverse())` return `ErrEncryptedOrdering` when keys are
   encrypted. Scan-based lookups (`All`, `Each`, `Count`, `Select`, `Find` on a
   non-indexed field) keep working, because they decode records instead of
   trusting key order; failing them would break correct behavior rather than
   prevent wrong results.
4. **One transaction per rotation.** Rotation is atomic and simple, at the cost
   of holding one write transaction for the whole database.

## Follow-ups

1. **Plaintext → encrypted migration.** Not implemented: opening a store that
   holds plaintext data with encryption enabled returns `ErrWrongMode`. A
   migration would walk the store once, seal every value/counter (and SIV the
   keys under `EncryptKeys`), then write the marker last. Until then, export and
   re-import.
2. **Large-store rotation.** Rotating inside a single write transaction can be
   expensive for big databases. A chunked/per-bucket rotation (like
   `fs.ReEncryptFS`) plus a progress callback would bound latency and memory.
3. **Online rotation.** A dual-write window (old and new key entries in the
   indexes) would let `ReEncrypt` run without stopping writers; today rotation
   must not run concurrently with other store operations, and `s.enc` is swapped
   without synchronization.
4. **Ordered queries under `EncryptKeys`.** Options: decode-and-sort scans,
   or bucketed range indexes like `ts` time blocks. Either way `Range`/`Prefix`
   could then stop returning `ErrEncryptedOrdering`.
5. **What still leaks.** Bucket names (type names), the `__metadata` codec name,
   the marker header (cipher + flags) and the *number* of records per bucket stay
   readable. Record sizes leak too. Document this in the package docs if
   confidentiality of shapes matters to a user.
6. **`Raw`/`RawEach`** return stored (sealed) bytes, and `db.Raw` output no
   longer decodes the way it does on a plaintext store. Worth an explicit note in
   the `Query` interface documentation.
7. **Marker shared with the version record.** `checkMode` deletes the plaintext
   version record so `checkVersion` rewrites it under the new settings; this is
   what allows an empty plaintext store (which already has a version record) to
   adopt encryption. If a future version records more metadata in `__database`,
   add it to the reserved list in `hasUserData` and `storeMarker`.
8. **Docs.** `README.md` now points at `OpenWithConfig` instead of the removed
   `db/codec/aead` codec, and `internal/encryption`'s package comment no longer
   mentions it. A short "Encryption" section next to the `ts`/`fs` ones (with a
   runnable example) would still help.
9. **Test ideas not yet covered.** A fuzz test for `keyBytes`/`open` on random
   ciphertext, a test for rotation of a store with nested `From(...)` buckets
   containing both records and KV entries, and a benchmark comparing encrypted
   and plaintext `Save`/`One` to quantify overhead.
