# object

`object` is tempest's independent flat object store. A bucket is a namespace and
a key is a slash-separated name inside it; the pair maps to exactly one record
in the object store's own root bucket (`object/`). It does not use the file
store, so it works without a file system and never touches file-store buckets.

```go
objects := store.Objects()
_, err := objects.Put(ctx, "tenant-a", "reports/q3.pdf", reader, 0, "application/pdf")
body, err := objects.ReadAll(ctx, "tenant-a", "reports/q3.pdf")
infos, err := objects.List(ctx, "tenant-a", "reports/")
```

## Semantics

- `Put` consumes the whole reader before writing. A reader that fails midway
  leaves the previous content unchanged and publishes no partial object.
- A key containing a `..` segment is rejected (`ErrInvalidKey`), so a key can
  never address a sibling bucket. Bucket names cannot contain `/`, be empty, or
  use the reserved `__` prefix.
- `Get` returns an `io.ReadCloser`; `ReadAll` is the convenience form.
- `Delete` of a missing key is a no-op. `Move` of a missing source is a no-op.
- `Stat` reports size and a content SHA-256 ETag; a missing object is not an
  error. `List` is recursive, prefix-scoped and sorted by key.
- `Grant`/`Minter` are unchanged from the blob API: a grant records which
  object was authorized, for which method, and until when.

## Streaming, range reads and metadata

`PutStream` consumes a reader in `DefaultChunkSize` (1 MiB) chunks, so memory
is bounded by one chunk rather than the object size. Chunks are stored by
SHA-256 under `__chunks__` with reference counts in `__chunk_refs__`; replacing
or deleting an object releases its chunks. `GetRange` reads only the chunks
that overlap the requested byte range, and `Get` joins the chunks.

`ObjectMeta` carries a content type, user metadata and a creation time.
`SetMetadata` updates those without rewriting content, `Stat`/`List` return
them, and `ListWithOptions` filters by exact metadata pairs.

A chunked object upgrades the store to format 2, which an older build refuses
rather than reading as an empty blob. The first `PutStream` in a store writes
that version; whole-object `Put` remains format 1.

```go
info, err := store.PutStream(ctx, "media", "clip.mp4", r, size, object.ObjectMeta{
	ContentType: "video/mp4",
	Metadata:    map[string]string{"owner": "alice"},
})
rc, err := store.GetRange(ctx, "media", "clip.mp4", offset, length)
```

## Lifecycle and integration

`object.Open` borrows a caller-owned `*bolt.DB` and never closes it. The root
`tempest.Store` opens the conventional store under `object/` and exposes it as
`Objects()`; extra segments select a rooted view (`store.Objects("tenant")`).
A transaction-bound view is available as `tx.Objects()`, so object writes join
the same bbolt transaction as the other stores.

- Values are sealed when `EncryptValues` is enabled. The root store enables it
  with `EncryptObjectValues` (or the db value flag on stores created before that
  flag existed).
- `Config.Trace` receives one event per committed object write under the
  `object` store identifier.
- `VerifyBackup` validates the object store's version and mode;
  `BackupFile` records the format version in its manifest.
- The store writes its own format version, encryption-mode marker and canary.
  Unknown versions and wrong modes fail closed.
- `Buckets` lists the object bucket names (sorted) and `DropBucket` removes one
  bucket and all its keys; a missing bucket is a no-op and the removal is
  traced.
- `RotateAll` re-seals every object record and the canary under a new key, and
  `VerifyAll` re-reads them, reporting `crypto.ErrAuthFailed` on a value that
  does not authenticate. `ReEncryptBucket` rotates one bucket and keeps the
  previous key as a fallback until `RotateAll`.
- Logical export (`tempest.Export`) writes one `object.put` record per object,
  across every bucket.

## Compression and deduplication

`Config.Compress` Zstd-compresses the whole object record (content plus its
size/ETag metadata) before sealing; `CompressLevel` and `MinCompress` tune it.
Reads auto-detect the envelope, so the setting may change between opens, and
rotation preserves the data.

`Config.Dedup` stores content once by SHA-256 and reference-counts it: blobs live
under `__blobs__`, counts under `__blobs_refs__`, and a record keeps only
`{SHA256, Size}`. `Put`, `Delete` and `Move` update the record and the count in
one transaction. The mode byte records dedup, so a store must be reopened with
the same setting or `Open` fails with `ErrWrongMode`.

## Migrating file-backed blobs

The previous `blob` package stored objects as files in a named `fstree` file
system. Those bytes are untouched and remain readable through `fstree`. To copy
a bucket into the independent store explicitly:

```go
fsys, err := store.Fstree().Open("objects")
if err != nil {
    return err
}
err = store.Objects().MigrateFromFile(ctx, fsys, "tenant-a")
```

The copy is idempotent. Nothing is deleted from the fstree store, and no old data
is silently interpreted as the new layout. The snapshot adapter
(`backup/blobstore`) accepts the independent `*object.Store`; a snapshot
taken over a file-backed bucket can be migrated the same way before it is used
with the new store.
