# object

`object` is eidos-db's independent flat object store. A bucket is a namespace and
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

## Lifecycle and integration

`object.Open` borrows a caller-owned `*bolt.DB` and never closes it. The root
`eidos.Store` opens the conventional store under `object/` and exposes it as
`Objects()`; extra segments select a rooted view (`store.Objects("tenant")`).
A transaction-bound view is available as `tx.Objects()`, so object writes join
the same bbolt transaction as the other stores.

- Values are sealed when `EncryptValues` is enabled. The root store enables it
  with `EncryptObjectValues` (or the db value flag on stores created before that
  flag existed).
- `Config.Audit` receives one event per committed object write under the
  `object` store identifier.
- `VerifyBackup` validates the object store's version and mode;
  `BackupFile` records the format version in its manifest.
- The store writes its own format version, encryption-mode marker and canary.
  Unknown versions and wrong modes fail closed.

## Migrating file-backed blobs

The previous `blob` package stored objects as files in a named `fstree` file
system. Those bytes are untouched and remain readable through `fstree`. To copy
a bucket into the independent store explicitly:

```go
fsys, err := store.File().Open("objects")
if err != nil {
    return err
}
err = store.Objects().MigrateFromFile(ctx, fsys, "tenant-a")
```

The copy is idempotent. Nothing is deleted from the file store, and no old data
is silently interpreted as the new layout. The snapshot adapter
(`pkg/snapshot/blobstore`) accepts the independent `*object.Store`; a snapshot
taken over a file-backed bucket can be migrated the same way before it is used
with the new store.
