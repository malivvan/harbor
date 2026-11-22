# Post-execution report and remaining work

This document records what was executed from [PLAN.md](PLAN.md), the
verification evidence, and anything still to do before tagging a release.

## What was executed

Each item below is backed by a descriptive commit; `git log --oneline` is the
authoritative list.

- **P0-1** MIT `LICENSE` restored with the upstream notices; sub-package
  license links and credits fixed.
- **P0-2/P0-3** decoding destinations are zeroed in `node.unmarshal`; `ReIndex`,
  `OrderBy` and filtered scans no longer leak values.
- **P0-4** list-index keys use an escaped, NUL-terminated value component with
  a trailing length; exact matches, range bounds, ordering and AEAD rotation
  are consistent. Regression tests cover `__`, prefix siblings and ranges.
- **P0-5** `Update` merges inline embedded structs (including pointer inlines)
  field by field; `Find`/`One` resolve promoted field names.
- **P0-6** snapshots materialize metadata and content at creation and release
  the read transaction; `CopyToFS` works and concurrent writes cannot deadlock.
- **P0-7** S3 multipart failures poison the file instead of storing the tail;
  the mock gained `UploadPartErr` injection.
- **P0-8/P2-1** external content is committed before the size-bearing inode,
  size/content mismatches are reported, and truncate-to-0 persists the inode
  before deleting content.
- **P0-9/P1-9** `DeleteRange` seeds counts from data and avoids false
  `Count: 0` records; per-series retention persists the pruned count.
- **P0-10** `ts.RotateAll` enumerates series inside the rewrite transaction.
- **P0-11** Go floor raised to 1.27.4; staticcheck and govulncheck pinned;
  every staticcheck finding in production and test code fixed.
- **P0-12** `FlatIndex.Search` clamps `topK`.
- **P1** S3 `x-amz-*` headers are all signed (the fake verifier enforces it);
  chunked writes commit the offset only on success; corrupt dirent/root/chunk
  metadata returns errors; db/fs/ts/vec reject unknown format versions;
  `ReadDir` distinguishes missing paths and files; `storm.Close` always releases
  the bbolt handle; a failed vec flush cannot resurrect deleted points;
  `EncryptPointIDs` also encrypts the stored value.
- **P2/P3** `Limit(0)` is an empty success; the pre-auth KDF ceiling is 1 GiB;
  reserved payload keys are rejected; S3 XML unmarshal errors are returned;
  `SECURITY.md`, `CONTRIBUTING.md` and `CHANGELOG.md` were added.

## Verification evidence

Run from the repository root after the final commit:

```sh
gofmt -l .                 # empty
go build ./...
go vet ./...
go test -count=1 -timeout 300s ./...
staticcheck@v0.8.2 ./...   # go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
govulncheck@v1.10.0 ./...  # go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...
```

`go test -race ./...` is part of CI and should be run before tagging.

## Remaining work before a public release

These are deliberately left as follow-ups; none is a P0 data-loss item.

1. **Quantizer-setting persistence (P1-6).** Persist `UseQuant` and the
   quantizer identity alongside the store metadata so a reopen with default
   options cannot make quantized collections unusable or silently misread. Add
   a reopen test for the default and a custom quantizer.
2. **Sub-store rotation through the root (P1-7).** Sub-store `RotateAll`
   currently rotates a key that `storm.Open` re-derives from the password.
   Either add root-level password re-derivation/re-sealing or make root-owned
   sub-stores reject rotation with a clear error. Until then, document the
   limitation in the rotation section of the README.
3. **Copy orphan cleanup (P2-2).** `FS.Copy` still commits new inodes/content
   before the final link; a late failure can leave unreferenced data. Delete
   the copied subtree on late failure.
4. **Snapshot documentation (P2-6).** Snapshots are in-memory and read external
   content live; the README examples still use backup names. Add a real backup
   recipe (`bdb.View` + `WriteTo`) and explain the hold-time/memory cost.
5. **README claims (P2-8).** Correct the `s3fs` “AWS SDK v2” wording (the
   package hand-rolls SigV4), the `Plaintext`/password interaction, and the
   vec buffering crash-loss window. The `UseQuant` promise is now true only
   after item 1 is completed.
6. **Fuzz coverage (P2-10).** Add targets for `decodeMetadata`, encryption
   markers, gob inodes, TS blocks/points, stored points and S3 XML/SigV4, with
   seeds taken from real records. A metadata seed set was started.
7. **P3 polish.** Audit the remaining dead code (`vec/hnsw/heap`, unused
   `subFS`/`subFiler` types are now detected by staticcheck when tests are
   excluded), document the error-wrapping contract for fs/ts/vec, decide the
   stability of the exported generated codec handles in `vec`, and pin the
   `binco` dependency policy.
8. **Integration tests.** Run multipart, rename and ranged reads against a
   real MinIO/S3 endpoint; the in-repo fake now enforces SigV4 header signing
   but is not a backend.
9. **Tagging.** Create `v0.1.0` (or `v1.0.0`) only after items 1–2 are closed
   and the full verification block above, including `-race`, is green.

## Notes and deviations

- The lint policy analyzes tests too; all findings were fixed rather than
  suppressed. Tool versions are pinned so a new staticcheck release cannot
  turn CI red unexpectedly.
- Snapshots now copy content into memory. This is a deliberate trade-off for
  correctness (no pinned mmap, no deadlock); large filesystems should use a
  real bbolt backup instead of long-lived snapshots.
- `fs.TraverseLink` was renamed to `fs.ErrTraverseLink` (pre-release API).
