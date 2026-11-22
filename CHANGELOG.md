# Changelog

All notable changes to this project are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## Unreleased

### Fixed

- Record decoding now resets the destination, so `ReIndex`, `OrderBy` and
  filtered scans no longer leak field values between records.
- List-index keys are unambiguous and ordered by value, fixing exact-match
  collisions, range bounds and index ordering for strings containing `__` or
  prefixing another value.
- Partial `Update` merges inline embedded fields individually instead of
  zeroing omitted siblings.
- Snapshots materialize their state and release the bbolt read transaction,
  removing the `CopyToFS` and concurrent-write deadlock.
- S3 multipart failures no longer store a truncated object; every `x-amz-*`
  header is signed.
- External-content writes commit content before the inode, and size/content
  mismatches are reported instead of silently shrinking the file.
- Time-series counts stay correct across `DeleteRange` and per-series
  retention, and `RotateAll` includes concurrently created series.
- Flat vector search clamps `topK`, avoiding panics and huge allocations.
- A failed vec flush can no longer resurrect concurrently deleted points.
- `EncryptPointIDs` also encrypts the stored point value.
- Unknown on-disk format versions are rejected.
- `Limit(0)` returns an empty success instead of `ErrNotFound`.
- Corrupt metadata and S3 responses return errors instead of panicking or
  being ignored.

### Added

- Restored `LICENSE` with the upstream MIT notices.
- `SECURITY.md` and `CONTRIBUTING.md`.
- Regression tests for the fixes above.
- Fuzzing seeds for the on-disk metadata parser.
