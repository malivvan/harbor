// Package fs provides a complete file system implementation for boltdb. This
// implementation includes support for symbolic links and includes a file system
// walker that works just like `filepath.Walk`.
//
// BoltFS now includes advanced features:
//   - Thread-safe in-memory inode cache for improved performance
//   - Snapshot support using BoltDB's MVCC for point-in-time views
//
// The inode cache significantly improves read performance by caching frequently
// accessed inodes in memory with configurable size and LRU eviction.
//
// Snapshots provide read-only, consistent views of the filesystem at a point in
// time without blocking writes, implemented using BoltDB's read-only transactions.
//
// # Optional encryption
//
// A Store can be opened with an opt-in AEAD configuration through
// [Open]. [Config.EncryptMetadata] seals inode blobs (hiding names,
// modes, sizes and times), [Config.EncryptContent] seals file content and
// symlink paths, and [Config.ChunkSize] stores content as independently sealed
// chunks. Plaintext stores keep their existing on-disk format. Key rotation is
// available through [Store.ReEncryptFS], [Store.RotateAll] and
// [Store.VerifyAll].
package fs
