// Package file implements a complete file system on top of bbolt, including
// symbolic links, a concurrency-friendly walker, snapshots and optional
// encryption.
//
// A [Store] wraps an already-open bbolt database and a root bucket, and manages
// any number of named file systems beneath it. Each file system is an [FS]
// value that implements the absfs interfaces used throughout the ecosystem
// (File, FileSystem and SymlinkFileSystem). Plaintext stores keep the existing
// on-disk format.
//
// # Snapshots
//
// A snapshot created by [FS.CreateSnapshot] is a read-only, point-in-time view
// of a file system. Its metadata and content are materialized into memory when
// the snapshot is created, so it does not pin the bbolt read transaction or
// the database mmap; the memory cost is proportional to the size of the file
// system. Release a snapshot with [Snapshot.Release] when done.
//
// # Storage layout and performance
//
// Large file content should be stored with [Config.ChunkSize]: non-chunked
// content is one sealed blob per file, so every write rewrites and re-seals the
// whole file. With ChunkSize set, a write or truncation touches only the
// affected chunks.
//
// Directory inodes are encoded as a single value containing their children, so
// creating, removing or renaming an entry rewrites that directory's inode.
// Directories with a very large number of direct children are therefore
// O(children) per mutation.
//
// # Optional encryption
//
// A Store can be opened with an opt-in AEAD configuration through [Open].
// [Config.EncryptMetadata] seals inode blobs (hiding names, modes, sizes and
// times), [Config.EncryptContent] seals file content and symlink paths, and
// [Config.ChunkSize] stores content as independently sealed chunks. Key
// rotation is available through [Store.ReEncryptFS], [Store.RotateAll] and
// [Store.VerifyAll].
package fstree
