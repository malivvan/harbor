// Package absfs provides file and file system interfaces for abstract file
// system primitives.
//
// A [Filer] is the minimal implementation; [ExtendFiler] adds the [FileSystem]
// convenience methods, and [SymlinkFileSystem] combines file system and
// symbolic-link support. All paths are Unix-style and slash-separated
// regardless of the host OS. Filer implementations only need to accept
// absolute paths; use path.Join rather than filepath.Join for virtual paths.
//
// See AGENTS.md at the repository root for interface contracts, path rules,
// security guidance and implementation notes.
package absfs
