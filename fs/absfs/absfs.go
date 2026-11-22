// Package absfs re-exports the filesystem interfaces used by the fs package.
//
// The implementation lives in internal/absfs; this package keeps the public
// import path stable for callers that name these types in their own APIs.
package absfs

import i "github.com/malivvan/storm/internal/absfs"

type (
	File              = i.File
	FileSystem        = i.FileSystem
	Filer             = i.Filer
	Flags             = i.Flags
	InvalidFile       = i.InvalidFile
	NoSymlinks        = i.NoSymlinks
	ReadOnlyFiler     = i.ReadOnlyFiler
	Seekable          = i.Seekable
	SymLinker         = i.SymLinker
	SymlinkFileSystem = i.SymlinkFileSystem
	SymlinkOverlay    = i.SymlinkOverlay
	UnSeekable        = i.UnSeekable
	WriteOnlyFiler    = i.WriteOnlyFiler
)

const (
	Separator     = i.Separator
	ListSeparator = i.ListSeparator

	O_ACCESS = i.O_ACCESS
	O_RDONLY = i.O_RDONLY
	O_WRONLY = i.O_WRONLY
	O_RDWR   = i.O_RDWR
	O_APPEND = i.O_APPEND
	O_CREATE = i.O_CREATE
	O_EXCL   = i.O_EXCL
	O_SYNC   = i.O_SYNC
	O_TRUNC  = i.O_TRUNC

	OS_READ        = i.OS_READ
	OS_WRITE       = i.OS_WRITE
	OS_EX          = i.OS_EX
	OS_USER_SHIFT  = i.OS_USER_SHIFT
	OS_GROUP_SHIFT = i.OS_GROUP_SHIFT
	OS_OTH_SHIFT   = i.OS_OTH_SHIFT
	OS_USER_R      = i.OS_USER_R
	OS_USER_W      = i.OS_USER_W
	OS_USER_X      = i.OS_USER_X
	OS_USER_RW     = i.OS_USER_RW
	OS_USER_RWX    = i.OS_USER_RWX
	OS_GROUP_R     = i.OS_GROUP_R
	OS_GROUP_W     = i.OS_GROUP_W
	OS_GROUP_X     = i.OS_GROUP_X
	OS_GROUP_RW    = i.OS_GROUP_RW
	OS_GROUP_RWX   = i.OS_GROUP_RWX
	OS_OTH_R       = i.OS_OTH_R
	OS_OTH_W       = i.OS_OTH_W
	OS_OTH_X       = i.OS_OTH_X
	OS_OTH_RW      = i.OS_OTH_RW
	OS_OTH_RWX     = i.OS_OTH_RWX
	OS_ALL_R       = i.OS_ALL_R
	OS_ALL_W       = i.OS_ALL_W
	OS_ALL_X       = i.OS_ALL_X
	OS_ALL_RW      = i.OS_ALL_RW
	OS_ALL_RWX     = i.OS_ALL_RWX
)

var (
	ErrNotImplemented = i.ErrNotImplemented

	FilerToFS          = i.FilerToFS
	ParseFileMode      = i.ParseFileMode
	ParseFlags         = i.ParseFlags
	ExtendFiler        = i.ExtendFiler
	ExtendSeekable     = i.ExtendSeekable
	ExtendSymlinkFiler = i.ExtendSymlinkFiler
	BlockSymlinks      = i.BlockSymlinks
)
