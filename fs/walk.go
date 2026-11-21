package fs

import (
	"os"
	slashpath "path"
	"path/filepath"
	"sort"

	"github.com/malivvan/storm/fs/absfs"
)

// Walk walks the file tree rooted at path in depth-first pre-order, calling fn
// for each file or directory in the tree, including path itself.
//
// Children of a directory are visited in lexical order. If fn returns
// [filepath.SkipDir] for a directory, its contents are skipped. Any other
// non-nil error stops the walk and is returned.
//
// Walk accepts any [absfs.Filer], including [FileSystem] and the snapshots
// created by [FileSystem.CreateSnapshot].
func Walk(filer absfs.Filer, path string, fn filepath.WalkFunc) error {
	info, err := filer.Stat(path)
	if err != nil {
		return err
	}

	stack := []*walkEntry{{parent: slashpath.Dir(path), info: info}}
	for len(stack) > 0 {
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if err := fn(e.path(), e.info, nil); err != nil {
			if err == filepath.SkipDir {
				continue
			}
			return err
		}

		if !e.info.IsDir() {
			continue
		}

		children, err := walkChildren(filer, e)
		if err != nil {
			return err
		}
		// Children are sorted in reverse so that popping the stack visits
		// them in lexical order.
		stack = append(stack, children...)
	}

	return nil
}

// walkEntry is a node scheduled for traversal by [Walk].
type walkEntry struct {
	parent string
	info   os.FileInfo
}

func (e *walkEntry) path() string {
	return slashpath.Join(e.parent, e.info.Name())
}

// walkChildren returns the entries of e's directory in reverse lexical order.
func walkChildren(filer absfs.Filer, e *walkEntry) ([]*walkEntry, error) {
	f, err := filer.OpenFile(e.path(), os.O_RDONLY, 0700)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	list, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(nameSorter(list)))

	children := make([]*walkEntry, 0, len(list))
	for _, info := range list {
		if info.Name() == "." || info.Name() == ".." {
			continue
		}
		children = append(children, &walkEntry{parent: e.path(), info: info})
	}
	return children, nil
}

// nameSorter sorts file infos by name.
type nameSorter []os.FileInfo

func (s nameSorter) Len() int           { return len(s) }
func (s nameSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nameSorter) Less(i, j int) bool { return s[i].Name() < s[j].Name() }
