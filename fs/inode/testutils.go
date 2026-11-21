package inode

import (
	"errors"
)

// Walk traverses a directory tree and calls fn for each file/directory encountered.
// The "." and ".." entries are skipped, and a cycle guard prevents unbounded
// recursion when a directory entry points at one of its ancestors.
func Walk(node *Inode, path string, fn func(path string, n *Inode) error) error {
	return walk(node, path, fn, make(map[*Inode]bool))
}

func walk(node *Inode, path string, fn func(path string, n *Inode) error, ancestors map[*Inode]bool) error {
	if err := fn(path, node); err != nil {
		return err
	}

	if !node.IsDir() {
		if node.Dir.Len() != 0 {
			return errors.New("is directory")
		}
		return nil
	}

	basePath := path
	if basePath == "/" {
		basePath = ""
	}

	// Track this directory as an ancestor so we do not descend into a cycle.
	ancestors[node] = true
	defer delete(ancestors, node)

	for _, entry := range node.ReadDir() {
		name := entry.Name()
		if name == "." || name == ".." {
			continue
		}
		if ancestors[entry.Inode] {
			// Would revisit an ancestor: stop to avoid infinite recursion.
			continue
		}
		if err := walk(entry.Inode, basePath+"/"+name, fn, ancestors); err != nil {
			return err
		}
	}
	return nil
}
