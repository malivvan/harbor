package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const direntsBucketName = "dirents"

// dirKey builds the key for one directory entry: ino (8 bytes) followed by the
// entry name. Keys for one inode therefore sort by name.
func dirKey(ino uint64, name string) []byte {
	key := make([]byte, 8+len(name))
	binary.BigEndian.PutUint64(key[:8], ino)
	copy(key[8:], name)
	return key
}

func dirPrefix(ino uint64) []byte {
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], ino)
	return prefix[:]
}

// hasDirents reports whether any entry key exists for ino.
func (f *fsBucket) hasDirents(ino uint64) bool {
	if f.dirents == nil {
		return false
	}
	prefix := dirPrefix(ino)
	key, _ := f.dirents.Cursor().Seek(prefix)
	return key != nil && bytes.HasPrefix(key, prefix)
}

// syncDirents persists the changes recorded on node into the dirents bucket.
// A node with no entry keys yet (a legacy inode) is migrated in full.
func (f *fsBucket) syncDirents(ino uint64, node *iNode) error {
	if f.dirents == nil {
		if f.parent == nil {
			return nil
		}
		b, err := f.parent.CreateBucketIfNotExists([]byte(direntsBucketName))
		if err != nil {
			return err
		}
		f.dirents = b
	}
	if f.dirents == nil || !f.dirents.Writable() {
		return nil
	}

	if node.dirSyncAll || !f.hasDirents(ino) {
		prefix := dirPrefix(ino)
		var stale [][]byte
		c := f.dirents.Cursor()
		for key, _ := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = c.Next() {
			stale = append(stale, append([]byte(nil), key...))
		}
		for _, key := range stale {
			if err := f.dirents.Delete(key); err != nil {
				return err
			}
		}
		for _, entry := range node.Children {
			if err := f.dirents.Put(dirKey(ino, entry.Name), i2b(entry.Ino)); err != nil {
				return err
			}
		}
	} else {
		for name := range node.dirRemoved {
			if err := f.dirents.Delete(dirKey(ino, name)); err != nil {
				return err
			}
		}
		for name, child := range node.dirChanged {
			if err := f.dirents.Put(dirKey(ino, name), i2b(child)); err != nil {
				return err
			}
		}
	}

	node.dirSyncAll = false
	node.dirChanged = nil
	node.dirRemoved = nil
	return nil
}

// decodeDirents replaces node.Children with the entry keys stored for ino.
// Inodes written before the dirents bucket existed keep their blob children.
func (f *fsBucket) decodeDirents(ino uint64, node *iNode) error {
	if f.dirents == nil {
		return nil
	}

	prefix := dirPrefix(ino)
	var children entries
	c := f.dirents.Cursor()
	for key, value := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = c.Next() {
		if value == nil {
			continue
		}
		// A truncated value would make b2i panic on a corrupted database.
		if len(value) != 8 {
			return fmt.Errorf("fs: corrupt directory entry %q for inode %d: value is %d bytes", key[8:], ino, len(value))
		}
		children = append(children, &entry{
			Name: string(key[8:]),
			Ino:  b2i(value),
		})
	}
	if len(children) > 0 {
		node.Children = children
	}
	node.dirSyncAll = false
	node.dirChanged = nil
	node.dirRemoved = nil
	return nil
}

// deleteDirents removes every entry key belonging to ino.
func (f *fsBucket) deleteDirents(ino uint64) error {
	if f.dirents == nil {
		return nil
	}
	prefix := dirPrefix(ino)
	var keys [][]byte
	c := f.dirents.Cursor()
	for key, _ := c.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = c.Next() {
		keys = append(keys, append([]byte(nil), key...))
	}
	for _, key := range keys {
		if err := f.dirents.Delete(key); err != nil {
			return err
		}
	}
	return nil
}
