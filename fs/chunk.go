package fs

import (
	"bytes"
	"encoding/binary"
	"os"

	bolt "go.etcd.io/bbolt"
)

const chunksBucketName = "chunks"

// chunkKey identifies a chunk by inode and chunk index.
func chunkKey(ino uint64, idx int64) []byte {
	var key [16]byte
	binary.BigEndian.PutUint64(key[:8], ino)
	binary.BigEndian.PutUint64(key[8:], uint64(idx))
	return key[:]
}

// chunked reports whether content is stored as encrypted chunks.
func (fsys *FS) chunked() bool {
	if fsys == nil {
		return false
	}
	e := fsys.enc.Load()
	return e != nil && e.chunkSize > 0
}

func (fsys *FS) chunkSize() int64 {
	e := fsys.enc.Load()
	if e == nil {
		return 0
	}
	return int64(e.chunkSize)
}

// openFsBucketForWrite opens the filesystem buckets and ensures the chunks
// bucket exists.
func (fsys *FS) openFsBucketForWrite(tx *bolt.Tx) (*fsBucket, error) {
	bb, err := openBucket(tx, fsys.bucket)
	if err != nil {
		return nil, err
	}
	if _, err := bb.CreateBucketIfNotExists([]byte(chunksBucketName)); err != nil {
		return nil, err
	}
	b := newFsBucketWithCache(bb, fsys.cache)
	if b == nil {
		return nil, os.ErrNotExist
	}
	b.enc = fsys.enc.Load()
	return b, nil
}

func (b *fsBucket) readChunk(ino uint64, idx int64) ([]byte, error) {
	if b == nil || b.chunks == nil {
		return nil, nil
	}
	raw := b.chunks.Get(chunkKey(ino, idx))
	if raw == nil {
		return nil, nil
	}
	return b.enc.openContent(raw)
}

func (b *fsBucket) writeChunk(ino uint64, idx int64, data []byte) error {
	if b == nil || b.chunks == nil {
		return os.ErrInvalid
	}
	key := chunkKey(ino, idx)
	if len(data) == 0 {
		return b.chunks.Delete(key)
	}
	raw, err := b.enc.sealContent(data)
	if err != nil {
		return err
	}
	return b.chunks.Put(key, raw)
}

// readDataRange reads n bytes at off, zero-filling missing chunks.
func (b *fsBucket) readDataRange(ino uint64, off, n, chunkSize int64) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if off < 0 {
		return nil, os.ErrInvalid
	}

	out := make([]byte, n)
	first := off / chunkSize
	last := (off + n - 1) / chunkSize
	for idx := first; idx <= last; idx++ {
		chunk, err := b.readChunk(ino, idx)
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			continue
		}

		start := idx * chunkSize
		from := off
		if from < start {
			from = start
		}
		to := off + n
		if to > start+chunkSize {
			to = start + chunkSize
		}
		if end := start + int64(len(chunk)); to > end {
			to = end
		}
		if to <= from {
			continue
		}
		copy(out[from-off:to-off], chunk[from-start:to-start])
	}
	return out, nil
}

// writeDataRange writes data at off, touching only the affected chunks.
func (b *fsBucket) writeDataRange(ino uint64, off int64, data []byte, chunkSize int64) error {
	if len(data) == 0 {
		return nil
	}
	if off < 0 {
		return os.ErrInvalid
	}

	first := off / chunkSize
	last := (off + int64(len(data)) - 1) / chunkSize
	for idx := first; idx <= last; idx++ {
		chunk, err := b.readChunk(ino, idx)
		if err != nil {
			return err
		}
		if int64(len(chunk)) < chunkSize {
			grown := make([]byte, chunkSize)
			copy(grown, chunk)
			chunk = grown
		}

		start := idx * chunkSize
		from := off
		if from < start {
			from = start
		}
		to := off + int64(len(data))
		if to > start+chunkSize {
			to = start + chunkSize
		}
		if to > from {
			copy(chunk[from-start:to-start], data[from-off:to-off])
		}
		if err := b.writeChunk(ino, idx, chunk); err != nil {
			return err
		}
	}
	return nil
}

// truncateData drops chunks beyond size and trims the last partial chunk.
func (b *fsBucket) truncateData(ino uint64, size, chunkSize int64) error {
	if b == nil || b.chunks == nil {
		return nil
	}

	firstDelete := (size + chunkSize - 1) / chunkSize
	prefix := chunkKey(ino, 0)[:8]
	var keys [][]byte
	cursor := b.chunks.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		idx := int64(binary.BigEndian.Uint64(key[8:]))
		if idx >= firstDelete {
			keys = append(keys, append([]byte(nil), key...))
		}
	}
	for _, key := range keys {
		if err := b.chunks.Delete(key); err != nil {
			return err
		}
	}

	rem := size % chunkSize
	if rem == 0 {
		return nil
	}
	idx := size / chunkSize
	chunk, err := b.readChunk(ino, idx)
	if err != nil {
		return err
	}
	if int64(len(chunk)) > rem {
		chunk = chunk[:rem]
	}
	return b.writeChunk(ino, idx, chunk)
}

// deleteAllChunks removes every chunk belonging to an inode.
func (b *fsBucket) deleteAllChunks(ino uint64) error {
	if b == nil || b.chunks == nil {
		return nil
	}
	prefix := chunkKey(ino, 0)[:8]
	var keys [][]byte
	cursor := b.chunks.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		keys = append(keys, append([]byte(nil), key...))
	}
	for _, key := range keys {
		if err := b.chunks.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// readAllChunks concatenates every chunk of an inode.
func (b *fsBucket) readAllChunks(ino uint64) ([]byte, error) {
	if b == nil || b.chunks == nil {
		return nil, nil
	}
	prefix := chunkKey(ino, 0)[:8]
	var out []byte
	cursor := b.chunks.Cursor()
	for key, raw := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, raw = cursor.Next() {
		plain, err := b.enc.openContent(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, plain...)
	}
	return out, nil
}

// writeAllChunks replaces every chunk of an inode with data.
func (b *fsBucket) writeAllChunks(ino uint64, data []byte, chunkSize int64) error {
	if b == nil || b.chunks == nil {
		return os.ErrInvalid
	}

	prefix := chunkKey(ino, 0)[:8]
	var keys [][]byte
	cursor := b.chunks.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		keys = append(keys, append([]byte(nil), key...))
	}
	for _, key := range keys {
		if err := b.chunks.Delete(key); err != nil {
			return err
		}
	}

	for idx := int64(0); len(data) > 0; idx++ {
		end := chunkSize
		if int64(len(data)) < end {
			end = int64(len(data))
		}
		if err := b.writeChunk(ino, idx, data[:end]); err != nil {
			return err
		}
		data = data[end:]
	}
	return nil
}

func (fsys *FS) readRange(ino uint64, off, n int64) ([]byte, error) {
	var out []byte
	err := fsys.db.View(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucket(tx)
		if err != nil {
			return err
		}
		out, err = b.readDataRange(ino, off, n, fsys.chunkSize())
		return err
	})
	return out, err
}

// writeRangeAndInode writes a range of chunks and updates the inode in one
// transaction, so a chunked Write commits (and fsyncs) once.
func (fsys *FS) writeRangeAndInode(ino uint64, off int64, data []byte, node *iNode) error {
	return fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		if err := b.writeDataRange(ino, off, data, fsys.chunkSize()); err != nil {
			return err
		}
		return b.PutInode(ino, node)
	})
}

func (fsys *FS) writeRange(ino uint64, off int64, data []byte) error {
	return fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.writeDataRange(ino, off, data, fsys.chunkSize())
	})
}

func (fsys *FS) truncateRange(ino uint64, size int64) error {
	return fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.truncateData(ino, size, fsys.chunkSize())
	})
}

// truncateRangeAndInode trims chunks and persists the inode in one transaction.
func (fsys *FS) truncateRangeAndInode(ino uint64, size int64, node *iNode) error {
	return fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		if err := b.truncateData(ino, size, fsys.chunkSize()); err != nil {
			return err
		}
		return b.PutInode(ino, node)
	})
}

func (fsys *FS) loadChunkedData(ino uint64) ([]byte, error) {
	var out []byte
	err := fsys.db.View(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucket(tx)
		if err != nil {
			return err
		}
		node, err := b.GetInode(ino)
		if err != nil {
			return err
		}
		out, err = b.readDataRange(ino, 0, node.Size, fsys.chunkSize())
		return err
	})
	return out, err
}

func (fsys *FS) saveChunkedData(ino uint64, data []byte) error {
	return fsys.db.Update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.writeAllChunks(ino, data, fsys.chunkSize())
	})
}
