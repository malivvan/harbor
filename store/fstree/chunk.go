package fstree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/malivvan/eidos-db/pkg/metrics"
	"github.com/malivvan/eidos-db/store"
	bolt "go.etcd.io/bbolt"
)

const chunksBucketName = "chunks"

// chunkKey writes the key identifying a chunk by inode and chunk index into
// dst and returns a slice of it. The caller owns dst, so the key does not have
// to escape to the heap.
func chunkKey(dst *[16]byte, ino uint64, idx int64) []byte {
	binary.BigEndian.PutUint64(dst[:8], ino)
	binary.BigEndian.PutUint64(dst[8:], uint64(idx))
	return dst[:]
}

// chunkPrefix writes the inode prefix shared by every chunk key of ino.
func chunkPrefix(dst *[8]byte, ino uint64) []byte {
	binary.BigEndian.PutUint64(dst[:], ino)
	return dst[:]
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
	if fsys.chunked() {
		if _, err := bb.CreateBucketIfNotExists([]byte(chunksBucketName)); err != nil {
			return nil, err
		}
	}
	b := newFsBucketWithCacheOpts(bb, fsys.cache, fsys.chunked())
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
	var key [16]byte
	raw := b.chunks.Get(chunkKey(&key, ino, idx))
	if raw == nil {
		return nil, nil
	}
	data, err := b.enc.openContent(raw)
	b.observeChunk("read", len(data), err)
	return data, err
}

func (b *fsBucket) writeChunk(ino uint64, idx int64, data []byte) error {
	if b == nil || b.chunks == nil {
		return os.ErrInvalid
	}
	var keyBuf [16]byte
	key := chunkKey(&keyBuf, ino, idx)
	if len(data) == 0 {
		return b.chunks.Delete(key)
	}
	raw, err := b.enc.sealContent(data)
	if err != nil {
		b.observeChunk("write", 0, err)
		return err
	}
	err = b.chunks.Put(key, raw)
	b.observeChunk("write", len(data), err)
	return err
}

// observeChunk reports one chunk read or write to the configured observer.
func (b *fsBucket) observeChunk(op string, n int, err error) {
	if b == nil || b.obs == nil {
		return
	}
	labels := metrics.Labels{
		metrics.LabelStore:  store.FSTREE,
		metrics.LabelOp:     op,
		metrics.LabelResult: resultLabel(err),
	}
	metrics.Counter(b.obs, metrics.MetricChunkOps, 1, labels)
	metrics.Histogram(b.obs, metrics.MetricChunkBytes, float64(n), labels)
}

// resultLabel maps an error to the ResultOK/ResultError label value.
func resultLabel(err error) string {
	if err != nil {
		return metrics.ResultError
	}
	return metrics.ResultOK
}

// readDataRange reads n bytes at off, zero-filling missing chunks. It walks
// the inode's chunks with a single cursor, so a sequential read does one seek
// and then advances forward instead of looking up each chunk separately.
func (b *fsBucket) readDataRange(ino uint64, off, n, chunkSize int64) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if off < 0 {
		return nil, os.ErrInvalid
	}

	out := make([]byte, n)
	if b == nil || b.chunks == nil {
		return out, nil
	}

	first := off / chunkSize
	last := (off + n - 1) / chunkSize

	var prefixBuf [8]byte
	prefix := chunkPrefix(&prefixBuf, ino)
	var keyBuf [16]byte

	cursor := b.chunks.Cursor()
	key, raw := cursor.Seek(chunkKey(&keyBuf, ino, first))
	for idx := first; idx <= last; idx++ {
		// Skip over chunks that were never written, and stop once the cursor
		// leaves this inode.
		for key != nil && (len(key) != 16 || !bytes.HasPrefix(key, prefix) ||
			int64(binary.BigEndian.Uint64(key[8:])) < idx) {
			if len(key) != 16 || !bytes.HasPrefix(key, prefix) {
				key = nil
				break
			}
			key, raw = cursor.Next()
		}
		if key == nil {
			break
		}
		ci := int64(binary.BigEndian.Uint64(key[8:]))
		if ci > last {
			break
		}
		if ci != idx {
			continue // gap: this chunk was never written, leave zeros
		}

		chunk, err := b.enc.openContent(raw)
		b.observeChunk("read", len(chunk), err)
		if err != nil {
			return nil, err
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
		if to > from {
			copy(out[from-off:to-off], chunk[from-start:to-start])
		}
		key, raw = cursor.Next()
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
	end := off + int64(len(data))
	for idx := first; idx <= last; idx++ {
		start := idx * chunkSize
		from := off
		if from < start {
			from = start
		}
		to := end
		if to > start+chunkSize {
			to = start + chunkSize
		}
		if to <= from {
			continue
		}

		// A write that covers the whole chunk replaces it outright, so there
		// is no existing content to read back and merge. This skips a decrypt
		// and a re-encrypt of the old chunk on every full-chunk overwrite.
		if from == start && to == start+chunkSize {
			if err := b.writeChunk(ino, idx, data[from-off:to-off]); err != nil {
				return err
			}
			continue
		}

		chunk, err := b.readChunk(ino, idx)
		if err != nil {
			return err
		}

		// Grow only to the end of the written range, not to a full chunk: a
		// partial tail chunk stays short, saving storage and crypto. Gaps
		// inside the range are zero-filled by make.
		need := to - start
		if int64(len(chunk)) < need {
			grown := make([]byte, need)
			copy(grown, chunk)
			chunk = grown
		}

		copy(chunk[from-start:to-start], data[from-off:to-off])
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
	var prefixBuf [8]byte
	prefix := chunkPrefix(&prefixBuf, ino)
	if err := b.deleteChunks(prefix, func(key []byte) (bool, error) {
		if len(key) != 16 {
			return false, fmt.Errorf("fstree: corrupt chunk key for inode %d: %d bytes", ino, len(key))
		}
		idx := int64(binary.BigEndian.Uint64(key[8:]))
		return idx >= firstDelete, nil
	}); err != nil {
		return err
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

// deleteChunks removes every chunk key under prefix for which match reports
// true. It deletes while iterating the cursor rather than first copying every
// matching key into memory, so deleting a very large file does not
// temporarily allocate O(chunks) key slices. Deleting the key the cursor is
// positioned on is safe: bbolt's Cursor.Delete removes it and leaves the
// cursor ready for Next.
func (b *fsBucket) deleteChunks(prefix []byte, match func(key []byte) (bool, error)) error {
	if b == nil || b.chunks == nil {
		return nil
	}
	cursor := b.chunks.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
		ok, err := match(key)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := cursor.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// deleteAllChunks removes every chunk belonging to an inode.
func (b *fsBucket) deleteAllChunks(ino uint64) error {
	var prefixBuf [8]byte
	prefix := chunkPrefix(&prefixBuf, ino)
	return b.deleteChunks(prefix, func([]byte) (bool, error) { return true, nil })
}

// readAllChunks concatenates every chunk of an inode.
func (b *fsBucket) readAllChunks(ino uint64) ([]byte, error) {
	if b == nil || b.chunks == nil {
		return nil, nil
	}
	var prefixBuf [8]byte
	prefix := chunkPrefix(&prefixBuf, ino)
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

	var prefixBuf [8]byte
	prefix := chunkPrefix(&prefixBuf, ino)
	if err := b.deleteChunks(prefix, func([]byte) (bool, error) { return true, nil }); err != nil {
		return err
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
	err := fsys.view(func(tx *bolt.Tx) error {
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
	return fsys.update(func(tx *bolt.Tx) error {
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
	return fsys.update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.writeDataRange(ino, off, data, fsys.chunkSize())
	})
}

func (fsys *FS) truncateRange(ino uint64, size int64) error {
	return fsys.update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.truncateData(ino, size, fsys.chunkSize())
	})
}

// truncateRangeAndInode trims chunks and persists the inode in one transaction.
func (fsys *FS) truncateRangeAndInode(ino uint64, size int64, node *iNode) error {
	return fsys.update(func(tx *bolt.Tx) error {
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
	err := fsys.view(func(tx *bolt.Tx) error {
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
	return fsys.update(func(tx *bolt.Tx) error {
		b, err := fsys.openFsBucketForWrite(tx)
		if err != nil {
			return err
		}
		return b.writeAllChunks(ino, data, fsys.chunkSize())
	})
}
