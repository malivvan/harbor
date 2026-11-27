package fstree

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	chunkMapBucketName  = "chunkmap"
	chunkRefsBucketName = "chunkrefs"
)

// dedupEnabled reports whether this filesystem bucket stores content-addressed,
// reference-counted chunks.
func (b *fsBucket) dedupEnabled() bool {
	return b != nil && b.enc != nil && b.enc.dedup
}

// chunkHash is the content address of one plaintext chunk. The zero hash is
// never produced by SHA-256 and is used as the "empty chunk" placeholder.
func chunkHash(chunk []byte) [32]byte {
	return sha256.Sum256(chunk)
}

func refsKey(dst *[8]byte, ino uint64) []byte {
	binary.BigEndian.PutUint64(dst[:], ino)
	return dst[:]
}

// readChunkList returns the ordered content hashes of an inode's chunks. The
// persisted list is sealed when metadata encryption is enabled.
func (b *fsBucket) readChunkList(ino uint64) ([][32]byte, error) {
	if b == nil || b.chunkmap == nil {
		return nil, nil
	}
	raw := b.chunkmap.Get(refsKey(&[8]byte{}, ino))
	if raw == nil {
		return nil, nil
	}
	plain, err := b.enc.openMetadata(raw)
	if err != nil {
		return nil, err
	}
	if len(plain) < 8 {
		return nil, fmt.Errorf("fstree: corrupt chunk list for inode %d", ino)
	}
	count := binary.BigEndian.Uint64(plain[:8])
	if uint64(len(plain)-8) != count*32 {
		return nil, fmt.Errorf("fstree: corrupt chunk list for inode %d: %d hashes in %d bytes", ino, count, len(plain)-8)
	}
	list := make([][32]byte, count)
	for i := range list {
		copy(list[i][:], plain[8+i*32:8+(i+1)*32])
	}
	return list, nil
}

// writeChunkList persists the ordered content hashes, deleting the record for an
// empty list.
func (b *fsBucket) writeChunkList(ino uint64, hashes [][32]byte) error {
	if b == nil || b.chunkmap == nil {
		return os.ErrInvalid
	}
	key := refsKey(&[8]byte{}, ino)
	if len(hashes) == 0 {
		return b.chunkmap.Delete(key)
	}
	raw := make([]byte, 8+len(hashes)*32)
	binary.BigEndian.PutUint64(raw[:8], uint64(len(hashes)))
	for i, h := range hashes {
		copy(raw[8+i*32:], h[:])
	}
	sealed, err := b.enc.sealMetadata(raw)
	if err != nil {
		return err
	}
	return b.chunkmap.Put(key, sealed)
}

// acquireChunk stores data under its hash when it is new and increments the
// reference count.
func (b *fsBucket) acquireChunk(hash [32]byte, data []byte) error {
	if b == nil || b.chunks == nil || b.chunkrefs == nil {
		return os.ErrInvalid
	}
	if b.chunks.Get(hash[:]) == nil {
		raw, err := b.enc.sealContent(data)
		if err != nil {
			b.observeChunk("write", 0, err)
			return err
		}
		if err := b.chunks.Put(hash[:], raw); err != nil {
			return err
		}
	}
	return b.bumpChunkRef(hash, 1)
}

// releaseChunk decrements the reference count and deletes the chunk when the
// count reaches zero. A missing reference is ignored.
func (b *fsBucket) releaseChunk(hash [32]byte) error {
	if b == nil || b.chunks == nil || b.chunkrefs == nil {
		return os.ErrInvalid
	}
	if hash == ([32]byte{}) {
		return nil
	}
	return b.bumpChunkRef(hash, -1)
}

// bumpChunkRef applies delta to a chunk's reference count and removes the chunk
// and its count when the result is not positive.
func (b *fsBucket) bumpChunkRef(hash [32]byte, delta int64) error {
	key := hash[:]
	raw := b.chunkrefs.Get(key)
	var count uint64
	if len(raw) == 8 {
		count = binary.BigEndian.Uint64(raw)
	}
	if delta < 0 {
		if count == 0 {
			return nil
		}
		count--
	} else {
		count += uint64(delta)
	}
	if count == 0 {
		if err := b.chunks.Delete(key); err != nil {
			return err
		}
		return b.chunkrefs.Delete(key)
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)
	return b.chunkrefs.Put(key, buf[:])
}

// readChunkContent returns the plaintext of one content hash.
func (b *fsBucket) readChunkContent(hash [32]byte) ([]byte, error) {
	if b == nil || b.chunks == nil {
		return nil, nil
	}
	raw := b.chunks.Get(hash[:])
	if raw == nil {
		return nil, fmt.Errorf("fstree: missing deduplicated chunk %x", hash)
	}
	data, err := b.enc.openContent(raw)
	b.observeChunk("read", len(data), err)
	return data, err
}

// dedupReadChunk resolves chunk idx of ino through the chunk list.
func (b *fsBucket) dedupReadChunk(ino uint64, idx int64) ([]byte, error) {
	list, err := b.readChunkList(ino)
	if err != nil {
		return nil, err
	}
	if idx < 0 || idx >= int64(len(list)) {
		return nil, nil
	}
	hash := list[idx]
	if hash == ([32]byte{}) {
		return nil, nil
	}
	return b.readChunkContent(hash)
}

// dedupWriteChunk replaces chunk idx of ino with data, updating the content
// reference counts. Empty data clears the slot.
func (b *fsBucket) dedupWriteChunk(ino uint64, idx int64, data []byte) error {
	if idx < 0 {
		return os.ErrInvalid
	}
	list, err := b.readChunkList(ino)
	if err != nil {
		return err
	}
	for int64(len(list)) <= idx {
		list = append(list, [32]byte{})
	}
	old := list[idx]

	if len(data) == 0 {
		if old == ([32]byte{}) {
			return nil
		}
		list[idx] = [32]byte{}
		if err := b.writeChunkList(ino, list); err != nil {
			return err
		}
		return b.releaseChunk(old)
	}

	hash := chunkHash(data)
	if err := b.acquireChunk(hash, data); err != nil {
		return err
	}
	list[idx] = hash
	if err := b.writeChunkList(ino, list); err != nil {
		_ = b.releaseChunk(hash)
		return err
	}
	if old != ([32]byte{}) && old != hash {
		return b.releaseChunk(old)
	}
	return nil
}

// dedupReadDataRange reads n bytes at off through the content-addressed list.
func (b *fsBucket) dedupReadDataRange(ino uint64, off, n, chunkSize int64) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if off < 0 {
		return nil, os.ErrInvalid
	}
	out := make([]byte, n)
	list, err := b.readChunkList(ino)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return out, nil
	}

	first := off / chunkSize
	last := (off + n - 1) / chunkSize
	if last >= int64(len(list)) {
		last = int64(len(list)) - 1
	}
	for idx := first; idx <= last; idx++ {
		if list[idx] == ([32]byte{}) {
			continue
		}
		chunk, err := b.readChunkContent(list[idx])
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
	}
	return out, nil
}

// dedupWriteDataRange writes data at off, rewriting only the affected chunks.
func (b *fsBucket) dedupWriteDataRange(ino uint64, off int64, data []byte, chunkSize int64) error {
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
		if from == start && to == start+chunkSize {
			if err := b.dedupWriteChunk(ino, idx, data[from-off:to-off]); err != nil {
				return err
			}
			continue
		}
		chunk, err := b.dedupReadChunk(ino, idx)
		if err != nil {
			return err
		}
		need := to - start
		if int64(len(chunk)) < need {
			grown := make([]byte, need)
			copy(grown, chunk)
			chunk = grown
		}
		copy(chunk[from-start:to-start], data[from-off:to-off])
		if err := b.dedupWriteChunk(ino, idx, chunk); err != nil {
			return err
		}
	}
	return nil
}

// dedupTruncateData drops chunks beyond size and trims the last partial chunk.
func (b *fsBucket) dedupTruncateData(ino uint64, size, chunkSize int64) error {
	list, err := b.readChunkList(ino)
	if err != nil {
		return err
	}
	firstDelete := (size + chunkSize - 1) / chunkSize
	if firstDelete > int64(len(list)) {
		firstDelete = int64(len(list))
	}
	for idx := firstDelete; idx < int64(len(list)); idx++ {
		if err := b.releaseChunk(list[idx]); err != nil {
			return err
		}
	}
	list = list[:firstDelete]
	if err := b.writeChunkList(ino, list); err != nil {
		return err
	}
	rem := size % chunkSize
	if rem == 0 {
		return nil
	}
	idx := size / chunkSize
	chunk, err := b.dedupReadChunk(ino, idx)
	if err != nil {
		return err
	}
	if int64(len(chunk)) > rem {
		chunk = chunk[:rem]
	}
	return b.dedupWriteChunk(ino, idx, chunk)
}

// dedupDeleteAllChunks releases every chunk of an inode and drops its list.
func (b *fsBucket) dedupDeleteAllChunks(ino uint64) error {
	if b == nil || b.chunkmap == nil {
		return nil
	}
	list, err := b.readChunkList(ino)
	if err != nil {
		return err
	}
	for _, hash := range list {
		if err := b.releaseChunk(hash); err != nil {
			return err
		}
	}
	return b.writeChunkList(ino, nil)
}

// dedupReadAllChunks concatenates every non-empty chunk of an inode.
func (b *fsBucket) dedupReadAllChunks(ino uint64) ([]byte, error) {
	list, err := b.readChunkList(ino)
	if err != nil {
		return nil, err
	}
	var out []byte
	for _, hash := range list {
		if hash == ([32]byte{}) {
			continue
		}
		chunk, err := b.readChunkContent(hash)
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
	}
	return out, nil
}

// dedupWriteAllChunks replaces every chunk of an inode with data.
func (b *fsBucket) dedupWriteAllChunks(ino uint64, data []byte, chunkSize int64) error {
	if err := b.dedupDeleteAllChunks(ino); err != nil {
		return err
	}
	for idx := int64(0); len(data) > 0; idx++ {
		end := chunkSize
		if int64(len(data)) < end {
			end = int64(len(data))
		}
		if err := b.dedupWriteChunk(ino, idx, data[:end]); err != nil {
			return err
		}
		data = data[end:]
	}
	return nil
}

// ensureDedupBuckets creates the content-addressed buckets in a write
// transaction.
func ensureDedupBuckets(bb bucketer) error {
	if bb == nil {
		return os.ErrInvalid
	}
	for _, name := range []string{chunksBucketName, chunkMapBucketName, chunkRefsBucketName} {
		if _, err := bb.CreateBucketIfNotExists([]byte(name)); err != nil {
			return err
		}
	}
	return nil
}
