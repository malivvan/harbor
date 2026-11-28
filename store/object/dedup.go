package object

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

// dedupEnabled reports whether content is stored content-addressed.
func (s *Store) dedupEnabled() bool {
	return s != nil && s.rootStore().dedup
}

// decodeHash turns a hex SHA-256 into its 32 raw bytes.
func decodeHash(hashHex string) ([32]byte, error) {
	var out [32]byte
	raw, err := hex.DecodeString(hashHex)
	if err != nil || len(raw) != 32 {
		return out, fmt.Errorf("%w: bad content hash %q", ErrInvalidKey, hashHex)
	}
	copy(out[:], raw)
	return out, nil
}

// blobBuckets returns the content and reference buckets, creating them when
// asked.
func (s *Store) blobBuckets(tx *bolt.Tx, create bool) (*bolt.Bucket, *bolt.Bucket, error) {
	root := s.rootBucket(tx)
	if root == nil {
		if !create {
			return nil, nil, nil
		}
		var err error
		root, err = s.createRootBucket(tx)
		if err != nil {
			return nil, nil, err
		}
	}
	if !create {
		return root.Bucket([]byte(blobsBucketName)), root.Bucket([]byte(blobRefsBucketName)), nil
	}
	blobs, err := root.CreateBucketIfNotExists([]byte(blobsBucketName))
	if err != nil {
		return nil, nil, err
	}
	refs, err := root.CreateBucketIfNotExists([]byte(blobRefsBucketName))
	if err != nil {
		return nil, nil, err
	}
	return blobs, refs, nil
}

func blobRef(refs *bolt.Bucket, hash [32]byte) uint64 {
	raw := refs.Get(hash[:])
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func putBlobRef(refs *bolt.Bucket, hash [32]byte, count uint64) error {
	if count == 0 {
		return refs.Delete(hash[:])
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)
	return refs.Put(hash[:], buf[:])
}

// acquireBlob stores data under its content hash when it is new and increments
// the reference count.
func (s *Store) acquireBlob(tx *bolt.Tx, hashHex string, data []byte) error {
	hash, err := decodeHash(hashHex)
	if err != nil {
		return err
	}
	blobs, refs, err := s.blobBuckets(tx, true)
	if err != nil {
		return err
	}
	if blobs.Get(hash[:]) == nil {
		raw, err := s.seal(data)
		if err != nil {
			return err
		}
		if err := blobs.Put(hash[:], raw); err != nil {
			return err
		}
	}
	return putBlobRef(refs, hash, blobRef(refs, hash)+1)
}

// releaseBlob decrements a content reference and deletes the blob when the
// count reaches zero.
func (s *Store) releaseBlob(tx *bolt.Tx, hashHex string) error {
	hash, err := decodeHash(hashHex)
	if err != nil {
		return err
	}
	blobs, refs, err := s.blobBuckets(tx, false)
	if err != nil {
		return err
	}
	if blobs == nil || refs == nil {
		return nil
	}
	count := blobRef(refs, hash)
	if count <= 1 {
		if err := blobs.Delete(hash[:]); err != nil {
			return err
		}
		return refs.Delete(hash[:])
	}
	return putBlobRef(refs, hash, count-1)
}

// readBlob returns the content of a blob bucket entry.
func (s *Store) readBlob(tx *bolt.Tx, hashHex string) ([]byte, error) {
	hash, err := decodeHash(hashHex)
	if err != nil {
		return nil, err
	}
	blobs, _, err := s.blobBuckets(tx, false)
	if err != nil {
		return nil, err
	}
	if blobs == nil {
		return nil, os.ErrNotExist
	}
	raw := blobs.Get(hash[:])
	if raw == nil {
		return nil, os.ErrNotExist
	}
	return s.openValue(raw)
}

// infoOf builds the public metadata of one record.
func (s *Store) infoOf(key string, rec record) Info {
	size := int64(len(rec.Data))
	if s.dedupEnabled() || rec.ChunkSize > 0 {
		size = rec.Size
	}
	return Info{
		Key:         key,
		Size:        size,
		ETag:        rec.SHA256,
		ContentType: rec.ContentType,
		Metadata:    rec.Metadata,
		CreatedAt:   rec.CreatedAt,
	}
}
