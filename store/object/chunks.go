package object

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/malivvan/tempest/store/ledger"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) chunkBuckets(tx *bolt.Tx, create bool) (chunks, refs *bolt.Bucket, err error) {
	root := s.rootBucket(tx)
	if root == nil {
		if !create {
			return nil, nil, nil
		}
		root, err = s.createRootBucket(tx)
		if err != nil {
			return nil, nil, err
		}
	}
	if create {
		chunks, err = root.CreateBucketIfNotExists([]byte(chunksBucketName))
		if err != nil {
			return nil, nil, err
		}
		refs, err = root.CreateBucketIfNotExists([]byte(chunkRefsBucketName))
		return chunks, refs, err
	}
	return root.Bucket([]byte(chunksBucketName)), root.Bucket([]byte(chunkRefsBucketName)), nil
}

func chunkRef(refs *bolt.Bucket, hash []byte) uint64 {
	raw := refs.Get(hash)
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func putChunkRef(refs *bolt.Bucket, hash []byte, count uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], count)
	return refs.Put(hash, raw[:])
}

func (s *Store) acquireChunk(tx *bolt.Tx, hashHex string, data []byte) error {
	hash, err := hex.DecodeString(hashHex)
	if err != nil || len(hash) != sha256.Size {
		return fmt.Errorf("object: invalid chunk hash %q", hashHex)
	}
	chunks, refs, err := s.chunkBuckets(tx, true)
	if err != nil {
		return err
	}
	if chunks.Get(hash) == nil {
		sealed, err := s.seal(data)
		if err != nil {
			return err
		}
		if err := chunks.Put(hash, sealed); err != nil {
			return err
		}
	}
	return putChunkRef(refs, hash, chunkRef(refs, hash)+1)
}

func (s *Store) releaseChunk(tx *bolt.Tx, hashHex string) error {
	hash, err := hex.DecodeString(hashHex)
	if err != nil || len(hash) != sha256.Size {
		return nil
	}
	chunks, refs, err := s.chunkBuckets(tx, false)
	if err != nil || chunks == nil || refs == nil {
		return err
	}
	count := chunkRef(refs, hash)
	if count <= 1 {
		if err := chunks.Delete(hash); err != nil {
			return err
		}
		return refs.Delete(hash)
	}
	return putChunkRef(refs, hash, count-1)
}

func (s *Store) readChunk(tx *bolt.Tx, hashHex string) ([]byte, error) {
	hash, err := hex.DecodeString(hashHex)
	if err != nil || len(hash) != sha256.Size {
		return nil, fmt.Errorf("object: invalid chunk hash %q", hashHex)
	}
	chunks, _, err := s.chunkBuckets(tx, false)
	if err != nil {
		return nil, err
	}
	if chunks == nil {
		return nil, os.ErrNotExist
	}
	raw := chunks.Get(hash)
	if raw == nil {
		return nil, os.ErrNotExist
	}
	return s.openValue(raw)
}

// releaseRecord drops the content references held by rec.
func (s *Store) releaseRecord(tx *bolt.Tx, rec *record) error {
	if rec == nil {
		return nil
	}
	if rec.ChunkSize > 0 {
		for _, hash := range rec.Chunks {
			if err := s.releaseChunk(tx, hash); err != nil {
				return err
			}
		}
		return nil
	}
	if s.dedupEnabled() && len(rec.Data) == 0 && rec.SHA256 != "" {
		return s.releaseBlob(tx, rec.SHA256)
	}
	return nil
}

// PutStream writes a reader in bounded chunks. The reader is consumed once and
// no chunk larger than DefaultChunkSize is held in memory. A failing reader
// leaves the previous object unchanged; chunks already written are released on
// the error path, and the object record is committed last.
func (s *Store) PutStream(ctx context.Context, bucket, key string, r io.Reader, size int64, meta ObjectMeta) (Info, error) {
	if err := ctxErr(ctx); err != nil {
		return Info{}, err
	}
	if s == nil || s.db == nil {
		return Info{}, ErrNoStore
	}
	okey, err := objectKey(key)
	if err != nil {
		return Info{}, err
	}
	if err := validateBucket(bucket); err != nil {
		return Info{}, err
	}
	if r == nil {
		return Info{}, fmt.Errorf("object: put stream %q: reader is required", key)
	}
	if size < 0 {
		return Info{}, fmt.Errorf("object: put stream %q: size must not be negative", key)
	}

	hasher := sha256.New()
	buf := make([]byte, DefaultChunkSize)
	var (
		chunks   []string
		acquired []string
		total    int64
	)
	releaseAcquired := func() {
		if len(acquired) == 0 {
			return
		}
		_ = s.update(context.Background(), func(tx *bolt.Tx) error {
			for _, hash := range acquired {
				if err := s.releaseChunk(tx, hash); err != nil {
					return err
				}
			}
			return nil
		})
	}
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			piece := buf[:n]
			sum := sha256.Sum256(piece)
			hashHex := hex.EncodeToString(sum[:])
			if err := s.update(ctx, func(tx *bolt.Tx) error {
				return s.acquireChunk(tx, hashHex, piece)
			}); err != nil {
				releaseAcquired()
				return Info{}, fmt.Errorf("object: put stream %q: %w", key, err)
			}
			acquired = append(acquired, hashHex)
			chunks = append(chunks, hashHex)
			_, _ = hasher.Write(piece)
			total += int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				break
			}
			releaseAcquired()
			return Info{}, fmt.Errorf("object: put stream %q: %w", key, readErr)
		}
		if n == 0 {
			break
		}
	}
	if size > 0 && total != size {
		releaseAcquired()
		return Info{}, fmt.Errorf("object: put stream %q: read %d bytes, expected %d", key, total, size)
	}

	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	rec := record{
		SHA256:      hex.EncodeToString(hasher.Sum(nil)),
		Size:        total,
		ContentType: meta.ContentType,
		Metadata:    meta.Metadata,
		CreatedAt:   meta.CreatedAt,
		ChunkSize:   DefaultChunkSize,
		Chunks:      chunks,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		releaseAcquired()
		return Info{}, err
	}
	sealed, err := s.seal(raw)
	if err != nil {
		releaseAcquired()
		return Info{}, err
	}

	var entries []ledger.Entry
	err = s.update(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, true)
		if err != nil {
			return err
		}
		if old := b.Get([]byte(okey)); old != nil {
			var previous record
			if err := s.recordFromRaw(old, &previous); err != nil {
				return err
			}
			if err := s.releaseRecord(tx, &previous); err != nil {
				return err
			}
		}
		if err := b.Put([]byte(okey), sealed); err != nil {
			return err
		}
		if err := s.setVersion(tx, FormatVersionChunked); err != nil {
			return err
		}
		if s.trace != nil {
			entries = append(entries, ledger.Entry{
				Store:  ledger.StoreObject,
				Op:     "put_stream",
				Entity: bucket + ":" + okey,
				Digest: append([]byte(nil), rec.SHA256...),
			})
		}
		return nil
	})
	if err != nil {
		releaseAcquired()
		return Info{}, fmt.Errorf("object: put stream %q: %w", key, err)
	}
	return s.infoOf(okey, rec), s.replay(entries)
}

// GetRange reads n bytes at off. n == 0 returns an empty reader. A range past
// the object is clipped, not an error.
func (s *Store) GetRange(ctx context.Context, bucket, key string, off, n int64) (io.ReadCloser, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.db == nil {
		return nil, ErrNoStore
	}
	if off < 0 || n < 0 {
		return nil, fmt.Errorf("object: get range %q: offset and length must not be negative", key)
	}
	okey, err := objectKey(key)
	if err != nil {
		return nil, err
	}
	var out []byte
	err = s.view(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, false)
		if err != nil {
			return err
		}
		if b == nil {
			return os.ErrNotExist
		}
		var rec record
		if err := s.getRecord(b, okey, &rec); err != nil {
			return err
		}
		out, err = s.readRange(tx, &rec, off, n)
		return err
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("object: get range %q: %w", key, os.ErrNotExist)
		}
		return nil, fmt.Errorf("object: get range %q: %w", key, err)
	}
	return io.NopCloser(bytes.NewReader(out)), nil
}

func (s *Store) readRange(tx *bolt.Tx, rec *record, off, n int64) ([]byte, error) {
	if rec.ChunkSize > 0 {
		var out []byte
		var pos int64
		for _, hash := range rec.Chunks {
			chunk, err := s.readChunk(tx, hash)
			if err != nil {
				return nil, err
			}
			start, end := pos, pos+int64(len(chunk))
			pos = end
			if end <= off {
				continue
			}
			from := int64(0)
			if off > start {
				from = off - start
			}
			to := int64(len(chunk))
			if n > 0 && off+n < end {
				to = off + n - start
			}
			if from < to {
				out = append(out, chunk[from:to]...)
			}
			if n > 0 && int64(len(out)) >= n {
				break
			}
		}
		if n > 0 && int64(len(out)) > n {
			out = out[:n]
		}
		return out, nil
	}
	data, err := s.recordData(tx, rec)
	if err != nil {
		return nil, err
	}
	if off >= int64(len(data)) {
		return nil, nil
	}
	data = data[off:]
	if n > 0 && int64(len(data)) > n {
		data = data[:n]
	}
	return append([]byte(nil), data...), nil
}

func (s *Store) recordData(tx *bolt.Tx, rec *record) ([]byte, error) {
	if rec.ChunkSize > 0 {
		var out bytes.Buffer
		for _, hash := range rec.Chunks {
			chunk, err := s.readChunk(tx, hash)
			if err != nil {
				return nil, err
			}
			out.Write(chunk)
		}
		return out.Bytes(), nil
	}
	if s.dedupEnabled() {
		return s.readBlob(tx, rec.SHA256)
	}
	return rec.Data, nil
}

// SetMetadata updates an object's user metadata without rewriting its content.
func (s *Store) SetMetadata(ctx context.Context, bucket, key string, meta ObjectMeta) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if s == nil || s.db == nil {
		return ErrNoStore
	}
	okey, err := objectKey(key)
	if err != nil {
		return err
	}
	err = s.update(ctx, func(tx *bolt.Tx) error {
		b, err := s.bucket(tx, bucket, false)
		if err != nil {
			return err
		}
		if b == nil {
			return os.ErrNotExist
		}
		var rec record
		if err := s.getRecord(b, okey, &rec); err != nil {
			return err
		}
		rec.ContentType = meta.ContentType
		rec.Metadata = meta.Metadata
		if !meta.CreatedAt.IsZero() {
			rec.CreatedAt = meta.CreatedAt
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		sealed, err := s.seal(raw)
		if err != nil {
			return err
		}
		return b.Put([]byte(okey), sealed)
	})
	if err != nil {
		return fmt.Errorf("object: set metadata %q: %w", key, err)
	}
	return nil
}

// setVersion writes the on-disk object format revision.
func (s *Store) setVersion(tx *bolt.Tx, version uint64) error {
	root := s.rootBucket(tx)
	if root == nil {
		return fmt.Errorf("%w: missing root bucket", ErrWrongMode)
	}
	var buf [8]byte
	for i := range buf {
		buf[i] = byte(version >> (8 * i))
	}
	return root.Put([]byte(versionKey), buf[:])
}
