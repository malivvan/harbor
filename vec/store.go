package vec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/malivvan/binco"
	"go.etcd.io/bbolt"
)

const collectionsMetaBucket = "__collections_meta__"

// Store handles local persistence using BoltDB (bbolt).
// It provides durable storage for vector collections and their points.
type Store struct {
	db *bbolt.DB

	// root is the optional parent bucket for collections and collection
	// metadata. An empty root means the database root.
	root []byte

	// closed reports whether Close has been called. It is an atomic so that
	// Close can race safely with the other Store methods.
	closed    atomic.Bool
	quantizer Quantizer
	useQuant  bool

	// enc is the optional encryption state. nil means a plaintext store.
	enc *encState
}

func toStoredPoint(p *PointStruct) *StoredPoint {
	stored := &StoredPoint{
		ID:      p.ID,
		Vector:  p.Vector,
		Version: p.Version,
	}

	if p.Payload != nil {
		stored.Payload = make(map[string]*Value)
		for k, v := range p.Payload {
			value := &Value{}
			switch val := v.(type) {
			case string:
				if !utf8.ValidString(val) {
					val = string(bytes.ToValidUTF8([]byte(val), nil)) // sanitize: replace invalid UTF-8 sequences
				}
				value.Kind = KindString
				value.StringValue = val
			case int:
				value.Kind = KindInt
				value.IntValue = int64(val)
			case int64:
				value.Kind = KindInt
				value.IntValue = val
			case float32:
				value.Kind = KindDouble
				value.DoubleValue = float64(val)
			case float64:
				value.Kind = KindDouble
				value.DoubleValue = val
			case bool:
				value.Kind = KindBool
				value.BoolValue = val
			case []byte:
				value.Kind = KindBytes
				value.BytesValue = val
			default:
				// For complex types (map, slice, etc.), JSON-serialize and keep
				// the document in a dedicated kind so that raw byte payloads are
				// not mistaken for JSON on the way back.
				if jsonBytes, err := json.Marshal(val); err == nil {
					value.Kind = KindJSON
					value.BytesValue = jsonBytes
				}
				// If JSON marshal fails, skip the value
			}
			stored.Payload[k] = value
		}
	}

	return stored
}

func fromStoredPoint(stored *StoredPoint) *PointStruct {
	p := &PointStruct{
		ID:      stored.ID,
		Vector:  stored.Vector,
		Version: stored.Version,
	}

	if stored.Payload != nil {
		p.Payload = make(Payload)
		for k, v := range stored.Payload {
			if v == nil {
				continue
			}
			switch v.Kind {
			case KindString:
				p.Payload[k] = v.StringValue
			case KindInt:
				p.Payload[k] = v.IntValue
			case KindDouble:
				p.Payload[k] = v.DoubleValue
			case KindBool:
				p.Payload[k] = v.BoolValue
			case KindBytes:
				// Raw bytes round-trip as raw bytes: never reinterpret them as
				// JSON, which used to change their type.
				p.Payload[k] = v.BytesValue
			case KindJSON:
				// Complex values are JSON documents. Numbers are decoded with
				// json.Number and normalized so that integers stay integers.
				p.Payload[k] = decodeJSONPayload(v.BytesValue)
			}
		}
	}

	return p
}

// decodeJSONPayload decodes a JSON payload document, preserving integer types
// instead of turning every number into a float64. Invalid documents are
// returned as raw bytes.
func decodeJSONPayload(data []byte) any {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	var obj any
	if err := dec.Decode(&obj); err != nil {
		return data
	}
	return normalizeJSONValue(obj)
}

// normalizeJSONValue converts json.Number values into int64 when they are
// integral, and float64 otherwise, recursively.
func normalizeJSONValue(v any) any {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return f
		}
		return n.String()
	case map[string]any:
		for k, item := range n {
			n[k] = normalizeJSONValue(item)
		}
		return n
	case []any:
		for i, item := range n {
			n[i] = normalizeJSONValue(item)
		}
		return n
	default:
		return v
	}
}

// NewStorage opens a bbolt database at path and returns a Store with default
// storage options.
func NewStorage(path string) (*Store, error) {
	return NewStorageWithQuantization(path, false, nil, false)
}

// NewStorageWithQuantization opens a bbolt database at path with optional
// vector quantization. When readOnly is true, the database is opened in
// read-only mode.
func NewStorageWithQuantization(path string, useQuant bool, quantizer Quantizer, readOnly bool) (*Store, error) {
	bdb, err := bbolt.Open(path, 0600, &bbolt.Options{
		Timeout:  time.Second,
		ReadOnly: readOnly,
	})
	if err != nil {
		return nil, err
	}

	store, err := Open(context.Background(), bdb, nil, Config{UseQuant: useQuant, Quantizer: quantizer})
	if err != nil {
		_ = bdb.Close()
		return nil, err
	}
	return store, nil
}

// Open initializes a new BoltDB storage engine with optional vector quantization
// and native AEAD encryption.
//
// A nil database is rejected with ErrInvalidConfig: the returned Store would
// have no way to persist anything and every method would otherwise panic.
// The root key, when non-empty, is used as the parent bucket for all collections
// and collection metadata.
// Returns an error if the context is invalid.
//
// A zero cfg creates a plaintext store with no quantization and its on-disk
// format is unchanged. cfg.UseQuant enables quantization; when cfg.Quantizer is
// nil the default SQ8 quantizer is used. cfg.Secret and the Encrypt fields
// enable optional encryption.
//
// When encryption is enabled the store writes an unencrypted marker into the
// collections metadata bucket and verifies it with a sealed canary, so:
//
//   - opening an encrypted store without a Config (or with the wrong settings)
//     fails with ErrWrongMode,
//   - opening an encrypted store with the wrong key fails with ErrAuthFailed,
//   - opening a store that already holds plaintext collections with encryption
//     requested fails with ErrWrongMode.
//
// Migrating an existing plaintext store to an encrypted one is not supported;
// re-import the data instead.
func Open(ctx context.Context, bdb *bbolt.DB, key []byte, cfg Config) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	var useQuant bool
	var quantizer Quantizer
	if cfg.UseQuant {
		useQuant = cfg.UseQuant
		quantizer = cfg.Quantizer
	}
	if useQuant && quantizer == nil {
		quantizer = NewSQ8Quantizer()
	}

	enc, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}

	if bdb == nil {
		// A nil database cannot store anything: every method would panic. Fail
		// fast instead of handing out a handle that explodes on first use.
		return nil, fmt.Errorf("%w: database handle is nil", ErrInvalidConfig)
	}

	store := &Store{
		db:        bdb,
		quantizer: quantizer,
		useQuant:  useQuant,
		root:      append([]byte(nil), key...),
		enc:       enc,
	}

	if err := store.checkMode(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

// ErrStorageNotOpen is returned when a Store method is called on a store that
// has no underlying database (for example a zero-value Store).
var ErrStorageNotOpen = errors.New("storage is not open")

// checkOpen reports whether the store can serve requests. Every exported
// method calls it before touching the database so that a Store without a
// database returns an error instead of panicking.
func (s *Store) checkOpen() error {
	if s.closed.Load() {
		return fmt.Errorf("storage is closed")
	}
	if s.db == nil {
		return ErrStorageNotOpen
	}
	return nil
}

// checkMode reconciles the encryption settings of the Store with the marker
// stored on disk. A plaintext store has no marker; an encrypted store always
// has one, which also proves the configured key can decrypt the store.
func (s *Store) checkMode(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	m, canary, found, err := s.loadMarker()
	if err != nil {
		return err
	}

	if s.enc == nil {
		if found {
			return fmt.Errorf("%w: store is encrypted", ErrWrongMode)
		}
		return nil
	}

	if found {
		if !m.matches(s.enc) {
			return fmt.Errorf("%w: encryption settings do not match the store", ErrWrongMode)
		}
		return s.enc.verifyCanary(canary)
	}

	// No marker: only a store without data may be initialized as encrypted.
	hasData, err := s.hasData()
	if err != nil {
		return err
	}
	if hasData {
		return fmt.Errorf("%w: store already contains plaintext data", ErrWrongMode)
	}

	return s.storeMarker(s.enc)
}

var ErrNilContext = fmt.Errorf("context is nil")

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

// rootBucket returns the configured root bucket, or nil when the store operates
// at the database root. The bucket must already exist.
func (s *Store) rootBucket(tx *bbolt.Tx) *bbolt.Bucket {
	if len(s.root) == 0 {
		return nil
	}
	return tx.Bucket(s.root)
}

// collectionBucket returns the bucket for a collection under the configured
// root, or nil when either the root or collection bucket does not exist.
func (s *Store) collectionBucket(tx *bbolt.Tx, name string) (*bbolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, err
	}

	if len(s.root) == 0 {
		return tx.Bucket(key), nil
	}

	root := s.rootBucket(tx)
	if root == nil {
		return nil, nil
	}
	return root.Bucket(key), nil
}

// createCollectionBucket creates the collection bucket under the configured
// root, creating the root bucket when necessary.
func (s *Store) createCollectionBucket(tx *bbolt.Tx, name string) (*bbolt.Bucket, error) {
	key, err := s.bucketKey(name)
	if err != nil {
		return nil, fmt.Errorf("failed to derive the bucket key for collection %q: %w", name, err)
	}

	if len(s.root) == 0 {
		return tx.CreateBucketIfNotExists(key)
	}

	root, err := tx.CreateBucketIfNotExists(s.root)
	if err != nil {
		return nil, err
	}
	return root.CreateBucketIfNotExists(key)
}

// metaBucket returns the collection metadata bucket under the configured root.
// When create is true, missing buckets are created.
func (s *Store) metaBucket(tx *bbolt.Tx, create bool) (*bbolt.Bucket, error) {
	var parent *bbolt.Bucket
	var err error

	if len(s.root) > 0 {
		if create {
			parent, err = tx.CreateBucketIfNotExists(s.root)
		} else {
			parent = tx.Bucket(s.root)
		}
		if err != nil {
			return nil, err
		}
		if parent == nil {
			return nil, nil
		}
		if create {
			return parent.CreateBucketIfNotExists([]byte(collectionsMetaBucket))
		}
		return parent.Bucket([]byte(collectionsMetaBucket)), nil
	}

	if create {
		return tx.CreateBucketIfNotExists([]byte(collectionsMetaBucket))
	}
	return tx.Bucket([]byte(collectionsMetaBucket)), nil
}

// Close gracefully closes the database connection.
// It is important to call this when done to ensure all data is flushed to disk.
// This method is idempotent and can be called multiple times.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil // already closed
	}

	if s.db == nil {
		return nil
	}

	if err := s.db.Close(); err != nil {
		// The database may still be usable: allow a retry.
		s.closed.Store(false)
		return err
	}
	return nil
}

// EnsureCollection creates a bucket for the collection if it doesn't already exist.
// Each collection is stored as a separate bucket in BoltDB.
func (s *Store) EnsureCollection(colName string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		_, err := s.createCollectionBucket(tx, colName)
		return err
	})
}

// UpsertPoints saves or updates a batch of points to disk.
// Points are serialized as Protocol Buffers and stored with their ID as the key.
// If quantization is enabled, vectors are compressed before storage.
// Returns an error if the collection doesn't exist or if serialization fails.
func (s *Store) UpsertPoints(colName string, points []PointStruct) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("collection bucket %s not found", colName)
		}

		for i := range points {
			p := &points[i]

			// Quantize vector if enabled
			if s.useQuant && s.quantizer != nil {
				// Create a copy to avoid modifying the original
				quantizedPoint := *p
				quantizedPoint.Vector = nil // Clear vector for storage

				// Store quantized vector in payload
				if quantizedPoint.Payload == nil {
					quantizedPoint.Payload = make(Payload)
				}
				quantizedPoint.Payload["__quantized_vector"] = s.quantizer.Quantize(p.Vector)

				p = &quantizedPoint
			}

			data := binco.Encode(StoredPointBinco, *toStoredPoint(p))
			sealed, err := s.sealBytes(data)
			if err != nil {
				return fmt.Errorf("failed to encrypt point %s: %w", p.ID, err)
			}

			key, err := s.keyBytes(p.ID)
			if err != nil {
				return fmt.Errorf("failed to derive the key for point %q: %w", p.ID, err)
			}

			if err := b.Put(key, sealed); err != nil {
				return fmt.Errorf("failed to write point %s to bbolt: %w", p.ID, err)
			}
		}
		return nil
	})
}

// LoadCollection loads all points for a collection from disk into memory.
// Returns a map of point IDs to PointStruct pointers.
// If quantization is enabled, compressed vectors are decompressed during loading.
// If the collection doesn't exist, returns an empty map without error.
func (s *Store) LoadCollection(colName string) (map[string]*PointStruct, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	points := make(map[string]*PointStruct)

	err := s.db.View(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			log.Printf("Bucket %s doesn't exist yet, starting empty", colName)
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			plain, err := s.openBytes(v)
			if err != nil {
				return fmt.Errorf("failed to decrypt point %q: %w", k, err)
			}

			stored, err := binco.DecodeExact(StoredPointBinco, plain)
			if err != nil {
				return err
			}

			p := fromStoredPoint(&stored)

			// Dequantize vector if enabled and quantized vector exists
			if s.useQuant && s.quantizer != nil {
				if quantizedData, ok := p.Payload["__quantized_vector"]; ok {
					if data, ok := quantizedData.([]byte); ok {
						p.Vector = s.quantizer.Dequantize(data)
						// Remove quantized vector from payload to save memory
						delete(p.Payload, "__quantized_vector")
					}
				}
			}

			points[p.ID] = p
			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to load points from bbolt: %w", err)
	}
	return points, nil
}

// ListCollections returns all collection names (bucket names) in the storage.
// It excludes the internal metadata bucket.
func (s *Store) ListCollections() ([]string, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	var collections []string
	addCollection := func(name []byte) error {
		if string(name) == collectionsMetaBucket {
			return nil
		}

		colName, err := s.logicalName(name)
		if err != nil {
			return fmt.Errorf("failed to decrypt collection name %q: %w", name, err)
		}

		collections = append(collections, colName)
		return nil
	}

	err := s.db.View(func(tx *bbolt.Tx) error {
		if len(s.root) > 0 {
			root := tx.Bucket(s.root)
			if root == nil {
				return nil
			}
			return root.ForEach(func(name, value []byte) error {
				if value != nil {
					return nil // keys are not collections
				}
				return addCollection(name)
			})
		}
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			return addCollection(name)
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// SaveCollectionMeta persists collection metadata to a special bucket.
func (s *Store) SaveCollectionMeta(name string, meta CollectionMeta) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, true)
		if err != nil {
			return err
		}

		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		sealed, err := s.sealBytes(data)
		if err != nil {
			return fmt.Errorf("failed to encrypt metadata for %s: %w", name, err)
		}

		key, err := s.bucketKey(name)
		if err != nil {
			return fmt.Errorf("failed to derive the metadata key for %q: %w", name, err)
		}

		return b.Put(key, sealed)
	})
}

// LoadCollectionMeta retrieves collection metadata from the special bucket.
func (s *Store) LoadCollectionMeta(name string) (*CollectionMeta, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	var meta CollectionMeta
	err := s.db.View(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if b == nil {
			return fmt.Errorf("metadata bucket not found")
		}

		key, err := s.bucketKey(name)
		if err != nil {
			return fmt.Errorf("failed to derive the metadata key for %q: %w", name, err)
		}

		data := b.Get(key)
		if data == nil {
			return fmt.Errorf("metadata for collection %s not found", name)
		}

		plain, err := s.openBytes(data)
		if err != nil {
			return fmt.Errorf("failed to decrypt metadata for %s: %w", name, err)
		}

		return json.Unmarshal(plain, &meta)
	})

	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// ListCollectionMetas returns all collection metadata stored in the database.
func (s *Store) ListCollectionMetas() ([]CollectionMeta, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	var metas []CollectionMeta
	err := s.db.View(func(tx *bbolt.Tx) error {
		b, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if b == nil {
			return nil // No metadata stored yet
		}

		return b.ForEach(func(k, v []byte) error {
			if string(k) == markerKey {
				return nil // the encryption marker is not collection metadata
			}

			plain, err := s.openBytes(v)
			if err != nil {
				return fmt.Errorf("failed to decrypt metadata %q: %w", k, err)
			}

			var meta CollectionMeta
			if err := json.Unmarshal(plain, &meta); err != nil {
				return err
			}
			metas = append(metas, meta)
			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list collection metas: %w", err)
	}
	return metas, nil
}

// DeletePoints deletes a batch of points from disk by their IDs.
// If a point ID doesn't exist, it is silently skipped.
// Returns an error if the collection doesn't exist.
func (s *Store) DeletePoints(colName string, ids []string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := s.collectionBucket(tx, colName)
		if err != nil {
			return err
		}
		if b == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}

		for _, id := range ids {
			key, err := s.keyBytes(id)
			if err != nil {
				return fmt.Errorf("failed to derive the key for point %q: %w", id, err)
			}
			if err := b.Delete(key); err != nil {
				return fmt.Errorf("failed to delete point %s from bbolt: %w", id, err)
			}
		}
		return nil
	})
}

// DropCollection removes a collection and its metadata from the storage.
func (s *Store) DropCollection(name string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	key, err := s.bucketKey(name)
	if err != nil {
		return fmt.Errorf("failed to derive the bucket key for collection %q: %w", name, err)
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		// Delete collection bucket under the configured root.
		if len(s.root) > 0 {
			if root := tx.Bucket(s.root); root != nil {
				if err := root.DeleteBucket(key); err != nil && err != bbolt.ErrBucketNotFound {
					return fmt.Errorf("failed to delete bucket: %w", err)
				}
			}
		} else if err := tx.DeleteBucket(key); err != nil && err != bbolt.ErrBucketNotFound {
			return fmt.Errorf("failed to delete bucket: %w", err)
		}

		// Delete metadata.
		metaBucket, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if metaBucket != nil {
			if err := metaBucket.Delete(key); err != nil {
				return fmt.Errorf("failed to delete metadata: %w", err)
			}
		}

		return nil
	})
}
