package vec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
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

	closed    bool
	quantizer Quantizer
	useQuant  bool
}

func toStoredPoint(p *PointStruct) *StoredPoint {
	stored := &StoredPoint{
		ID:     p.ID,
		Vector: p.Vector,
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
				// For complex types (map, slice, etc.), JSON-serialize and store as bytes
				if jsonBytes, err := json.Marshal(val); err == nil {
					value.Kind = KindBytes
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
		ID:     stored.ID,
		Vector: stored.Vector,
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
				// Try to deserialize as JSON for complex nested objects (map, slice, etc.)
				// If it's not valid JSON, return raw bytes
				var obj any
				if err := json.Unmarshal(v.BytesValue, &obj); err == nil {
					p.Payload[k] = obj
				} else {
					p.Payload[k] = v.BytesValue
				}
			}
		}
	}

	return p
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

	store, err := Open(context.Background(), bdb, nil, useQuant, quantizer)
	if err != nil {
		_ = bdb.Close()
		return nil, err
	}
	return store, nil
}

// Open initializes a new BoltDB storage engine with optional vector quantization.
// If useQuant is true, vectors will be compressed using the provided quantizer.
// The root key, when non-empty, is used as the parent bucket for all collections
// and collection metadata.
// Returns an error if the context is invalid.
func Open(ctx context.Context, bdb *bbolt.DB, root []byte, useQuant bool, quantizer Quantizer) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	if useQuant && quantizer == nil {
		quantizer = NewSQ8Quantizer()
	}

	return &Store{
		db:        bdb,
		closed:    false,
		quantizer: quantizer,
		useQuant:  useQuant,
		root:      append([]byte(nil), root...),
	}, nil
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
func (s *Store) collectionBucket(tx *bbolt.Tx, name string) *bbolt.Bucket {
	if len(s.root) == 0 {
		return tx.Bucket([]byte(name))
	}

	root := s.rootBucket(tx)
	if root == nil {
		return nil
	}
	return root.Bucket([]byte(name))
}

// createCollectionBucket creates the collection bucket under the configured
// root, creating the root bucket when necessary.
func (s *Store) createCollectionBucket(tx *bbolt.Tx, name string) (*bbolt.Bucket, error) {
	if len(s.root) == 0 {
		return tx.CreateBucketIfNotExists([]byte(name))
	}

	root, err := tx.CreateBucketIfNotExists(s.root)
	if err != nil {
		return nil, err
	}
	return root.CreateBucketIfNotExists([]byte(name))
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
	if s.closed {
		return nil
	}

	err := s.db.Close()
	if err == nil {
		s.closed = true
	}
	return err
}

// EnsureCollection creates a bucket for the collection if it doesn't already exist.
// Each collection is stored as a separate bucket in BoltDB.
func (s *Store) EnsureCollection(colName string) error {
	if s.closed {
		return fmt.Errorf("storage is closed")
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
	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := s.collectionBucket(tx, colName)
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
			if err := b.Put([]byte(p.ID), data); err != nil {
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
	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	points := make(map[string]*PointStruct)

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := s.collectionBucket(tx, colName)
		if b == nil {
			log.Printf("Bucket %s doesn't exist yet, starting empty", colName)
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			stored, err := binco.DecodeExact(StoredPointBinco, v)
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

			points[string(k)] = p
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
	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	var collections []string
	addCollection := func(name []byte) error {
		colName := string(name)
		if colName != collectionsMetaBucket {
			collections = append(collections, colName)
		}
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
	if s.closed {
		return fmt.Errorf("storage is closed")
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

		return b.Put([]byte(name), data)
	})
}

// LoadCollectionMeta retrieves collection metadata from the special bucket.
func (s *Store) LoadCollectionMeta(name string) (*CollectionMeta, error) {
	if s.closed {
		return nil, fmt.Errorf("storage is closed")
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

		data := b.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("metadata for collection %s not found", name)
		}

		return json.Unmarshal(data, &meta)
	})

	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// ListCollectionMetas returns all collection metadata stored in the database.
func (s *Store) ListCollectionMetas() ([]CollectionMeta, error) {
	if s.closed {
		return nil, fmt.Errorf("storage is closed")
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
			var meta CollectionMeta
			if err := json.Unmarshal(v, &meta); err != nil {
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
	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := s.collectionBucket(tx, colName)
		if b == nil {
			return nil // Bucket doesn't exist, nothing to delete
		}

		for _, id := range ids {
			if err := b.Delete([]byte(id)); err != nil {
				return fmt.Errorf("failed to delete point %s from bbolt: %w", id, err)
			}
		}
		return nil
	})
}

// DropCollection removes a collection and its metadata from the storage.
func (s *Store) DropCollection(name string) error {
	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		// Delete collection bucket under the configured root.
		if len(s.root) > 0 {
			if root := tx.Bucket(s.root); root != nil {
				if err := root.DeleteBucket([]byte(name)); err != nil && err != bbolt.ErrBucketNotFound {
					return fmt.Errorf("failed to delete bucket: %w", err)
				}
			}
		} else if err := tx.DeleteBucket([]byte(name)); err != nil && err != bbolt.ErrBucketNotFound {
			return fmt.Errorf("failed to delete bucket: %w", err)
		}

		// Delete metadata.
		metaBucket, err := s.metaBucket(tx, false)
		if err != nil {
			return err
		}
		if metaBucket != nil {
			if err := metaBucket.Delete([]byte(name)); err != nil {
				return fmt.Errorf("failed to delete metadata: %w", err)
			}
		}

		return nil
	})
}
