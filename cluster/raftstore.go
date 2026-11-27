package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// The bolt-backed raft log store and stable store.
//
// A Raft node needs a durable log and a small key/value store; both live in one
// bbolt file next to the replica. This implementation replaces the
// github.com/hashicorp/raft-boltdb/v2 dependency: it keeps the same on-disk
// layout (the "logs" and "conf" buckets, log entries keyed by the 8-byte
// big-endian index and msgpack encoded), so a raft.db written by that library
// stays readable, and it satisfies raft.LogStore and raft.StableStore.
const raftDBFileMode os.FileMode = 0o600

var (
	// raftLogsBucket holds the encoded raft log entries, keyed by index.
	raftLogsBucket = []byte("logs")

	// raftConfBucket holds the stable store key/value pairs (term, vote and
	// whatever else raft persists outside the log).
	raftConfBucket = []byte("conf")

	// errRaftKeyNotFound reports a stable-store key that was never written.
	// raft detects a missing key by comparing the error text to "not found"
	// (what raft-boltdb returned), so the message has to stay exactly that.
	errRaftKeyNotFound = errors.New("not found")
)

// boltStore is the raft log and stable store. It is opened once per node and
// closed with it; all methods are safe for concurrent use because bbolt
// serializes writers and allows concurrent readers.
type boltStore struct {
	conn *bolt.DB
	path string
}

// openBoltStore opens (creating it when necessary) the raft store at path.
func openBoltStore(path string) (*boltStore, error) {
	conn, err := bolt.Open(path, raftDBFileMode, nil)
	if err != nil {
		return nil, err
	}
	s := &boltStore{conn: conn, path: path}
	if err := s.initialize(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s, nil
}

// initialize creates the buckets the store writes into.
func (s *boltStore) initialize() error {
	return s.conn.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(raftLogsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(raftConfBucket)
		return err
	})
}

// Path returns the file the store was opened on.
func (s *boltStore) Path() string { return s.path }

// Close releases the database handle.
func (s *boltStore) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

// Sync fsyncs the database file.
func (s *boltStore) Sync() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Sync()
}

// FirstIndex returns the lowest log index on disk, or 0 for an empty log.
func (s *boltStore) FirstIndex() (uint64, error) {
	var first uint64
	err := s.conn.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(raftLogsBucket).Cursor().First()
		if k == nil {
			return nil
		}
		first = binary.BigEndian.Uint64(k)
		return nil
	})
	return first, err
}

// LastIndex returns the highest log index on disk, or 0 for an empty log.
func (s *boltStore) LastIndex() (uint64, error) {
	var last uint64
	err := s.conn.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(raftLogsBucket).Cursor().Last()
		if k == nil {
			return nil
		}
		last = binary.BigEndian.Uint64(k)
		return nil
	})
	return last, err
}

// GetLog reads the log entry at idx into log.
func (s *boltStore) GetLog(idx uint64, log *raft.Log) error {
	return s.conn.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(raftLogsBucket).Get(raftIndexKey(idx))
		if raw == nil {
			return raft.ErrLogNotFound
		}
		return decodeRaftLog(raw, log)
	})
}

// StoreLog writes one log entry.
func (s *boltStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs writes a batch of log entries in one transaction.
func (s *boltStore) StoreLogs(logs []*raft.Log) error {
	encoded := make([][]byte, len(logs))
	keys := make([][]byte, len(logs))
	for i, log := range logs {
		raw, err := encodeRaftLog(log)
		if err != nil {
			return err
		}
		encoded[i] = raw
		keys[i] = raftIndexKey(log.Index)
	}
	return s.conn.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(raftLogsBucket)
		for i := range keys {
			if err := bucket.Put(keys[i], encoded[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteRange removes every log entry in [min, max].
func (s *boltStore) DeleteRange(min, max uint64) error {
	return s.conn.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(raftLogsBucket).Cursor()
		for k, _ := cursor.Seek(raftIndexKey(min)); k != nil; k, _ = cursor.Next() {
			if binary.BigEndian.Uint64(k) > max {
				break
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Set stores a stable key/value pair.
func (s *boltStore) Set(k, v []byte) error {
	return s.conn.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(raftConfBucket).Put(k, v)
	})
}

// Get reads a stable key/value pair. A key that was never written reports
// errRaftKeyNotFound.
func (s *boltStore) Get(k []byte) ([]byte, error) {
	var out []byte
	err := s.conn.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(raftConfBucket).Get(k)
		if raw == nil {
			return errRaftKeyNotFound
		}
		out = append([]byte(nil), raw...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SetUint64 stores a uint64 value in the stable store.
func (s *boltStore) SetUint64(key []byte, val uint64) error {
	return s.Set(key, raftIndexKey(val))
}

// GetUint64 reads a uint64 value from the stable store.
func (s *boltStore) GetUint64(key []byte) (uint64, error) {
	raw, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("cluster: raft key %q holds %d bytes, want 8", key, len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

// raftIndexKey encodes a log index as the big-endian key the store sorts by.
func raftIndexKey(u uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, u)
	return key
}

// encodeRaftLog serializes a log entry for storage.
func encodeRaftLog(log *raft.Log) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	handle := codec.MsgpackHandle{}
	if err := codec.NewEncoder(buf, &handle).Encode(log); err != nil {
		return nil, fmt.Errorf("cluster: encode raft log %d: %w", log.Index, err)
	}
	return buf.Bytes(), nil
}

// decodeRaftLog parses a stored log entry. Decoding accepts both the builtin
// timestamp encoding and the older extension-based one, so a log written by
// raft-boltdb stays readable.
func decodeRaftLog(raw []byte, log *raft.Log) error {
	handle := codec.MsgpackHandle{}
	if err := codec.NewDecoder(bytes.NewReader(raw), &handle).Decode(log); err != nil {
		return fmt.Errorf("cluster: decode raft log: %w", err)
	}
	return nil
}

// The store is handed to raft as both halves of its persistent state.
var (
	_ raft.LogStore    = (*boltStore)(nil)
	_ raft.StableStore = (*boltStore)(nil)
)
