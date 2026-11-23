// Package audit provides a tamper-evident, append-only audit trail for Harbor.
//
// A Recorder appends signed, hash-linked events to a bbolt database: each event
// carries the store, operation, entity and a SHA-256 digest of the written
// payload, is signed with an Ed25519 key derived from the store's key material
// (see DeriveKey), and links to its predecessor with a hash chain. Verify
// replays the chain and rejects forged or reordered events.
package audit

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/malivvan/harbor/internal/slip10"
	"github.com/malivvan/harbor/pkg/sign/canonical"
	bolt "go.etcd.io/bbolt"
)

// Reserved bucket and record names under the database root.
const (
	auditBucket  = "audit"
	eventsBucket = "events"
	headKey      = "head"

	// FormatVersion is the on-disk audit format. Unknown versions fail closed.
	FormatVersion = 1

	// genesisDomain anchors the start of every chain.
	genesisDomain = "harbor/audit/v1/genesis"
)

// Store names used by the four stores.
const (
	StoreDB  = "db"
	StoreFS  = "fs"
	StoreTS  = "ts"
	StoreVec = "vec"
)

var (
	// ErrNoLog is returned when verification finds no audit log.
	ErrNoLog = errors.New("audit: no audit log")
)

// Entry is what a store reports after a committed write. It is the unsigned
// raw material that Record turns into a signed Event.
type Entry struct {
	Store  string
	Op     string
	Entity string
	Digest []byte // SHA-256 of the written payload; nil for deletes
	At     time.Time
}

// Sink receives committed write entries. The four stores accept an optional
// Sink and call it after each committed write.
type Sink interface {
	Record(Entry) error
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(Entry) error

// Record implements Sink.
func (f SinkFunc) Record(e Entry) error { return f(e) }

// Event is the signed, hash-linked unit persisted to disk.
type Event struct {
	Seq      uint64 `json:"seq"`
	Store    string `json:"store"`
	Op       string `json:"op"`
	Entity   string `json:"entity"`
	Digest   []byte `json:"digest,omitempty"`
	At       int64  `json:"at"`
	PrevHash []byte `json:"prev"`
}

// Result reports the outcome of a verification.
type Result struct {
	// Count is the number of events in the chain.
	Count uint64
	// Tip is the SHA-256 of the last event, or the genesis hash when empty.
	Tip []byte
}

// Recorder appends signed, hash-linked events to a bbolt database.
type Recorder struct {
	b    *bolt.DB
	keys map[string]ed25519.PrivateKey
	now  func() time.Time
}

// NewRecorder returns a Recorder writing to b, signing events with the private
// key selected by entry.Store. It is safe for concurrent use.
func NewRecorder(b *bolt.DB, keys map[string]ed25519.PrivateKey) *Recorder {
	return &Recorder{b: b, keys: keys, now: time.Now}
}

// Record appends one signed event to the log.
func (r *Recorder) Record(entry Entry) error {
	key, ok := r.keys[entry.Store]
	if !ok {
		return fmt.Errorf("audit: no key for store %q", entry.Store)
	}
	return r.b.Update(func(tx *bolt.Tx) error {
		return r.recordTx(tx, key, entry)
	})
}

// Head returns the current event count and chain tip, or the genesis hash when
// the log is empty. Operators use it to anchor the tip externally.
func (r *Recorder) Head() (count uint64, tip []byte, err error) {
	err = r.b.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(auditBucket))
		if root == nil {
			count, tip = 0, genesisHash()
			return nil
		}
		count, tip, err = readHead(root)
		return err
	})
	return count, tip, err
}

func (r *Recorder) recordTx(tx *bolt.Tx, key ed25519.PrivateKey, entry Entry) error {
	root, err := tx.CreateBucketIfNotExists([]byte(auditBucket))
	if err != nil {
		return err
	}

	count, tip, err := readHead(root)
	if err != nil {
		return err
	}

	at := entry.At
	if at.IsZero() {
		at = r.now()
	}

	ev := Event{
		Seq:      count,
		Store:    entry.Store,
		Op:       entry.Op,
		Entity:   entry.Entity,
		Digest:   entry.Digest,
		At:       at.UnixNano(),
		PrevHash: tip,
	}
	if len(ev.Digest) != 0 && len(ev.Digest) != sha256.Size {
		return fmt.Errorf("audit: digest must be %d bytes, got %d", sha256.Size, len(ev.Digest))
	}

	// Sign the canonical encoding, not encoding/json's struct order: an event
	// that later grows a map field must still produce one signature for one
	// logical value. The bytes stored are the bytes signed.
	encoded, err := canonical.Bytes(ev)
	if err != nil {
		return fmt.Errorf("audit: encode event: %w", err)
	}
	sig := ed25519.Sign(key, encoded)

	events, err := root.CreateBucketIfNotExists([]byte(eventsBucket))
	if err != nil {
		return err
	}
	if err := events.Put(seqKey(ev.Seq), append(encoded, sig...)); err != nil {
		return err
	}

	sum := sha256.Sum256(encoded)
	return writeHead(root, ev.Seq+1, sum)
}

// Verify replays the audit log in b and checks that every event has a valid
// signature and links to its predecessor. It returns the event count and tip.
func Verify(b *bolt.DB, keys map[string]ed25519.PublicKey) (Result, error) {
	var res Result
	err := b.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(auditBucket))
		if root == nil {
			return ErrNoLog
		}
		events := root.Bucket([]byte(eventsBucket))
		if events == nil {
			res.Tip = genesisHash()
			return nil
		}

		prev := genesisHash()
		var count uint64
		err := events.ForEach(func(_, v []byte) error {
			if len(v) < ed25519.SignatureSize {
				return fmt.Errorf("audit: event %d: truncated record", count)
			}
			encoded := v[:len(v)-ed25519.SignatureSize]
			sig := v[len(v)-ed25519.SignatureSize:]

			var ev Event
			if err := json.Unmarshal(encoded, &ev); err != nil {
				return fmt.Errorf("audit: decode event %d: %w", count, err)
			}
			if ev.Seq != count {
				return fmt.Errorf("audit: non-monotonic sequence: got %d want %d", ev.Seq, count)
			}
			pub, ok := keys[ev.Store]
			if !ok {
				return fmt.Errorf("audit: no public key for store %q", ev.Store)
			}
			if !ed25519.Verify(pub, encoded, sig) {
				return fmt.Errorf("audit: bad signature on event %d", ev.Seq)
			}
			if !bytes.Equal(ev.PrevHash, prev) {
				return fmt.Errorf("audit: broken chain at event %d", ev.Seq)
			}
			sum := sha256.Sum256(encoded)
			prev = sum[:]
			count++
			return nil
		})
		if err != nil {
			return err
		}
		res.Count = count
		res.Tip = prev
		return nil
	})
	return res, err
}

// DeriveKey derives an Ed25519 key pair from the Argon2id seed at a hardened
// SLIP-0010 path (for example "m/0'"), mirroring the datastore sub-key
// derivation.
func DeriveKey(seed []byte, path string) (ed25519.PublicKey, ed25519.PrivateKey, error) {
	node, err := slip10.DeriveForPath(path, seed)
	if err != nil {
		return nil, nil, fmt.Errorf("audit: derive %s: %w", path, err)
	}
	pub, priv := node.KeyPair()
	return pub, priv, nil
}

func seqKey(seq uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], seq)
	return k[:]
}

func genesisHash() []byte {
	sum := sha256.Sum256([]byte(genesisDomain))
	return sum[:]
}

// readHead returns the event count and chain tip, or the genesis state when no
// head has been written yet.
func readHead(root *bolt.Bucket) (count uint64, tip []byte, err error) {
	raw := root.Get([]byte(headKey))
	if raw == nil {
		return 0, genesisHash(), nil
	}
	if len(raw) != 1+8+sha256.Size {
		return 0, nil, fmt.Errorf("audit: corrupt head record")
	}
	if raw[0] != FormatVersion {
		return 0, nil, fmt.Errorf("audit: unsupported audit format %d", raw[0])
	}
	count = binary.BigEndian.Uint64(raw[1 : 1+8])
	tip = append([]byte(nil), raw[1+8:]...)
	return count, tip, nil
}

func writeHead(root *bolt.Bucket, count uint64, tip [32]byte) error {
	raw := make([]byte, 1+8+sha256.Size)
	raw[0] = FormatVersion
	binary.BigEndian.PutUint64(raw[1:1+8], count)
	copy(raw[1+8:], tip[:])
	return root.Put([]byte(headKey), raw)
}
