package record

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	bolt "go.etcd.io/bbolt"
)

const historyBucketName = "__tempest_history__"

// ErrHistoryDisabled reports a history operation on a store that does not
// record revisions.
var ErrHistoryDisabled = errors.New("record: history is not enabled")

// RevisionOp is the kind of mutation a revision records.
type RevisionOp string

const (
	// RevisionCreate is the first value written for a key.
	RevisionCreate RevisionOp = "created"
	// RevisionUpdate replaces an existing value.
	RevisionUpdate RevisionOp = "updated"
	// RevisionDelete removes the live value; Value is empty.
	RevisionDelete RevisionOp = "deleted"
)

// Revision is one persisted record revision.
type Revision struct {
	Seq    uint64
	Op     RevisionOp
	At     time.Time
	Digest []byte
	Value  []byte
}

// RecordDiff is the pair of revisions around a time range. Either side may be
// nil when the record did not exist at that bound.
type RecordDiff struct {
	Before  *Revision
	After   *Revision
	Changed bool
}

func historySeqKey(seq uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], seq)
	return key[:]
}

// historyBucket returns the per-record revision bucket. bbolt bucket names are
// arbitrary bytes, so an encoded primary key is a valid child name.
func (n *node) historyBucket(bucket *bolt.Bucket, id []byte, create bool) (*bolt.Bucket, error) {
	if bucket == nil {
		return nil, nil
	}
	root := bucket.Bucket([]byte(historyBucketName))
	if root == nil {
		if !create {
			return nil, nil
		}
		var err error
		root, err = bucket.CreateBucketIfNotExists([]byte(historyBucketName))
		if err != nil {
			return nil, err
		}
	}
	if create {
		return root.CreateBucketIfNotExists(id)
	}
	return root.Bucket(id), nil
}

// appendRevision stores one revision. It is a no-op when history is disabled or
// the caller is a maintenance rewrite (ReIndex).
func (n *node) appendRevision(ctx context.Context, tx *bolt.Tx, bucket *bolt.Bucket, id []byte, op RevisionOp, value []byte, at time.Time) error {
	if n == nil || n.s == nil || !n.s.history || n.skipHistory {
		return nil
	}
	if err := n.ensureCapability(ctx, tx, CapHistory); err != nil {
		return err
	}
	if at.IsZero() {
		at = n.s.now()
	}
	hb, err := n.historyBucket(bucket, id, true)
	if err != nil {
		return err
	}
	var seq uint64 = 1
	if k, _ := hb.Cursor().Last(); len(k) == 8 {
		seq = binary.BigEndian.Uint64(k) + 1
	}
	var digest []byte
	if len(value) > 0 {
		sum := sha256.Sum256(value)
		digest = sum[:]
	}
	rev := Revision{Seq: seq, Op: op, At: at.UTC(), Digest: digest, Value: append([]byte(nil), value...)}
	raw, err := json.Marshal(rev)
	if err != nil {
		return err
	}
	sealed, err := n.seal(raw)
	if err != nil {
		return err
	}
	return hb.Put(historySeqKey(seq), sealed)
}

// History returns every revision of data's primary key, oldest first.
func (n *node) History(ctx context.Context, data any) ([]Revision, error) {
	if err := checkContext(ctx); err != nil {
		return nil, wrapError("history", err)
	}
	if n == nil || n.s == nil || !n.s.history {
		return nil, wrapError("history", ErrHistoryDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return nil, wrapError("history", err)
	}
	if cfg.ID.IsZero {
		return nil, wrapError("history", ErrNoID)
	}
	var out []Revision
	err = n.readTx(ctx, func(tx *bolt.Tx) error {
		id, err := n.keyBytes(cfg.ID.Value.Interface(), keyDomainID)
		if err != nil {
			return err
		}
		b := n.getBucket(tx, cfg.Name)
		if b == nil {
			return ErrNotFound
		}
		hb, err := n.historyBucket(b, id, false)
		if err != nil || hb == nil {
			return err
		}
		return hb.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			plain, err := n.open(v)
			if err != nil {
				return err
			}
			var rev Revision
			if err := json.Unmarshal(plain, &rev); err != nil {
				return err
			}
			out = append(out, rev)
			return nil
		})
	})
	if err != nil {
		return nil, wrapError("history", err)
	}
	return out, nil
}

// AsOf returns the record as it stood at at. A deleted or never-created record
// returns ErrNotFound.
func (n *node) AsOf(ctx context.Context, data any, at time.Time) (any, error) {
	h, err := n.History(ctx, data)
	if err != nil {
		return nil, err
	}
	var chosen *Revision
	for i := range h {
		if !h[i].At.After(at) {
			chosen = &h[i]
		}
	}
	if chosen == nil || chosen.Op == RevisionDelete || len(chosen.Value) == 0 {
		return nil, wrapError("as of", ErrNotFound)
	}
	typ := reflect.TypeOf(data)
	if typ == nil || typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct {
		return nil, wrapError("as of", ErrStructPtrNeeded)
	}
	out := reflect.New(typ.Elem())
	if err := n.unmarshal(chosen.Value, out.Interface()); err != nil {
		return nil, wrapError("as of", err)
	}
	return out.Interface(), nil
}

// Diff returns the revisions at or before from and to.
func (n *node) Diff(ctx context.Context, data any, from, to time.Time) (*RecordDiff, error) {
	h, err := n.History(ctx, data)
	if err != nil {
		return nil, err
	}
	diff := &RecordDiff{}
	for i := range h {
		if !h[i].At.After(from) {
			diff.Before = &h[i]
		}
		if !h[i].At.After(to) {
			diff.After = &h[i]
		}
	}
	if diff.Before != nil && diff.After != nil {
		diff.Changed = diff.Before.Seq != diff.After.Seq
	}
	return diff, nil
}

// TrimHistory removes revisions older than before and returns the count.
func (n *node) TrimHistory(ctx context.Context, data any, before time.Time) (int, error) {
	if err := checkContext(ctx); err != nil {
		return 0, wrapError("trim history", err)
	}
	if n == nil || n.s == nil || !n.s.history {
		return 0, wrapError("trim history", ErrHistoryDisabled)
	}
	cfg, err := structConfigOf(data)
	if err != nil {
		return 0, wrapError("trim history", err)
	}
	if cfg.ID.IsZero {
		return 0, wrapError("trim history", ErrNoID)
	}
	removed := 0
	err = n.readWriteTx(ctx, func(tx *bolt.Tx) error {
		id, err := n.keyBytes(cfg.ID.Value.Interface(), keyDomainID)
		if err != nil {
			return err
		}
		b := n.getBucket(tx, cfg.Name)
		if b == nil {
			return nil
		}
		hb, err := n.historyBucket(b, id, false)
		if err != nil || hb == nil {
			return err
		}
		var drop [][]byte
		if err := hb.ForEach(func(k, v []byte) error {
			if v == nil {
				return nil
			}
			plain, err := n.open(v)
			if err != nil {
				return err
			}
			var rev Revision
			if err := json.Unmarshal(plain, &rev); err != nil {
				return err
			}
			if rev.At.Before(before) {
				drop = append(drop, append([]byte(nil), k...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, key := range drop {
			if err := hb.Delete(key); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return removed, wrapError("trim history", err)
	}
	return removed, nil
}

// historyAt is a small helper for maintenance paths.
func historyAt(n *node, tx *bolt.Tx, cfg *structConfig, id []byte) (*bolt.Bucket, error) {
	b := n.getBucket(tx, cfg.Name)
	if b == nil {
		return nil, fmt.Errorf("record: history bucket for %q is missing", cfg.Name)
	}
	return n.historyBucket(b, id, false)
}
