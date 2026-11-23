package record

import (
	"github.com/malivvan/harbor/pkg/ledger"
	bolt "go.etcd.io/bbolt"
)

// Tx is a Harbor facade bound to a bbolt transaction owned by the caller.
//
// It exposes the complete Node operation surface — Save, Update,
// DeleteStruct, One, Find, All, AllByIndex, Range, Prefix, Count, Select
// queries, bucket scans, and key/value access — and every operation executes
// inside the supplied transaction. Uncommitted writes are visible to reads
// performed through the same Tx.
//
// Tx intentionally exposes no commit, rollback, or close operations, and it
// never opens a managed transaction of its own. The caller retains full
// ownership of the transaction and must Commit or RollBack it.
type Tx interface {
	Node
}

// Compile-time assertion: the transaction-bound node implements Tx.
var _ Tx = (*node)(nil)

// Attach returns a transaction-bound facade for an existing bbolt transaction
// owned by the caller.
//
// Guarantees:
//
//   - Harbor never commits, rolls back, closes, or replaces the supplied
//     transaction. No operation on the returned Tx acquires a managed
//     transaction.
//   - All reads and writes through the returned Tx, including nodes derived
//     via From, WithCodec, PrefixScan, and RangeScan — use the supplied
//     transaction.
//   - Attach accepts writable and read-only transactions. Writes through a
//     read-only transaction fail with bbolt's ErrTxNotWritable.
//
// Attach fails with ErrNilParam for a nil receiver or transaction, and with
// ErrTxFromDifferentDB when the transaction belongs to a different database.
//
// The returned Tx is valid only for the lifetime of the supplied transaction.
// Using it after the caller commits or rolls back is undefined behavior, just
// as using the bbolt transaction itself would be: reads may panic and writes
// fail with bbolt's ErrTxClosed. Harbor never falls back to a managed
// transaction for a dead handle.
func (s *Store) Attach(tx *bolt.Tx) (Tx, error) {
	return s.attach(tx, nil)
}

// AttachWithAudit is Attach with an explicit audit sink for writes performed
// through the returned Tx. It lets a caller that owns a bbolt transaction stage
// audit events and record them after committing, instead of calling the
// store's recorder from inside the transaction (which would deadlock).
//
// A nil sink attaches with no audit recording at all, even when the store has
// an audit trail configured.
func (s *Store) AttachWithAudit(tx *bolt.Tx, sink ledger.Sink) (Tx, error) {
	return s.attach(tx, sink)
}

func (s *Store) attach(tx *bolt.Tx, sink ledger.Sink) (Tx, error) {
	if s == nil || tx == nil {
		return nil, wrapError("attach", ErrNilParam)
	}

	if s.bolt == nil || tx.DB() != s.bolt {
		return nil, wrapError("attach", ErrTxFromDifferentDB)
	}

	root, ok := s.Node.(*node)
	if !ok {
		return nil, wrapError("attach", ErrBadType)
	}

	bound := root.withTransaction(tx)
	bound.audit = sink
	return bound, nil
}
