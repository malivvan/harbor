package harbor

import (
	"context"
	"errors"
	"fmt"

	"github.com/malivvan/harbor/pkg/audit"
	"github.com/malivvan/harbor/store/file"
	"github.com/malivvan/harbor/store/record"
	"github.com/malivvan/harbor/store/search"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/vector"
	bolt "go.etcd.io/bbolt"
)

// ErrTxNotWritable reports a write through a read-only transaction.
var ErrTxNotWritable = errors.New("harbor: transaction is read-only")

// Tx is one bbolt transaction bound to all four sub-stores.
//
// A Tx is created by Store.WriteTransaction or Store.ReadTransaction and is
// valid only inside that callback. Every operation performed through DB, FS,
// TS and Vec runs in the same bbolt transaction, so a write that touches
// several stores commits or rolls back as one unit.
//
// The views are single-goroutine: a callback must not use them from another
// goroutine, and must not store them beyond the callback. Harbor never commits,
// rolls back or closes the transaction on its own; the callback's return value
// decides the outcome.
type Tx struct {
	store    *Store
	btx      *bolt.Tx
	writable bool

	record record.Tx
	file   *file.Store
	series *series.Store
	vector *vector.Store
	search *search.Store
}

// newTx binds the sub-stores to btx. sink, when non-nil, collects audit events
// so they can be recorded after the transaction commits.
func newTx(s *Store, btx *bolt.Tx, writable bool, sink audit.Sink) (*Tx, error) {
	attached, err := s.record.AttachWithAudit(btx, sink)
	if err != nil {
		return nil, err
	}
	tx := &Tx{
		store:    s,
		btx:      btx,
		writable: writable,
		record:   attached,
		file:     s.file.WithTransaction(btx),
		series:   s.series.WithTransaction(btx),
		vector:   s.vector.WithTransaction(btx),
		search:   s.search.WithTransaction(btx),
	}
	if sink != nil {
		tx.file.SetAuditSink(sink)
		tx.series.SetAuditSink(sink)
		tx.vector.SetAuditSink(sink)
	} else {
		// Never let a bound view call the recorder: its Record opens its own
		// write transaction, which would deadlock against the one we hold.
		tx.file.SetAuditSink(nil)
		tx.series.SetAuditSink(nil)
		tx.vector.SetAuditSink(nil)
	}
	return tx, nil
}

// Record returns the record sub-store bound to this transaction, or nil after
// the transaction ended.
func (tx *Tx) Record() record.Tx {
	if tx == nil {
		return nil
	}
	return tx.record
}

// File returns the file sub-store bound to this transaction, or nil after the
// transaction ended. File systems opened through it are bound to the same
// transaction.
func (tx *Tx) File() *file.Store {
	if tx == nil {
		return nil
	}
	return tx.file
}

// Series returns the time-series sub-store bound to this transaction, or nil
// after the transaction ended.
func (tx *Tx) Series() *series.Store {
	if tx == nil {
		return nil
	}
	return tx.series
}

// Vector returns the vector sub-store bound to this transaction, or nil after
// the transaction ended. Points upserted through it are staged in the
// transaction and become visible to searches on the live store when it commits;
// searches through this view see the staged points immediately.
func (tx *Tx) Vector() *vector.Store {
	if tx == nil {
		return nil
	}
	return tx.vector
}

// Search returns the full-text sub-store bound to this transaction, or nil
// after the transaction ended.
func (tx *Tx) Search() *search.Store {
	if tx == nil {
		return nil
	}
	return tx.search
}

// Writable reports whether the transaction accepts writes.
func (tx *Tx) Writable() bool {
	return tx != nil && tx.writable
}

// txAuditSink collects the audit events recorded by a transaction-bound store.
// Recording them inside the transaction is impossible: audit.Record opens its
// own bbolt write transaction, which would deadlock against ours. The root
// package replays the collected entries after a successful commit.
type txAuditSink struct {
	entries []audit.Entry
}

// Record implements audit.Sink.
func (t *txAuditSink) Record(e audit.Entry) error {
	t.entries = append(t.entries, e)
	return nil
}

// replay records the collected entries in the live audit trail.
func (t *txAuditSink) replay(rec *audit.Recorder) error {
	if rec == nil {
		return nil
	}
	for _, e := range t.entries {
		if err := rec.Record(e); err != nil {
			return err
		}
	}
	return nil
}

// WriteTransaction runs fn inside one bbolt write transaction bound to all four
// sub-stores. Returning nil commits every change made through tx; returning an
// error (or canceling ctx) rolls all of them back.
//
// Vector points buffered by earlier calls are flushed before the transaction
// starts, so the transaction sees the state the store has accepted. Points
// upserted through tx.Vector() are staged and written inside the transaction; the
// live collection indexes adopt them after the commit, so searches outside the
// transaction observe them only once it commits.
//
// Audit events for writes inside the transaction are collected and appended to
// the audit trail after the commit, never inside it.
//
// Write transactions are serialized: a second WriteTransaction waits for the
// first to finish. Starting one from inside another transaction on the same
// handle is not supported (it blocks); pass tx down instead of calling
// WriteTransaction again.
func (s *Store) WriteTransaction(ctx context.Context, fn func(tx *Tx) error) error {
	if fn == nil {
		return fmt.Errorf("%w: callback is required", ErrInvalidOptions)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return ErrClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}

	// Buffered vector points are only in memory; drain them so the transaction
	// starts from the state the store has accepted.
	if s.vector != nil {
		if err := s.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			return fmt.Errorf("harbor: transaction: flush vec store: %w", err)
		}
	}

	s.txMu.Lock()
	defer s.txMu.Unlock()

	var sink *txAuditSink
	var collector audit.Sink
	if s.audit != nil {
		sink = &txAuditSink{}
		collector = sink
	}

	var bound *Tx
	err := s.bs.Update(func(btx *bolt.Tx) error {
		var err error
		bound, err = newTx(s, btx, true, collector)
		if err != nil {
			return err
		}
		if err := fn(bound); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Flush staged vector points into the transaction before it commits.
		return bound.vector.FlushStaged()
	})
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return err
	}

	// The transaction committed. Install the index state built inside it and
	// replay the staged audit events against the real recorder.
	s.vector.AdoptTransaction(bound.vector)
	if sink != nil {
		if err := sink.replay(s.audit); err != nil {
			return fmt.Errorf("harbor: transaction committed, audit append failed: %w", err)
		}
	}
	return nil
}

// ReadTransaction runs fn inside one consistent read-only snapshot bound to all
// four sub-stores. The snapshot does not include writes committed after it
// started.
//
// Vector points buffered by earlier calls are flushed first, so the snapshot
// reflects every point the store has accepted. Writes through the callback fail
// with ErrTxNotWritable (db) or bbolt's read-only error (fs, ts, vec).
//
// A read transaction blocks while a write transaction is open, matching bbolt's
// single-writer model, and must not be started from inside a write
// transaction's callback.
func (s *Store) ReadTransaction(ctx context.Context, fn func(tx *Tx) error) error {
	if fn == nil {
		return fmt.Errorf("%w: callback is required", ErrInvalidOptions)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return ErrClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}

	if s.vector != nil {
		if err := s.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			return fmt.Errorf("harbor: transaction: flush vec store: %w", err)
		}
	}

	return s.bs.View(func(btx *bolt.Tx) error {
		bound, err := newTx(s, btx, false, nil)
		if err != nil {
			return err
		}
		if err := fn(bound); err != nil {
			return err
		}
		return ctx.Err()
	})
}
