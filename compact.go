package harbor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/malivvan/harbor/vec"
	bolt "go.etcd.io/bbolt"
)

// compactTxMaxSize bounds each rewrite transaction during compaction. Committing
// periodically keeps memory bounded for large stores.
const compactTxMaxSize = 64 << 10

// CompactTo writes a compacted copy of the database to path.
//
// bbolt appends pages and only reuses the ones it has freed, so a store that
// grows and shrinks keeps a file larger than its live data needs. CompactTo
// rewrites the live pages into a fresh file, dropping free space.
//
// Buffered vector points are flushed first. path must not exist; it is created
// with mode (0600 when mode is zero) and removed again if compaction fails.
// Writers may continue during the copy because bbolt compacts from a read
// transaction; Close waits for the copy to finish.
//
// The result is the on-disk representation, not a logical export: an encrypted
// store yields an encrypted compacted file that opens with the same password.
// bbolt.Compact cannot be interrupted once started, so ctx is checked before the
// copy only.
func (s *Store) CompactTo(ctx context.Context, path string, mode os.FileMode) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("harbor: compact: %w: path is required", ErrInvalidOptions)
	}
	if s == nil {
		return ErrClosed
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.bs == nil {
		return ErrClosed
	}

	// Buffered vector points are not in the database yet; drain them so the
	// copy is complete.
	if s.vec != nil {
		if err := s.vec.Flush(); err != nil && !errors.Is(err, vec.ErrClosed) {
			return fmt.Errorf("harbor: compact: flush vec store: %w", err)
		}
	}

	if _, err := os.Stat(path); err == nil {
		return &os.PathError{Op: "compact", Path: path, Err: os.ErrExist}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("harbor: compact: %w", err)
	}

	if mode == 0 {
		mode = 0o600
	}
	dst, err := bolt.Open(path, mode, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("harbor: compact: %w", err)
	}
	compactErr := bolt.Compact(dst, s.bs, compactTxMaxSize)
	closeErr := dst.Close()
	if compactErr == nil {
		compactErr = closeErr
	}
	if compactErr != nil {
		_ = os.Remove(path)
		return fmt.Errorf("harbor: compact: %w", compactErr)
	}
	return nil
}
