// Package consensus provides cluster-wide replication for a Harbor store using
// the Raft consensus algorithm (github.com/hashicorp/raft).
//
// A Node owns one *harbor.Store replica and the raft machinery that keeps it in
// sync with the rest of the cluster. Writes are proposed through raft on the
// leader and applied, in raft-log order, to the local store on every node;
// reads are served directly from the local store. See PLAN.md for the full
// design and the consistency model.
package consensus

import (
	"errors"
	"fmt"
)

var (
	// ErrClosed is returned by operations on a closed Node.
	ErrClosed = errors.New("consensus: node is closed")

	// ErrNotLeader is returned when a write is attempted on a node that is not
	// the current leader. Callers can redirect using Node.Leader.
	ErrNotLeader = errors.New("consensus: not leader")

	// ErrLeaderNotFound is returned when the cluster has no elected leader.
	ErrLeaderNotFound = errors.New("consensus: leader not found")

	// ErrInvalidConfig reports a nil or unusable Config.
	ErrInvalidConfig = errors.New("consensus: invalid config")
)

// ApplyError reports a committed log entry the FSM could not apply: an
// undecodable command, a malformed argument, or an operation this build does
// not know. It carries the raft log index and operation so an operator can
// locate the offending entry.
//
// A corrupt or incompatible log is not recoverable by retrying, so the entry
// is reported rather than applied: the node keeps serving reads and the rest
// of the log, and the error surfaces to the proposing caller.
type ApplyError struct {
	// Index is the raft log index of the failed entry.
	Index uint64

	// Op is the command operation, empty when the entry could not be decoded.
	Op string

	// Err is the underlying cause.
	Err error
}

// Error implements the error interface.
func (e *ApplyError) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("consensus: apply log index %d: %v", e.Index, e.Err)
	}
	return fmt.Sprintf("consensus: apply log index %d op %q: %v", e.Index, e.Op, e.Err)
}

// Unwrap returns the underlying cause.
func (e *ApplyError) Unwrap() error { return e.Err }
