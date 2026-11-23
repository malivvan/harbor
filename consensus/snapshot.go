package consensus

import (
	"github.com/hashicorp/raft"
)

// fsmSnapshot is a byte-serialized harbor database.
type fsmSnapshot struct {
	data []byte
}

var _ raft.FSMSnapshot = (*fsmSnapshot)(nil)

// Persist writes the snapshot to the sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release frees the snapshot. The harbor database needs no cleanup.
func (s *fsmSnapshot) Release() {}
