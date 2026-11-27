package consensus

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/malivvan/eidos-db"
	"github.com/malivvan/eidos-db/pkg/metrics"
)

const (
	// DefaultApplyTimeout bounds how long a write waits for raft to commit and
	// apply the entry.
	DefaultApplyTimeout = 10 * time.Second

	// retainSnapshotCount is how many raft snapshots are kept on disk.
	retainSnapshotCount = 2

	// transportMaxPool and transportTimeout configure the TCP transport.
	transportMaxPool = 3
	transportTimeout = 10 * time.Second
)

// Config configures one node in an eidos-db cluster.
type Config struct {
	// NodeID is the stable identifier of this node. It must be unique within
	// the cluster and stable across restarts.
	NodeID raft.ServerID

	// BindAddr is the host:port the raft transport binds to, and the address
	// other nodes use to reach it, e.g. "127.0.0.1:7001".
	BindAddr string

	// Dir is a directory for the raft log, stable store and snapshots. It is
	// created if missing and must be local and writable.
	Dir string

	// Bootstrap starts a new cluster with this node as its only member. Set it
	// only on the first node; subsequent nodes join with Join.
	Bootstrap bool

	// StorePath is the eidos-db file. It is created if missing.
	StorePath string

	// StoreMode is the file mode used when creating the store file. Zero
	// selects 0600.
	StoreMode os.FileMode

	// StoreOpts configures the eidos-db store (password, KDF, encryption, codec).
	// The password must be identical across the cluster; encryption flags matter
	// only when the store is first created.
	StoreOpts *eidos.Options

	// ApplyTimeout bounds each raft.Apply. Zero selects DefaultApplyTimeout.
	ApplyTimeout time.Duration

	// LogOutput receives raft logs. Nil discards them.
	LogOutput io.Writer

	// Metrics, when non-nil, receives consensus apply-lag metrics and tracing
	// spans. A nil Observer disables instrumentation.
	Metrics metrics.Observer

	// RaftConfig, when non-nil, replaces the default raft configuration.
	RaftConfig *raft.Config
}

func (c Config) validate() error {
	switch {
	case c.NodeID == "":
		return fmt.Errorf("%w: NodeID is required", ErrInvalidConfig)
	case c.BindAddr == "":
		return fmt.Errorf("%w: BindAddr is required", ErrInvalidConfig)
	case c.Dir == "":
		return fmt.Errorf("%w: Dir is required", ErrInvalidConfig)
	case c.StorePath == "":
		return fmt.Errorf("%w: StorePath is required", ErrInvalidConfig)
	case c.StoreOpts == nil:
		return fmt.Errorf("%w: StoreOpts is required", ErrInvalidConfig)
	}
	return nil
}

func (c Config) applyTimeout() time.Duration {
	if c.ApplyTimeout <= 0 {
		return DefaultApplyTimeout
	}
	return c.ApplyTimeout
}

func (c Config) storeMode() os.FileMode {
	if c.StoreMode == 0 {
		return 0o600
	}
	return c.StoreMode
}
