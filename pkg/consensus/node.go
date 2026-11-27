package consensus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/malivvan/eidos-db"
	"github.com/malivvan/eidos-db/pkg/metrics"
)

// Node is one member of an eidos-db cluster. It owns the local *eidos.Store
// replica and the raft state machine that keeps it in sync with the cluster.
type Node struct {
	cfg Config

	raft          *raft.Raft
	transport     *raft.NetworkTransport
	raftDB        *boltStore
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore

	// now is the clock pinned into replicated ts commands. It defaults to the
	// wall clock; tests override it to model clock divergence between nodes.
	now func() int64

	// obs receives operation metrics and spans, or nil when the node was
	// opened without instrumentation. It is set once at Open and never
	// mutated.
	obs metrics.Observer

	mu     sync.RWMutex
	store  *eidos.Store
	closed bool
}

// logf writes a diagnostic line to the configured raft log output, if any.
func (n *Node) logf(format string, args ...any) {
	if n == nil {
		return
	}
	w := n.cfg.LogOutput
	if w == nil {
		return
	}
	fmt.Fprintf(w, format+"\n", args...)
}

// nowNanos returns the node's clock reading in unix nanoseconds.
func (n *Node) nowNanos() int64 {
	if n == nil || n.now == nil {
		return time.Now().UnixNano()
	}
	return n.now()
}

// SetNow overrides the clock the leader pins into replicated ts commands.
// Passing nil restores the wall clock. It is intended for tests that model
// clock divergence between nodes.
func (n *Node) SetNow(fn func() int64) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.now = fn
}

// Open starts a cluster node. It opens the eidos-db store at StorePath, wires up
// the raft transport, log/stable/snapshot stores, and — when Bootstrap is set
// and no state exists yet — forms a single-node cluster.
func Open(ctx context.Context, cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("consensus: create data dir: %w", err)
	}

	st, err := eidos.Open(ctx, cfg.StorePath, cfg.storeMode(), cfg.StoreOpts)
	if err != nil {
		return nil, fmt.Errorf("consensus: open eidos-db store: %w", err)
	}

	n := &Node{cfg: cfg, store: st, obs: cfg.Metrics}
	if err := n.startRaft(); err != nil {
		_ = st.Close()
		return nil, err
	}
	return n, nil
}

// startRaft builds the transport, stores and raft instance, then bootstraps
// when requested and no state exists yet.
func (n *Node) startRaft() error {
	logWriter := n.cfg.LogOutput
	if logWriter == nil {
		logWriter = io.Discard
	}

	raftCfg := n.cfg.RaftConfig
	if raftCfg == nil {
		raftCfg = raft.DefaultConfig()
	}
	raftCfg.LocalID = n.cfg.NodeID
	if raftCfg.Logger == nil {
		raftCfg.Logger = newLogger(logWriter)
	}

	addr, err := net.ResolveTCPAddr("tcp", n.cfg.BindAddr)
	if err != nil {
		return fmt.Errorf("consensus: resolve bind address: %w", err)
	}
	transport, err := raft.NewTCPTransport(n.cfg.BindAddr, addr, transportMaxPool, transportTimeout, logWriter)
	if err != nil {
		return fmt.Errorf("consensus: create transport: %w", err)
	}
	n.transport = transport

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(n.cfg.Dir, "snapshots"), retainSnapshotCount, logWriter)
	if err != nil {
		return fmt.Errorf("consensus: create snapshot store: %w", err)
	}
	n.snapshotStore = snapshotStore

	raftDB, err := openBoltStore(filepath.Join(n.cfg.Dir, "raft.db"))
	if err != nil {
		return fmt.Errorf("consensus: create raft log store: %w", err)
	}
	n.raftDB = raftDB
	n.logStore = raftDB
	n.stableStore = raftDB

	ra, err := raft.NewRaft(raftCfg, &fsm{node: n}, n.logStore, n.stableStore, n.snapshotStore, n.transport)
	if err != nil {
		_ = raftDB.Close()
		return fmt.Errorf("consensus: start raft: %w", err)
	}
	n.raft = ra

	if n.cfg.Bootstrap {
		existing, err := raft.HasExistingState(n.logStore, n.stableStore, n.snapshotStore)
		if err != nil {
			return fmt.Errorf("consensus: check existing raft state: %w", err)
		}
		if !existing {
			configuration := raft.Configuration{Servers: []raft.Server{{
				ID:      n.cfg.NodeID,
				Address: transport.LocalAddr(),
			}}}
			if err := ra.BootstrapCluster(configuration).Error(); err != nil {
				return fmt.Errorf("consensus: bootstrap cluster: %w", err)
			}
		}
	}
	return nil
}

// newLogger builds the raft logger.
func newLogger(w io.Writer) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   "consensus",
		Output: w,
		Level:  hclog.Info,
	})
}

// Close shuts down raft and closes the local eidos-db store. It is safe to call
// more than once.
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.mu.Unlock()

	var errs []error
	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			errs = append(errs, fmt.Errorf("shutdown raft: %w", err))
		}
	}
	if n.transport != nil {
		if err := n.transport.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close transport: %w", err))
		}
	}
	if n.raftDB != nil {
		if err := n.raftDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close raft log store: %w", err))
		}
	}
	// Close the eidos-db store (flushing vec) last, after raft is stopped.
	n.mu.Lock()
	st := n.store
	n.store = nil
	n.mu.Unlock()
	if st != nil {
		if err := st.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close eidos-db store: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Store returns the local eidos-db store for reads. It must not be written to
// directly: writes made outside the facade methods are not replicated.
func (n *Node) Store() *eidos.Store {
	if n == nil {
		return nil
	}
	return n.storeHandle()
}

// Leader returns the current leader's address and ID, or ("", "") when there
// is no leader yet.
func (n *Node) Leader() (raft.ServerAddress, raft.ServerID) {
	if n == nil || n.raft == nil {
		return "", ""
	}
	return n.raft.LeaderWithID()
}

// State reports this node's raft state.
func (n *Node) State() raft.RaftState {
	if n == nil || n.raft == nil {
		return raft.Shutdown
	}
	return n.raft.State()
}

// Stats returns raft statistics for this node.
func (n *Node) Stats() map[string]string {
	if n == nil || n.raft == nil {
		return nil
	}
	return n.raft.Stats()
}

// AppliedIndex returns the highest raft log index applied to this node's store.
func (n *Node) AppliedIndex() uint64 {
	if n == nil || n.raft == nil {
		return 0
	}
	return n.raft.AppliedIndex()
}

// Barrier blocks until all prior log entries are applied, so a follower that
// just wrote can serve its own read (read-your-writes).
func (n *Node) Barrier(timeout time.Duration) error {
	if n == nil || n.raft == nil {
		return ErrClosed
	}
	if timeout <= 0 {
		timeout = n.cfg.applyTimeout()
	}
	return n.raft.Barrier(timeout).Error()
}

// Snapshot forces an immediate raft snapshot of the current state.
func (n *Node) Snapshot() error {
	if n == nil || n.raft == nil {
		return ErrClosed
	}
	return n.raft.Snapshot().Error()
}

// Join adds a node to the cluster. It must be called on the leader.
func (n *Node) Join(id raft.ServerID, addr raft.ServerAddress) error {
	if n == nil || n.raft == nil {
		return ErrClosed
	}
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	for _, srv := range configFuture.Configuration().Servers {
		// A node already exists with either the joining ID or address; it must
		// be removed first, unless it is exactly this node.
		if srv.ID == id || srv.Address == addr {
			if srv.ID == id && srv.Address == addr {
				return nil
			}
			if err := n.raft.RemoveServer(srv.ID, 0, 0).Error(); err != nil {
				return fmt.Errorf("consensus: remove conflicting server %s: %w", srv.ID, err)
			}
		}
	}
	if err := n.raft.AddVoter(id, addr, 0, 0).Error(); err != nil {
		return fmt.Errorf("consensus: add voter %s: %w", id, err)
	}
	return nil
}

// Leave removes a node from the cluster. It must be called on the leader.
func (n *Node) Leave(id raft.ServerID) error {
	if n == nil || n.raft == nil {
		return ErrClosed
	}
	if n.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	return n.raft.RemoveServer(id, 0, 0).Error()
}

// apply proposes a command through raft and returns the FSM's response. It
// returns ErrNotLeader when this node is not the leader.
func (n *Node) apply(op string, arg any) (any, error) {
	if n == nil || n.raft == nil {
		return nil, ErrClosed
	}
	if n.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	data, err := encodeCommand(op, arg)
	if err != nil {
		return nil, err
	}
	fut := n.raft.Apply(data, n.cfg.applyTimeout())
	if err := fut.Error(); err != nil {
		return nil, err
	}
	if resp, ok := fut.Response().(error); ok {
		return nil, resp
	}
	return fut.Response(), nil
}

// storeHandle returns the current store, or nil when closed.
func (n *Node) storeHandle() *eidos.Store {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.store
}

// serializeStore writes a consistent snapshot of the eidos-db store.
//
// Backup drains the vector write buffer first, so a point the leader accepted
// but has not yet flushed is part of the snapshot instead of being silently
// dropped when a follower restores it.
func (n *Node) serializeStore() ([]byte, error) {
	st := n.storeHandle()
	if st == nil {
		return nil, ErrClosed
	}
	var buf bytes.Buffer
	if err := st.Backup(context.Background(), &buf); err != nil {
		return nil, fmt.Errorf("consensus: serialize store: %w", err)
	}
	return buf.Bytes(), nil
}

// restoreStore atomically replaces the eidos-db store with the snapshot
// contents and reopens it, rebuilding the in-memory caches from disk.
func (n *Node) restoreStore(data []byte) error {
	// Stage the snapshot first: a failed write leaves the running store alone.
	tmp := n.cfg.StorePath + ".restore"
	if err := os.WriteFile(tmp, data, n.cfg.storeMode()); err != nil {
		return fmt.Errorf("consensus: write restored store: %w", err)
	}

	n.mu.Lock()
	st := n.store
	n.store = nil
	n.mu.Unlock()
	if st != nil {
		_ = st.Close()
	}

	// The handle is gone from here on, so every failure path reopens the file
	// at StorePath instead of leaving the node with no store at all.
	failed := func(err error) error {
		reopened, rerr := eidos.Open(context.Background(), n.cfg.StorePath, n.cfg.storeMode(), n.cfg.StoreOpts)
		if rerr != nil {
			return fmt.Errorf("%w (reopen: %v)", err, rerr)
		}
		n.mu.Lock()
		n.store = reopened
		n.mu.Unlock()
		return err
	}

	if err := os.Rename(tmp, n.cfg.StorePath); err != nil {
		_ = os.Remove(tmp)
		return failed(fmt.Errorf("consensus: replace store: %w", err))
	}

	newStore, err := eidos.Open(context.Background(), n.cfg.StorePath, n.cfg.storeMode(), n.cfg.StoreOpts)
	if err != nil {
		return failed(fmt.Errorf("consensus: reopen restored store: %w", err))
	}

	n.mu.Lock()
	n.store = newStore
	n.mu.Unlock()
	return nil
}
