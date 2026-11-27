package cluster

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest"
	bolt "go.etcd.io/bbolt"
)

// TestApplyErrorMessageAndUnwrap locks in the two Error layouts (with and
// without an op) and that Unwrap exposes the cause for errors.Is/As.
func TestApplyErrorMessageAndUnwrap(t *testing.T) {
	cause := errors.New("boom")

	noOp := &ApplyError{Index: 5, Err: cause}
	require.Contains(t, noOp.Error(), "apply log index 5")
	require.NotContains(t, noOp.Error(), "op ")
	require.ErrorIs(t, noOp, cause)

	withOp := &ApplyError{Index: 6, Op: "x.y", Err: cause}
	require.Contains(t, withOp.Error(), `op "x.y"`)
	require.Same(t, cause, withOp.Unwrap())
}

// TestEncodeCommandError locks in that an unencodable argument surfaces a
// wrapped error instead of a panic.
func TestEncodeCommandError(t *testing.T) {
	_, err := encodeCommand("op", make(chan int))
	require.Error(t, err)
	require.Contains(t, err.Error(), "encode op argument")
}

// TestDecodeCommandErrors locks in that missing and malformed payloads are
// rejected with a descriptive error.
func TestDecodeCommandErrors(t *testing.T) {
	_, err := decodeCommand([]byte(`{}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing op")

	_, err = decodeCommand([]byte("not json"))
	require.Error(t, err)
}

// TestConfigValidateErrors locks in that every required Config field is
// enforced with ErrInvalidConfig.
func TestConfigValidateErrors(t *testing.T) {
	good := Config{
		NodeID:    "node-1",
		BindAddr:  "127.0.0.1:1",
		Dir:       "dir",
		StorePath: "store.db",
		StoreOpts: &tempest.Options{},
	}
	require.NoError(t, good.validate())

	mutations := map[string]func(*Config){
		"missing NodeID":    func(c *Config) { c.NodeID = "" },
		"missing BindAddr":  func(c *Config) { c.BindAddr = "" },
		"missing Dir":       func(c *Config) { c.Dir = "" },
		"missing StorePath": func(c *Config) { c.StorePath = "" },
		"missing StoreOpts": func(c *Config) { c.StoreOpts = nil },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			c := good
			mutate(&c)
			require.ErrorIs(t, c.validate(), ErrInvalidConfig)
		})
	}
}

// TestConfigDefaultsAndOverrides locks in the zero-value defaults and the
// explicit overrides for ApplyTimeout and StoreMode.
func TestConfigDefaultsAndOverrides(t *testing.T) {
	var c Config
	require.Equal(t, DefaultApplyTimeout, c.applyTimeout())
	require.Equal(t, os.FileMode(0o600), c.storeMode())

	c.ApplyTimeout = 3 * time.Second
	c.StoreMode = 0o644
	require.Equal(t, 3*time.Second, c.applyTimeout())
	require.Equal(t, os.FileMode(0o644), c.storeMode())
}

// fakeSink is a raft.SnapshotSink that can be told to fail on Write.
type fakeSink struct {
	writeErr error
	canceled bool
	closed   bool
	data     []byte
}

func (s *fakeSink) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	s.data = append(s.data, p...)
	return len(p), nil
}
func (s *fakeSink) Close() error  { s.closed = true; return nil }
func (s *fakeSink) ID() string    { return "snap" }
func (s *fakeSink) Cancel() error { s.canceled = true; return nil }

// TestFsmSnapshotPersistAndRelease locks in that Persist writes then closes a
// healthy sink, cancels a failing one, and that Release is a safe no-op.
func TestFsmSnapshotPersistAndRelease(t *testing.T) {
	snap := &fsmSnapshot{data: []byte("abc")}

	ok := &fakeSink{}
	require.NoError(t, snap.Persist(ok))
	require.True(t, ok.closed)
	require.Equal(t, []byte("abc"), ok.data)

	bad := &fakeSink{writeErr: errors.New("disk full")}
	require.Error(t, snap.Persist(bad))
	require.True(t, bad.canceled)
	require.False(t, bad.closed)

	snap.Release()
}

// TestBoltStoreNilSafetyAndSync locks in the nil-receiver guards on Close/Sync
// and that Sync fsyncs a live store.
func TestBoltStoreNilSafetyAndSync(t *testing.T) {
	var nilStore *boltStore
	require.NoError(t, nilStore.Close())
	require.NoError(t, nilStore.Sync())

	empty := &boltStore{}
	require.NoError(t, empty.Close())
	require.NoError(t, empty.Sync())

	s := openTestStore(t)
	require.NoError(t, s.Sync())
}

// TestBoltStoreDecodeError locks in that a corrupt log entry stored on disk
// surfaces as a decode error rather than being silently dropped.
func TestBoltStoreDecodeError(t *testing.T) {
	s := openTestStore(t)

	// 0xc1 is never produced by msgpack, so decoding is guaranteed to fail.
	require.NoError(t, s.conn.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(raftLogsBucket).Put(raftIndexKey(10), []byte{0xc1})
	}))

	var got raft.Log
	err := s.GetLog(10, &got)
	require.Error(t, err)
	require.NotErrorIs(t, err, raft.ErrLogNotFound)
	require.Contains(t, err.Error(), "decode raft log")
}

// TestBoltStoreOpenError locks in that opening a directory as the raft store
// fails instead of silently succeeding.
func TestBoltStoreOpenError(t *testing.T) {
	_, err := openBoltStore(t.TempDir())
	require.Error(t, err)
}

// TestNodeNilSafety locks in that every nil-receiver method degrades safely
// instead of panicking.
func TestNodeNilSafety(t *testing.T) {
	var n *Node

	require.Nil(t, n.Store())
	addr, id := n.Leader()
	require.Empty(t, addr)
	require.Empty(t, id)
	require.Equal(t, raft.Shutdown, n.State())
	require.Nil(t, n.Stats())
	require.Equal(t, uint64(0), n.AppliedIndex())
	require.ErrorIs(t, n.Barrier(0), ErrClosed)
	require.ErrorIs(t, n.Snapshot(), ErrClosed)
	require.ErrorIs(t, n.Join("x", "y"), ErrClosed)
	require.ErrorIs(t, n.Leave("x"), ErrClosed)
	require.NoError(t, n.Close())

	require.Nil(t, n.Fstree())
	require.Nil(t, n.Record())
	require.Nil(t, n.Series())
	require.Nil(t, n.Vector())

	// logf and SetNow must tolerate a nil receiver.
	n.logf("ignored %d", 1)
	n.SetNow(nil)
}

// TestNodeLogfOutput locks in that logf writes to the configured sink and
// discards when none is set.
func TestNodeLogfOutput(t *testing.T) {
	var buf bytes.Buffer
	n := &Node{cfg: Config{LogOutput: &buf}}
	n.logf("value %d", 7)
	require.Contains(t, buf.String(), "value 7")

	// A node without a LogOutput must not panic.
	(&Node{}).logf("dropped")
}

// TestNodeSetNowAndClock locks in the clock override behaviour used for
// deterministic replicated timestamps.
func TestNodeSetNowAndClock(t *testing.T) {
	n := &Node{}
	n.SetNow(func() int64 { return 12345 })
	require.Equal(t, int64(12345), n.nowNanos())

	n.SetNow(nil)
	require.NotZero(t, n.nowNanos())

	// A nil node falls back to the wall clock without panicking.
	var nilNode *Node
	require.NotZero(t, nilNode.nowNanos())
}

// TestOpenConfigAndSetupErrors locks in every early failure path of Open so a
// misconfigured node never starts raft.
func TestOpenConfigAndSetupErrors(t *testing.T) {
	t.Run("invalid config", func(t *testing.T) {
		_, err := Open(context.Background(), Config{})
		require.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("cancelled context", func(t *testing.T) {
		cfg := testConfig("node-1", freeAddr(t), true, t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := Open(ctx, cfg)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("nil context and bad dir", func(t *testing.T) {
		// A file used as the data directory makes MkdirAll fail after the nil
		// context is normalised to Background.
		base := t.TempDir()
		blocker := filepath.Join(base, "blocker")
		require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))
		cfg := testConfig("node-1", freeAddr(t), true, t)
		cfg.Dir = filepath.Join(blocker, "sub")
		//lint:ignore SA1012 nil ctx is the path under test
		_, err := Open(nil, cfg)
		require.Error(t, err)
	})

	t.Run("store open fails", func(t *testing.T) {
		cfg := testConfig("node-1", freeAddr(t), true, t)
		cfg.StorePath = cfg.Dir // a directory cannot be opened as the store file
		_, err := Open(context.Background(), cfg)
		require.Error(t, err)
	})

	t.Run("bad bind address", func(t *testing.T) {
		cfg := testConfig("node-1", "127.0.0.1:notaport", true, t)
		_, err := Open(context.Background(), cfg)
		require.Error(t, err)
	})

	t.Run("bind address in use", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l.Close()

		cfg := testConfig("node-1", l.Addr().String(), true, t)
		_, err = Open(context.Background(), cfg)
		require.Error(t, err)
	})
}
