package consensus

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// openTestStore opens a raft store in a temporary directory.
func openTestStore(t *testing.T) *boltStore {
	t.Helper()
	s, err := openBoltStore(filepath.Join(t.TempDir(), "raft.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestBoltStoreLogRoundTrip(t *testing.T) {
	s := openTestStore(t)

	// An empty log reports index 0 at both ends.
	first, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), first)
	last, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), last)

	appended := time.Date(2026, 5, 4, 3, 2, 1, 0, time.UTC)
	require.NoError(t, s.StoreLogs([]*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("one"), AppendedAt: appended},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("two")},
		{Index: 5, Term: 2, Type: raft.LogNoop},
	}))

	first, err = s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	last, err = s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)

	var got raft.Log
	require.NoError(t, s.GetLog(1, &got))
	require.Equal(t, uint64(1), got.Index)
	require.Equal(t, uint64(1), got.Term)
	require.Equal(t, raft.LogCommand, got.Type)
	require.Equal(t, []byte("one"), got.Data)
	require.True(t, appended.Equal(got.AppendedAt))

	// A missing index reports raft's sentinel, not a decode failure.
	err = s.GetLog(3, &got)
	require.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestBoltStoreDeleteRange(t *testing.T) {
	s := openTestStore(t)

	logs := make([]*raft.Log, 0, 6)
	for i := uint64(1); i <= 6; i++ {
		logs = append(logs, &raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: []byte{byte(i)}})
	}
	require.NoError(t, s.StoreLogs(logs))

	// The range is inclusive on both ends.
	require.NoError(t, s.DeleteRange(2, 4))

	first, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	last, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), last)

	var got raft.Log
	for _, idx := range []uint64{2, 3, 4} {
		require.ErrorIs(t, s.GetLog(idx, &got), raft.ErrLogNotFound)
	}
	for _, idx := range []uint64{1, 5, 6} {
		require.NoError(t, s.GetLog(idx, &got))
		require.Equal(t, idx, got.Index)
	}

	// Deleting everything leaves an empty log behind.
	require.NoError(t, s.DeleteRange(1, 6))
	last, err = s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), last)
}

func TestBoltStoreStableStore(t *testing.T) {
	s := openTestStore(t)

	require.NoError(t, s.Set([]byte("key"), []byte("value")))
	got, err := s.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), got)

	// Get returns a copy: mutating it must not write through to the file.
	got[0] = 'V'
	again, err := s.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), again)

	require.NoError(t, s.SetUint64([]byte("term"), 42))
	term, err := s.GetUint64([]byte("term"))
	require.NoError(t, err)
	require.Equal(t, uint64(42), term)

	// raft detects a missing stable key by comparing the error text exactly.
	_, err = s.GetUint64([]byte("missing"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())
	_, err = s.Get([]byte("missing"))
	require.Error(t, err)
	require.Equal(t, "not found", err.Error())

	require.NoError(t, s.Set([]byte("short"), []byte{1, 2, 3}))
	_, err = s.GetUint64([]byte("short"))
	require.Error(t, err)
	require.NotEqual(t, "not found", err.Error())
}

func TestBoltStorePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")

	s, err := openBoltStore(path)
	require.NoError(t, err)
	require.NoError(t, s.StoreLog(&raft.Log{Index: 7, Term: 3, Type: raft.LogCommand, Data: []byte("payload")}))
	require.NoError(t, s.SetUint64([]byte("CurrentTerm"), 3))
	require.NoError(t, s.Close())

	reopened, err := openBoltStore(path)
	require.NoError(t, err)
	defer reopened.Close()

	var got raft.Log
	require.NoError(t, reopened.GetLog(7, &got))
	require.Equal(t, []byte("payload"), got.Data)
	term, err := reopened.GetUint64([]byte("CurrentTerm"))
	require.NoError(t, err)
	require.Equal(t, uint64(3), term)
	require.Equal(t, path, reopened.Path())
}

// TestBoltStoreReadsLegacyTimeEncoding ensures an entry written with the
// extension-based timestamp encoding that raft-boltdb used by default still
// decodes, so an existing raft.db stays readable.
func TestBoltStoreReadsLegacyTimeEncoding(t *testing.T) {
	s := openTestStore(t)

	appended := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	buf := bytes.NewBuffer(nil)
	handle := codec.MsgpackHandle{BasicHandle: codec.BasicHandle{TimeNotBuiltin: true}}
	require.NoError(t, codec.NewEncoder(buf, &handle).Encode(&raft.Log{
		Index:      9,
		Term:       2,
		Type:       raft.LogCommand,
		Data:       []byte("legacy"),
		AppendedAt: appended,
	}))

	require.NoError(t, s.conn.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(raftLogsBucket).Put(raftIndexKey(9), buf.Bytes())
	}))

	var got raft.Log
	require.NoError(t, s.GetLog(9, &got))
	require.Equal(t, uint64(9), got.Index)
	require.Equal(t, []byte("legacy"), got.Data)
	require.True(t, appended.Equal(got.AppendedAt))
}
