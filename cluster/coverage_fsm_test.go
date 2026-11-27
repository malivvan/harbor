package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest"
	"github.com/malivvan/tempest/metrics"
	"github.com/malivvan/tempest/store/record"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

// newStoreNode opens a real tempest store and wraps it in a Node without raft,
// so FSM apply/snapshot/restore can be exercised without a cluster.
func newStoreNode(t *testing.T) *Node {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "tempest.db")
	opts := testStoreOpts()
	st, err := tempest.Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	n := &Node{
		cfg:   Config{Dir: dir, StorePath: path, StoreOpts: opts},
		store: st,
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return raw
}

// applyLog encodes op/arg and feeds it to the FSM, returning the raw response.
func applyLog(t *testing.T, f *fsm, index uint64, op string, arg any) any {
	t.Helper()
	data, err := encodeCommand(op, arg)
	require.NoError(t, err)
	return f.Apply(&raft.Log{Index: index, Data: data})
}

// TestFsmApplyAllCommands locks in that every command op dispatches to the
// right store and applies without error against a live store.
func TestFsmApplyAllCommands(t *testing.T) {
	n := newStoreNode(t)
	f := &fsm{node: n}
	name := registerType(reflect.TypeOf(testUser{}))
	user := &testUser{ID: "u1", Name: "alice"}

	// Raw key/value: pre-encode the key and value exactly as the facade would.
	rn := recordNode(n.store, nil)
	kb, err := record.EncodeKey("theme", rn.Codec())
	require.NoError(t, err)
	vb, err := rn.Codec().Marshal("dark")
	require.NoError(t, err)

	var idx uint64
	next := func() uint64 { idx++; return idx }
	ok := func(op string, arg any) {
		require.NoError(t, asError(applyLog(t, f, next(), op, arg)), op)
	}

	// record
	ok(opRecordSave, recordChange{Type: name, Data: mustJSON(t, user)})
	ok(opRecordUpdate, recordChange{Type: name, Data: mustJSON(t, &testUser{ID: "u1", Name: "alice2"})})
	ok(opRecordUpdateField, recordFieldChange{Type: name, Data: mustJSON(t, &testUser{ID: "u1"}), Field: "Name", Value: mustJSON(t, "alice3")})
	ok(opRecordSet, recordKVArg{Bucket: "settings", Key: kb, Value: vb})
	ok(opRecordDeleteKey, recordKVArg{Bucket: "settings", Key: kb})
	ok(opRecordDelete, recordChange{Type: name, Data: mustJSON(t, &testUser{ID: "u1"})})
	ok(opRecordDrop, recordChange{Type: name, Data: mustJSON(t, &testUser{})})

	// series
	now := time.Now().UnixNano()
	ok(opSeriesInsert, seriesInsertArg{Series: "s1", Point: series.Point{Timestamp: 1, Value: 1}, Now: now})
	ok(opSeriesInsertMany, seriesInsertManyArg{Series: "s1", Points: []series.Point{{Timestamp: 2, Value: 2}}, Now: now})
	ok(opSeriesSetLabels, seriesLabelsArg{Series: "s1", Labels: map[string]string{"a": "b"}})
	ok(opSeriesSetRetention, seriesRetentionArg{Series: "s1", Retention: series.Retention{MaxPoints: 5}})
	ok(opSeriesDeleteRange, seriesRangeArg{Series: "s1", Start: 1, End: 1})
	ok(opSeriesDrop, seriesNameArg{Series: "s1"})
	// DeleteSeries needs an existing series, so create one just for it.
	ok(opSeriesInsert, seriesInsertArg{Series: "sdel", Point: series.Point{Timestamp: 1, Value: 1}, Now: now})
	ok(opSeriesDeleteSeries, seriesNameArg{Series: "sdel"})

	// vector
	ok(opVectorCreateCollection, vectorCreateArg{Name: "vc", VectorLen: 2, Metric: vector.Dot})
	ok(opVectorCreateCollectionOptions, vectorCreateOptionsArg{Options: vector.CollectionOptions{Name: "vo", VectorLen: 2, Metric: vector.Dot}})
	ok(opVectorEnsureCollection, vectorNameArg{Name: "ve"})
	ok(opVectorUpsert, vectorUpsertArg{Collection: "vc", Points: []vector.PointStruct{{ID: "a", Vector: []float32{1, 0}}}})
	ok(opVectorFlush, nil)
	ok(opVectorDelete, vectorDeleteArg{Collection: "vc", IDs: []string{"a"}})
	ok(opVectorFlush, nil)
	ok(opVectorDropCollection, vectorDropArg{Name: "vc"})

	// fstree
	at := time.Unix(2000, 0)
	ok(opFileMkdir, filePathArg{File: "data", Name: "/d", Perm: 0o755, Atime: at, Mtime: at})
	ok(opFileMkdirAll, filePathArg{File: "data", Name: "/a/b/c", Perm: 0o755, Atime: at, Mtime: at})
	ok(opFileWrite, filePathArg{File: "data", Name: "/d/f", Perm: 0o644, Atime: at, Mtime: at, Data: []byte("hello")})
	ok(opFileTruncate, filePathArg{File: "data", Name: "/d/f", Size: 2})
	ok(opFileChmod, filePathArg{File: "data", Name: "/d/f", Perm: 0o600})
	ok(opFileChtimes, filePathArg{File: "data", Name: "/d/f", Atime: at, Mtime: at})
	ok(opFileChown, filePathArg{File: "data", Name: "/d/f", Uid: 1, Gid: 1})
	ok(opFileSymlink, filePathArg{File: "data", Name: "/d/f", New: "/link"})
	ok(opFileRemove, filePathArg{File: "data", Name: "/link"})
	ok(opFileRename, filePathArg{File: "data", Name: "/d/f", New: "/d/g"})
	ok(opFileRemoveAll, filePathArg{File: "data", Name: "/a"})

	// Verify a representative effect before tearing the file system down.
	fsys, err := n.Store().File().Open("data")
	require.NoError(t, err)
	info, err := fsys.Stat("/d/g")
	require.NoError(t, err)
	require.Equal(t, int64(2), info.Size())

	ok(opFileDeleteFileSystem, filePathArg{File: "data"})
}

// TestFsmApplyRejectsBadArgs locks in that malformed arguments and unknown
// record types are reported as ApplyError instead of panicking.
func TestFsmApplyRejectsBadArgs(t *testing.T) {
	n := newStoreNode(t)
	f := &fsm{node: n}

	// A record command naming an unregistered type.
	resp := applyLog(t, f, 1, opRecordSave, recordChange{Type: "consensus.ghost", Data: json.RawMessage(`{}`)})
	require.Error(t, resp.(error))

	// UpdateField for a field that does not exist on the type.
	registerType(reflect.TypeOf(testUser{}))
	resp = applyLog(t, f, 2, opRecordUpdateField, recordFieldChange{
		Type:  reflect.TypeOf(testUser{}).String(),
		Data:  mustJSON(t, &testUser{ID: "u1"}),
		Field: "Nope",
		Value: mustJSON(t, "x"),
	})
	require.Error(t, resp.(error))

	// A malformed argument for a handful of op families.
	for i, tc := range []struct {
		op  string
		raw string
	}{
		{opRecordSave, `{"type":42}`},
		{opSeriesInsert, `{"series":42}`},
		{opVectorUpsert, `{"collection":42}`},
		{opFileMkdir, `{"file":42}`},
	} {
		data := mustJSON(t, Command{Op: tc.op, Arg: json.RawMessage(tc.raw)})
		resp := f.Apply(&raft.Log{Index: uint64(10 + i), Data: data})
		require.Error(t, resp.(error), tc.op)
	}
}

// TestFsmApplyClosedStore locks in that every apply helper reports ErrClosed
// when the node has no store.
func TestFsmApplyClosedStore(t *testing.T) {
	n := &Node{} // no store
	f := &fsm{node: n}

	for _, tc := range []struct {
		op  string
		arg any
	}{
		{opRecordSave, recordChange{Type: "x", Data: json.RawMessage(`{}`)}},
		{opSeriesInsert, seriesInsertArg{Series: "s"}},
		{opVectorCreateCollection, vectorCreateArg{Name: "v", VectorLen: 1}},
		{opFileMkdir, filePathArg{File: "data", Name: "/d"}},
		{opFileDeleteFileSystem, filePathArg{File: "data"}},
		{opVectorFlush, nil},
	} {
		resp := applyLog(t, f, 1, tc.op, tc.arg)
		require.ErrorIs(t, asError(resp), ErrClosed, tc.op)
	}
}

// countingObserver records consensus apply counters so the instrumented path of
// observeApply can be asserted.
type countingObserver struct {
	ok, fail int
}

func (o *countingObserver) Counter(name string, delta int64, l metrics.Labels) {
	if name != metrics.MetricClusterApplyTotal {
		return
	}
	if l[metrics.LabelResult] == metrics.ResultOK {
		o.ok++
	} else {
		o.fail++
	}
}
func (o *countingObserver) Histogram(string, float64, metrics.Labels) {}
func (o *countingObserver) Gauge(string, float64, metrics.Labels)     {}
func (o *countingObserver) StartSpan(ctx context.Context, _ string, _ metrics.Labels) (context.Context, func(error)) {
	return ctx, func(error) {}
}

// TestFsmApplyReportsMetrics locks in that an instrumented node records both the
// success and the error result of an applied entry.
func TestFsmApplyReportsMetrics(t *testing.T) {
	n := newStoreNode(t)
	obs := &countingObserver{}
	n.obs = obs
	f := &fsm{node: n}

	registerType(reflect.TypeOf(testUser{}))
	applyLog(t, f, 1, opRecordSave, recordChange{
		Type: reflect.TypeOf(testUser{}).String(),
		Data: mustJSON(t, &testUser{ID: "u1", Name: "a"}),
	})
	applyLog(t, f, 2, opRecordSave, recordChange{Type: "consensus.ghost", Data: json.RawMessage(`{}`)})

	require.Equal(t, 1, obs.ok)
	require.Equal(t, 1, obs.fail)
}

// TestFsmSnapshotRestoreRoundTrip locks in that Snapshot serializes the store and
// Restore replaces it, dropping writes made after the snapshot was taken.
func TestFsmSnapshotRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	n := newStoreNode(t)
	f := &fsm{node: n}
	name := registerType(reflect.TypeOf(testUser{}))

	require.NoError(t, asError(applyLog(t, f, 1, opRecordSave, recordChange{Type: name, Data: mustJSON(t, &testUser{ID: "keep", Name: "kept"})})))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	data := snap.(*fsmSnapshot).data
	require.NotEmpty(t, data)

	// A second write after the snapshot must be gone after restore.
	require.NoError(t, asError(applyLog(t, f, 2, opRecordSave, recordChange{Type: name, Data: mustJSON(t, &testUser{ID: "drop", Name: "gone"})})))

	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(data))))

	var got testUser
	require.NoError(t, n.Store().Record().One(ctx, "ID", "keep", &got), "restored record missing")
	require.Equal(t, "kept", got.Name)
	require.Error(t, n.Store().Record().One(ctx, "ID", "drop", &got), "post-snapshot write survived restore")
}

// TestFsmRecordDecodeErrors locks in that a registered type with a bad payload,
// and an UpdateField value that does not fit the field, are reported as errors.
func TestFsmRecordDecodeErrors(t *testing.T) {
	n := newStoreNode(t)
	f := &fsm{node: n}
	name := registerType(reflect.TypeOf(testUser{}))

	// Type is registered but the payload is not an object.
	resp := applyLog(t, f, 1, opRecordSave, recordChange{Type: name, Data: json.RawMessage(`"notanobject"`)})
	require.Error(t, resp.(error))

	// UpdateField value cannot unmarshal into a string field.
	resp = applyLog(t, f, 2, opRecordUpdateField, recordFieldChange{
		Type:  name,
		Data:  mustJSON(t, &testUser{ID: "u1"}),
		Field: "Name",
		Value: json.RawMessage(`123`),
	})
	require.Error(t, resp.(error))
}

// TestFsmFileErrorPaths locks in the fstree error branches: creating a directory
// tree below an existing file (ENOTDIR) and writing below a file fail cleanly.
func TestFsmFileErrorPaths(t *testing.T) {
	n := newStoreNode(t)
	f := &fsm{node: n}
	at := time.Unix(1000, 0)

	require.NoError(t, asError(applyLog(t, f, 1, opFileWrite, filePathArg{
		File: "data", Name: "/f", Perm: 0o644, Atime: at, Mtime: at, Data: []byte("x"),
	})))

	// A path component that is a file cannot be traversed to make a directory.
	resp := applyLog(t, f, 2, opFileMkdirAll, filePathArg{
		File: "data", Name: "/f/sub", Perm: 0o755, Atime: at, Mtime: at,
	})
	require.Error(t, resp.(error))

	// Opening a file below a plain file fails inside the store.
	resp = applyLog(t, f, 3, opFileWrite, filePathArg{
		File: "data", Name: "/f/sub/x", Perm: 0o644, Atime: at, Mtime: at, Data: []byte("y"),
	})
	require.Error(t, resp.(error))
}
