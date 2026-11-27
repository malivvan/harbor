package tempest

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/record/q"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

// exportTestRecord is the db record shape used by the export/import tests.
type exportTestRecord struct {
	ID   int    `db:"increment"`
	Name string `db:"index"`
}

// seedExportStore writes representative data into all four stores.
func seedExportStore(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, store.Record().Save(ctx, &exportTestRecord{Name: "alice"}))
	require.NoError(t, store.Record().Save(ctx, &exportTestRecord{Name: "bob"}))

	vfs, err := store.File().Open("docs")
	require.NoError(t, err)
	require.NoError(t, vfs.MkdirAll("/reports/2026", 0o750))
	file, err := vfs.OpenFile("/reports/2026/q1.txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	require.NoError(t, err)
	_, err = file.Write([]byte("first quarter"))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, vfs.Symlink("/reports/2026/q1.txt", "/reports/latest"))

	cpu := store.Series().Series("cpu")
	require.NotNil(t, cpu)
	require.NoError(t, cpu.InsertMany(ctx, []series.Point{
		{Timestamp: 1, Value: 1.5},
		{Timestamp: 2, Value: 2.5},
	}))
	require.NoError(t, cpu.SetLabels(ctx, map[string]string{"host": "a"}))
	require.NoError(t, cpu.SetRetention(ctx, series.Retention{MaxPoints: 100}))

	col, err := store.Vector().NewCollection("colors", 2, vector.Dot, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{{
		ID:      "red",
		Vector:  []float32{1, 0},
		Payload: vector.Payload{"hex": "#f00", "shade": 1},
	}}))
	require.NoError(t, col.Flush())
}

func verifyExportStore(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	var rec exportTestRecord
	require.NoError(t, store.Record().Select(q.Eq("Name", "alice")).First(ctx, &rec))

	vfs, err := store.File().Open("docs")
	require.NoError(t, err)
	data, err := vfs.ReadFile("/reports/2026/q1.txt")
	require.NoError(t, err)
	require.Equal(t, "first quarter", string(data))
	info, err := vfs.Stat("/reports/2026/q1.txt")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
	target, err := vfs.Readlink("/reports/latest")
	require.NoError(t, err)
	require.Equal(t, "/reports/2026/q1.txt", target)

	series := store.Series().Series("cpu")
	points, err := series.Range(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, points, 2)
	labels, err := series.Labels(ctx)
	require.NoError(t, err)
	require.Equal(t, "a", labels["host"])

	infos, err := store.Series().ListSeries(ctx)
	require.NoError(t, err)
	found := false
	for _, seriesInfo := range infos {
		if seriesInfo.Name == "cpu" {
			require.Equal(t, 100, seriesInfo.Retention.MaxPoints)
			found = true
		}
	}
	require.True(t, found, "series metadata missing")

	loaded, err := store.Vector().LoadCollection("colors")
	require.NoError(t, err)
	point, ok := loaded["red"]
	require.True(t, ok)
	require.Equal(t, []float32{1, 0}, point.Vector)
	require.Equal(t, int64(1), point.Payload["shade"], "payload integers survive the round trip")
	require.Equal(t, "#f00", point.Payload["hex"])
}

// exportTestData seeds a store and returns its export bytes.
func exportTestData(t *testing.T) []byte {
	t.Helper()
	ctx := context.Background()

	src := mustOpen(t, filepath.Join(t.TempDir(), "export.db"), "pw")
	seedExportStore(t, src)

	var buf bytes.Buffer
	_, err := Export(ctx, src, &buf, ExportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	require.NoError(t, src.Close())
	return buf.Bytes()
}

// TestExportImportMigratesEncryption exports a plaintext store and imports it
// into an encrypted one: the logical format is the migration path between
// encryption modes.
func TestExportImportMigratesEncryption(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	srcOpts := testOptions("pw")
	srcOpts.Plaintext = true
	src, err := Open(ctx, filepath.Join(dir, "source.db"), 0o600, srcOpts)
	require.NoError(t, err)
	seedExportStore(t, src)

	var buf bytes.Buffer
	summary, err := Export(ctx, src, &buf, ExportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	require.Greater(t, summary.Records, int64(0))
	require.NotEmpty(t, summary.Checksum)
	require.NoError(t, src.Close())

	dst := mustOpen(t, filepath.Join(dir, "target.db"), "pw")
	defer func() { require.NoError(t, dst.Close()) }()

	imported, err := Import(ctx, dst, bytes.NewReader(buf.Bytes()), ImportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	require.Equal(t, summary.Records, imported.Records)
	require.Equal(t, exportVersion, imported.Version)
	require.NotEqual(t, EncryptNone, dst.Flags(), "the destination keeps its own encryption")
	verifyExportStore(t, dst)
}

// TestExportIsDeterministic guards reproducible containers: map and record
// ordering must not leak into the output.
func TestExportIsDeterministic(t *testing.T) {
	ctx := context.Background()
	src := mustOpen(t, filepath.Join(t.TempDir(), "source.db"), "pw")
	defer func() { require.NoError(t, src.Close()) }()
	seedExportStore(t, src)

	var first, second bytes.Buffer
	_, err := Export(ctx, src, &first, ExportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	_, err = Export(ctx, src, &second, ExportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	require.True(t, bytes.Equal(first.Bytes(), second.Bytes()), "export must be deterministic")
}

func TestExportImportProgress(t *testing.T) {
	ctx := context.Background()
	src := mustOpen(t, filepath.Join(t.TempDir(), "source.db"), "pw")
	seedExportStore(t, src)

	var exportCalls []Progress
	var buf bytes.Buffer
	summary, err := Export(ctx, src, &buf, ExportOptions{
		RecordTypes: []any{&exportTestRecord{}},
		Progress:    func(p Progress) { exportCalls = append(exportCalls, p) },
	})
	require.NoError(t, err)
	require.Len(t, exportCalls, int(summary.Records))
	require.Equal(t, summary.Records, exportCalls[len(exportCalls)-1].Records)
	require.NoError(t, src.Close())

	dst := mustOpen(t, filepath.Join(t.TempDir(), "target.db"), "pw")
	defer func() { require.NoError(t, dst.Close()) }()

	var importCalls []Progress
	_, err = Import(ctx, dst, bytes.NewReader(buf.Bytes()), ImportOptions{
		RecordTypes: []any{&exportTestRecord{}},
		Progress:    func(p Progress) { importCalls = append(importCalls, p) },
	})
	require.NoError(t, err)
	require.Len(t, importCalls, len(exportCalls))
}

func TestImportDryRunValidatesWithoutWriting(t *testing.T) {
	ctx := context.Background()
	data := exportTestData(t)

	dst := mustOpen(t, filepath.Join(t.TempDir(), "target.db"), "pw")
	defer func() { require.NoError(t, dst.Close()) }()

	summary, err := Import(ctx, dst, bytes.NewReader(data), ImportOptions{
		RecordTypes: []any{&exportTestRecord{}},
		DryRun:      true,
	})
	require.NoError(t, err)
	require.True(t, summary.DryRun)
	require.Greater(t, summary.Records, int64(0))

	count, err := dst.Record().Select().Count(ctx, &exportTestRecord{})
	require.NoError(t, err)
	require.Zero(t, count, "a dry run must not write")

	// A dry run does not need a destination.
	_, err = Import(ctx, nil, bytes.NewReader(data), ImportOptions{
		RecordTypes: []any{&exportTestRecord{}},
		DryRun:      true,
	})
	require.NoError(t, err)

	// Unregistered db record types are rejected instead of skipped.
	_, err = Import(ctx, nil, bytes.NewReader(data), ImportOptions{DryRun: true})
	require.ErrorIs(t, err, ErrInvalidOptions)
}

func TestImportRejectsCorruptExport(t *testing.T) {
	data := exportTestData(t)
	corrupt := bytes.Replace(data, []byte(`"container":"cpu"`), []byte(`"container":"cpv"`), 1)
	require.False(t, bytes.Equal(data, corrupt), "test fixture must contain the tampered field")

	_, err := Import(context.Background(), nil, bytes.NewReader(corrupt), ImportOptions{
		RecordTypes: []any{&exportTestRecord{}},
		DryRun:      true,
	})
	require.ErrorIs(t, err, ErrExportChecksum)
}

func TestImportRejectsIncompatibleVersion(t *testing.T) {
	data := exportTestData(t)
	lines := bytes.SplitN(data, []byte("\n"), 2)
	require.Len(t, lines, 2)

	header := bytes.Replace(lines[0], []byte(`"version":1`), []byte(`"version":2`), 1)
	require.NotEqual(t, lines[0], header)
	mutated := append(append([]byte{}, header...), '\n')
	mutated = append(mutated, lines[1]...)

	_, err := Import(context.Background(), nil, bytes.NewReader(mutated), ImportOptions{
		RecordTypes: []any{&exportTestRecord{}},
		DryRun:      true,
	})
	require.ErrorIs(t, err, ErrExportVersion)
}

func TestImportRejectsTruncatedExport(t *testing.T) {
	data := exportTestData(t)
	trimmed := bytes.TrimRight(data, "\n")
	footerStart := bytes.LastIndexByte(trimmed, '\n') + 1
	require.Positive(t, footerStart)

	_, err := Import(context.Background(), nil, bytes.NewReader(data[:footerStart]), ImportOptions{
		RecordTypes: []any{&exportTestRecord{}},
		DryRun:      true,
	})
	require.ErrorIs(t, err, ErrExportTruncated)
}

func TestExportImportArgumentErrors(t *testing.T) {
	ctx := context.Background()
	src := mustOpen(t, filepath.Join(t.TempDir(), "source.db"), "pw")

	_, err := Export(ctx, src, nil, ExportOptions{})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.NoError(t, src.Close())
	_, err = Export(ctx, src, &bytes.Buffer{}, ExportOptions{})
	require.ErrorIs(t, err, ErrClosed)

	_, err = Import(ctx, nil, nil, ImportOptions{})
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = Import(ctx, nil, bytes.NewReader(nil), ImportOptions{})
	require.ErrorIs(t, err, ErrClosed)
}

func TestCompactTo(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src := mustOpen(t, filepath.Join(dir, "source.db"), "pw")
	seedExportStore(t, src)

	// Delete a record so the compacted fstree has less live data to carry.
	require.NoError(t, src.Record().DeleteStruct(ctx, &exportTestRecord{ID: 2}))

	compacted := filepath.Join(dir, "compacted.db")
	require.NoError(t, src.CompactTo(ctx, compacted, 0o600))
	require.ErrorIs(t, src.CompactTo(ctx, compacted, 0o600), os.ErrExist)
	require.ErrorIs(t, src.CompactTo(ctx, "", 0o600), ErrInvalidOptions)

	restored := mustOpen(t, compacted, "pw")
	defer func() { require.NoError(t, restored.Close()) }()
	verifyExportStore(t, restored)

	var gone exportTestRecord
	require.Error(t, restored.Record().Select(q.Eq("Name", "bob")).First(ctx, &gone))

	srcInfo, err := os.Stat(filepath.Join(dir, "source.db"))
	require.NoError(t, err)
	dstInfo, err := os.Stat(compacted)
	require.NoError(t, err)
	require.LessOrEqual(t, dstInfo.Size(), srcInfo.Size())

	require.NoError(t, src.Close())
	require.ErrorIs(t, src.CompactTo(ctx, filepath.Join(dir, "closed.db"), 0o600), ErrClosed)
}

// testCustomTokenizer is a caller-supplied tokenizer: it cannot be
// reconstructed from its persisted name, so import needs it explicitly.
type testCustomTokenizer struct{}

func (testCustomTokenizer) Tokenize(text string) []string { return strings.Fields(text) }

func seedLogicalStores(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	words, err := store.Search().Open("words", search.Config{})
	require.NoError(t, err)
	require.NoError(t, words.Upsert(ctx, "d1", "alpha beta"))
	require.NoError(t, words.Upsert(ctx, "d2", "beta gamma"))

	cjk, err := store.Search().Open("cjk", search.Config{Tokenizer: search.WordsWithCJK()})
	require.NoError(t, err)
	require.NoError(t, cjk.Upsert(ctx, "c1", "世界 和平"))

	g, err := store.Graphs().Open("social")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person", Content: "alice"}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "b", Type: "person", Content: "bob"}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "e1", From: "a", To: "b", Type: "knows"}))

	_, err = store.Objects().Put(ctx, "tenant-a", "reports/q3.pdf",
		strings.NewReader("quarter three"), 13, "application/pdf")
	require.NoError(t, err)
	_, err = store.Objects().Put(ctx, "tenant-b", "notes.txt",
		strings.NewReader("hello"), 5, "text/plain")
	require.NoError(t, err)
}

func verifyLogicalStores(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	words, err := store.Search().Open("words", search.Config{})
	require.NoError(t, err)
	count, err := words.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	hits, err := words.Search(ctx, "beta", 0)
	require.NoError(t, err)
	require.Len(t, hits, 2)

	cjk, err := store.Search().Open("cjk", search.Config{Tokenizer: search.WordsWithCJK()})
	require.NoError(t, err)
	hits, err = cjk.Search(ctx, "世界", 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "c1", hits[0].ID)

	g, err := store.Graphs().Open("social")
	require.NoError(t, err)
	nodes, err := g.GetAllNodes(ctx, nil)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	edges, err := g.GetAllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, "knows", edges[0].Type)

	data, err := store.Objects().ReadAll(ctx, "tenant-a", "reports/q3.pdf")
	require.NoError(t, err)
	require.Equal(t, "quarter three", string(data))
	data, err = store.Objects().ReadAll(ctx, "tenant-b", "notes.txt")
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	buckets, err := store.Objects().Buckets(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-a", "tenant-b"}, buckets)
}

func TestExportImportSearch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	srcOpts := testOptions("pw")
	srcOpts.Plaintext = true
	src, err := Open(ctx, filepath.Join(dir, "source.db"), 0o600, srcOpts)
	require.NoError(t, err)
	seedLogicalStores(t, src)

	var buf bytes.Buffer
	_, err = Export(ctx, src, &buf, ExportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	dst := mustOpen(t, filepath.Join(dir, "target.db"), "pw")
	defer func() { require.NoError(t, dst.Close()) }()
	_, err = Import(ctx, dst, bytes.NewReader(buf.Bytes()), ImportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)

	verifyLogicalStores(t, dst)
}

func TestExportImportGraphsLiveOnly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := mustOpen(t, filepath.Join(dir, "source.db"), "pw")

	g, err := src.Graphs().Open("social")
	require.NoError(t, err)
	// Two upserts close a version of node "a"; only the live record is exported.
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person", Content: "alice"}))
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "person", Content: "alice v2"}))
	require.NoError(t, g.UpsertEdge(ctx, &graphs.Edge{ID: "e1", From: "a", To: "a", Type: "self"}))

	var buf bytes.Buffer
	_, err = Export(ctx, src, &buf, ExportOptions{})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	dst := mustOpen(t, filepath.Join(dir, "target.db"), "pw")
	defer func() { require.NoError(t, dst.Close()) }()
	_, err = Import(ctx, dst, bytes.NewReader(buf.Bytes()), ImportOptions{})
	require.NoError(t, err)

	imported, err := dst.Graphs().Open("social")
	require.NoError(t, err)
	node, err := imported.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "alice v2", node.Content)
	edges, err := imported.GetAllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
}

func TestExportImportObjects(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := mustOpen(t, filepath.Join(dir, "source.db"), "pw")

	_, err := src.Objects().Put(ctx, "tenant", "a/b.bin", strings.NewReader("payload"), 7, "")
	require.NoError(t, err)
	info, ok, err := src.Objects().Stat(ctx, "tenant", "a/b.bin")
	require.NoError(t, err)
	require.True(t, ok)

	var buf bytes.Buffer
	_, err = Export(ctx, src, &buf, ExportOptions{})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	dst := mustOpen(t, filepath.Join(dir, "target.db"), "pw")
	defer func() { require.NoError(t, dst.Close()) }()
	_, err = Import(ctx, dst, bytes.NewReader(buf.Bytes()), ImportOptions{})
	require.NoError(t, err)

	got, ok, err := dst.Objects().Stat(ctx, "tenant", "a/b.bin")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, info.ETag, got.ETag)
	data, err := dst.Objects().ReadAll(ctx, "tenant", "a/b.bin")
	require.NoError(t, err)
	require.Equal(t, "payload", string(data))
}

func TestExportImportAllStores(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// plaintext -> encrypted
	srcOpts := testOptions("pw")
	srcOpts.Plaintext = true
	src, err := Open(ctx, filepath.Join(dir, "source.db"), 0o600, srcOpts)
	require.NoError(t, err)
	seedExportStore(t, src)
	seedLogicalStores(t, src)

	var buf bytes.Buffer
	_, err = Export(ctx, src, &buf, ExportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	encrypted := mustOpen(t, filepath.Join(dir, "encrypted.db"), "pw")
	_, err = Import(ctx, encrypted, bytes.NewReader(buf.Bytes()), ImportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	verifyExportStore(t, encrypted)
	verifyLogicalStores(t, encrypted)

	// encrypted -> plaintext
	var back bytes.Buffer
	_, err = Export(ctx, encrypted, &back, ExportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	require.NoError(t, encrypted.Close())

	plainOpts := testOptions("pw")
	plainOpts.Plaintext = true
	plain, err := Open(ctx, filepath.Join(dir, "plain.db"), 0o600, plainOpts)
	require.NoError(t, err)
	defer func() { require.NoError(t, plain.Close()) }()
	_, err = Import(ctx, plain, bytes.NewReader(back.Bytes()), ImportOptions{RecordTypes: []any{&exportTestRecord{}}})
	require.NoError(t, err)
	verifyExportStore(t, plain)
	verifyLogicalStores(t, plain)
}

func TestImportSearchCustomTokenizerFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := mustOpen(t, filepath.Join(dir, "source.db"), "pw")

	idx, err := src.Search().Open("custom", search.Config{Tokenizer: testCustomTokenizer{}})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "d1", "one two"))

	var buf bytes.Buffer
	_, err = Export(ctx, src, &buf, ExportOptions{})
	require.NoError(t, err)
	require.NoError(t, src.Close())

	dst := mustOpen(t, filepath.Join(dir, "target.db"), "pw")
	defer func() { require.NoError(t, dst.Close()) }()

	_, err = Import(ctx, dst, bytes.NewReader(buf.Bytes()), ImportOptions{})
	require.ErrorIs(t, err, search.ErrInvalidConfig)

	_, err = Import(ctx, dst, bytes.NewReader(buf.Bytes()), ImportOptions{SearchTokenizer: testCustomTokenizer{}})
	require.NoError(t, err)
	imported, err := dst.Search().Open("custom", search.Config{Tokenizer: testCustomTokenizer{}})
	require.NoError(t, err)
	count, err := imported.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
