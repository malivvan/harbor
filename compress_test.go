package tempest

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/object"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

func compressContent() string {
	return strings.Repeat("tempest compression payload that repeats; ", 200)
}

// seedEveryStore writes one representative value into each sub-store.
func seedEveryStore(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, s.Record().Save(ctx, &tempestRecord{Name: "alice"}))

	fsys, err := s.Fstree().Open("data")
	require.NoError(t, err)
	f, err := fsys.Create("/note.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte(compressContent()))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, s.Series().Series("cpu").Insert(ctx, series.Point{Timestamp: 1, Value: 2.5}))

	col, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{{ID: "p1", Vector: []float32{1, 0},
		Payload: vector.Payload{"body": compressContent()}}}))
	require.NoError(t, col.Flush())

	idx, err := s.Search().Open("fts", search.Config{})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "d1", compressContent()))

	g, err := s.Graphs().Open("social")
	require.NoError(t, err)
	require.NoError(t, g.UpsertNode(ctx, &graphs.Node{ID: "a", Type: "doc", Content: compressContent()}))

	_, err = s.Objects().Put(ctx, "tenant", "a/b.txt", strings.NewReader(compressContent()), int64(len(compressContent())), "")
	require.NoError(t, err)
}

// verifyEveryStore reads every seeded value back.
func verifyEveryStore(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()

	var rec tempestRecord
	require.NoError(t, s.Record().Select().First(ctx, &rec))

	fsys, err := s.Fstree().Open("data")
	require.NoError(t, err)
	data, err := fsys.ReadFile("/note.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(data))

	point, err := s.Series().Series("cpu").Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 2.5, point.Value)

	points, err := s.Vector().LoadCollection("docs")
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.Equal(t, compressContent(), points["p1"].Payload["body"])

	idx, err := s.Search().Open("fts", search.Config{})
	require.NoError(t, err)
	hits, err := idx.Search(ctx, "compression", 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)

	g, err := s.Graphs().Open("social")
	require.NoError(t, err)
	node, err := g.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, compressContent(), node.Content)

	data, err = s.Objects().ReadAll(ctx, "tenant", "a/b.txt")
	require.NoError(t, err)
	require.Equal(t, compressContent(), string(data))
}

func TestOpenWithCompression(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tempest.db")

	opts := testOptions("pw")
	opts.Compression = Compression{Enable: true, Level: 3}
	s, err := Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	seedEveryStore(t, s)
	require.NoError(t, s.Close())

	// Reopening with compression disabled still reads every value: the
	// envelope is self-describing.
	plain := testOptions("pw")
	s, err = Open(ctx, path, 0o600, plain)
	require.NoError(t, err)
	verifyEveryStore(t, s)
	require.NoError(t, s.Close())

	// And a compressed reopen keeps working, including values written while
	// compression was off.
	s, err = Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	verifyEveryStore(t, s)
}

func TestOpenWithDedup(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tempest.db")

	opts := testOptions("pw")
	opts.Dedup = Dedup{FSTree: true, Objects: true}
	s, err := Open(ctx, path, 0o600, opts)
	require.NoError(t, err)

	fsys, err := s.Fstree().Open("data")
	require.NoError(t, err)
	for _, name := range []string{"/a.txt", "/b.txt"} {
		f, err := fsys.Create(name)
		require.NoError(t, err)
		_, err = f.Write([]byte(compressContent()))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	for _, key := range []string{"a", "b"} {
		_, err = s.Objects().Put(ctx, "tenant", key, strings.NewReader(compressContent()), int64(len(compressContent())), "")
		require.NoError(t, err)
	}
	require.NoError(t, s.Close())

	// The matching configuration reopens and reads every duplicate.
	s, err = Open(ctx, path, 0o600, opts)
	require.NoError(t, err)
	fsys, err = s.Fstree().Open("data")
	require.NoError(t, err)
	for _, name := range []string{"/a.txt", "/b.txt"} {
		data, err := fsys.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, compressContent(), string(data))
	}
	for _, key := range []string{"a", "b"} {
		data, err := s.Objects().ReadAll(ctx, "tenant", key)
		require.NoError(t, err)
		require.Equal(t, compressContent(), string(data))
	}
	require.NoError(t, s.Close())

	// Dropping the object dedup setting fails closed on the persisted mode.
	_, err = Open(ctx, path, 0o600, testOptions("pw"))
	require.ErrorIs(t, err, object.ErrWrongMode)
}

func TestDedupFilesRequiresEncryptedFiles(t *testing.T) {
	ctx := context.Background()
	opts := testOptions("pw")
	opts.Plaintext = true
	opts.Dedup = Dedup{FSTree: true}
	_, err := Open(ctx, filepath.Join(t.TempDir(), "tempest.db"), 0o600, opts)
	require.True(t, errors.Is(err, ErrInvalidOptions), "want ErrInvalidOptions, got %v", err)
}
