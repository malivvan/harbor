package record

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/tempest/internal/compress"
)

type compressDoc struct {
	ID      int `db:"id,increment"`
	Content string
}

func (compressDoc) TempestBucketName() string { return "compress_docs" }

func compressPayload() string {
	return strings.Repeat("the quick brown fox jumps over the lazy dog; ", 200)
}

func compressOpen(t *testing.T, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "record.db"), 0600, &bolt.Options{Timeout: 1000000000})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	st, err := Open(context.Background(), bdb, cfg)
	require.NoError(t, err)
	return st, bdb
}

func TestRecordCompressRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, bdb := compressOpen(t, Config{Secret: aeadKey(1), EncryptValues: true, Compress: true})

	doc := &compressDoc{Content: compressPayload()}
	require.NoError(t, st.Save(ctx, doc))
	require.NoError(t, st.Set(ctx, "compress_kv", "payload", compressPayload()))

	var got compressDoc
	require.NoError(t, st.One(ctx, "ID", doc.ID, &got))
	require.Equal(t, doc.Content, got.Content)

	var kv string
	require.NoError(t, st.Get(ctx, "compress_kv", "payload", &kv))
	require.Equal(t, compressPayload(), kv)

	// Compare the sealed size against the same value written without
	// compression: compression must actually shrink the stored bytes.
	plain, err := json.Marshal(compressDoc{Content: compressPayload()})
	require.NoError(t, err)
	records := aeadRawEntries(t, bdb, "compress_docs")
	require.NotEmpty(t, records)
	for _, raw := range records {
		require.Less(t, len(raw), len(plain), "compressed record should be smaller than its JSON")
	}
}

func TestRecordCompressMixedOpen(t *testing.T) {
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "record.db"), 0600, &bolt.Options{Timeout: 1000000000})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	compressed, err := Open(ctx, bdb, Config{Secret: aeadKey(2), EncryptValues: true, Compress: true})
	require.NoError(t, err)
	first := &compressDoc{Content: compressPayload()}
	require.NoError(t, compressed.Save(ctx, first))

	// Reopening without compression still reads the envelope.
	plain, err := Open(ctx, bdb, Config{Secret: aeadKey(2), EncryptValues: true})
	require.NoError(t, err)
	var got compressDoc
	require.NoError(t, plain.One(ctx, "ID", first.ID, &got))
	require.Equal(t, first.Content, got.Content)

	// Values written without compression are readable by the compressed store.
	second := &compressDoc{Content: "short payload"}
	require.NoError(t, plain.Save(ctx, second))
	require.NoError(t, compressed.One(ctx, "ID", second.ID, &got))
	require.Equal(t, second.Content, got.Content)
}

func TestRecordCompressPlaintextStore(t *testing.T) {
	ctx := context.Background()
	st, bdb := compressOpen(t, Config{Compress: true})

	doc := &compressDoc{Content: compressPayload()}
	require.NoError(t, st.Save(ctx, doc))

	var got compressDoc
	require.NoError(t, st.One(ctx, "ID", doc.ID, &got))
	require.Equal(t, doc.Content, got.Content)

	// The value is not sealed, so the envelope is visible on disk and smaller
	// than the JSON encoding.
	records := aeadRawEntries(t, bdb, "compress_docs")
	require.NotEmpty(t, records)
	raw, err := json.Marshal(compressDoc{Content: compressPayload()})
	require.NoError(t, err)
	for _, value := range records {
		require.True(t, bytes.HasPrefix(value, []byte(compress.Magic)), "expected a ZCMP envelope")
		require.Less(t, len(value), len(raw))
	}
}

func TestRecordCompressRotation(t *testing.T) {
	ctx := context.Background()
	oldKey, newKey := aeadKey(3), aeadKey(4)
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "record.db"), 0600, &bolt.Options{Timeout: 1000000000})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	st, err := Open(ctx, bdb, Config{Secret: oldKey, EncryptValues: true, Compress: true})
	require.NoError(t, err)
	doc := &compressDoc{Content: compressPayload()}
	require.NoError(t, st.Save(ctx, doc))
	require.NoError(t, st.RotateAll(ctx, newKey))

	again, err := Open(ctx, bdb, Config{Secret: newKey, EncryptValues: true})
	require.NoError(t, err)
	var got compressDoc
	require.NoError(t, again.One(ctx, "ID", doc.ID, &got))
	require.Equal(t, doc.Content, got.Content)
}
