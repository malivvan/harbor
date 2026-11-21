package storm

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/storm/ts"
	"github.com/malivvan/storm/vec"
)

func testSecret() []byte { return bytes.Repeat([]byte{7}, 32) }

func TestStorm(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "storm.db")

	s, err := Open(ctx, path, 0600, nil, Config{Secret: testSecret()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	if s.ID() == uuid.Nil {
		t.Fatal("store ID was not assigned")
	}

	col, err := s.Vec().NewCollection("documents", 3, vec.Cosine, true)
	if err != nil {
		t.Fatalf("new collection: %v", err)
	}
	if err := col.Upsert([]vec.PointStruct{
		{
			ID:      "user_101",
			Vector:  []float32{0.5, 0.5, 0.5},
			Payload: vec.Payload{"role": "admin", "age": 30},
		},
		{
			ID:      "user_202",
			Vector:  []float32{0.8, 0.2, 0.0},
			Payload: vec.Payload{"role": "guest", "age": 22},
		},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	filter := &vec.Filter{
		Must: []vec.Condition{
			{Key: "role", Match: vec.MatchValue{Value: "admin"}},
		},
	}
	results, err := col.Search([]float32{1.0, 0.0, 0.0}, filter, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "user_101" {
		t.Fatalf("search results = %+v, want user_101", results)
	}

	cpu := s.TS().Series("cpu")
	now := time.Now().UnixNano()
	if err := cpu.Insert(ctx, ts.Point{Timestamp: now, Value: 0.5}); err != nil {
		t.Fatalf("ts insert: %v", err)
	}
	points, err := cpu.Range(ctx, now, now)
	if err != nil {
		t.Fatalf("ts range: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("ts range returned %d points, want 1", len(points))
	}
	if err := cpu.SetLabels(ctx, map[string]string{"host": "test"}); err != nil {
		t.Fatalf("ts labels: %v", err)
	}
	samples, err := cpu.Aggregate(ctx, now, now, 1, ts.AggAvg)
	if err != nil {
		t.Fatalf("ts aggregate: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("ts aggregate returned %d samples, want 1", len(samples))
	}

	vfs, err := s.FS().Open("test_fs")
	if err != nil {
		t.Fatalf("fs open: %v", err)
	}
	f, err := vfs.Create("blob.txt")
	if err != nil {
		t.Fatalf("fs create: %v", err)
	}
	want := time.Now().Format(time.RFC3339) + "\n"
	if _, err := f.Write([]byte(want)); err != nil {
		_ = f.Close()
		t.Fatalf("fs write: %v", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatalf("fs sync: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("fs close: %v", err)
	}

	content, err := vfs.ReadFile("blob.txt")
	if err != nil {
		t.Fatalf("fs read: %v", err)
	}
	if string(content) != want {
		t.Fatalf("content = %q, want %q", content, want)
	}
}

func TestOpenRejectsInvalidSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-key.db")
	for _, secret := range [][]byte{nil, make([]byte, 16), make([]byte, 33)} {
		if _, err := Open(context.Background(), path, 0600, nil, Config{Secret: secret}); err == nil {
			t.Fatalf("Open accepted a %d-byte secret", len(secret))
		}
	}
}

func TestSetGetRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.db")
	s, err := Open(context.Background(), path, 0600, nil, Config{Secret: testSecret()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Set("key", "value"); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Get("key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "value" {
		t.Fatalf("get = %q, want %q", got, "value")
	}
	if err := s.Set("key", ""); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestGetMissingKeyReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.db")
	s, err := Open(context.Background(), path, 0600, nil, Config{Secret: testSecret()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, err := s.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrKeyNotFound", err)
	}
}

func TestOpenReleasesHandleOnSubStoreFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leak.db")
	key := testSecret()

	s, err := Open(context.Background(), path, 0600, &bolt.Options{Timeout: time.Second}, Config{Secret: key})
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	// A wrong key makes db.Open fail after bolt.Open succeeded.
	wrong := bytes.Repeat([]byte{8}, len(key))
	if _, err := Open(context.Background(), path, 0600, &bolt.Options{Timeout: time.Second}, Config{Secret: wrong}); err == nil {
		t.Fatal("Open with the wrong key succeeded")
	}

	// If the failed Open leaked its handle, this second Open cannot take the
	// file lock and times out.
	s2, err := Open(context.Background(), path, 0600, &bolt.Options{Timeout: 500 * time.Millisecond}, Config{Secret: key})
	if err != nil {
		t.Fatalf("failed Open leaked the database handle: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
