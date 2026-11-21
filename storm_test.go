package storm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/malivvan/storm/ts"
	"github.com/malivvan/storm/vec"
)

func TestStorm(t *testing.T) {
	s, err := Open(nil, "storm.db", 0600, nil, Config{Secret: bytes.Repeat([]byte{9}, 32)})
	if err != nil {
		t.Fatalf("failed to open storm: %v", err)
	}
	defer s.Close()

	t.Log("Storm opened successfully")
	t.Log("Store ID:", s.ID())

	// 2. Create a Collection (Automatic Persistence)
	col, _ := s.Vec().NewCollection("documents", 3, vec.Cosine, true)

	err = col.Upsert([]vec.PointStruct{
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
	})
	if err != nil {
		log.Fatalf("Upsert error: %v", err)
	}

	// 4. Perform a rich query using Go structs directly
	fmt.Println("\nSearching for an admin closest to [1.0, 0.0, 0.0]...")
	query := []float32{1.0, 0.0, 0.0}

	filter := &vec.Filter{
		Must: []vec.Condition{
			{Key: "role", Match: vec.MatchValue{Value: "admin"}},
		},
	}

	results, err := col.Search(query, filter, 1) // Get Top 1
	if err != nil {
		log.Fatalf("Search error: %v", err)
	}

	fmt.Printf("Best Match: %s (Score: %f)\n", results[0].ID, results[0].Score)

	// 3. Time series store
	cpu := s.TS().Series("cpu")
	now := time.Now().UnixNano()
	if err := cpu.Insert(context.Background(), ts.Point{Timestamp: now, Value: 0.5}); err != nil {
		t.Fatalf("TS insert error: %v", err)
	}
	points, err := cpu.Range(context.Background(), now, now)
	if err != nil || len(points) != 1 {
		t.Fatalf("TS range error: %v, points: %v", err, points)
	}
	if err := cpu.SetLabels(context.Background(), map[string]string{"host": "test"}); err != nil {
		t.Fatalf("TS labels error: %v", err)
	}
	samples, err := cpu.Aggregate(context.Background(), now, now, 1, ts.AggAvg)
	if err != nil || len(samples) != 1 {
		t.Fatalf("TS aggregate error: %v, samples: %v", err, samples)
	}
	t.Log("Time series store operations successful")

	vfs, err := s.FS().Open("test_fs")
	if err != nil {
		log.Fatalf("FS open error: %v", err)
	}
	// append timestamp and newline to the file blob.txt
	f, err := vfs.Create("blob.txt")
	if err != nil {
		log.Fatalf("FS Create error: %v", err)
	}
	defer f.Close()
	_, err = f.Write([]byte(time.Now().Format(time.RFC3339) + "\n"))
	if err != nil {
		log.Fatalf("FS Write error: %v", err)
	}
	err = f.Sync()
	if err != nil {
		log.Fatalf("FS Sync error: %v", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatalf("FS Close error: %v", err)
	}

	// read the current content of the file
	content, err := vfs.ReadFile("blob.txt")
	if err != nil {
		log.Fatalf("FS ReadFile error: %v", err)
	}
	log.Printf("Current content of blob.txt: %s", string(content))
}

func TestOpenRejectsInvalidSecret(t *testing.T) {
	path := t.TempDir() + "/invalid-key.db"
	for _, secret := range [][]byte{nil, make([]byte, 16), make([]byte, 33)} {
		if _, err := Open(context.Background(), path, 0600, nil, Config{Secret: secret}); err == nil {
			t.Fatalf("Open accepted a %d-byte secret", len(secret))
		}
	}
}

func TestSetGetRoundTrip(t *testing.T) {
	path := t.TempDir() + "/kv.db"
	s, err := Open(context.Background(), path, 0600, nil, Config{Secret: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

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
	path := t.TempDir() + "/kv.db"
	s, err := Open(context.Background(), path, 0600, nil, Config{Secret: bytes.Repeat([]byte{2}, 32)})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if _, err := s.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrKeyNotFound", err)
	}
}
