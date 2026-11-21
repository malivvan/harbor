package storm

import (
	"context"
	"testing"
)

func TestStorm(t *testing.T) {
	s, err := Open(nil, "test.db", 0600, nil)
	if err != nil {
		t.Fatalf("failed to open storm: %v", err)
	}
	defer s.Close()

	t.Log("Storm opened successfully")
	t.Log("Store ID:", s.ID())

	err = s.KV().Bucket("test").Set(context.Background(), "key", "value")
	if err != nil {
		t.Fatalf("failed to set key in KV store: %v", err)
	}
	var val string
	err = s.KV().Bucket("test").Get(context.Background(), "key", &val)
	if err != nil {
		t.Fatalf("failed to get key from KV store: %v", err)
	}
	if val != "value" {
		t.Fatalf("expected value 'value', got '%s'", val)
	}

	t.Log("KV store operations successful")
}
