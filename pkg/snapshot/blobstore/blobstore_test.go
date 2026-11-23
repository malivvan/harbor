package blobstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/malivvan/harbor"
	"github.com/malivvan/harbor/pkg/snapshot"
)

// newStore returns an adapter over a throwaway Harbor database.
func newStore(t *testing.T, bucket string) *Store {
	t.Helper()
	ctx := context.Background()
	harborStore, err := harbor.Open(ctx, filepath.Join(t.TempDir(), "test.db"), 0o600, &harbor.Options{Password: "test-password"})
	if err != nil {
		t.Fatalf("harbor.Open: %v", err)
	}
	t.Cleanup(func() { _ = harborStore.Close() })

	return New(harborStore.Objects(), bucket)
}

func readAll(t *testing.T, s *Store, key string) (string, error) {
	t.Helper()
	rc, err := s.Get(context.Background(), key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}

// TestObjectStoreConformance exercises every method of the contract the
// snapshotter relies on.
func TestObjectStoreConformance(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, "box")
	if s.Bucket() != "box" {
		t.Fatalf("Bucket() = %q, want %q", s.Bucket(), "box")
	}

	// Stat of a missing key is not an error and reports non-existence.
	if _, ok, err := s.Stat(ctx, "os/missing.txt"); err != nil || ok {
		t.Fatalf("Stat(missing) = ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	// Put, then Stat and Get.
	if err := s.Put(ctx, "os/a.txt", strings.NewReader("alpha"), 5, "text/plain"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	info, ok, err := s.Stat(ctx, "os/a.txt")
	if err != nil || !ok {
		t.Fatalf("Stat(a.txt) = ok=%v err=%v", ok, err)
	}
	if info.Key != "os/a.txt" || info.Size != 5 || info.ETag == "" {
		t.Fatalf("Stat(a.txt) = %+v", info)
	}
	got, err := readAll(t, s, "os/a.txt")
	if err != nil || got != "alpha" {
		t.Fatalf("Get(a.txt) = %q err=%v", got, err)
	}

	// Put may stream with an unknown size (-1), as the contract allows.
	if err := s.Put(ctx, "os/b.txt", strings.NewReader("beta"), -1, ""); err != nil {
		t.Fatalf("Put(size=-1): %v", err)
	}
	if got, err := readAll(t, s, "os/b.txt"); err != nil || got != "beta" {
		t.Fatalf("Get(b.txt) = %q err=%v", got, err)
	}

	// List is recursive and prefix-scoped.
	if err := s.Put(ctx, "other/c.txt", strings.NewReader("gamma"), 0, ""); err != nil {
		t.Fatalf("Put(c.txt): %v", err)
	}
	infos, err := s.List(ctx, "os/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	keys := make([]string, 0, len(infos))
	for _, i := range infos {
		keys = append(keys, i.Key)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "os/a.txt,os/b.txt" {
		t.Fatalf("List(os/) = %v", keys)
	}

	// A prefix matching nothing is empty, not an error.
	none, err := s.List(ctx, "nothing/")
	if err != nil || len(none) != 0 {
		t.Fatalf("List(absent prefix) = %v err=%v", none, err)
	}

	// Get of a missing key returns the wrapped not-exist error.
	if _, err := s.Get(ctx, "os/missing.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Get(missing) err = %v, want os.ErrNotExist", err)
	}

	// Delete removes the object, and deleting it again is a no-op.
	if err := s.Delete(ctx, "os/a.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := s.Stat(ctx, "os/a.txt"); err != nil || ok {
		t.Fatalf("Stat after Delete = ok=%v err=%v", ok, err)
	}
	if err := s.Delete(ctx, "os/a.txt"); err != nil {
		t.Fatalf("second Delete must be a no-op, got %v", err)
	}
}

// TestNilStoreErrors ensures a not-yet-open database yields errors, not panics.
func TestNilStoreErrors(t *testing.T) {
	ctx := context.Background()
	s := New(nil, "box")
	if _, err := s.List(ctx, ""); !errors.Is(err, snapshot.ErrNotConfigured) {
		t.Fatalf("List: err = %v", err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, snapshot.ErrNotConfigured) {
		t.Fatalf("Get: err = %v", err)
	}
	if err := s.Put(ctx, "k", strings.NewReader("x"), 1, ""); !errors.Is(err, snapshot.ErrNotConfigured) {
		t.Fatalf("Put: err = %v", err)
	}
	if err := s.Delete(ctx, "k"); !errors.Is(err, snapshot.ErrNotConfigured) {
		t.Fatalf("Delete: err = %v", err)
	}
	if _, _, err := s.Stat(ctx, "k"); !errors.Is(err, snapshot.ErrNotConfigured) {
		t.Fatalf("Stat: err = %v", err)
	}

	var nilStore *Store
	if _, err := nilStore.List(ctx, ""); !errors.Is(err, snapshot.ErrNotConfigured) {
		t.Fatalf("nil *Store List: err = %v", err)
	}
}

// TestSnapshotRoundTripThroughAdapter runs the snapshot suite over a real blob
// store: capture, verify, mutate live data, restore and check the bytes.
func TestSnapshotRoundTripThroughAdapter(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, "box")

	live := map[string]string{
		"os/a.txt":     "alpha",
		"os/b.txt":     "beta",
		"os/dup/1.txt": "same",
		"os/dup/2.txt": "same", // deduped to one blob
	}
	for key, body := range live {
		if err := s.Put(ctx, key, strings.NewReader(body), int64(len(body)), ""); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	snap := snapshot.New(s, snapshot.Config{DataPrefix: "os", AccountID: "acct"})
	idx, err := snap.Create(ctx, snapshot.KindManual, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if idx.ObjectCount != len(live) {
		t.Fatalf("ObjectCount = %d, want %d", idx.ObjectCount, len(live))
	}
	if err := snap.Verify(ctx, idx.ID); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	// Change and add live objects after the snapshot.
	if err := s.Put(ctx, "os/a.txt", strings.NewReader("changed"), 7, ""); err != nil {
		t.Fatalf("Put changed: %v", err)
	}
	if err := s.Put(ctx, "os/new.txt", strings.NewReader("new"), 3, ""); err != nil {
		t.Fatalf("Put new: %v", err)
	}

	if _, err := snap.Restore(ctx, idx.ID, snapshot.RestoreOptions{SkipSafetySnapshot: true}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for key, want := range live {
		got, err := readAll(t, s, key)
		if err != nil {
			t.Fatalf("after restore, Get(%s): %v", key, err)
		}
		if got != want {
			t.Fatalf("after restore, %s = %q, want %q", key, got, want)
		}
	}
	if _, ok, _ := s.Stat(ctx, "os/new.txt"); ok {
		t.Fatal("object added after the snapshot survives a restore")
	}

	// The snapshot index is listed through the same adapter.
	list, err := snap.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].ID != idx.ID {
		t.Fatalf("List = %+v", list)
	}
}

// failingReader yields bytes and then fails, like a dropped stream.
type failingReader struct{ sent int }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.sent >= 512 {
		return 0, errors.New("stream failed")
	}
	n := len(p)
	if remaining := 512 - r.sent; n > remaining {
		n = remaining
	}
	r.sent += n
	return n, nil
}

// TestFailedPutLeavesNoBlob proves the atomic Put contract the snapshotter
// depends on: a partially written object is never visible through Stat, so the
// next capture uploads the complete content instead of trusting a partial blob.
func TestFailedPutLeavesNoBlob(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, "box")

	if err := s.Put(ctx, "os/partial.txt", &failingReader{}, -1, ""); err == nil {
		t.Fatal("Put with a failing reader: want an error")
	}
	if _, ok, err := s.Stat(ctx, "os/partial.txt"); err != nil || ok {
		t.Fatalf("Stat after a failed Put = ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	snap := snapshot.New(s, snapshot.Config{DataPrefix: "os"})
	idx, err := snap.Create(ctx, snapshot.KindManual, "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := snap.Verify(ctx, idx.ID); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}
