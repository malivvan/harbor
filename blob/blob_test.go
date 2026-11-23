package blob

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/malivvan/harbor"
	hfs "github.com/malivvan/harbor/fs"
)

// newBlob returns a Store over a throwaway database.
func newBlob(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	st, err := harbor.Open(ctx, filepath.Join(t.TempDir(), "test.db"), 0o600, &harbor.Options{Password: "test-password"})
	if err != nil {
		t.Fatalf("harbor.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	s, err := Open(st.FS(), "objects")
	if err != nil {
		t.Fatalf("blob.Open: %v", err)
	}
	return s
}

func TestPutGetRoundTrip(t *testing.T) {
	s := newBlob(t)
	ctx := context.Background()

	if _, err := s.Put(ctx, "tenant-a", "drive/notes.txt", strings.NewReader("hello world"), 11, "text/plain"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.ReadAll(ctx, "tenant-a", "drive/notes.txt")
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("read %q, want %q", got, "hello world")
	}

	// Get must report the size.
	rc, size, err := s.Get(ctx, "tenant-a", "drive/notes.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	rc.Close()
	if size != 11 {
		t.Fatalf("size = %d, want 11", size)
	}
}

func TestGetMissingObjectErrors(t *testing.T) {
	s := newBlob(t)
	if _, _, err := s.Get(context.Background(), "b", "nope"); err == nil {
		t.Fatal("Get of a missing object: want an error")
	}
	if _, err := s.ReadAll(context.Background(), "b", "nope"); err == nil {
		t.Fatal("ReadAll of a missing object: want an error")
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	s := newBlob(t)
	ctx := context.Background()

	if err := s.Delete(ctx, "b", "not-there"); err != nil {
		t.Fatalf("Delete of a missing object must be a no-op, got %v", err)
	}
	if _, err := s.Put(ctx, "b", "f", strings.NewReader("x"), 1, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(ctx, "b", "f"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, "b", "f"); err != nil {
		t.Fatalf("second Delete must be a no-op, got %v", err)
	}
	if s.Exists(ctx, "b", "f") {
		t.Fatal("object still present after Delete")
	}
}

func TestMoveMissingSourceIsNoOp(t *testing.T) {
	s := newBlob(t)
	// An index record can exist before its bytes are uploaded, so moving a key
	// with no bytes must succeed silently.
	if err := s.Move(context.Background(), "b", "from", "to"); err != nil {
		t.Fatalf("Move of a missing source must be a no-op, got %v", err)
	}
}

func TestMoveRelocatesBytes(t *testing.T) {
	s := newBlob(t)
	ctx := context.Background()
	if _, err := s.Put(ctx, "b", "a/old.txt", strings.NewReader("payload"), 7, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Move(ctx, "b", "a/old.txt", "c/new.txt"); err != nil {
		t.Fatalf("Move: %v", err)
	}
	if s.Exists(ctx, "b", "a/old.txt") {
		t.Fatal("source still present after Move")
	}
	got, err := s.ReadAll(ctx, "b", "c/new.txt")
	if err != nil {
		t.Fatalf("ReadAll destination: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("destination contents = %q, want %q", got, "payload")
	}
}

func TestMoveSameKeyIsNoOp(t *testing.T) {
	s := newBlob(t)
	ctx := context.Background()
	if _, err := s.Put(ctx, "b", "k", strings.NewReader("v"), 1, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Move(ctx, "b", "k", "k"); err != nil {
		t.Fatalf("Move to the same key: %v", err)
	}
	if !s.Exists(ctx, "b", "k") {
		t.Fatal("object vanished after a same-key Move")
	}
}

// TestTraversalCannotEscapeBucket is the containment guarantee: a key must never
// reach a sibling bucket, whichever way it is spelled.
func TestTraversalCannotEscapeBucket(t *testing.T) {
	s := newBlob(t)
	ctx := context.Background()

	// Seed a secret in another bucket.
	if _, err := s.Put(ctx, "tenant-b", "secret.txt", strings.NewReader("SECRET"), 6, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}

	bad := []string{
		"../tenant-b/secret.txt",
		"a/../../tenant-b/secret.txt",
		"..",
		"a/..",
	}
	for _, key := range bad {
		if _, err := s.Put(ctx, "tenant-a", key, strings.NewReader("x"), 1, ""); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Put(%q) err = %v, want ErrInvalidKey", key, err)
		}
		if _, _, err := s.Get(ctx, "tenant-a", key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Get(%q) err = %v, want ErrInvalidKey", key, err)
		}
		if err := s.Delete(ctx, "tenant-a", key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Delete(%q) err = %v, want ErrInvalidKey", key, err)
		}
		if err := s.Move(ctx, "tenant-a", key, "fine.txt"); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Move(%q) err = %v, want ErrInvalidKey", key, err)
		}
	}

	// The secret is untouched and unreadable through the other bucket.
	if got, err := s.ReadAll(ctx, "tenant-b", "secret.txt"); err != nil || string(got) != "SECRET" {
		t.Fatalf("secret bucket damaged: %q err=%v", got, err)
	}
	if _, err := s.ReadAll(ctx, "tenant-a", "tenant-b/secret.txt"); err == nil {
		t.Fatal("a key naming another bucket read across the boundary")
	}
}

func TestInvalidBucketRejected(t *testing.T) {
	s := newBlob(t)
	for _, bucket := range []string{"", "a/b", ".", ".."} {
		if _, err := s.Put(context.Background(), bucket, "k", strings.NewReader("x"), 1, ""); err == nil {
			t.Errorf("Put with bucket %q: want an error", bucket)
		}
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	s := newBlob(t)
	if _, err := s.Put(context.Background(), "b", "", strings.NewReader("x"), 1, ""); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Put with an empty key: err = %v, want ErrInvalidKey", err)
	}
}

func TestListIsRecursiveAndPrefixed(t *testing.T) {
	s := newBlob(t)
	ctx := context.Background()

	keys := []string{"a/1.txt", "a/b/2.txt", "a/b/c/3.txt", "other/4.txt"}
	for _, k := range keys {
		if _, err := s.Put(ctx, "bucket", k, strings.NewReader(k), int64(len(k)), ""); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	all, err := s.List(ctx, "bucket", "")
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	gotKeys := infoKeys(all)
	sort.Strings(gotKeys)
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if strings.Join(gotKeys, ",") != strings.Join(want, ",") {
		t.Fatalf("List all = %v, want %v", gotKeys, want)
	}

	sub, err := s.List(ctx, "bucket", "a/")
	if err != nil {
		t.Fatalf("List a/: %v", err)
	}
	gotSub := infoKeys(sub)
	sort.Strings(gotSub)
	wantSub := []string{"a/1.txt", "a/b/2.txt", "a/b/c/3.txt"}
	sort.Strings(wantSub)
	if strings.Join(gotSub, ",") != strings.Join(wantSub, ",") {
		t.Fatalf("List a/ = %v, want %v", gotSub, wantSub)
	}

	// A prefix that matches nothing is empty, not an error.
	none, err := s.List(ctx, "bucket", "nothing/here")
	if err != nil {
		t.Fatalf("List of an absent prefix: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("List of an absent prefix returned %d entries", len(none))
	}

	// Listing is scoped to one bucket.
	other, err := s.List(ctx, "another", "")
	if err != nil {
		t.Fatalf("List of an empty bucket: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("another bucket listed %d entries, want 0", len(other))
	}
}

func infoKeys(infos []Info) []string {
	out := make([]string, 0, len(infos))
	for _, i := range infos {
		out = append(out, i.Key)
	}
	return out
}

func TestStatReportsExistenceAndSize(t *testing.T) {
	s := newBlob(t)
	ctx := context.Background()

	if _, ok, err := s.Stat(ctx, "b", "missing"); err != nil || ok {
		t.Fatalf("Stat(missing) = ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if _, err := s.Put(ctx, "b", "present", strings.NewReader("12345"), 5, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	info, ok, err := s.Stat(ctx, "b", "present")
	if err != nil || !ok {
		t.Fatalf("Stat(present) = ok=%v err=%v", ok, err)
	}
	if info.Size != 5 {
		t.Errorf("size = %d, want 5", info.Size)
	}
	if info.ETag == "" {
		t.Error("ETag is empty; it should carry a version marker")
	}
	if !s.Exists(ctx, "b", "present") {
		t.Error("Exists = false for a stored object")
	}
}

func TestNilStoreReportsNoStore(t *testing.T) {
	var s *Store
	if _, err := s.Put(context.Background(), "b", "k", strings.NewReader("x"), 1, ""); !errors.Is(err, ErrNoStore) {
		t.Fatalf("Put on a nil Store: err = %v, want ErrNoStore", err)
	}
	if _, _, err := s.Get(context.Background(), "b", "k"); !errors.Is(err, ErrNoStore) {
		t.Fatalf("Get on a nil Store: err = %v, want ErrNoStore", err)
	}
	if _, _, err := (*Store)(nil).Stat(context.Background(), "b", "k"); !errors.Is(err, ErrNoStore) {
		t.Fatalf("Stat on a nil Store: err = %v, want ErrNoStore", err)
	}
}

func TestCancelledContext(t *testing.T) {
	s := newBlob(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Put(ctx, "b", "k", strings.NewReader("x"), 1, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put with a cancelled context: err = %v, want context.Canceled", err)
	}
}

func TestMinterTTL(t *testing.T) {
	m := NewMinter(time.Minute)
	before := time.Now()
	g := m.Mint("bucket", "key", "GET", 0)
	if g.Type != GrantLocal {
		t.Errorf("grant type = %q, want %q", g.Type, GrantLocal)
	}
	if g.Bucket != "bucket" || g.Key != "key" || g.Method != "GET" {
		t.Errorf("grant = %+v", g)
	}
	if !g.ExpiresAt.After(before) || g.ExpiresAt.After(before.Add(time.Minute+time.Second)) {
		t.Errorf("expiry %v outside (now, now+1m]", g.ExpiresAt)
	}

	// An explicit TTL wins over the minter's default.
	g = m.Mint("b", "k", "PUT", 2*time.Minute)
	if got := time.Until(g.ExpiresAt); got < 90*time.Second {
		t.Errorf("explicit TTL not honoured: %v", got)
	}

	// A non-positive TTL falls back to DefaultGrantTTL.
	if NewMinter(0).TTL() != DefaultGrantTTL {
		t.Errorf("NewMinter(0).TTL() = %v, want %v", NewMinter(0).TTL(), DefaultGrantTTL)
	}
}

func TestOpenCreatesNamedFileSystem(t *testing.T) {
	ctx := context.Background()
	st, err := harbor.Open(ctx, filepath.Join(t.TempDir(), "t.db"), 0o600, &harbor.Options{Password: "p"})
	if err != nil {
		t.Fatalf("harbor.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	s, err := Open(st.FS(), "custom-name")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Put(ctx, "b", "k", strings.NewReader("v"), 1, ""); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// The named file system is visible through the fs store.
	names, err := st.FS().FileSystems()
	if err != nil {
		t.Fatalf("FileSystems: %v", err)
	}
	found := false
	for _, n := range names {
		if n == "custom-name" {
			found = true
		}
	}
	if !found {
		t.Fatalf("custom-name not among %v", names)
	}

	if _, err := Open(nil, "x"); !errors.Is(err, ErrNoStore) {
		t.Fatalf("Open(nil) err = %v, want ErrNoStore", err)
	}
	_ = hfs.Name // keep the fs import meaningful for readers
	_ = os.ErrNotExist
	_ = io.EOF
}
