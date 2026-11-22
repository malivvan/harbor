package harbor

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/harbor/db"
	"github.com/malivvan/harbor/internal/crypto"
	"github.com/malivvan/harbor/ts"
	"github.com/malivvan/harbor/vec"
)

type harborRecord struct {
	ID   int    `db:"id,increment"`
	Name string `db:"index"`
}

// testOptions keeps the tests quick: valid Argon2id parameters well below the
// cost of the production default.
func testOptions(password string) *Options {
	return &Options{
		Options:    bolt.Options{Timeout: time.Second},
		Password:   password,
		KDFTime:    1,
		KDFMemory:  8 * MiB,
		KDFThreads: 1,
	}
}

func mustOpen(t *testing.T, path, password string) *Store {
	t.Helper()
	s, err := Open(context.Background(), path, 0600, testOptions(password))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func mustClose(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "harbor.db")
	const password = "correct horse battery staple"

	s := mustOpen(t, path, password)

	if s.DB() == nil || s.FS() == nil || s.TS() == nil || s.Vec() == nil {
		t.Fatal("one or more sub-stores are nil")
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
	filter := &vec.Filter{Must: []vec.Condition{
		{Key: "role", Match: vec.MatchValue{Value: "admin"}},
	}}
	results, err := col.Search([]float32{1, 0, 0}, filter, 1)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].ID != "user_101" {
		t.Fatalf("search results = %+v, want user_101", results)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush collection: %v", err)
	}

	cpu := s.TS().Series("cpu")
	now := time.Now().UnixNano()
	if err := cpu.Insert(ctx, ts.Point{Timestamp: now, Value: 0.5}); err != nil {
		t.Fatalf("ts insert: %v", err)
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

	if err := s.DB().Save(ctx, &harborRecord{Name: "alice"}); err != nil {
		t.Fatalf("db save: %v", err)
	}

	vfs, err := s.FS().Open("test_fs")
	if err != nil {
		t.Fatalf("fs open: %v", err)
	}
	want := time.Now().Format(time.RFC3339) + "\n"
	f, err := vfs.Create("blob.txt")
	if err != nil {
		t.Fatalf("fs create: %v", err)
	}
	if _, err := f.Write([]byte(want)); err != nil {
		_ = f.Close()
		t.Fatalf("fs write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("fs close: %v", err)
	}

	mustClose(t, s)

	// Everything must survive a close/reopen with the same password.
	s = mustOpen(t, path, password)
	t.Cleanup(func() { _ = s.Close() })

	loaded, err := s.Vec().LoadCollection("documents")
	if err != nil {
		t.Fatalf("load collection: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d points, want 2", len(loaded))
	}

	points, err := s.TS().Series("cpu").Range(ctx, now, now)
	if err != nil || len(points) != 1 {
		t.Fatalf("ts range after reopen = %v, %v; want 1 point", points, err)
	}

	var rec harborRecord
	if err := s.DB().One(ctx, "Name", "alice", &rec); err != nil {
		t.Fatalf("db one after reopen: %v", err)
	}

	vfs, err = s.FS().Open("test_fs")
	if err != nil {
		t.Fatalf("fs open after reopen: %v", err)
	}
	content, err := vfs.ReadFile("blob.txt")
	if err != nil {
		t.Fatalf("fs read after reopen: %v", err)
	}
	if string(content) != want {
		t.Fatalf("fs content = %q, want %q", content, want)
	}
}

func TestKDFParamsAndFlags(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "kdf.db"), "pw")
	t.Cleanup(func() { _ = s.Close() })

	salt, kdfTime, kdfMemory, kdfThreads, flags, err := s.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(salt) != SaltSize {
		t.Fatalf("salt length = %d, want %d", len(salt), SaltSize)
	}
	if kdfTime != 1 || kdfMemory != 8*MiB || kdfThreads != 1 {
		t.Fatalf("KDF params = (%d, %d, %d)", kdfTime, kdfMemory, kdfThreads)
	}
	if flags != EncryptAll {
		t.Fatalf("flags = %#x, want EncryptAll", flags)
	}
	if got := s.EncryptFlags(); got != EncryptAll {
		t.Fatalf("EncryptFlags() = %#x, want EncryptAll", got)
	}
}

func TestKeyValueStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.db")
	password := "pw"
	keys := []string{"0", "1", "version", "Metadata", "kdf", "plain"}

	s := mustOpen(t, path, password)
	for _, key := range keys {
		if err := s.Set(key, "value-"+key); err != nil {
			t.Fatalf("Set %q: %v", key, err)
		}
	}
	if n, err := s.Len(); err != nil || n != len(keys) {
		t.Fatalf("Len() = %d, %v; want %d", n, err, len(keys))
	}
	allKeys, err := s.Keys()
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if !reflect.DeepEqual(allKeys, []string{"0", "1", "Metadata", "kdf", "plain", "version"}) {
		t.Fatalf("Keys() = %v", allKeys)
	}
	mustClose(t, s)

	s = mustOpen(t, path, password)
	t.Cleanup(func() { _ = s.Close() })
	for _, key := range keys {
		got, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		if want := "value-" + key; got != want {
			t.Fatalf("Get %q = %q, want %q", key, got, want)
		}
		if ok, err := s.Has(key); err != nil || !ok {
			t.Fatalf("Has %q = %v, %v", key, ok, err)
		}
	}

	if err := s.Delete("version"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get("version"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(deleted) = %v, want ErrKeyNotFound", err)
	}
	if ok, err := s.Has("version"); err != nil || ok {
		t.Fatalf("Has(deleted) = %v, %v", ok, err)
	}
	if err := s.Delete("version"); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
}

func TestKeyValueValidation(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "kv.db"), "pw")
	t.Cleanup(func() { _ = s.Close() })

	if _, err := s.Get(""); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Get(empty) = %v, want ErrInvalidKey", err)
	}
	if err := s.Set("", "v"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Set(empty) = %v, want ErrInvalidKey", err)
	}
	if err := s.Delete(""); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Delete(empty) = %v, want ErrInvalidKey", err)
	}
	if _, err := s.Has(""); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Has(empty) = %v, want ErrInvalidKey", err)
	}
	if err := s.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Set("k", ""); err != nil {
		t.Fatalf("Set(empty value): %v", err)
	}
	if _, err := s.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get after empty-value delete = %v, want ErrKeyNotFound", err)
	}
}

func TestEmptyKeyClosedPrecedence(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "kv.db"), "pw")
	mustClose(t, s)

	if _, err := s.Get(""); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get(empty) after Close = %v, want ErrClosed", err)
	}
	if err := s.Set("", "v"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set(empty) after Close = %v, want ErrClosed", err)
	}
	if err := s.Delete(""); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete(empty) after Close = %v, want ErrClosed", err)
	}
	if _, err := s.Has(""); !errors.Is(err, ErrClosed) {
		t.Fatalf("Has(empty) after Close = %v, want ErrClosed", err)
	}

	var nilStore *Store
	if _, err := nilStore.Get(""); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil Get(empty) = %v, want ErrClosed", err)
	}
}

func TestOpenValidation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "invalid.db")
	unknownFlag := uint16(1) << 15

	tests := []struct {
		name    string
		ctx     context.Context
		options *Options
		want    error
	}{
		{name: "nil options", ctx: ctx, options: nil, want: ErrInvalidOptions},
		{name: "empty password", ctx: ctx, options: testOptions(""), want: ErrInvalidPassword},
		{
			name: "incomplete KDF",
			ctx:  ctx,
			options: &Options{
				Password: "pw",
				KDFTime:  1,
			},
			want: ErrInvalidOptions,
		},
		{
			name: "unknown encryption flags",
			ctx:  ctx,
			options: &Options{
				Password:   "pw",
				KDFTime:    1,
				KDFMemory:  8 * MiB,
				KDFThreads: 1,
				Encrypt:    unknownFlag,
			},
			want: ErrInvalidOptions,
		},
		{
			name: "plaintext conflicts with flags",
			ctx:  ctx,
			options: &Options{
				Password:   "pw",
				KDFTime:    1,
				KDFMemory:  8 * MiB,
				KDFThreads: 1,
				Encrypt:    EncryptDB,
				Plaintext:  true,
			},
			want: ErrInvalidOptions,
		},
		{
			name:    "canceled context",
			ctx:     canceledContext(),
			options: testOptions("pw"),
			want:    context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Open(tt.ctx, path, 0600, tt.options)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Open() = %v, want %v", err, tt.want)
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestWrongPasswordClosesHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrong-password.db")
	s := mustOpen(t, path, "right")
	if err := s.Set("key", "value"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	mustClose(t, s)

	_, err := Open(context.Background(), path, 0600, testOptions("wrong"))
	if !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("wrong password = %v, want ErrInvalidPassword", err)
	}
	if !errors.Is(err, crypto.ErrAuthFailed) {
		t.Fatalf("wrong password = %v, want it to wrap ErrAuthFailed", err)
	}

	// A leaked handle would make this reopen time out on the file lock.
	s = mustOpen(t, path, "right")
	t.Cleanup(func() { _ = s.Close() })
	if got, err := s.Get("key"); err != nil || got != "value" {
		t.Fatalf("Get after reopen = %q, %v", got, err)
	}
}

func TestPlaintextStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plain.db")
	password := "pw"

	opts := testOptions(password)
	opts.Plaintext = true
	s, err := Open(context.Background(), path, 0600, opts)
	if err != nil {
		t.Fatalf("Open plaintext: %v", err)
	}
	if got := s.EncryptFlags(); got != EncryptNone {
		t.Fatalf("EncryptFlags() = %#x, want EncryptNone", got)
	}
	if err := s.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	mustClose(t, s)

	// Stored settings win: default options must not turn encryption on.
	s = mustOpen(t, path, password)
	if got := s.EncryptFlags(); got != EncryptNone {
		t.Fatalf("EncryptFlags() after reopen = %#x, want EncryptNone", got)
	}
	if got, err := s.Get("k"); err != nil || got != "v" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	mustClose(t, s)

	if _, err := Open(context.Background(), path, 0600, testOptions("wrong")); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("wrong password = %v, want ErrInvalidPassword", err)
	}
}

func TestStoredSettingsWin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.db")
	password := "pw"

	opts := testOptions(password)
	opts.Encrypt = EncryptDB | EncryptTS
	s, err := Open(context.Background(), path, 0600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	wantSalt, wantTime, wantMemory, wantThreads, wantFlags, err := s.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	mustClose(t, s)

	// Different cost parameters and flags are ignored for an existing store.
	opts = testOptions(password)
	opts.KDFTime = 2
	opts.KDFMemory = 16 * MiB
	opts.KDFThreads = 1
	opts.Encrypt = EncryptAll
	s, err = Open(context.Background(), path, 0600, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got := s.EncryptFlags(); got != EncryptDB|EncryptTS {
		t.Fatalf("EncryptFlags() = %#x, want stored flags", got)
	}
	salt, kdfTime, kdfMemory, kdfThreads, flags, err := s.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if kdfTime != wantTime || kdfMemory != wantMemory || kdfThreads != wantThreads || flags != wantFlags {
		t.Fatalf("KDF params changed on reopen")
	}
	if string(salt) != string(wantSalt) {
		t.Fatal("KDF salt changed on reopen")
	}

	// Metadata returns a fresh copy of the salt.
	salt[0] ^= 0xFF
	if again, _, _, _, _, err := s.Metadata(); err != nil || again[0] == salt[0] {
		t.Fatalf("mutating Metadata salt changed the store (err=%v)", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "close.db"), "pw")
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if s.DB() != nil || s.FS() != nil || s.TS() != nil || s.Vec() != nil {
		t.Fatal("sub-store accessors must return nil after Close")
	}
	if _, err := s.Get("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	if err := s.Set("k", "v"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close = %v, want ErrClosed", err)
	}
	if err := s.Delete("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete after Close = %v, want ErrClosed", err)
	}
	if _, _, _, _, _, err := s.Metadata(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Metadata after Close = %v, want ErrClosed", err)
	}

	var nilStore *Store
	if err := nilStore.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
	if nilStore.DB() != nil || nilStore.EncryptFlags() != 0 {
		t.Fatal("nil receiver accessors must return zero values")
	}
	if _, err := nilStore.Get("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil Get = %v, want ErrClosed", err)
	}
}

func TestOpenRejectsBadMetadata(t *testing.T) {
	t.Run("missing Metadata record", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.db")
		bdb, err := bolt.Open(path, 0600, nil)
		if err != nil {
			t.Fatalf("bolt.Open: %v", err)
		}
		if err := bdb.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket(metadataBucket)
			return err
		}); err != nil {
			t.Fatalf("create meta bucket: %v", err)
		}
		_ = bdb.Close()

		if _, err := Open(context.Background(), path, 0600, testOptions("pw")); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("Open missing Metadata = %v, want ErrCorruptStore", err)
		}
	})

	t.Run("malformed Metadata", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "malformed.db")
		bdb, err := bolt.Open(path, 0600, nil)
		if err != nil {
			t.Fatalf("bolt.Open: %v", err)
		}
		if err := bdb.Update(func(tx *bolt.Tx) error {
			meta, err := tx.CreateBucket(metadataBucket)
			if err != nil {
				return err
			}
			md := Metadata{
				Salt:    []byte("0123456789abcdef"),
				Time:    FastKDFTime,
				Flags:   EncryptAll,
				Memory:  FastKDFMemory,
				Threads: FastKDFThreads,
				Canary:  []byte("canary"),
			}
			truncated := md.encode()
			return meta.Put(metadataRecordKey, truncated[:len(truncated)-1])
		}); err != nil {
			t.Fatalf("write Metadata: %v", err)
		}
		_ = bdb.Close()

		if _, err := Open(context.Background(), path, 0600, testOptions("pw")); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("Open malformed Metadata = %v, want ErrCorruptStore", err)
		}
	})
}

// TestKeyValueEncryptedAtRest proves the top-level key/value values are sealed
// when EncryptKVValues is enabled (the default via EncryptAll).
func TestKeyValueEncryptedAtRest(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "kv-enc.db"), "pw")
	t.Cleanup(func() { _ = s.Close() })

	const (
		key    = "token"
		secret = "hunter2-super-secret"
	)
	if err := s.Set(key, secret); err != nil {
		t.Fatalf("Set: %v", err)
	}

	var raw []byte
	if err := s.bs.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvBucket)
		if b == nil {
			t.Fatal("key/value bucket missing")
		}
		raw = append([]byte(nil), b.Get([]byte(key))...)
		return nil
	}); err != nil {
		t.Fatalf("read raw bucket: %v", err)
	}
	if bytes.Equal(raw, []byte(secret)) || bytes.Contains(raw, []byte(secret)) {
		t.Fatalf("stored value %q leaks the plaintext", raw)
	}

	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != secret {
		t.Fatalf("Get = %q, want %q", got, secret)
	}
}

// TestKeyValuePlaintextAtRest documents the Plaintext contract: the value is
// byte-for-byte identical in the bucket.
func TestKeyValuePlaintextAtRest(t *testing.T) {
	opts := testOptions("pw")
	opts.Plaintext = true
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "kv-plain.db"), 0600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const secret = "hunter2"
	if err := s.Set("token", secret); err != nil {
		t.Fatalf("Set: %v", err)
	}
	var raw []byte
	if err := s.bs.View(func(tx *bolt.Tx) error {
		raw = append([]byte(nil), tx.Bucket(kvBucket).Get([]byte("token"))...)
		return nil
	}); err != nil {
		t.Fatalf("read raw bucket: %v", err)
	}
	if string(raw) != secret {
		t.Fatalf("plaintext store value = %q, want %q", raw, secret)
	}
}

// TestCanaryBindsEncryptionFlags flips one persisted flag bit and expects Open
// to reject the store instead of silently opening it in another mode.
func TestCanaryBindsEncryptionFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flags.db")
	s := mustOpen(t, path, "pw")
	if err := s.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	mustClose(t, s)

	bdb, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	if err := bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metadataBucket)
		md, err := decodeMetadata(append([]byte(nil), b.Get(metadataRecordKey)...))
		if err != nil {
			return err
		}
		md.Flags &^= EncryptKVValues
		return b.Put(metadataRecordKey, md.encode())
	}); err != nil {
		_ = bdb.Close()
		t.Fatalf("tamper flags: %v", err)
	}
	if err := bdb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err = Open(context.Background(), path, 0600, testOptions("pw"))
	if !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("Open after flag tampering = %v, want ErrInvalidPassword", err)
	}
	if !errors.Is(err, crypto.ErrAuthFailed) {
		t.Fatalf("Open after flag tampering = %v, want it to wrap ErrAuthFailed", err)
	}
}

// TestMissingMetadataBucketIsCorrupt ensures a store that lost only its
// Metadata is not silently re-initialized.
func TestMissingMetadataBucketIsCorrupt(t *testing.T) {
	for name, buckets := range map[string][][]byte{
		"key/value bucket": {kvBucket},
		"sub-store root":   {[]byte(db.Name)},
		"unrelated bucket": {[]byte("something-else")},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "corrupt.db")
			bdb, err := bolt.Open(path, 0600, nil)
			if err != nil {
				t.Fatalf("bolt.Open: %v", err)
			}
			if err := bdb.Update(func(tx *bolt.Tx) error {
				for _, name := range buckets {
					if _, err := tx.CreateBucket(name); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				_ = bdb.Close()
				t.Fatalf("create buckets: %v", err)
			}
			_ = bdb.Close()

			if _, err := Open(context.Background(), path, 0600, testOptions("pw")); !errors.Is(err, ErrCorruptStore) {
				t.Fatalf("Open = %v, want ErrCorruptStore", err)
			}
		})
	}
}

func TestDeriveKeys(t *testing.T) {
	salt := []byte("0123456789abcdef")
	first, err := deriveKeys("password", salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}
	t.Cleanup(first.wipe)

	keys := map[string][]byte{
		"canary": first.canary,
		"db":     first.db,
		"fs":     first.fs,
		"ts":     first.ts,
		"vec":    first.vec,
		"kv":     first.kv,
	}
	seen := make(map[string]string, len(keys))
	for name, key := range keys {
		if len(key) != KeySize {
			t.Errorf("%s key length = %d, want %d", name, len(key), KeySize)
		}
		if other, ok := seen[string(key)]; ok {
			t.Errorf("%s and %s derived the same key", name, other)
		}
		seen[string(key)] = name
	}

	again, err := deriveKeys("password", salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys again: %v", err)
	}
	t.Cleanup(again.wipe)
	if !bytes.Equal(first.db, again.db) {
		t.Fatal("deriveKeys is not deterministic for the same password and salt")
	}

	otherPassword, err := deriveKeys("other", salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys other password: %v", err)
	}
	t.Cleanup(otherPassword.wipe)
	if bytes.Equal(first.db, otherPassword.db) {
		t.Fatal("different passwords derived the same key")
	}

	otherSalt, err := deriveKeys("password", []byte("fedcba9876543210"), FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys other salt: %v", err)
	}
	t.Cleanup(otherSalt.wipe)
	if bytes.Equal(first.db, otherSalt.db) {
		t.Fatal("different salts derived the same key")
	}
}

func TestStoreKeysWipe(t *testing.T) {
	keys, err := deriveKeys("password", []byte("0123456789abcdef"), FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}
	keys.wipe()
	for name, key := range map[string][]byte{
		"canary": keys.canary,
		"db":     keys.db,
		"fs":     keys.fs,
		"ts":     keys.ts,
		"vec":    keys.vec,
		"kv":     keys.kv,
	} {
		if !bytes.Equal(key, make([]byte, len(key))) {
			t.Errorf("%s key was not wiped", name)
		}
	}
	var nilKeys *storeKeys
	nilKeys.wipe() // must not panic
}
