package harbor

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	file "github.com/malivvan/harbor/store/fstree"
	"github.com/malivvan/harbor/store/record"
	"github.com/malivvan/harbor/store/search"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/harbor/internal/crypto"
	hstore "github.com/malivvan/harbor/store"
	audit "github.com/malivvan/harbor/store/ledger"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/system"
	"github.com/malivvan/harbor/store/vector"
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

	if s.Record() == nil || s.File() == nil || s.Series() == nil || s.Vector() == nil {
		t.Fatal("one or more sub-stores are nil")
	}

	col, err := s.Vector().NewCollection("documents", 3, vector.Cosine, true)
	if err != nil {
		t.Fatalf("new collection: %v", err)
	}
	if err := col.Upsert([]vector.PointStruct{
		{
			ID:      "user_101",
			Vector:  []float32{0.5, 0.5, 0.5},
			Payload: vector.Payload{"role": "admin", "age": 30},
		},
		{
			ID:      "user_202",
			Vector:  []float32{0.8, 0.2, 0.0},
			Payload: vector.Payload{"role": "guest", "age": 22},
		},
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	filter := &vector.Filter{Must: []vector.Condition{
		{Key: "role", Match: vector.MatchValue{Value: "admin"}},
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

	cpu := s.Series().Series("cpu")
	now := time.Now().UnixNano()
	if err := cpu.Insert(ctx, series.Point{Timestamp: now, Value: 0.5}); err != nil {
		t.Fatalf("ts insert: %v", err)
	}
	if err := cpu.SetLabels(ctx, map[string]string{"host": "test"}); err != nil {
		t.Fatalf("ts labels: %v", err)
	}
	samples, err := cpu.Aggregate(ctx, now, now, 1, series.AggAvg)
	if err != nil {
		t.Fatalf("ts aggregate: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("ts aggregate returned %d samples, want 1", len(samples))
	}

	if err := s.Record().Save(ctx, &harborRecord{Name: "alice"}); err != nil {
		t.Fatalf("db save: %v", err)
	}

	vfs, err := s.File().Open("test_fs")
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

	loaded, err := s.Vector().LoadCollection("documents")
	if err != nil {
		t.Fatalf("load collection: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded %d points, want 2", len(loaded))
	}

	points, err := s.Series().Series("cpu").Range(ctx, now, now)
	if err != nil || len(points) != 1 {
		t.Fatalf("ts range after reopen = %v, %v; want 1 point", points, err)
	}

	var rec harborRecord
	if err := s.Record().One(ctx, "Name", "alice", &rec); err != nil {
		t.Fatalf("db one after reopen: %v", err)
	}

	vfs, err = s.File().Open("test_fs")
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

	salt, kdfTime, kdfMemory, kdfThreads, flags, err := s.Meta()
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
	if got := s.Flags(); got != EncryptAll {
		t.Fatalf("Flags() = %#x, want EncryptAll", got)
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
				Encrypt:    EncryptRecord,
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
	mustClose(t, s)

	_, err := Open(context.Background(), path, 0600, testOptions("wrong"))
	if !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("wrong password = %v, want ErrInvalidPassword", err)
	}
	if !errors.Is(err, crypto.ErrAuthFailed) {
		t.Fatalf("wrong password = %v, want it to wrap ErrAuthFailed", err)
	}

	// A leaked handle would make this reopen time out on the fstree lock.
	s = mustOpen(t, path, "right")
	t.Cleanup(func() { _ = s.Close() })
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
	if got := s.Flags(); got != EncryptNone {
		t.Fatalf("Flags() = %#x, want EncryptNone", got)
	}
	mustClose(t, s)

	// Stored settings win: default options must not turn encryption on.
	s = mustOpen(t, path, password)
	if got := s.Flags(); got != EncryptNone {
		t.Fatalf("Flags() after reopen = %#x, want EncryptNone", got)
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
	opts.Encrypt = EncryptRecord | EncryptSeries
	s, err := Open(context.Background(), path, 0600, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	wantSalt, wantTime, wantMemory, wantThreads, wantFlags, err := s.Meta()
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

	if got := s.Flags(); got != EncryptRecord|EncryptSeries {
		t.Fatalf("Flags() = %#x, want stored flags", got)
	}
	salt, kdfTime, kdfMemory, kdfThreads, flags, err := s.Meta()
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
	if again, _, _, _, _, err := s.Meta(); err != nil || again[0] == salt[0] {
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
	if s.Record() != nil || s.File() != nil || s.Series() != nil || s.Vector() != nil {
		t.Fatal("sub-store accessors must return nil after Close")
	}
	if _, _, _, _, _, err := s.Meta(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Metadata after Close = %v, want ErrClosed", err)
	}

	var nilStore *Store
	if err := nilStore.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
	if nilStore.Record() != nil || nilStore.Flags() != 0 {
		t.Fatal("nil receiver accessors must return zero values")
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
			_, err := tx.CreateBucket([]byte(hstore.SYSTEM))
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
			meta, err := tx.CreateBucket([]byte(hstore.SYSTEM))
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
			truncated := md.Encode()
			return meta.Put([]byte("Metadata"), truncated[:len(truncated)-1])
		}); err != nil {
			t.Fatalf("write Metadata: %v", err)
		}
		_ = bdb.Close()

		if _, err := Open(context.Background(), path, 0600, testOptions("pw")); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("Open malformed Metadata = %v, want ErrCorruptStore", err)
		}
	})
}

// TestCanaryBindsEncryptionFlags flips one persisted flag bit and expects Open
// to reject the store instead of silently opening it in another mode.
func TestCanaryBindsEncryptionFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flags.db")
	s := mustOpen(t, path, "pw")
	mustClose(t, s)

	bdb, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	if err := bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(hstore.SYSTEM))
		md, err := system.DecodeMetadata(append([]byte(nil), b.Get([]byte("Metadata"))...))
		if err != nil {
			return err
		}
		md.Flags &^= EncryptRecordKeys
		return b.Put([]byte("Metadata"), md.Encode())
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
		"sub-store root":   {[]byte(hstore.RECORD)},
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
	first, err := system.Derive("password", salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}
	t.Cleanup(first.Wipe)

	keys := map[string][]byte{
		"canary": first.Canary,
		"db":     first.Record,
		"fs":     first.File,
		"ts":     first.Series,
		"vec":    first.Vector,
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

	again, err := system.Derive("password", salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys again: %v", err)
	}
	t.Cleanup(again.Wipe)
	if !bytes.Equal(first.Record, again.Record) {
		t.Fatal("deriveKeys is not deterministic for the same password and salt")
	}

	otherPassword, err := system.Derive("other", salt, FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys other password: %v", err)
	}
	t.Cleanup(otherPassword.Wipe)
	if bytes.Equal(first.Record, otherPassword.Record) {
		t.Fatal("different passwords derived the same key")
	}

	otherSalt, err := system.Derive("password", []byte("fedcba9876543210"), FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys other salt: %v", err)
	}
	t.Cleanup(otherSalt.Wipe)
	if bytes.Equal(first.Record, otherSalt.Record) {
		t.Fatal("different salts derived the same key")
	}
}

func TestStoreKeysWipe(t *testing.T) {
	keys, err := system.Derive("password", []byte("0123456789abcdef"), FastKDFTime, FastKDFMemory, FastKDFThreads)
	if err != nil {
		t.Fatalf("deriveKeys: %v", err)
	}
	keys.Wipe()
	for name, key := range map[string][]byte{
		"canary": keys.Canary,
		"db":     keys.Record,
		"fs":     keys.File,
		"ts":     keys.Series,
		"vec":    keys.Vector,
	} {
		if !bytes.Equal(key, make([]byte, len(key))) {
			t.Errorf("%s key was not wiped", name)
		}
	}
	var nilKeys *system.Keys
	nilKeys.Wipe() // must not panic
}

func TestCanaryHelpers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canary.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	md, keys, err := system.LoadOrCreate(bdb, testOptions("pw").systemConfig())
	require.NoError(t, err)
	defer keys.Wipe()

	sealed, err := system.SealCanary(keys.Canary, md.Flags)
	require.NoError(t, err)
	require.NoError(t, system.VerifyCanary(keys.Canary, sealed, md.Flags))

	// Flags are authenticated: flipping one breaks verification.
	require.Error(t, system.VerifyCanary(keys.Canary, sealed, md.Flags^1))
	// Corrupt ciphertext and a wrong-size key fail cleanly.
	require.Error(t, system.VerifyCanary(keys.Canary, []byte("garbage"), md.Flags))
	require.Error(t, system.VerifyCanary(make([]byte, crypto.KeySize), sealed, md.Flags))
	require.Error(t, system.VerifyCanary([]byte("short"), sealed, md.Flags))

	_, err = system.SealCanary([]byte("short"), md.Flags)
	require.Error(t, err)

	// The state was persisted; reading it again is not a create.
	again, creating, err := system.ReadMetadata(bdb)
	require.NoError(t, err)
	require.False(t, creating)
	require.Equal(t, md.Flags, again.Flags)
}

func TestValidateAndWriteMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	valid := Metadata{
		Salt:    make([]byte, SaltSize),
		Canary:  []byte("canary"),
		Time:    FastKDFTime,
		Memory:  FastKDFMemory,
		Threads: FastKDFThreads,
		Flags:   EncryptAll,
	}

	invalid := []Metadata{
		{Canary: valid.Canary, Time: valid.Time, Memory: valid.Memory, Threads: valid.Threads},
		{Salt: valid.Salt, Time: valid.Time, Memory: valid.Memory, Threads: valid.Threads},
		{Salt: valid.Salt, Canary: valid.Canary},
		{Salt: valid.Salt, Canary: valid.Canary, Time: valid.Time, Memory: valid.Memory, Threads: valid.Threads, Flags: 1 << 15},
	}
	for i, md := range invalid {
		require.ErrorIs(t, system.ValidateMetadata(md), ErrCorruptStore, "case %d", i)
		require.ErrorIs(t, system.WriteMetadata(bdb, md), ErrCorruptStore, "case %d", i)
	}

	require.NoError(t, system.WriteMetadata(bdb, valid))
	read, creating, err := system.ReadMetadata(bdb)
	require.NoError(t, err)
	require.False(t, creating)
	require.Equal(t, valid.Flags, read.Flags)
}

func TestMetadataClosedStore(t *testing.T) {
	s := mustOpen(t, filepath.Join(t.TempDir(), "closed.db"), "pw")
	salt, _, _, _, flags, err := s.Meta()
	require.NoError(t, err)
	require.Len(t, salt, SaltSize)
	require.Equal(t, EncryptAll, flags)

	require.NoError(t, s.Close())
	_, _, _, _, _, err = s.Meta()
	require.ErrorIs(t, err, ErrClosed)
}

func TestLoadOrCreateStateRejectsTampering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tamper.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)

	md, keys, err := system.LoadOrCreate(bdb, testOptions("pw").systemConfig())
	require.NoError(t, err)
	keys.Wipe()
	require.NoError(t, bdb.Close())

	tamper := func(mutate func(*Metadata)) error {
		db, err := bolt.Open(path, 0600, nil)
		require.NoError(t, err)
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("close bolt db: %v", err)
			}
		}()
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(hstore.SYSTEM))
			raw := b.Get([]byte("Metadata"))
			parsed, err := system.DecodeMetadata(raw)
			if err != nil {
				return err
			}
			mutate(&parsed)
			return b.Put([]byte("Metadata"), parsed.Encode())
		}))
		_, _, err = system.LoadOrCreate(db, testOptions("pw").systemConfig())
		return err
	}

	require.ErrorIs(t, tamper(func(m *Metadata) { m.Flags ^= 1 }), ErrInvalidPassword)
	require.ErrorIs(t, tamper(func(m *Metadata) { m.Canary = []byte("garbage") }), ErrInvalidPassword)
	// A changed salt decodes as a different (zero-padded) salt, which the
	// canary reports as a wrong password.
	require.ErrorIs(t, tamper(func(m *Metadata) { m.Salt = []byte("short") }), ErrInvalidPassword)

	// Restoring the original metadata makes the store readable again.
	restore := func(m *Metadata) { *m = md }
	require.NoError(t, tamper(restore))
}

func TestOpenReopenIgnoresCreationOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")
	first := mustOpen(t, path, "pw")
	require.NoError(t, first.Close())

	// A later Open uses the stored KDF/encryption settings even if the caller
	// passes different creation options.
	second, err := Open(context.Background(), path, 0600, &Options{
		Password:   "pw",
		KDFTime:    1,
		KDFMemory:  8 * MiB,
		KDFThreads: 1,
		Plaintext:  true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	require.Equal(t, EncryptAll, second.Flags())
}

func TestReadMetadataMissingRecord(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "empty.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	_, creating, err := system.ReadMetadata(bdb)
	require.NoError(t, err)
	require.True(t, creating)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(hstore.SYSTEM))
		return err
	}))
	_, _, err = system.ReadMetadata(bdb)
	require.True(t, errors.Is(err, ErrCorruptStore))
}

type corruptSubRecord struct {
	ID   int `db:"id"`
	Name string
}

func TestOpenFailsWhenSubstoreIsCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt-sub.db")
	s := mustOpen(t, path, "pw")
	require.NoError(t, s.Record().Save(context.Background(), &corruptSubRecord{ID: 1, Name: "x"}))
	require.NoError(t, s.Close())

	raw, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, raw.Update(func(tx *bolt.Tx) error {
		sub := tx.Bucket([]byte(hstore.RECORD))
		require.NotNil(t, sub)
		info := sub.Bucket([]byte("__database"))
		require.NotNil(t, info)
		marker := info.Get([]byte("\x00__db_enc__"))
		require.NotNil(t, marker)
		corrupt := append([]byte(nil), marker...)
		corrupt[len(corrupt)-1] ^= 0xff
		return info.Put([]byte("\x00__db_enc__"), corrupt)
	}))
	require.NoError(t, raw.Close())

	_, err = Open(context.Background(), path, 0600, testOptions("pw"))
	require.Error(t, err)
}

func TestMetadataOnEmptyDatabase(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "empty.db"), 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	s := &Store{bs: bdb}
	_, _, _, _, _, err = s.Meta()
	require.ErrorIs(t, err, ErrCorruptStore)
}

func TestOpenPathIsDirectory(t *testing.T) {
	_, err := Open(context.Background(), t.TempDir(), 0600, testOptions("pw"))
	require.Error(t, err)
}

func TestWriteMetadataClosedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, bdb.Close())

	md := Metadata{
		Salt:    make([]byte, SaltSize),
		Canary:  []byte("canary"),
		Time:    FastKDFTime,
		Memory:  FastKDFMemory,
		Threads: FastKDFThreads,
		Flags:   EncryptAll,
	}
	require.Error(t, system.WriteMetadata(bdb, md))
}

func TestLoadOrCreateStateReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly.db")
	bdb, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, bdb.Close())

	ro, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() {
		if err := ro.Close(); err != nil {
			t.Errorf("close read-only bolt db: %v", err)
		}
	}()

	// A new store needs to persist metadata, which a read-only handle cannot do.
	_, _, err = system.LoadOrCreate(ro, testOptions("pw").systemConfig())
	require.Error(t, err)
}

func TestOpenRejectsCorruptCanaryAndFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canary.db")
	s := mustOpen(t, path, "pw")
	require.NoError(t, s.Close())

	// Flip a bit in the persisted canary.
	raw, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, raw.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(hstore.SYSTEM))
		value := b.Get([]byte("Metadata"))
		md, err := system.DecodeMetadata(append([]byte(nil), value...))
		if err != nil {
			return err
		}
		md.Canary[0] ^= 0xff
		return b.Put([]byte("Metadata"), md.Encode())
	}))
	require.NoError(t, raw.Close())

	_, err = Open(context.Background(), path, 0600, testOptions("pw"))
	require.ErrorIs(t, err, ErrInvalidPassword)
}

func TestOpenNilReceiverHelpers(t *testing.T) {
	var s *Store
	if got := s.Flags(); got != EncryptNone {
		t.Fatalf("nil Flags() = %#x, want EncryptNone", got)
	}
	_, _, _, _, _, err := s.Meta()
	require.ErrorIs(t, err, ErrClosed)
	if s.Record() != nil || s.File() != nil || s.Series() != nil || s.Vector() != nil {
		t.Fatal("nil sub-store accessors must return nil")
	}
}

func TestAuditTrailEndToEnd(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "harbor-audit.db")
	const password = "correct horse battery staple"

	opts := testOptions(password)
	opts.Audit = AuditAll
	s, err := Open(ctx, path, 0600, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.Equal(t, AuditAll, s.AuditFlags())
	require.NotNil(t, s.Audit())

	// One write per store.
	require.NoError(t, s.Record().Save(ctx, &harborRecord{Name: "alice"}))
	require.NoError(t, s.Series().Series("cpu").Insert(ctx, series.Point{Timestamp: 1, Value: 1}))

	col, err := s.Vector().NewCollection("docs", 3, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{{ID: "a", Vector: []float32{1, 2, 3}}}))
	require.NoError(t, col.Flush())

	vfs, err := s.File().Open("audit_fs")
	require.NoError(t, err)
	f, err := vfs.Create("blob.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	res, err := s.VerifyAudit()
	require.NoError(t, err)
	require.Greater(t, res.Count, uint64(0))
	require.Len(t, res.Tip, 32)

	// The tip matches the recorder's head.
	count, tip, err := s.Audit().Head()
	require.NoError(t, err)
	require.Equal(t, res.Count, count)
	require.Equal(t, res.Tip, tip)
}

func TestAuditDisabledHasNoTrail(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "harbor-noaudit.db")

	s := mustOpen(t, path, "pw")
	t.Cleanup(func() { _ = s.Close() })

	require.Equal(t, AuditNone, s.AuditFlags())
	require.Nil(t, s.Audit())

	require.NoError(t, s.Record().Save(ctx, &harborRecord{Name: "alice"}))

	_, err := s.VerifyAudit()
	require.ErrorIs(t, err, audit.ErrNoLog)
}

func TestAuditWrongPasswordIsRejectedOnOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "harbor-audit-pw.db")

	opts := testOptions("right")
	opts.Audit = AuditRecord
	s, err := Open(ctx, path, 0600, opts)
	require.NoError(t, err)
	require.NoError(t, s.Record().Save(ctx, &harborRecord{Name: "alice"}))
	require.NoError(t, s.Close())

	// The audit signing keys derive from the same seed as the encryption keys,
	// so a wrong password is caught by the canary at Open. Verification is
	// therefore never reachable with a key that could not have signed the
	// trail: a wrong password cannot even open the store.
	_, err = Open(ctx, path, 0600, testOptions("wrong"))
	require.ErrorIs(t, err, ErrInvalidPassword)
}

type rootedRow struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

// TestRootedViewsAreIsolatedAndPersist proves the variadic bucket path on the
// store accessors selects a rooted view instead of the conventional root, and
// that the view is flushed and reopened correctly.
func TestRootedViewsAreIsolatedAndPersist(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rooted.db")
	s := mustOpen(t, path, "rooted-password")

	// Record: a custom root is separate from the conventional "db" root.
	require.NoError(t, s.Record("tenant", "records").Save(ctx, &rootedRow{ID: "r1", Name: "custom"}))
	require.NoError(t, s.Record().Save(ctx, &rootedRow{ID: "r2", Name: "default"}))

	var row rootedRow
	require.NoError(t, s.Record("tenant", "records").One(ctx, "ID", "r1", &row))
	require.Equal(t, "custom", row.Name)
	require.Error(t, s.Record().One(ctx, "ID", "r1", &row), "the default root must not see a custom-root record")
	require.NoError(t, s.Record().One(ctx, "ID", "r2", &row))

	// File.
	tenantFS, err := s.File("tenant").Open("app")
	require.NoError(t, err)
	f, err := tenantFS.Create("/note.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("tenant"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	defaultFS, err := s.File().Open("app")
	require.NoError(t, err)
	_, err = defaultFS.ReadFile("/note.txt")
	require.Error(t, err, "the default root must not see a custom-root fstree")

	// Series.
	require.NoError(t, s.Series("tenant").Series("cpu").Insert(ctx, series.Point{Timestamp: 1_000, Value: 7}))
	defaultPoints, err := s.Series().Series("cpu").Range(ctx, 0, 10_000)
	require.NoError(t, err)
	require.Empty(t, defaultPoints)
	tenantPoints, err := s.Series("tenant").Series("cpu").Range(ctx, 0, 10_000)
	require.NoError(t, err)
	require.Len(t, tenantPoints, 1)

	// Vector.
	col, err := s.Vector("tenant").NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.NoError(t, col.Upsert([]vector.PointStruct{{ID: "p1", Vector: []float32{1, 0}}}))
	defaultHits, err := s.Vector().NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.Zero(t, defaultHits.Count())
	require.Equal(t, 1, col.Count())

	// Search.
	idx, err := s.Search("tenant").Open("docs", search.Config{})
	require.NoError(t, err)
	require.NoError(t, idx.Upsert(ctx, "d1", "tenant text"))
	defaultIdx, err := s.Search().Open("docs", search.Config{})
	require.NoError(t, err)
	defaultCount, err := defaultIdx.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, defaultCount)

	// The base Flush drains vector views too.
	require.NoError(t, s.Vector().Flush())
	require.NoError(t, s.Close())

	// Reopen: custom roots persisted and remain separate from the defaults.
	reopened := mustOpen(t, path, "rooted-password")
	defer mustClose(t, reopened)

	require.NoError(t, reopened.Record("tenant", "records").One(ctx, "ID", "r1", &row))
	require.Equal(t, "custom", row.Name)
	require.NoError(t, reopened.Record().One(ctx, "ID", "r2", &row))

	reopenedCol, err := reopened.Vector("tenant").NewCollection("docs", 2, vector.Cosine, false)
	require.NoError(t, err)
	require.Equal(t, 1, reopenedCol.Count())

	reopenedIdx, err := reopened.Search("tenant").Open("docs", search.Config{})
	require.NoError(t, err)
	count, err := reopenedIdx.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestAccessorPathsDoNotChangeDefaults keeps the no-argument accessors on the
// legacy on-disk roots so existing databases stay readable.
func TestAccessorPathsDoNotChangeDefaults(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "defaults.db")
	s := mustOpen(t, path, "default-password")
	require.NoError(t, s.Record().Save(ctx, &rootedRow{ID: "d1"}))
	require.NoError(t, s.Close())

	reopened := mustOpen(t, path, "default-password")
	defer mustClose(t, reopened)
	var row rootedRow
	require.NoError(t, reopened.Record().One(ctx, "ID", "d1", &row))
}

// TestSingleVersionOnePolicy pins the first-release policy: every persisted
// format has exactly one version, number 1. A future format change must bump the
// relevant exported constant and ship a compatibility path, which makes this
// test fail until that change is deliberate and documented.
func TestSingleVersionOnePolicy(t *testing.T) {
	require.Equal(t, "1.0.0", record.Version)
	require.Equal(t, uint64(1), file.FormatVersion)
	require.Equal(t, uint64(1), series.FormatVersion)
	require.Equal(t, uint32(1), vector.CollectionFormatVersion)
	require.Equal(t, 1, audit.FormatVersion)
}

// TestOptionsValidate covers the creation-time option checks exposed by
// Options.Validate and enforced by Open.
func TestOptionsValidate(t *testing.T) {
	var nilOptions *Options
	require.ErrorIs(t, nilOptions.Validate(), ErrInvalidOptions)

	require.ErrorIs(t, (&Options{}).Validate(), ErrInvalidPassword)

	valid := testOptions("pw")
	require.NoError(t, valid.Validate())

	partial := testOptions("pw")
	partial.KDFThreads = 0
	require.ErrorIs(t, partial.Validate(), ErrInvalidOptions)

	conflict := testOptions("pw")
	conflict.Plaintext = true
	conflict.Encrypt = EncryptAll
	require.ErrorIs(t, conflict.Validate(), ErrInvalidOptions)

	unknownEnc := testOptions("pw")
	unknownEnc.Encrypt = 1 << 15
	require.ErrorIs(t, unknownEnc.Validate(), ErrInvalidOptions)

	unknownAudit := testOptions("pw")
	unknownAudit.Audit = 1 << 15
	require.ErrorIs(t, unknownAudit.Validate(), ErrInvalidOptions)

	beyondLimits := testOptions("pw")
	beyondLimits.KDFTime = system.MaxKDFTime + 1
	require.ErrorIs(t, beyondLimits.Validate(), ErrInvalidOptions)

	plaintext := testOptions("pw")
	plaintext.Plaintext = true
	require.NoError(t, plaintext.Validate())
}

func TestOpenRejectsUnknownAuditFlags(t *testing.T) {
	opts := testOptions("pw")
	opts.Audit = 1 << 15
	_, err := Open(context.Background(), filepath.Join(t.TempDir(), "harbor.db"), 0o600, opts)
	require.ErrorIs(t, err, ErrInvalidOptions)
}

// TestOptionsValidateMatchesOpen ties the public validation to Open for a valid
// and an invalid configuration.
func TestOptionsValidateMatchesOpen(t *testing.T) {
	dir := t.TempDir()

	valid := testOptions("pw")
	require.NoError(t, valid.Validate())
	store, err := Open(context.Background(), filepath.Join(dir, "valid.db"), 0o600, valid)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	invalid := testOptions("pw")
	invalid.KDFTime = system.MaxKDFTime + 1
	require.ErrorIs(t, invalid.Validate(), ErrInvalidOptions)
	_, err = Open(context.Background(), filepath.Join(dir, "invalid.db"), 0o600, invalid)
	require.ErrorIs(t, err, ErrInvalidOptions)
}

type sessionState struct {
	User  string   `json:"user"`
	Roles []string `json:"roles"`
}

// TestFSSessionSurvivesReopen documents the application-session pattern: a
// named fstree system holds JSON files, and the store reopens with the same
// contents.
func TestFSSessionSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "app.db")

	s := mustOpen(t, path, "session-password")
	fsys, err := s.File().Open("app")
	require.NoError(t, err)
	require.NoError(t, fsys.MkdirAll("/sessions", 0o755))

	state := sessionState{User: "alice", Roles: []string{"admin", "editor"}}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	f, err := fsys.Create("/sessions/alice.json")
	require.NoError(t, err)
	_, err = f.Write(raw)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	mustClose(t, s)

	reopened := mustOpen(t, path, "session-password")
	defer mustClose(t, reopened)

	app, err := reopened.File().Open("app")
	require.NoError(t, err)
	body, err := app.ReadFile("/sessions/alice.json")
	require.NoError(t, err)

	var got sessionState
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, state.User, got.User)
	require.Equal(t, state.Roles, got.Roles)

	// The session fstree is part of the encrypted at-rest store.
	_ = ctx
}
