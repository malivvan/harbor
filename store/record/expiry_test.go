package record

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type expiryDoc struct {
	ID       string `db:"id"`
	Category string `db:"index"`
}

func openExpiryStore(t *testing.T, cfg Config) (*Store, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "expiry.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	store, err := Open(context.Background(), bdb, cfg)
	require.NoError(t, err)
	return store, bdb
}

func fixedClock(t *testing.T, store *Store) time.Time {
	t.Helper()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return now })
	return now
}

func TestExpiryHidesRecordFromEveryReadPath(t *testing.T) {
	store, _ := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &expiryDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.SetExpiry(ctx, doc, now.Add(-time.Minute)))

	err := store.One(ctx, "ID", "a", &expiryDoc{})
	require.ErrorIs(t, err, ErrNotFound)

	var all []expiryDoc
	err = store.All(ctx, &all)
	require.NoError(t, err)
	require.Empty(t, all)

	var byIndex []expiryDoc
	err = store.Find(ctx, "Category", "x", &byIndex)
	require.ErrorIs(t, err, ErrNotFound)

	var byAllIndex []expiryDoc
	err = store.AllByIndex(ctx, "Category", &byAllIndex)
	if err != nil {
		require.ErrorIs(t, err, ErrNotFound)
	}
	require.Empty(t, byAllIndex)

	count, err := store.Count(ctx, &expiryDoc{})
	require.NoError(t, err)
	require.Zero(t, count)

	var many []expiryDoc
	missing, err := store.GetMany(ctx, []any{"a"}, &many)
	require.NoError(t, err)
	require.Equal(t, []any{"a"}, missing)
	require.Empty(t, many)

	iter, err := store.Select().Iter(ctx, &expiryDoc{})
	require.NoError(t, err)
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())

	// The audit escape hatch still sees it.
	var seen expiryDoc
	require.NoError(t, store.One(WithExpired(ctx), "ID", "a", &seen))
	require.Equal(t, "x", seen.Category)

	at, ok, err := store.Expiry(ctx, doc)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, at.Equal(now.Add(-time.Minute)))
}

func TestExpirySweep(t *testing.T) {
	store, _ := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	for _, id := range []string{"a", "b"} {
		doc := &expiryDoc{ID: id, Category: "x"}
		require.NoError(t, store.Save(ctx, doc))
		require.NoError(t, store.SetExpiry(ctx, doc, now.Add(-time.Minute)))
	}
	live := &expiryDoc{ID: "c", Category: "x"}
	require.NoError(t, store.Save(ctx, live))
	require.NoError(t, store.SetExpiry(ctx, live, now.Add(time.Hour)))

	// A bounded sweep removes one at a time.
	n, err := store.SweepExpired(ctx, &expiryDoc{}, now, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = store.SweepExpired(ctx, &expiryDoc{}, now, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = store.SweepExpired(ctx, &expiryDoc{}, now, 1)
	require.NoError(t, err)
	require.Zero(t, n)

	// The live record and its index survive.
	var out []expiryDoc
	require.NoError(t, store.Find(ctx, "Category", "x", &out))
	require.Len(t, out, 1)
	require.Equal(t, "c", out[0].ID)
}

func TestExpiryZeroClears(t *testing.T) {
	store, _ := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &expiryDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.SetExpiry(ctx, doc, now.Add(-time.Minute)))
	err := store.One(ctx, "ID", "a", &expiryDoc{})
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, store.SetExpiry(ctx, doc, time.Time{}))
	var got expiryDoc
	require.NoError(t, store.One(ctx, "ID", "a", &got))
	_, ok, err := store.Expiry(ctx, doc)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestExpiryDisabledByDefault(t *testing.T) {
	store, _ := openExpiryStore(t, Config{})
	ctx := context.Background()
	doc := &expiryDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.ErrorIs(t, store.SetExpiry(ctx, doc, time.Now()), ErrExpiryDisabled)
	_, err := store.SweepExpired(ctx, &expiryDoc{}, time.Now(), 0)
	require.ErrorIs(t, err, ErrExpiryDisabled)

	// With expiry never enabled, reads are unchanged.
	var got expiryDoc
	require.NoError(t, store.One(ctx, "ID", "a", &got))
}

func TestExpiryCapabilitySurvivesReopenWithoutOption(t *testing.T) {
	store, bdb := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &expiryDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.SetExpiry(ctx, doc, now.Add(-time.Minute)))

	// A build that forgets Config.Expiry still filters, because the capability
	// bit records that the store contains expiring records.
	reopened, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	reopened.SetNow(func() time.Time { return now })
	err = reopened.One(ctx, "ID", "a", &expiryDoc{})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestExpiryUnknownCapabilityFailsClosed(t *testing.T) {
	store, bdb := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	doc := &expiryDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.SetExpiry(ctx, doc, time.Now().Add(time.Hour)))

	raw, err := store.GetBytes(ctx, databaseInfo, capabilitiesKey)
	require.NoError(t, err)
	require.Len(t, raw, 8)
	caps := binary.BigEndian.Uint64(raw) | (1 << 40)
	raw = make([]byte, 8)
	binary.BigEndian.PutUint64(raw, caps)
	require.NoError(t, store.SetBytes(ctx, databaseInfo, capabilitiesKey, raw))

	_, err = Open(ctx, bdb, Config{})
	require.ErrorIs(t, err, ErrIncompatibleVersion)
}

func TestExpiryUpdateStillWorksOnExpiredRecord(t *testing.T) {
	store, _ := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &expiryDoc{ID: "a", Category: "old"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.SetExpiry(ctx, doc, now.Add(-time.Minute)))

	// Maintenance paths may read and update an expired record.
	require.NoError(t, store.Update(ctx, &expiryDoc{ID: "a", Category: "new"}))
	var seen expiryDoc
	require.NoError(t, store.One(WithExpired(ctx), "ID", "a", &seen))
	require.Equal(t, "new", seen.Category)

	// It stays hidden until the expiry is cleared or the sweep removes it.
	err := store.One(ctx, "ID", "a", &expiryDoc{})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestExpiryEncryptedMetadata(t *testing.T) {
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = 9
	}
	store, bdb := openExpiryStore(t, Config{Secret: secret, EncryptValues: true, EncryptMetadata: true, Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &expiryDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.SetExpiry(ctx, doc, now.Add(time.Hour)))

	reopened, err := Open(ctx, bdb, Config{Secret: secret, EncryptValues: true, EncryptMetadata: true})
	require.NoError(t, err)
	reopened.SetNow(func() time.Time { return now })
	at, ok, err := reopened.Expiry(ctx, doc)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, at.Equal(now.Add(time.Hour)))

	// One hour later it is hidden and sweepable.
	later := now.Add(2 * time.Hour)
	reopened.SetNow(func() time.Time { return later })
	err = reopened.One(ctx, "ID", "a", &expiryDoc{})
	require.ErrorIs(t, err, ErrNotFound)
	n, err := reopened.SweepExpired(ctx, &expiryDoc{}, later, 0)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, reopened.VerifyAll(ctx))
}

func TestExpirySweepStaleIndexIsIdempotent(t *testing.T) {
	store, _ := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	doc := &expiryDoc{ID: "a", Category: "x"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.SetExpiry(ctx, doc, now.Add(-time.Minute)))
	// Delete the record directly; the index entry remains.
	require.NoError(t, store.DeleteStruct(ctx, doc))

	n, err := store.SweepExpired(ctx, &expiryDoc{}, now, 0)
	require.NoError(t, err)
	require.Zero(t, n)

	// A second sweep is still clean.
	n, err = store.SweepExpired(ctx, &expiryDoc{}, now, 0)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestExpiryGeneralityCacheTTL(t *testing.T) {
	// A non-companion caller: a cache entry with a TTL.
	type cacheEntry struct {
		Key   string `db:"id"`
		Value string
	}
	store, _ := openExpiryStore(t, Config{Expiry: true})
	ctx := context.Background()
	now := fixedClock(t, store)

	entry := &cacheEntry{Key: "k", Value: "v"}
	require.NoError(t, store.Save(ctx, entry))
	require.NoError(t, store.SetExpiry(ctx, entry, now.Add(time.Minute)))

	var got cacheEntry
	require.NoError(t, store.One(ctx, "Key", "k", &got))

	store.SetNow(func() time.Time { return now.Add(2 * time.Minute) })
	err := store.One(ctx, "Key", "k", &got)
	require.True(t, errors.Is(err, ErrNotFound))
}
