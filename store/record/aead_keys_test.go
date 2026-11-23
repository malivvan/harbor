package record

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// aeadKeyConfig returns a config that encrypts everything the db supports.
func aeadKeyConfig() Config {
	return Config{
		Secret:          aeadKey(11),
		EncryptValues:   true,
		EncryptMetadata: true,
		EncryptKeys:     true,
	}
}

// TestAEADDeterministicKeyEncryption covers the SIV helper directly:
// determinism, domain separation and authentication.
func TestAEADDeterministicKeyEncryption(t *testing.T) {
	enc, err := resolveConfig(Config{Secret: aeadKey(9), EncryptKeys: true})
	require.NoError(t, err)
	require.NotNil(t, enc)

	plain := []byte("42")

	id, err := enc.sealKey(plain, keyDomainID)
	require.NoError(t, err)
	idAgain, err := enc.sealKey(plain, keyDomainID)
	require.NoError(t, err)
	require.Equal(t, id, idAgain, "the same value in the same domain must map to the same key")

	index, err := enc.sealKey(plain, keyDomainIndex)
	require.NoError(t, err)
	kv, err := enc.sealKey(plain, keyDomainKV)
	require.NoError(t, err)
	require.NotEqual(t, id, index, "domains must be separated")
	require.NotEqual(t, id, kv)
	require.NotEqual(t, index, kv)

	other, err := enc.sealKey([]byte("43"), keyDomainID)
	require.NoError(t, err)
	require.NotEqual(t, id, other, "distinct values must produce distinct keys")

	require.NotContains(t, string(id), "42")

	opened, err := enc.openKey(id, keyDomainID)
	require.NoError(t, err)
	require.Equal(t, plain, opened)

	_, err = enc.openKey(id, keyDomainIndex)
	require.ErrorIs(t, err, ErrAuthFailed, "a key of another domain must not authenticate")

	// A plaintext store must not touch keys at all.
	plainEnc, err := resolveConfig(Config{})
	require.NoError(t, err)
	require.Nil(t, plainEnc)
	raw, err := plainEnc.sealKey(plain, keyDomainID)
	require.NoError(t, err)
	require.Equal(t, plain, raw)
}

// TestAEADKeyEncryptionHidesKeys checks that the stored keys no longer reveal
// ids or key/value keys.
func TestAEADKeyEncryptionHidesKeys(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()
	st := aeadOpenStore(t, bdb, aeadKeyConfig())

	require.NoError(t, st.Save(ctx, &aeadUser{ID: 7, Name: "alice", Email: "alice@example.com", Group: "staff"}))
	require.NoError(t, st.Set(ctx, "aeadkv", "customer", "acme"))

	rawID, err := toBytes(7, nil)
	require.NoError(t, err)

	records := aeadRawEntries(t, bdb, "aeadUser")
	require.Len(t, records, 1)
	require.NotContains(t, records, string(rawID), "the primary key must be encrypted")
	for key := range records {
		require.NotContains(t, []byte(key), rawID, "the encoded primary key must not appear in the stored key")
	}

	kv := aeadRawEntries(t, bdb, "aeadkv")
	require.NotContains(t, kv, "customer", "key/value keys must be encrypted")
	require.Len(t, kv, 1)
}

// TestAEADKeyEncryptionRoundTrip proves exact lookups keep working when keys
// are encrypted.
func TestAEADKeyEncryptionRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()
	st := aeadOpenStore(t, bdb, aeadKeyConfig())

	users := []*aeadUser{
		{Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30},
		{Name: "bob", Email: "bob@example.com", Group: "staff", Age: 42},
		{Name: "carol", Email: "carol@example.com", Group: "guest", Age: 25},
	}
	for _, u := range users {
		require.NoError(t, st.Save(ctx, u))
	}

	// Primary-key lookup.
	var byID aeadUser
	require.NoError(t, st.One(ctx, "ID", users[1].ID, &byID))
	require.Equal(t, "bob", byID.Name)

	// Unique index lookup.
	var byEmail aeadUser
	require.NoError(t, st.One(ctx, "Email", "carol@example.com", &byEmail))
	require.Equal(t, users[2].ID, byEmail.ID)

	// List index exact lookup.
	var staff []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "staff", &staff))
	require.Equal(t, []int{users[0].ID, users[1].ID}, aeadIDs(aeadSortUsers(staff)))

	// Scans still work; records come back in ciphertext key order.
	var all []aeadUser
	require.NoError(t, st.All(ctx, &all))
	require.Equal(t, []int{users[0].ID, users[1].ID, users[2].ID}, aeadIDs(aeadSortUsers(all)))

	seen := 0
	require.NoError(t, st.Select().Each(ctx, &aeadUser{}, func(any) error {
		seen++
		return nil
	}))
	require.Equal(t, 3, seen)

	count, err := st.Count(ctx, &aeadUser{})
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Updates keep indexes consistent.
	require.NoError(t, st.Update(ctx, &aeadUser{ID: users[0].ID, Group: "core"}))
	var staffAfter []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "staff", &staffAfter))
	require.Equal(t, []int{users[1].ID}, aeadIDs(aeadSortUsers(staffAfter)))

	var core []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "core", &core))
	require.Equal(t, []int{users[0].ID}, aeadIDs(aeadSortUsers(core)))

	// Unique index collisions are still detected.
	err = st.Save(ctx, &aeadUser{Name: "dave", Email: "alice@example.com"})
	require.ErrorIs(t, err, ErrAlreadyExists)

	// Key/value operations.
	require.NoError(t, st.Set(ctx, "aeadkv", "customer", aeadPayload{Name: "acme", N: 1}))
	var payload aeadPayload
	require.NoError(t, st.Get(ctx, "aeadkv", "customer", &payload))
	require.Equal(t, "acme", payload.Name)

	require.NoError(t, st.SetBytes(ctx, "aeadkv", "blob", []byte("raw-bytes")))
	blob, err := st.GetBytes(ctx, "aeadkv", "blob")
	require.NoError(t, err)
	require.Equal(t, "raw-bytes", string(blob))

	exists, err := st.KeyExists(ctx, "aeadkv", "customer")
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, st.Delete(ctx, "aeadkv", "customer"))
	exists, err = st.KeyExists(ctx, "aeadkv", "customer")
	require.NoError(t, err)
	require.False(t, exists)

	// Deletion and reindexing.
	require.NoError(t, st.DeleteStruct(ctx, &aeadUser{ID: users[2].ID}))
	var remaining []aeadUser
	require.NoError(t, st.All(ctx, &remaining))
	require.Equal(t, []int{users[0].ID, users[1].ID}, aeadIDs(aeadSortUsers(remaining)))

	require.NoError(t, st.ReIndex(ctx, &aeadUser{}))
	var reindexed []aeadUser
	require.NoError(t, st.All(ctx, &reindexed))
	require.Equal(t, []int{users[0].ID, users[1].ID}, aeadIDs(aeadSortUsers(reindexed)))

	var staffReindexed []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "staff", &staffReindexed))
	require.Equal(t, []int{users[1].ID}, aeadIDs(aeadSortUsers(staffReindexed)))
}

// TestAEADEncryptedKeysRejectOrderedQueries proves ordered and prefix lookups
// fail loud instead of returning ciphertext-ordered results.
func TestAEADEncryptedKeysRejectOrderedQueries(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()
	st := aeadOpenStore(t, bdb, aeadKeyConfig())

	require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))
	require.NoError(t, st.Save(ctx, &aeadUser{ID: 2, Name: "bob", Email: "bob@example.com", Group: "staff", Age: 42}))

	var ranged []aeadUser
	err := st.Range(ctx, "Age", 20, 50, &ranged)
	require.ErrorIs(t, err, ErrEncryptedOrdering)

	var prefixed []aeadUser
	err = st.Prefix(ctx, "Name", "al", &prefixed)
	require.ErrorIs(t, err, ErrEncryptedOrdering)

	var byIndex []aeadUser
	err = st.AllByIndex(ctx, "Group", &byIndex)
	require.ErrorIs(t, err, ErrEncryptedOrdering)

	// Reverse over an index scan is rejected; a scan over a non-indexed field
	// stays available because it decodes records instead of trusting key order.
	var reversed []aeadUser
	err = st.Find(ctx, "Name", "alice", &reversed, Reverse())
	require.ErrorIs(t, err, ErrEncryptedOrdering)

	var scanned []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "staff", &scanned))
	require.Len(t, scanned, 2)

	// A plaintext store keeps all of them working.
	plainPath := filepath.Join(t.TempDir(), "plain.db")
	plainDB := aeadOpenBolt(t, plainPath)
	defer plainDB.Close()
	plain := aeadOpenStore(t, plainDB, Config{})
	require.NoError(t, plain.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))
	require.NoError(t, plain.Range(ctx, "Age", 20, 50, &ranged))
	require.NoError(t, plain.Prefix(ctx, "Name", "al", &prefixed))
	require.NoError(t, plain.AllByIndex(ctx, "Age", &byIndex))
}

// TestAEADEncryptedKeysSurviveReopen proves encrypted keys are stable across
// store instances opened with the same key.
func TestAEADEncryptedKeysSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	cfg := aeadKeyConfig()

	bdb := aeadOpenBolt(t, path)
	st := aeadOpenStore(t, bdb, cfg)
	require.NoError(t, st.Save(ctx, &aeadUser{ID: 5, Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))
	require.NoError(t, bdb.Close())

	bdb = aeadOpenBolt(t, path)
	defer bdb.Close()
	st = aeadOpenStore(t, bdb, cfg)

	var u aeadUser
	require.NoError(t, st.One(ctx, "ID", 5, &u))
	require.Equal(t, "alice", u.Name)

	var byEmail aeadUser
	require.NoError(t, st.One(ctx, "Email", "alice@example.com", &byEmail))
	require.Equal(t, 5, byEmail.ID)

	var staff []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "staff", &staff))
	require.Len(t, staff, 1)

	// A wrong key must fail before any key can be derived.
	otherPath := filepath.Join(t.TempDir(), "other.db")
	otherDB := aeadOpenBolt(t, otherPath)
	defer otherDB.Close()
	_, err := Open(ctx, otherDB, Config{
		Secret:          aeadKey(12),
		EncryptValues:   true,
		EncryptMetadata: true,
		EncryptKeys:     true,
	})
	require.NoError(t, err)
}

// aeadIDs extracts the ids of users.
func aeadIDs(users []aeadUser) []int {
	ids := make([]int, len(users))
	for i, u := range users {
		ids[i] = u.ID
	}
	return ids
}

// TestAEADBucketScannerWithEncryptedKeys proves bucket scans and drops keep
// working, since bucket names are never encrypted.
func TestAEADBucketScannerWithEncryptedKeys(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()
	st := aeadOpenStore(t, bdb, aeadKeyConfig())

	require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com", Group: "staff"}))
	require.NoError(t, st.From("nested").Save(ctx, &aeadUser{ID: 2, Name: "bob", Email: "bob@example.com", Group: "staff"}))

	nodes, err := st.From("nested").PrefixScan(ctx, "")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)

	var u aeadUser
	require.NoError(t, st.From("nested").One(ctx, "ID", 2, &u))
	require.Equal(t, "bob", u.Name)

	require.NoError(t, st.Drop(ctx, &aeadUser{}))
	var all []aeadUser
	require.NoError(t, st.All(ctx, &all))
	require.Empty(t, all)
}

// TestAEADEncryptedKeysRejectTamperedKeys proves a corrupted index key fails
// authentication rather than silently matching nothing.
func TestAEADEncryptedKeysRejectTamperedKeys(t *testing.T) {
	enc, err := resolveConfig(Config{Secret: aeadKey(13), EncryptKeys: true})
	require.NoError(t, err)

	sealed, err := enc.sealKey([]byte("value"), keyDomainID)
	require.NoError(t, err)

	tampered := append([]byte(nil), sealed...)
	tampered[0] ^= 0xff

	_, err = enc.openKey(tampered, keyDomainID)
	require.ErrorIs(t, err, ErrAuthFailed)

	_, err = enc.openKey([]byte("not-base64!!"), keyDomainID)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAuthFailed))
}
