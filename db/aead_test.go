package db

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// aeadUser is the record type used by the encryption tests.
type aeadUser struct {
	ID    int    `storm:"id,increment"`
	Name  string `storm:"index"`
	Email string `storm:"unique"`
	Group string
	Age   int `storm:"index"`
}

// aeadPayload is the key/value payload used by the encryption tests.
type aeadPayload struct {
	Name string
	N    int
}

// aeadKey returns a deterministic 32-byte test key.
func aeadKey(fill byte) []byte { return bytes.Repeat([]byte{fill}, KeySize) }

// aeadOpenBolt opens a bbolt database at path.
func aeadOpenBolt(t *testing.T, path string) *bolt.DB {
	t.Helper()
	bdb, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	return bdb
}

// aeadOpenStore opens a Storm store on bdb with the given encryption config.
func aeadOpenStore(t *testing.T, bdb *bolt.DB, cfg *Config) *Store {
	t.Helper()
	st, err := Open(context.Background(), bdb, nil, cfg)
	require.NoError(t, err)
	return st
}

// aeadRawEntries returns the raw key/value pairs of a bucket path, skipping
// nested buckets.
func aeadRawEntries(t *testing.T, bdb *bolt.DB, path ...string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		for _, name := range path {
			if b == nil {
				b = tx.Bucket([]byte(name))
			} else {
				b = b.Bucket([]byte(name))
			}
			if b == nil {
				return nil
			}
		}
		return b.ForEach(func(k, v []byte) error {
			if v != nil {
				out[string(k)] = append([]byte(nil), v...)
			}
			return nil
		})
	}))
	return out
}

// aeadSortUsers returns users sorted by ID so comparisons ignore iteration
// order.
func aeadSortUsers(users []aeadUser) []aeadUser {
	out := append([]aeadUser(nil), users...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// aeadExercise runs the same operation sequence against a store and returns
// everything an observer can see.
func aeadExercise(t *testing.T, st *Store) map[string]any {
	t.Helper()
	ctx := context.Background()
	out := map[string]any{}

	users := []*aeadUser{
		{Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30},
		{Name: "bob", Email: "bob@example.com", Group: "staff", Age: 42},
		{Name: "carol", Email: "carol@example.com", Group: "guest", Age: 25},
	}
	for _, u := range users {
		require.NoError(t, st.Save(ctx, u))
		require.NotZero(t, u.ID)
	}
	out["ids"] = []int{users[0].ID, users[1].ID, users[2].ID}

	var byID aeadUser
	require.NoError(t, st.One(ctx, "ID", users[1].ID, &byID))
	out["oneByID"] = byID

	var byEmail aeadUser
	require.NoError(t, st.One(ctx, "Email", "carol@example.com", &byEmail))
	out["oneByUnique"] = byEmail

	var staff []aeadUser
	require.NoError(t, st.Find(ctx, "Group", "staff", &staff))
	out["find"] = aeadSortUsers(staff)

	var all []aeadUser
	require.NoError(t, st.All(ctx, &all))
	out["all"] = aeadSortUsers(all)

	var ranged []aeadUser
	require.NoError(t, st.Range(ctx, "Age", 26, 45, &ranged))
	out["range"] = aeadSortUsers(ranged)

	var prefixed []aeadUser
	require.NoError(t, st.Prefix(ctx, "Name", "al", &prefixed))
	out["prefix"] = aeadSortUsers(prefixed)

	require.NoError(t, st.Update(ctx, &aeadUser{ID: users[0].ID, Group: "lead"}))
	var updated aeadUser
	require.NoError(t, st.One(ctx, "ID", users[0].ID, &updated))
	out["updated"] = updated

	require.NoError(t, st.UpdateField(ctx, &aeadUser{ID: users[1].ID}, "Group", "core"))
	var updatedField aeadUser
	require.NoError(t, st.One(ctx, "ID", users[1].ID, &updatedField))
	out["updatedField"] = updatedField

	count, err := st.Count(ctx, &aeadUser{})
	require.NoError(t, err)
	out["count"] = count

	require.NoError(t, st.ReIndex(ctx, &aeadUser{}))
	var afterReIndex []aeadUser
	require.NoError(t, st.All(ctx, &afterReIndex))
	out["afterReIndex"] = aeadSortUsers(afterReIndex)

	require.NoError(t, st.Set(ctx, "aeadkv", "k1", aeadPayload{Name: "x", N: 1}))
	var payload aeadPayload
	require.NoError(t, st.Get(ctx, "aeadkv", "k1", &payload))
	out["kv"] = payload

	require.NoError(t, st.SetBytes(ctx, "aeadkv", "k2", []byte("raw-bytes")))
	blob, err := st.GetBytes(ctx, "aeadkv", "k2")
	require.NoError(t, err)
	out["kvBytes"] = string(blob)

	exists, err := st.KeyExists(ctx, "aeadkv", "k1")
	require.NoError(t, err)
	out["keyExists"] = exists

	require.NoError(t, st.Delete(ctx, "aeadkv", "k1"))
	var gone aeadPayload
	err = st.Get(ctx, "aeadkv", "k1", &gone)
	require.Error(t, err)
	out["deletedKVIsNotFound"] = errors.Is(err, ErrNotFound)

	require.NoError(t, st.DeleteStruct(ctx, &aeadUser{ID: users[2].ID}))
	var afterDelete []aeadUser
	require.NoError(t, st.All(ctx, &afterDelete))
	out["afterDelete"] = aeadSortUsers(afterDelete)

	require.NoError(t, st.Drop(ctx, &aeadUser{}))
	var afterDrop []aeadUser
	require.NoError(t, st.All(ctx, &afterDrop))
	out["afterDrop"] = len(afterDrop)

	return out
}

// TestAEADOptionalityKeepsPlaintextFormat proves that a store opened without
// encryption writes byte-for-byte the same data as before: raw codec values,
// raw keys and no encryption marker.
func TestAEADOptionalityKeepsPlaintextFormat(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "plain.db")

	bdb := aeadOpenBolt(t, path)
	st := aeadOpenStore(t, bdb, nil)
	require.Nil(t, st.enc)

	u := &aeadUser{ID: 7, Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}
	require.NoError(t, st.Save(ctx, u))
	require.NoError(t, st.Set(ctx, "aeadsettings", "theme", "dark"))
	require.NoError(t, st.SetBytes(ctx, "aeadsettings", "blob", []byte{1, 2, 3}))

	rawID, err := toBytes(7, nil)
	require.NoError(t, err)
	rawUser, err := JSONCodec.Marshal(u)
	require.NoError(t, err)

	records := aeadRawEntries(t, bdb, "aeadUser")
	require.Len(t, records, 1)
	require.Equal(t, rawUser, records[string(rawID)])

	settings := aeadRawEntries(t, bdb, "aeadsettings")
	rawTheme, err := JSONCodec.Marshal("dark")
	require.NoError(t, err)
	require.Equal(t, rawTheme, settings["theme"])
	require.Equal(t, []byte{1, 2, 3}, settings["blob"])

	info := aeadRawEntries(t, bdb, databaseInfo)
	require.NotContains(t, info, markerKey, "a plaintext store must not write an encryption marker")
	rawVersion, err := JSONCodec.Marshal(Version)
	require.NoError(t, err)
	require.Equal(t, rawVersion, info[versionKey])
	require.NoError(t, bdb.Close())

	// Reopening the same file with an explicit Config that enables no feature
	// must leave the bytes untouched.
	bdb = aeadOpenBolt(t, path)
	defer bdb.Close()
	st = aeadOpenStore(t, bdb, &Config{})
	require.Nil(t, st.enc)

	records = aeadRawEntries(t, bdb, "aeadUser")
	require.Equal(t, rawUser, records[string(rawID)])
	info = aeadRawEntries(t, bdb, databaseInfo)
	require.NotContains(t, info, markerKey)
	require.Equal(t, rawVersion, info[versionKey])
}

// TestAEADRoundTripMatchesPlaintext runs the same operations on a plaintext
// control store and on an encrypted store and requires identical results.
func TestAEADRoundTripMatchesPlaintext(t *testing.T) {
	dir := t.TempDir()

	plainDB := aeadOpenBolt(t, filepath.Join(dir, "plain.db"))
	plain := aeadOpenStore(t, plainDB, nil)
	want := aeadExercise(t, plain)
	require.NoError(t, plainDB.Close())

	encDB := aeadOpenBolt(t, filepath.Join(dir, "enc.db"))
	enc := aeadOpenStore(t, encDB, &Config{
		Secret:          aeadKey(1),
		EncryptValues:   true,
		EncryptMetadata: true,
	})
	got := aeadExercise(t, enc)
	require.NoError(t, encDB.Close())

	require.Equal(t, want, got)
}

// TestAEADHidesValuesAndCounters checks what an encrypted store leaves in the
// bbolt file: readable keys (Tier 1 does not encrypt keys) but sealed values
// and sealed auto-increment counters.
func TestAEADHidesValuesAndCounters(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()
	st := aeadOpenStore(t, bdb, &Config{Secret: aeadKey(3), EncryptValues: true, EncryptMetadata: true})

	require.NoError(t, st.Save(ctx, &aeadUser{Name: "alice", Email: "alice@example.com", Group: "staff", Age: 30}))

	records := aeadRawEntries(t, bdb, "aeadUser")
	require.Len(t, records, 1)
	for _, value := range records {
		require.NotContains(t, string(value), "alice")
		require.NotContains(t, string(value), "staff")
	}

	rawID, err := toBytes(1, nil)
	require.NoError(t, err)
	require.Contains(t, records, string(rawID), "tier 1 leaves primary keys readable")

	meta := aeadRawEntries(t, bdb, "aeadUser", metadataBucket)
	require.Equal(t, "json", string(meta[metaCodec]))
	counter, ok := meta["IDcounter"]
	require.True(t, ok)
	plainCounter, err := numbertob(int64(1))
	require.NoError(t, err)
	require.NotEqual(t, plainCounter, counter, "auto-increment counters must be sealed")

	var u aeadUser
	require.NoError(t, st.Save(ctx, &aeadUser{Name: "bob", Email: "bob@example.com"}))
	require.NoError(t, st.One(ctx, "ID", 2, &u))
	require.Equal(t, "bob", u.Name)
}

// TestAEADWrongKeyFails proves the canary rejects an incorrect key while the
// right key keeps working.
func TestAEADWrongKeyFails(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	cfg := &Config{Secret: aeadKey(1), EncryptValues: true}

	bdb := aeadOpenBolt(t, path)
	st := aeadOpenStore(t, bdb, cfg)
	require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com"}))
	require.NoError(t, bdb.Close())

	bdb = aeadOpenBolt(t, path)
	_, err := Open(ctx, bdb, nil, &Config{Secret: aeadKey(2), EncryptValues: true})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAuthFailed)
	require.NoError(t, bdb.Close())

	bdb = aeadOpenBolt(t, path)
	defer bdb.Close()
	st = aeadOpenStore(t, bdb, cfg)
	var u aeadUser
	require.NoError(t, st.One(ctx, "ID", 1, &u))
	require.Equal(t, "alice", u.Name)
}

// TestAEADTamperedValueFails proves authentication failures surface as
// ErrAuthFailed instead of a wrong result, a "not found" or a panic.
func TestAEADTamperedValueFails(t *testing.T) {
	mutations := map[string]func([]byte) []byte{
		"bit flip": func(raw []byte) []byte {
			out := append([]byte(nil), raw...)
			out[len(out)-1] ^= 0xff
			return out
		},
		"truncated": func(raw []byte) []byte {
			return append([]byte(nil), raw[:len(raw)-4]...)
		},
		// An emptied value stays empty so Set(key, nil) keeps reading back as
		// not found; it fails the codec instead of the MAC.
		"empty": func([]byte) []byte {
			return []byte{}
		},
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "enc.db")

			bdb := aeadOpenBolt(t, path)
			st := aeadOpenStore(t, bdb, &Config{Secret: aeadKey(4), EncryptValues: true})
			require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com"}))

			require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("aeadUser"))
				c := b.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					if v == nil {
						continue
					}
					return b.Put(append([]byte(nil), k...), mutate(append([]byte(nil), v...)))
				}
				return errors.New("no record found to tamper with")
			}))

			var u aeadUser
			err := st.One(ctx, "ID", 1, &u)
			require.Error(t, err)
			if name != "empty" {
				require.ErrorIs(t, err, ErrAuthFailed)
			}
			require.Equal(t, aeadUser{}, u, "a corrupt value must never surface as data")
			require.NoError(t, bdb.Close())
		})
	}
}

// TestAEADFreshNoncePerWrite proves two writes of equal data store different
// ciphertext.
func TestAEADFreshNoncePerWrite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()
	st := aeadOpenStore(t, bdb, &Config{Secret: aeadKey(5), EncryptValues: true})

	require.NoError(t, st.Set(ctx, "aeadnonce", "a", "same"))
	require.NoError(t, st.Set(ctx, "aeadnonce", "b", "same"))

	entries := aeadRawEntries(t, bdb, "aeadnonce")
	require.Contains(t, entries, "a")
	require.Contains(t, entries, "b")
	require.NotEqual(t, entries["a"], entries["b"], "each write must use a fresh nonce")
	require.NotContains(t, string(entries["a"]), "same")
}

// TestAEADModeMismatch covers every way the on-disk mode and the configured
// mode can disagree.
func TestAEADModeMismatch(t *testing.T) {
	ctx := context.Background()

	t.Run("plaintext open of encrypted store", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "enc.db")
		bdb := aeadOpenBolt(t, path)
		st := aeadOpenStore(t, bdb, &Config{Secret: aeadKey(6), EncryptValues: true})
		require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com"}))
		require.NoError(t, bdb.Close())

		bdb = aeadOpenBolt(t, path)
		defer bdb.Close()
		_, err := Open(ctx, bdb, nil, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrWrongMode)
	})

	t.Run("encrypted open of plaintext store with data", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plain.db")
		bdb := aeadOpenBolt(t, path)
		st := aeadOpenStore(t, bdb, nil)
		require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com"}))
		require.NoError(t, bdb.Close())

		bdb = aeadOpenBolt(t, path)
		defer bdb.Close()
		_, err := Open(ctx, bdb, nil, &Config{Secret: aeadKey(6), EncryptValues: true})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrWrongMode)
	})

	t.Run("settings mismatch", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "enc.db")
		bdb := aeadOpenBolt(t, path)
		st := aeadOpenStore(t, bdb, &Config{Secret: aeadKey(6), EncryptValues: true})
		require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com"}))
		require.NoError(t, bdb.Close())

		bdb = aeadOpenBolt(t, path)
		defer bdb.Close()
		_, err := Open(ctx, bdb, nil, &Config{
			Secret:          aeadKey(6),
			EncryptValues:   true,
			EncryptMetadata: true,
		})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrWrongMode)
	})
}

// TestAEADEmptyPlaintextStoreAdoptsEncryption proves a store that holds no user
// data yet can be opened with encryption, and that the version record is
// rewritten under the new settings.
func TestAEADEmptyPlaintextStoreAdoptsEncryption(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	bdb := aeadOpenBolt(t, path)
	require.NoError(t, aeadOpenStore(t, bdb, nil).Close())
	require.NoError(t, bdb.Close())

	bdb = aeadOpenBolt(t, path)
	st := aeadOpenStore(t, bdb, &Config{Secret: aeadKey(7), EncryptValues: true})

	info := aeadRawEntries(t, bdb, databaseInfo)
	require.Contains(t, info, markerKey)
	rawVersion, err := JSONCodec.Marshal(Version)
	require.NoError(t, err)
	require.NotEqual(t, rawVersion, info[versionKey], "the version record must be re-encoded under encryption")

	var v string
	require.NoError(t, st.Get(ctx, databaseInfo, versionKey, &v))
	require.Equal(t, Version, v)

	require.NoError(t, st.Save(ctx, &aeadUser{ID: 1, Name: "alice", Email: "alice@example.com"}))
	require.NoError(t, bdb.Close())

	bdb = aeadOpenBolt(t, path)
	defer bdb.Close()
	_, err = Open(ctx, bdb, nil, nil)
	require.ErrorIs(t, err, ErrWrongMode)
}

// TestAEADInvalidConfig covers configuration validation.
func TestAEADInvalidConfig(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.db")

	bdb := aeadOpenBolt(t, path)
	defer bdb.Close()

	_, err := Open(ctx, bdb, nil, &Config{Secret: []byte("short"), EncryptValues: true})
	require.ErrorIs(t, err, ErrInvalidKey)
}
