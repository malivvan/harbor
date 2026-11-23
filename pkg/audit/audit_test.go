package audit

import (
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/malivvan/harbor/pkg/sign/canonical"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func testSeed() []byte {
	return []byte("000102030405060708090a0b0c0d0e0f")
}

func testKey(t *testing.T, path string) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := DeriveKey(testSeed(), path)
	require.NoError(t, err)
	return pub, priv
}

func testDB(t *testing.T) *bolt.DB {
	t.Helper()
	b, err := bolt.Open(filepath.Join(t.TempDir(), "audit.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestRecordAndVerify(t *testing.T) {
	pub, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})

	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "users:u1", Digest: []byte("0123456789abcdef0123456789abcdef")}))
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "delete", Entity: "users:u1"}))

	res, err := Verify(b, map[string]ed25519.PublicKey{StoreDB: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res.Count)

	count, tip, err := rec.Head()
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
	require.Equal(t, res.Tip, tip)
}

func TestVerifyRejectsForgery(t *testing.T) {
	pub, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "a"}))

	// Tamper with the stored event payload without re-signing.
	require.NoError(t, b.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(auditBucket))
		events := root.Bucket([]byte(eventsBucket))
		v := events.Get(seqKey(0))
		copy(v, []byte("x")) // corrupt the encoded JSON
		return events.Put(seqKey(0), v)
	}))

	_, err := Verify(b, map[string]ed25519.PublicKey{StoreDB: pub})
	require.Error(t, err)
}

func TestVerifyRejectsMissingEvent(t *testing.T) {
	pub, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "a"}))
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "b"}))
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "c"}))

	// Delete the middle event; the chain link must break.
	require.NoError(t, b.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(auditBucket))
		events := root.Bucket([]byte(eventsBucket))
		return events.Delete(seqKey(1))
	}))

	_, err := Verify(b, map[string]ed25519.PublicKey{StoreDB: pub})
	require.Error(t, err)
}

func TestVerifyTruncatedTailStillVerifiesPrefix(t *testing.T) {
	pub, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})
	for i := 0; i < 3; i++ {
		require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "e"}))
	}

	// Delete the last event: the remaining prefix is internally consistent, so
	// Verify succeeds but reports a shorter chain. Tail anchoring is external.
	require.NoError(t, b.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(auditBucket))
		events := root.Bucket([]byte(eventsBucket))
		return events.Delete(seqKey(2))
	}))

	res, err := Verify(b, map[string]ed25519.PublicKey{StoreDB: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res.Count)
}

func TestRecordUnknownStoreFails(t *testing.T) {
	_, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})
	err := rec.Record(Entry{Store: "nope", Op: "save"})
	require.Error(t, err)
}

func TestDeriveKeyDeterministic(t *testing.T) {
	pub1, priv1, err := DeriveKey(testSeed(), "m/1'")
	require.NoError(t, err)
	pub2, priv2, err := DeriveKey(testSeed(), "m/1'")
	require.NoError(t, err)
	require.Equal(t, pub1, pub2)
	require.Equal(t, priv1, priv2)

	// Different paths derive different keys.
	pub3, _, err := DeriveKey(testSeed(), "m/2'")
	require.NoError(t, err)
	require.NotEqual(t, pub1, pub3)

	// A different seed derives a different key.
	pub4, _, err := DeriveKey([]byte("different-seed"), "m/1'")
	require.NoError(t, err)
	require.NotEqual(t, pub1, pub4)
}

func TestRecordFillsTime(t *testing.T) {
	_, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "a"}))

	var ev Event
	require.NoError(t, b.View(func(tx *bolt.Tx) error {
		events := tx.Bucket([]byte(auditBucket)).Bucket([]byte(eventsBucket))
		v := events.Get(seqKey(0))
		encoded := v[:len(v)-ed25519.SignatureSize]
		return json.Unmarshal(encoded, &ev)
	}))
	require.True(t, ev.At > 0)
	require.WithinDuration(t, time.Unix(0, ev.At), time.Now(), 10*time.Second)
}

// TestRecordConcurrentSequence verifies that concurrent writers keep the chain
// contiguous: bbolt serializes Record, and each event links to the one before
// it. Run with -race.
func TestRecordConcurrentSequence(t *testing.T) {
	_, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})

	const records = 32
	var wg sync.WaitGroup
	for i := 0; i < records; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "e"}); err != nil {
				t.Errorf("concurrent Record: %v", err)
			}
		}()
	}
	wg.Wait()

	pub, _ := testKey(t, "m/0'")
	res, err := Verify(b, map[string]ed25519.PublicKey{StoreDB: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(records), res.Count)

	count, tip, err := rec.Head()
	require.NoError(t, err)
	require.Equal(t, res.Count, count)
	require.Equal(t, res.Tip, tip)
}

// TestVerifyRejectsWrongKeys covers the wrong-password case at the audit layer:
// a verifier holding keys derived from another seed cannot validate the
// signatures, even if it has the trail.
func TestVerifyRejectsWrongKeys(t *testing.T) {
	_, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "a"}))

	wrongPub, _, err := DeriveKey([]byte("different-seed"), "m/0'")
	require.NoError(t, err)
	_, err = Verify(b, map[string]ed25519.PublicKey{StoreDB: wrongPub})
	require.Error(t, err)
}

// Rule: the bytes stored for an event are the bytes signed, and they are the
// canonical encoding of the event rather than encoding/json's struct field order.
func TestEventEncodingIsCanonical(t *testing.T) {
	_, priv := testKey(t, "m/0'")
	b := testDB(t)
	rec := NewRecorder(b, map[string]ed25519.PrivateKey{StoreDB: priv})
	require.NoError(t, rec.Record(Entry{Store: StoreDB, Op: "save", Entity: "users:u1"}))

	var stored []byte
	require.NoError(t, b.View(func(tx *bolt.Tx) error {
		events := tx.Bucket([]byte(auditBucket)).Bucket([]byte(eventsBucket))
		v := events.Get(seqKey(0))
		stored = append([]byte(nil), v[:len(v)-ed25519.SignatureSize]...)
		return nil
	}))

	var ev Event
	require.NoError(t, json.Unmarshal(stored, &ev))
	want, err := canonical.Bytes(ev)
	require.NoError(t, err)
	require.Equal(t, want, stored, "stored event bytes are not canonical")
}

// Rule: signing is over a canonical encoding, so a future event field holding a
// map has one signature per logical value regardless of insertion order.
func TestCanonicalEncodingIsStableForMapFields(t *testing.T) {
	type eventWithMap struct {
		Seq   uint64            `json:"seq"`
		Extra map[string]string `json:"extra"`
	}
	_, priv := testKey(t, "m/0'")

	first := eventWithMap{Seq: 1, Extra: map[string]string{}}
	first.Extra["b"] = "2"
	first.Extra["a"] = "1"

	second := eventWithMap{Seq: 1, Extra: map[string]string{}}
	second.Extra["a"] = "1"
	second.Extra["b"] = "2"

	a, err := canonical.Bytes(first)
	require.NoError(t, err)
	c, err := canonical.Bytes(second)
	require.NoError(t, err)
	require.Equal(t, a, c)
	require.Equal(t, ed25519.Sign(priv, a), ed25519.Sign(priv, c))
}
