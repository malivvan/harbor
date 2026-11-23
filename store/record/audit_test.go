package record

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	audit "github.com/malivvan/harbor/pkg/ledger"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type auditUser struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

func (auditUser) HarborBucketName() string { return "audit_users" }

// auditSeed is a fixed 32-byte seed so the derived key is deterministic.
func auditSeed() []byte {
	return []byte("000102030405060708090a0b0c0d0e0f")
}

func openAudited(t *testing.T) (*Store, *bolt.DB, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := audit.DeriveKey(auditSeed(), "m/0'")
	require.NoError(t, err)

	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "audit.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	rec := audit.NewRecorder(bdb, map[string]ed25519.PrivateKey{audit.StoreDB: priv})
	st, err := Open(context.Background(), bdb, Config{Audit: rec})
	require.NoError(t, err)
	return st, bdb, pub
}

func TestAuditRecordsStructWrites(t *testing.T) {
	st, bdb, pub := openAudited(t)
	ctx := context.Background()

	u := auditUser{ID: "u1", Name: "alice"}
	require.NoError(t, st.Save(ctx, &u))
	u.Name = "bob"
	require.NoError(t, st.Update(ctx, &u))
	require.NoError(t, st.UpdateField(ctx, &u, "Name", "carol"))
	require.NoError(t, st.DeleteStruct(ctx, &u))

	res, err := audit.Verify(bdb, map[string]ed25519.PublicKey{audit.StoreDB: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(4), res.Count)
}

func TestAuditRecordsKeyValueWrites(t *testing.T) {
	st, bdb, pub := openAudited(t)
	ctx := context.Background()

	require.NoError(t, st.Set(ctx, "cfg", "k", "v"))
	require.NoError(t, st.SetBytes(ctx, "cfg", "raw", []byte("bytes")))
	require.NoError(t, st.Delete(ctx, "cfg", "k"))

	res, err := audit.Verify(bdb, map[string]ed25519.PublicKey{audit.StoreDB: pub})
	require.NoError(t, err)
	require.Equal(t, uint64(3), res.Count)
}

func TestAuditDisabledRecordsNothing(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "noaudit.db"), 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })

	st, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)

	u := auditUser{ID: "u1", Name: "alice"}
	require.NoError(t, st.Save(context.Background(), &u))

	_, err = audit.Verify(bdb, map[string]ed25519.PublicKey{audit.StoreDB: nil})
	require.ErrorIs(t, err, audit.ErrNoLog)
}

func TestAuditDigestMatchesPayload(t *testing.T) {
	st, bdb, pub := openAudited(t)
	ctx := context.Background()

	u := auditUser{ID: "u1", Name: "alice"}
	require.NoError(t, st.Save(ctx, &u))

	var ev audit.Event
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		events := tx.Bucket([]byte("audit")).Bucket([]byte("events"))
		v := events.Get([]byte{0, 0, 0, 0, 0, 0, 0, 0})
		encoded := v[:len(v)-ed25519.SignatureSize]
		return json.Unmarshal(encoded, &ev)
	}))
	require.Equal(t, "save", ev.Op)
	require.Equal(t, "audit_users:u1", ev.Entity)
	require.Len(t, ev.Digest, 32)

	// The recorded digest is the SHA-256 of the codec-encoded record.
	raw, err := st.Node.Codec().Marshal(&u)
	require.NoError(t, err)
	require.Equal(t, digestOf(raw), ev.Digest)
	_ = pub
}

// Rule: Set and SetBytes are distinguishable in the audit trail even though
// they share the write path.
func TestAuditSetOperationNames(t *testing.T) {
	st, bdb, _ := openAudited(t)
	ctx := context.Background()

	require.NoError(t, st.Set(ctx, "cfg", "typed", "v"))
	require.NoError(t, st.SetBytes(ctx, "cfg", "raw", []byte("bytes")))

	var ops []string
	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte("audit"))
		events := root.Bucket([]byte("events"))
		return events.ForEach(func(_, v []byte) error {
			var event audit.Event
			if err := json.Unmarshal(v[:len(v)-ed25519.SignatureSize], &event); err != nil {
				return err
			}
			ops = append(ops, event.Op)
			return nil
		})
	}))
	require.Equal(t, []string{"set", "set_bytes"}, ops)
}
