package record

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHistoryLifecycleAsOfAndDiff(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{History: true})
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	store.SetNow(func() time.Time { return t0 })

	doc := &revisionDoc{ID: "a", Name: "one", N: 1}
	require.NoError(t, store.Save(ctx, doc))
	t1 := t0.Add(time.Minute)
	store.SetNow(func() time.Time { return t1 })
	require.NoError(t, store.Update(ctx, &revisionDoc{ID: "a", Name: "two"}))

	h, err := store.History(ctx, doc)
	require.NoError(t, err)
	require.Len(t, h, 2)
	require.Equal(t, RevisionCreate, h[0].Op)
	require.Equal(t, RevisionUpdate, h[1].Op)
	require.Equal(t, uint64(1), h[0].Seq)
	require.Equal(t, uint64(2), h[1].Seq)

	var decoded revisionDoc
	require.NoError(t, json.Unmarshal(h[0].Value, &decoded))
	require.Equal(t, "one", decoded.Name)
	require.NoError(t, json.Unmarshal(h[1].Value, &decoded))
	require.Equal(t, "two", decoded.Name)
	require.Equal(t, 1, decoded.N) // update merged, history kept the stored value

	asOf, err := store.AsOf(ctx, &revisionDoc{ID: "a"}, t0.Add(30*time.Second))
	require.NoError(t, err)
	require.Equal(t, "one", asOf.(*revisionDoc).Name)
	asOf, err = store.AsOf(ctx, &revisionDoc{ID: "a"}, t1.Add(30*time.Second))
	require.NoError(t, err)
	require.Equal(t, "two", asOf.(*revisionDoc).Name)

	diff, err := store.Diff(ctx, doc, t0, t1)
	require.NoError(t, err)
	require.True(t, diff.Changed)
	require.NotNil(t, diff.Before)
	require.NotNil(t, diff.After)

	t2 := t1.Add(time.Minute)
	store.SetNow(func() time.Time { return t2 })
	require.NoError(t, store.DeleteStruct(ctx, doc))
	h, err = store.History(ctx, doc)
	require.NoError(t, err)
	require.Len(t, h, 3)
	require.Equal(t, RevisionDelete, h[2].Op)
	require.Empty(t, h[2].Value)

	_, err = store.AsOf(ctx, &revisionDoc{ID: "a"}, t2.Add(time.Second))
	require.ErrorIs(t, err, ErrNotFound)

	removed, err := store.TrimHistory(ctx, doc, t1)
	require.NoError(t, err)
	require.Equal(t, 1, removed)
	h, err = store.History(ctx, doc)
	require.NoError(t, err)
	require.Len(t, h, 2)
}

func TestHistoryDisabled(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{})
	ctx := context.Background()
	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))
	_, err := store.History(ctx, doc)
	require.ErrorIs(t, err, ErrHistoryDisabled)
	_, err = store.AsOf(ctx, doc, time.Now())
	require.ErrorIs(t, err, ErrHistoryDisabled)
	_, err = store.TrimHistory(ctx, doc, time.Now())
	require.ErrorIs(t, err, ErrHistoryDisabled)
}

func TestHistoryCapabilitySurvivesReopenWithoutOption(t *testing.T) {
	store, bdb := openTombstoneStore(t, Config{History: true})
	ctx := context.Background()
	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))

	reopened, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	h, err := reopened.History(ctx, doc)
	require.NoError(t, err)
	require.Len(t, h, 1)
}

func TestReIndexDoesNotAppendHistory(t *testing.T) {
	store, _ := openTombstoneStore(t, Config{History: true})
	ctx := context.Background()
	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.ReIndex(ctx, &revisionDoc{}))
	h, err := store.History(ctx, doc)
	require.NoError(t, err)
	require.Len(t, h, 1)
}

func TestHistoryRotatesAndVerifies(t *testing.T) {
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = 7
	}
	store, _ := openTombstoneStore(t, Config{Secret: secret, EncryptValues: true, EncryptMetadata: true, History: true})
	ctx := context.Background()
	doc := &revisionDoc{ID: "a", Name: "one"}
	require.NoError(t, store.Save(ctx, doc))
	require.NoError(t, store.Update(ctx, &revisionDoc{ID: "a", Name: "two"}))
	require.NoError(t, store.VerifyAll(ctx))

	newKey := make([]byte, 32)
	for i := range newKey {
		newKey[i] = 8
	}
	require.NoError(t, store.RotateAll(ctx, newKey))
	require.NoError(t, store.VerifyAll(ctx))
	h, err := store.History(ctx, doc)
	require.NoError(t, err)
	require.Len(t, h, 2)
}
