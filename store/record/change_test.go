package record

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type changeUser struct {
	ID   string `db:"id"`
	Name string `db:"index"`
}

func (changeUser) TempestBucketName() string { return "change_users" }

func TestChangeHookReceivesUpsertUpdateDelete(t *testing.T) {
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	st, err := Open(context.Background(), bdb, Config{})
	if err != nil {
		t.Fatal(err)
	}
	var got []Change
	remove := st.AddChangeHook(func(c Change) { got = append(got, c) })
	defer remove()
	ctx := context.Background()

	u := changeUser{ID: "u1", Name: "alice"}
	if err := st.Save(ctx, &u); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateField(ctx, &changeUser{ID: "u1"}, "Name", "bob"); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteStruct(ctx, &changeUser{ID: "u1"}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d changes, want 3: %+v", len(got), got)
	}
	if got[0].Op != ChangeUpsert || got[0].ID != "u1" || got[0].Bucket != "change_users" || len(got[0].Data) == 0 {
		t.Fatalf("save change = %+v", got[0])
	}
	if got[1].Op != ChangeUpsert || got[1].ID != "u1" {
		t.Fatalf("update change = %+v", got[1])
	}
	if got[2].Op != ChangeDelete || got[2].ID != "u1" {
		t.Fatalf("delete change = %+v", got[2])
	}
}
