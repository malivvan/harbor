package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type pointerNamed struct {
	ID   int    `db:"id"`
	Name string `db:"index"`
}

// StormBucketName is deliberately defined on the pointer receiver: Save must
// honour it rather than falling back to the static type name.
func (p *pointerNamed) StormBucketName() string { return "pointer_named_bucket" }

func TestBucketNamerPointerReceiver(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &pointerNamed{ID: 1, Name: "alice"}))

	var got pointerNamed
	require.NoError(t, db.One(ctx, "Name", "alice", &got))
	require.Equal(t, 1, got.ID)

	var all []pointerNamed
	require.NoError(t, db.Select().Bucket("pointer_named_bucket").Find(ctx, &all))
	require.Len(t, all, 1)
}

func TestPrefixEscapesRegexpMetacharacters(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i, name := range []string{"a.b", "axb", "a.bc"} {
		require.NoError(t, db.Save(ctx, &User{ID: i + 1, Name: name, Slug: name}))
	}

	var users []User
	require.NoError(t, db.Prefix(ctx, "Slug", "a.", &users))
	require.Len(t, users, 2, "prefix must be literal")
}
