package record

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/malivvan/eidos-db/store/record/q"
)

type projUser struct {
	ID   int    `db:"id,increment"`
	Name string `db:"index"`
	Age  int    `db:"index"`
	Bio  string
}

func newProjStore(t *testing.T) *Store {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "proj.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	st, err := Open(context.Background(), bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })
	require.NoError(t, st.Save(context.Background(), &projUser{Name: "alice", Age: 30, Bio: "secret"}))
	return st
}

func TestProjectFind(t *testing.T) {
	ctx := context.Background()
	st := newProjStore(t)

	var users []projUser
	require.NoError(t, st.Select().Project("Name").Find(ctx, &users))
	require.Len(t, users, 1)
	require.Equal(t, "alice", users[0].Name)
	require.Zero(t, users[0].Age)
	require.Zero(t, users[0].Bio)
	require.Zero(t, users[0].ID)
}

func TestProjectEachAndIter(t *testing.T) {
	ctx := context.Background()
	st := newProjStore(t)

	var names []string
	require.NoError(t, st.Select().Project("Name").Each(ctx, &projUser{}, func(v any) error {
		names = append(names, v.(*projUser).Name)
		if v.(*projUser).Age != 0 {
			t.Errorf("Age was decoded: %+v", v)
		}
		return nil
	}))
	require.Equal(t, []string{"alice"}, names)

	it, err := st.Select().Project("Age").Iter(ctx, &projUser{})
	require.NoError(t, err)
	defer it.Close()
	require.True(t, it.Next())
	var got projUser
	require.NoError(t, it.Decode(&got))
	require.Equal(t, 30, got.Age)
	require.Empty(t, got.Name)
	require.NoError(t, it.Err())
}

func TestProjectRejectsMatchersAndOrdering(t *testing.T) {
	ctx := context.Background()
	st := newProjStore(t)

	var users []projUser
	err := st.Select(q.Eq("Name", "alice")).Project("Name").Find(ctx, &users)
	require.ErrorIs(t, err, ErrProjectionUnsupported)

	err = st.Select().OrderBy("Name").Project("Name").Find(ctx, &users)
	require.ErrorIs(t, err, ErrProjectionUnsupported)
}

func TestProjectUnknownField(t *testing.T) {
	ctx := context.Background()
	st := newProjStore(t)

	var users []projUser
	err := st.Select().Project("Nope").Find(ctx, &users)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestProjectJSONTagName(t *testing.T) {
	type tagged struct {
		ID   int    `db:"id,increment"`
		Name string `json:"display_name"`
		Age  int    `json:"age"`
	}
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "tagged.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	st, err := Open(ctx, bdb, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })
	require.NoError(t, st.Save(ctx, &tagged{Name: "alice", Age: 30}))

	var rows []tagged
	require.NoError(t, st.Select().Project("display_name").Find(ctx, &rows))
	require.Len(t, rows, 1)
	require.Equal(t, "alice", rows[0].Name)
	require.Zero(t, rows[0].Age)
}

// plainCodec is a Codec without the ProjectedCodec capability. The fallback
// must decode fully and zero the unprojected fields.
type plainCodec struct{}

func (plainCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }
func (plainCodec) Unmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}
func (plainCodec) Name() string { return "plain" }

func TestProjectCodecFallback(t *testing.T) {
	type row struct {
		ID   int    `db:"id,increment"`
		Name string `json:"Name"`
		Age  int    `json:"Age"`
	}
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "fallback.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	st, err := Open(ctx, bdb, Config{Codec: plainCodec{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })
	require.NoError(t, st.Save(ctx, &row{Name: "alice", Age: 30}))

	var rows []row
	require.NoError(t, st.Select().Project("Name").Find(ctx, &rows))
	require.Len(t, rows, 1)
	require.Equal(t, "alice", rows[0].Name)
	require.Zero(t, rows[0].Age)
}
