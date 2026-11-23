package record

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// strictCodec enforces the documented pointer contract: Marshal gets a non-nil
// pointer to a struct; Unmarshal gets *T or **T (for []*T reads) and nothing
// deeper.
type strictCodec struct{}

func (strictCodec) Name() string { return "strict" }

func (strictCodec) Marshal(v any) ([]byte, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, fmt.Errorf("strict codec: Marshal expects a value")
	}
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("strict codec: Marshal rejects a nil pointer (%T)", v)
		}
		if rv.Elem().Kind() == reflect.Pointer {
			return nil, fmt.Errorf("strict codec: Marshal rejects extra indirection (%T)", v)
		}
	}
	return json.Marshal(v)
}

func (strictCodec) Unmarshal(b []byte, v any) error {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("strict codec: Unmarshal expects a non-nil pointer, got %T", v)
	}
	target := rv
	if target.Elem().Kind() == reflect.Pointer {
		// **T for a []*T slot: allocate the element and decode into it. A third
		// level of indirection is outside the contract.
		inner := target.Elem()
		if inner.Type().Elem().Kind() == reflect.Pointer {
			return fmt.Errorf("strict codec: Unmarshal rejects extra indirection (%T)", v)
		}
		if inner.IsNil() {
			inner.Set(reflect.New(inner.Type().Elem()))
		}
		target = inner
	}
	return json.Unmarshal(b, target.Interface())
}

type contractRow struct {
	ID   int    `db:"id,increment"`
	Name string `db:"index"`
}

func newContractStore(t *testing.T) *Store {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "codec.db"), 0o600, &bolt.Options{Timeout: 1e9})
	require.NoError(t, err)
	st, err := Open(context.Background(), bdb, Config{Codec: strictCodec{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close(); _ = bdb.Close() })
	return st
}

func TestCodecPointerContractMatrix(t *testing.T) {
	ctx := context.Background()
	st := newContractStore(t)

	// Save receives the caller's pointer.
	for i := 0; i < 3; i++ {
		row := &contractRow{Name: fmt.Sprintf("row-%d", i)}
		require.NoError(t, st.Save(ctx, row))
	}

	// One decodes into *T.
	var one contractRow
	require.NoError(t, st.One(ctx, "Name", "row-1", &one))
	require.Equal(t, "row-1", one.Name)

	// Find into []T decodes into *T slots.
	var values []contractRow
	require.NoError(t, st.Find(ctx, "Name", "row-0", &values))
	require.Len(t, values, 1)

	// Find into []*T decodes into **T slots.
	var pointers []*contractRow
	require.NoError(t, st.Find(ctx, "Name", "row-2", &pointers))
	require.Len(t, pointers, 1)
	require.Equal(t, "row-2", pointers[0].Name)

	// Each decodes into *T.
	require.NoError(t, st.Select().Each(ctx, &contractRow{}, func(v any) error {
		_, ok := v.(*contractRow)
		if !ok {
			return fmt.Errorf("Each value is %T, want *contractRow", v)
		}
		return nil
	}))

	// Iter/Decode: internal decode uses *T and Decode receives the caller's
	// pointer.
	it, err := st.Select().Iter(ctx, &contractRow{})
	require.NoError(t, err)
	require.True(t, it.Next())
	var decoded contractRow
	require.NoError(t, it.Decode(&decoded))
	require.NotEmpty(t, decoded.Name)
	require.NoError(t, it.Err())
	// An iterator holds a read transaction; it must be closed before a write,
	// or the write's commit waits for the mmap lock forever.
	require.NoError(t, it.Close())

	// UpdateField marshals the full entity pointer.
	require.NoError(t, st.UpdateField(ctx, &contractRow{ID: one.ID}, "Name", "renamed"))
	require.NoError(t, st.One(ctx, "ID", one.ID, &one))
	require.Equal(t, "renamed", one.Name)
}
