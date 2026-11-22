package crypto

import (
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// memBucket is a minimal EntryReader/EntryWriter used to exercise the rotation
// helpers without bbolt.
type memBucket struct {
	entries map[string][]byte
	nils    map[string]bool // keys stored as nested buckets
	delErr  error
	putErr  error
	iterErr error
}

func newMemBucket() *memBucket {
	return &memBucket{entries: map[string][]byte{}, nils: map[string]bool{}}
}

func (b *memBucket) Put(key, value []byte) error {
	if b.putErr != nil {
		return b.putErr
	}
	b.entries[string(key)] = append([]byte(nil), value...)
	return nil
}

func (b *memBucket) Delete(key []byte) error {
	if b.delErr != nil {
		return b.delErr
	}
	delete(b.entries, string(key))
	return nil
}

func (b *memBucket) ForEach(fn func(k, v []byte) error) error {
	if b.iterErr != nil {
		return b.iterErr
	}

	keys := make([]string, 0, len(b.entries)+len(b.nils))
	for k := range b.entries {
		keys = append(keys, k)
	}
	for k := range b.nils {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if b.nils[k] {
			if err := fn([]byte(k), nil); err != nil {
				return err
			}
			continue
		}
		if err := fn([]byte(k), b.entries[k]); err != nil {
			return err
		}
	}
	return nil
}

func TestApplyEntries(t *testing.T) {
	bucket := newMemBucket()
	require.NoError(t, bucket.Put([]byte("old-1"), []byte("v1")))
	require.NoError(t, bucket.Put([]byte("old-2"), []byte("v2")))
	require.NoError(t, bucket.Put([]byte("untouched"), []byte("keep")))

	err := ApplyEntries(bucket,
		[][]byte{[]byte("old-1"), []byte("old-2")},
		[]Pair{
			{Key: []byte("new-1"), Value: []byte("w1")},
			{Key: []byte("new-2"), Value: []byte("w2")},
		})
	require.NoError(t, err)

	require.NotContains(t, bucket.entries, "old-1")
	require.NotContains(t, bucket.entries, "old-2")
	require.Equal(t, []byte("keep"), bucket.entries["untouched"])
	require.Equal(t, []byte("w1"), bucket.entries["new-1"])
	require.Equal(t, []byte("w2"), bucket.entries["new-2"])

	// Errors are propagated.
	boom := errors.New("boom")
	bucket.delErr = boom
	require.ErrorIs(t, ApplyEntries(bucket, [][]byte{[]byte("x")}, nil), boom)

	bucket.delErr = nil
	bucket.putErr = boom
	require.ErrorIs(t, ApplyEntries(bucket, nil, []Pair{{Key: []byte("x"), Value: []byte("y")}}), boom)
}

func TestKeysOf(t *testing.T) {
	bucket := newMemBucket()
	require.NoError(t, bucket.Put([]byte("a"), []byte("1")))
	require.NoError(t, bucket.Put([]byte("b"), []byte("2")))
	bucket.nils["nested"] = true

	keys, err := KeysOf(bucket)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, keys, "nested buckets are skipped")

	// The result must be a copy.
	bucket.entries["a"][0] = 'z'
	require.Equal(t, []byte("a"), keys[0])

	boom := errors.New("boom")
	bucket.iterErr = boom
	_, err = KeysOf(bucket)
	require.ErrorIs(t, err, boom)
}
