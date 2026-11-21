package encryption

// Pair is one key/value entry staged by a rotation.
type Pair struct {
	Key   []byte
	Value []byte
}

// EntryWriter is the subset of a bucket that ApplyEntries needs.
type EntryWriter interface {
	Delete(key []byte) error
	Put(key, value []byte) error
}

// ApplyEntries deletes drop and then writes entries.
//
// Rotations stage their rewrites first and apply them afterwards, because a
// bbolt cursor must not have entries removed while it is being walked.
func ApplyEntries(w EntryWriter, drop [][]byte, entries []Pair) error {
	for _, key := range drop {
		if err := w.Delete(key); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		if err := w.Put(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	return nil
}

// EntryReader is the subset of a bucket that KeysOf needs.
type EntryReader interface {
	ForEach(func(k, v []byte) error) error
}

// KeysOf returns a copy of the plain keys of a bucket, skipping nested buckets.
func KeysOf(r EntryReader) ([][]byte, error) {
	var keys [][]byte
	if err := r.ForEach(func(key, value []byte) error {
		if value == nil {
			return nil
		}
		keys = append(keys, append([]byte(nil), key...))
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}
