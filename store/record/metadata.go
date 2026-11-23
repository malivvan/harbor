package record

import (
	"reflect"

	bolt "go.etcd.io/bbolt"
)

const (
	metaCodec = "codec"
)

func newMeta(b *bolt.Bucket, n Node) (*meta, error) {
	m := b.Bucket([]byte(metadataBucket))
	if m != nil {
		name := m.Get([]byte(metaCodec))
		if string(name) != n.Codec().Name() {
			return nil, ErrDifferentCodec
		}
		return &meta{
			node:   n,
			bucket: m,
			enc:    encFromNode(n),
		}, nil
	}

	m, err := b.CreateBucket([]byte(metadataBucket))
	if err != nil {
		return nil, err
	}

	if err := m.Put([]byte(metaCodec), []byte(n.Codec().Name())); err != nil {
		return nil, err
	}
	return &meta{
		node:   n,
		bucket: m,
		enc:    encFromNode(n),
	}, nil
}

type meta struct {
	node   Node
	bucket *bolt.Bucket

	// enc is the encryption state of the owning store. nil means plaintext,
	// in which case counters are stored exactly as before.
	enc *encState
}

func (m *meta) increment(field *fieldConfig) error {
	var err error
	counter := field.IncrementStart

	raw := m.bucket.Get([]byte(field.Name + "counter"))
	if raw != nil {
		plain, err := m.enc.openMetadata(raw)
		if err != nil {
			return err
		}
		counter, err = numberfromb(plain)
		if err != nil {
			return err
		}
		counter++
	}

	raw, err = numbertob(counter)
	if err != nil {
		return err
	}

	raw, err = m.enc.sealMetadata(raw)
	if err != nil {
		return err
	}

	err = m.bucket.Put([]byte(field.Name+"counter"), raw)
	if err != nil {
		return err
	}

	field.Value.Set(reflect.ValueOf(counter).Convert(field.Value.Type()))
	field.IsZero = false
	return nil
}
