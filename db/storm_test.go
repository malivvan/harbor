package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/malivvan/storm/codec/json"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

const maxInt = 1<<(bits.UintSize-1) - 1

func TestNewStorm(t *testing.T) {
	db, err := Open(context.Background(), nil, nil, nil)

	require.Error(t, err)
	require.Nil(t, db)

	dir, err := os.MkdirTemp(os.TempDir(), "storm")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "storm.db")
	bdb, err := bolt.Open(file, 0600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer bdb.Close()

	db, err = Open(context.Background(), bdb, nil, nil)
	require.NoError(t, err)
	defer db.Close()

	require.Implements(t, (*Node)(nil), db)
	require.NotNil(t, db.NativeDB())
	require.Equal(t, defaultCodec, db.Codec())

	var v string
	err = db.Get(context.Background(), dbinfo, "version", &v)
	require.NoError(t, err)
	require.Equal(t, Version, v)
}

func TestNewStormWithRootAndCodec(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "storm")
	defer os.RemoveAll(dir)

	dc := new(dummyCodec)

	bdb1, err := bolt.Open(filepath.Join(dir, "storm1.db"), 0660, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bdb1.Close()
	db1, err := Open(context.Background(), bdb1, []byte("a"), dc)
	require.NoError(t, err)
	require.Equal(t, dc, db1.Codec())
	require.Equal(t, []string{"a"}, db1.Node.(*node).rootBucket)

	err = db1.Save(context.Background(), &SimpleUser{ID: 1})
	require.NoError(t, err)

	bdb2, err := bolt.Open(filepath.Join(dir, "storm2.db"), 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bdb2.Close()
	db2, err := Open(context.Background(), bdb2, nil, dc)
	require.NoError(t, err)
	require.Equal(t, dc, db2.Codec())
}

func TestBoltDB(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "storm")
	defer os.RemoveAll(dir)
	bDB, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer bDB.Close()

	sDB, err := Open(context.Background(), bDB, nil, nil)
	require.NoError(t, err)
	defer sDB.Close()
	err = sDB.Save(context.Background(), &SimpleUser{ID: 10})
	require.NoError(t, err)
}

type dummyCodec int

func (c dummyCodec) Marshal(v interface{}) ([]byte, error) {
	return []byte("dummy"), nil
}

func (c dummyCodec) Unmarshal(b []byte, v interface{}) error {
	return nil
}

func (c dummyCodec) Name() string {
	return "dummy"
}

func TestCodec(t *testing.T) {
	u1 := &SimpleUser{Name: "John"}
	encoded, err := defaultCodec.Marshal(u1)
	require.Nil(t, err)
	u2 := &SimpleUser{}
	err = defaultCodec.Unmarshal(encoded, u2)
	require.Nil(t, err)
	if !reflect.DeepEqual(u1, u2) {
		t.Fatal("Codec mismatch")
	}
}

func TestToBytes(t *testing.T) {
	b, err := toBytes([]byte("a slice of bytes"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("a slice of bytes"), b)

	b, err = toBytes("a string", nil)
	require.NoError(t, err)
	require.Equal(t, []byte("a string"), b)

	b, err = toBytes(&SimpleUser{ID: 10, Name: "John", age: 100}, json.Codec)
	require.NoError(t, err)
	require.Equal(t, `{"ID":10,"Name":"John"}`, string(b))

	tests := map[interface{}]interface{}{
		int(-maxInt):           int64(-maxInt),
		int(maxInt):            int64(maxInt),
		int8(-math.MaxInt8):    int8(-math.MaxInt8),
		int8(math.MaxInt8):     int8(math.MaxInt8),
		int16(-math.MaxInt16):  int16(-math.MaxInt16),
		int16(math.MaxInt16):   int16(math.MaxInt16),
		int32(-math.MaxInt32):  int32(-math.MaxInt32),
		int32(math.MaxInt32):   int32(math.MaxInt32),
		int64(-math.MaxInt64):  int64(-math.MaxInt64),
		int64(math.MaxInt64):   int64(math.MaxInt64),
		uint(maxInt):           uint64(maxInt),
		uint64(math.MaxUint64): uint64(math.MaxUint64),
	}

	for v, expected := range tests {
		b, err = toBytes(v, nil)
		require.NoError(t, err)
		require.NotNil(t, b)
		buf := bytes.NewReader(b)
		typ := reflect.TypeOf(expected)
		actual := reflect.New(typ)
		err = binary.Read(buf, binary.BigEndian, actual.Interface())
		require.NoError(t, err)
		require.Equal(t, expected, actual.Elem().Interface())
	}
}

func createDB(t errorHandler, opts ...testOpenOption) (*Store, func()) {
	cfg := defaultTestOpenOptions()
	for _, opt := range opts {
		opt(&cfg)
	}

	dir, err := os.MkdirTemp(os.TempDir(), "storm")
	if err != nil {
		t.Error(err)
	}

	bdb, err := bolt.Open(filepath.Join(dir, "storm.db"), cfg.mode, cfg.boltOptions)
	if err != nil {
		t.Error(err)
	}

	var root []byte
	if len(cfg.root) > 0 {
		root = []byte(cfg.root[0])
	}

	db, err := Open(context.Background(), bdb, root, cfg.codec)
	if err != nil {
		t.Error(err)
	}

	if db != nil && len(cfg.root) > 1 {
		db.Node = db.Node.From(cfg.root[1:]...)
	}

	return db, func() {
		if db != nil {
			_ = db.Close()
		}
		if bdb != nil {
			_ = bdb.Close()
		}
		_ = os.RemoveAll(dir)
	}
}

type errorHandler interface {
	Error(args ...interface{})
}
