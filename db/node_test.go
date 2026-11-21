package db

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestNode(t *testing.T) {
	db, cleanup := createDB(t, testRoot("a"))
	defer cleanup()

	n1 := db.From("b", "c")
	node1, ok := n1.(*node)
	require.True(t, ok)
	require.Equal(t, db, node1.s)
	require.NotEqual(t, db.Node, n1)
	require.Equal(t, []string{"a"}, db.Node.(*node).rootBucket)
	require.Equal(t, []string{"a", "b", "c"}, node1.rootBucket)
	n2 := n1.From("d", "e")
	node2, ok := n2.(*node)
	require.True(t, ok)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, node2.rootBucket)
}

func TestNodeWithTransaction(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	var user User
	db.NativeDB().Update(func(tx *bolt.Tx) error {
		dbx := db.Node.(*node).withTransaction(tx)
		err := dbx.Save(ctx, &User{ID: 10, Name: "John"})
		require.NoError(t, err)
		err = dbx.One(ctx, "ID", 10, &user)
		require.NoError(t, err)
		require.Equal(t, "John", user.Name)
		return nil
	})

	err := db.One(ctx, "ID", 10, &user)
	require.NoError(t, err)
}

func TestNodeWithCodec(t *testing.T) {
	t.Run("Inheritance", func(t *testing.T) {
		db, cleanup := createDB(t)
		defer cleanup()

		n := db.From("a").(*node)
		require.Equal(t, JSONCodec, n.codec)
		n = n.From("b", "c", "d").(*node)
		require.Equal(t, JSONCodec, n.codec)
		n = db.WithCodec(altCodec).(*node)
		n = n.From("e").(*node)
		require.Equal(t, altCodec, n.codec)
		o := n.From("f").WithCodec(JSONCodec).(*node)
		require.Equal(t, altCodec, n.codec)
		require.Equal(t, JSONCodec, o.codec)
	})

	t.Run("CodecCall", func(t *testing.T) {
		db, cleanup := createDB(t)
		defer cleanup()

		ctx := context.Background()

		type User struct {
			ID   int
			Name string `db:"index"`
		}

		requireBytesEqual := func(raw []byte, expected interface{}) {
			var u User
			err := altCodec.Unmarshal(raw, &u)
			require.NoError(t, err)
			require.Equal(t, expected, u)
		}

		n := db.From("a").WithCodec(altCodec)
		err := n.Set(ctx, "gobBucket", "key", &User{ID: 10, Name: "John"})
		require.NoError(t, err)
		b, err := n.GetBytes(ctx, "gobBucket", "key")
		require.NoError(t, err)
		requireBytesEqual(b, User{ID: 10, Name: "John"})

		id, err := toBytes(10, n.(*node).codec)
		require.NoError(t, err)

		err = n.Save(ctx, &User{ID: 10, Name: "John"})
		require.NoError(t, err)
		b, err = n.GetBytes(ctx, "User", id)
		require.NoError(t, err)
		requireBytesEqual(b, User{ID: 10, Name: "John"})

		err = n.Update(ctx, &User{ID: 10, Name: "Jack"})
		require.NoError(t, err)
		b, err = n.GetBytes(ctx, "User", id)
		require.NoError(t, err)
		requireBytesEqual(b, User{ID: 10, Name: "Jack"})

		err = n.UpdateField(ctx, &User{ID: 10}, "Name", "John")
		require.NoError(t, err)
		b, err = n.GetBytes(ctx, "User", id)
		require.NoError(t, err)
		requireBytesEqual(b, User{ID: 10, Name: "John"})

		var users []User
		err = n.Find(ctx, "Name", "John", &users)
		require.NoError(t, err)

		var user User
		err = n.One(ctx, "Name", "John", &user)
		require.NoError(t, err)

		err = n.AllByIndex(ctx, "Name", &users)
		require.NoError(t, err)

		err = n.All(ctx, &users)
		require.NoError(t, err)

		err = n.Range(ctx, "Name", "J", "K", &users)
		require.NoError(t, err)

		err = n.Prefix(ctx, "Name", "J", &users)
		require.NoError(t, err)

		_, err = n.Count(ctx, new(User))
		require.NoError(t, err)

		err = n.Select().Find(ctx, &users)
		require.NoError(t, err)
	})
}

// testOpenOption configures createDB. It mirrors the removed Open options for
// test convenience only.
type testOpenOption func(*testOpenOptions)

type testOpenOptions struct {
	root        []string
	codec       Codec
	mode        os.FileMode
	boltOptions *bolt.Options
}

// testRoot selects a root bucket path for tests. Only the first element is passed
// to Open as its root key; the remaining elements are applied through From.
func testRoot(root ...string) testOpenOption {
	path := cloneBucketPath(root)
	return func(opts *testOpenOptions) {
		opts.root = cloneBucketPath(path)
	}
}

// testCodec selects the codec for tests.
func testCodec(c Codec) testOpenOption {
	return func(opts *testOpenOptions) {
		opts.codec = c
	}
}

// testBoltOptions selects the bbolt file mode and options for tests.
func testBoltOptions(mode os.FileMode, options *bolt.Options) testOpenOption {
	return func(opts *testOpenOptions) {
		opts.mode = mode
		opts.boltOptions = options
	}
}

func defaultTestOpenOptions() testOpenOptions {
	return testOpenOptions{
		mode:        0600,
		boltOptions: &bolt.Options{Timeout: time.Second},
	}
}

// testUser is used in external-package consumer tests.
type testUser struct {
	ID   int    `db:"id"`
	Name string `db:"index"`
}

// TestNode_ConsumerCanUseNode verifies that an external consumer can declare
// and use a Node.
func TestNode_ConsumerCanUseNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storm.db")
	bDB, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bDB.Close())
	})

	db, err := Open(context.Background(), bDB, nil, Config{Codec: altCodec})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()

	// Exercise Node interface: From, Bucket, Codec, WithCodec.
	var n Node = db.From("tenant", "ns")
	require.Equal(t, []string{"tenant", "ns"}, n.Bucket())

	n2 := n.From("child")
	require.Equal(t, []string{"tenant", "ns", "child"}, n2.Bucket())

	require.Equal(t, altCodec, n.Codec())

	nWithCodec := n.WithCodec(altCodec)
	require.Equal(t, altCodec, nWithCodec.Codec())

	// Perform a public operation through the Node.
	err = n.Save(ctx, &testUser{ID: 1, Name: "test"})
	require.NoError(t, err)

	// Read back using the same nested node.
	var result testUser
	err = n.One(ctx, "ID", 1, &result)
	require.NoError(t, err)
	require.Equal(t, "test", result.Name)
}

// ---------------------------------------------------------------------------
// Reflection helpers
// ---------------------------------------------------------------------------

// containsBboltType recursively checks whether typ contains a type from bboltPkg.
func containsBboltType(typ reflect.Type, bboltPkg string) bool {
	seen := make(map[reflect.Type]bool)
	var visit func(reflect.Type) bool
	visit = func(typ reflect.Type) bool {
		if typ == nil || seen[typ] {
			return false
		}
		seen[typ] = true
		if typ.PkgPath() == bboltPkg {
			return true
		}
		switch typ.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
			return visit(typ.Elem())
		case reflect.Map:
			return visit(typ.Key()) || visit(typ.Elem())
		case reflect.Func:
			for i := 0; i < typ.NumIn(); i++ {
				if visit(typ.In(i)) {
					return true
				}
			}
			for i := 0; i < typ.NumOut(); i++ {
				if visit(typ.Out(i)) {
					return true
				}
			}
		case reflect.Interface:
			for i := 0; i < typ.NumMethod(); i++ {
				if visit(typ.Method(i).Type) {
					return true
				}
			}
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				if visit(typ.Field(i).Type) {
					return true
				}
			}
		}
		return false
	}
	return visit(typ)
}

// TestNodeInterface_NoBboltExposure uses reflection on the public Node interface
// to prove that no method parameter or return type exposes a bbolt type.
func TestNodeInterface_NoBboltExposure(t *testing.T) {
	iface := reflect.TypeOf((*Node)(nil)).Elem()

	bboltPkg := "go.etcd.io/bbolt"

	forbiddenMethods := map[string]bool{
		"Begin":                   true,
		"Commit":                  true,
		"Rollback":                true,
		"WithTransaction":         true,
		"GetBucket":               true,
		"CreateBucketIfNotExists": true,
	}

	for i := 0; i < iface.NumMethod(); i++ {
		m := iface.Method(i)

		// Assert no forbidden method names.
		require.False(t, forbiddenMethods[m.Name],
			"forbidden method %q found in Node interface", m.Name)

		// Inspect parameters for bbolt types.
		for j := 0; j < m.Type.NumIn(); j++ {
			paramType := m.Type.In(j)
			require.False(t, containsBboltType(paramType, bboltPkg),
				"method %q parameter %d exposes bbolt type %v", m.Name, j, paramType)
		}

		// Inspect return types for bbolt types.
		for j := 0; j < m.Type.NumOut(); j++ {
			outType := m.Type.Out(j)
			require.False(t, containsBboltType(outType, bboltPkg),
				"method %q return %d exposes bbolt type %v", m.Name, j, outType)
		}
	}
}

// TestDBInterface_NoBboltExposure uses reflection on the Store type to prove:
// - Store has no exported field named Bolt;
// - Store has no exported field recursively containing a bbolt type;
// - NativeDB is the only method returning a bbolt type.
func TestDBInterface_NoBboltExposure(t *testing.T) {
	dbType := reflect.TypeOf((*Store)(nil)).Elem()
	dbPtrType := reflect.TypeOf((*Store)(nil))
	bboltPkg := "go.etcd.io/bbolt"

	// Store has no exported field named Bolt.
	for i := 0; i < dbType.NumField(); i++ {
		f := dbType.Field(i)
		// Only check exported (public) fields.
		if !f.IsExported() {
			continue
		}
		require.NotEqual(t, "Bolt", f.Name,
			"Store must not have an exported field named Bolt (found %q)", f.Name)

		// Store has no exported field whose type exposes a bbolt type.
		require.False(t, containsBboltType(f.Type, bboltPkg),
			"Store field %q exposes bbolt type %v", f.Name, f.Type)
	}

	// Verify NativeDB exists and returns the expected type.
	// NativeDB has a pointer receiver, so look it up on *Store.
	nativeDB, ok := dbPtrType.MethodByName("NativeDB")
	require.True(t, ok, "Store must have a NativeDB method")
	require.Equal(t, 1, nativeDB.Type.NumOut(), "NativeDB must return exactly one value")
	outType := nativeDB.Type.Out(0)
	require.Equal(t, reflect.Pointer, outType.Kind(), "NativeDB must return a pointer")
	require.Equal(t, bboltPkg, outType.Elem().PkgPath(), "NativeDB must return *bolt.Store")

	// Verify no other method on *Store returns a bbolt type (except NativeDB).
	for i := 0; i < dbPtrType.NumMethod(); i++ {
		m := dbPtrType.Method(i)
		if m.Name == "NativeDB" {
			continue // already validated above
		}
		for j := 0; j < m.Type.NumOut(); j++ {
			outType := m.Type.Out(j)
			require.False(t, containsBboltType(outType, bboltPkg),
				"Store method %q return %d exposes bbolt type %v", m.Name, j, outType)
		}
	}
}
