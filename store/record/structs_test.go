package record

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type ClassicNoTags struct {
	PublicField int
	//lint:ignore U1000 Private fields are deliberate reflection fixtures.
	privateField string
	Date         time.Time
	InlineStruct struct {
		a float32
		B float64
	}
	Interf io.Writer
}

type ClassicBadTags struct {
	ID          string
	PublicField int `db:"mrots"`
	//lint:ignore U1000 Private fields are deliberate reflection fixtures.
	privateField string
	Date         time.Time
	InlineStruct struct {
		a float32
		B float64
	}
	Interf io.Writer
}

type ClassicUnique struct {
	ID          string
	PublicField int `db:"unique"`
	//lint:ignore U1000 Private tagged fields verify reflection visibility rules.
	privateField string `db:"unique"`
	//lint:ignore U1000 Private tagged fields verify reflection visibility rules.
	privateField2 string    `db:"unique"`
	Date          time.Time `db:"unique"`
	InlineStruct  struct {
		A float32
		B float64
	} `db:"unique"`
	Interf io.Writer `db:"unique"`
}

type ClassicIndex struct {
	ID          string
	PublicField int `db:"index"`
	//lint:ignore U1000 Private tagged fields verify reflection visibility rules.
	privateField string    `db:"index"`
	Date         time.Time `db:"index"`
	InlineStruct struct {
		a float32
		B float64
	} `db:"index"`
	InlineStructPtr *UserWithNoID `db:"index"`
	Interf          io.Writer     `db:"index"`
}

type ClassicInline struct {
	PublicField  int `db:"unique"`
	ClassicIndex `db:"inline"`
	*ToEmbed     `db:"inline"`
	Date         time.Time `db:"unique"`
}

type User struct {
	ID          int       `db:"id,increment"`
	Name        string    `db:"index"`
	Age         int       `db:"index,increment"`
	DateOfBirth time.Time `db:"index"`
	Group       string
	//lint:ignore U1000 Private fields are deliberate reflection fixtures.
	unexportedField int
	Slug            string `db:"unique"`
}

type ToEmbed struct {
	ID string
}

type NestedID struct {
	ToEmbed `db:"inline"`
	Name    string
}

type SimpleUser struct {
	ID   int `db:"id"`
	Name string
	age  int
}

type UserWithNoID struct {
	Name string
}

type UserWithIDField struct {
	ID   int
	Name string
}

type UserWithUint64IDField struct {
	ID   uint64
	Name string
}

type UserWithStringIDField struct {
	ID   string
	Name string
}

type UserWithEmbeddedIDField struct {
	UserWithIDField `db:"inline"`
	Age             int
}

type UserWithEmbeddedField struct {
	UserWithNoID `db:"inline"`
	ID           uint64
}

type UserWithIncrementField struct {
	ID   int
	Name string
	Age  int `db:"unique,increment"`
}

type IndexedNameUser struct {
	ID          int    `db:"id"`
	Name        string `db:"index"`
	Score       int    `db:"index,increment"`
	age         int
	DateOfBirth time.Time `db:"index"`
	Group       string
}

type UniqueNameUser struct {
	ID   int    `db:"id"`
	Name string `db:"unique"`
	Age  int    `db:"index,increment"`
}

// ===================================================
// Test types that implement BucketNamer
// ============================================================================

// CustomBucketUser demonstrates a struct with a custom bucket name.
type CustomBucketUser struct {
	ID   int    `db:"id,increment"`
	Name string `db:"index"`
}

func (c CustomBucketUser) EidosBucketName() string {
	return "my_custom_bucket"
}

// NamerUser is used for update/updatefield tests with BucketNamer.
type NamerUser struct {
	ID    int    `db:"id,increment"`
	Name  string `db:"index"`
	Email string `db:"unique"`
}

func (n NamerUser) EidosBucketName() string {
	return "namer_users"
}

// DropUser is used for drop tests with BucketNamer.
type DropUser struct {
	ID   int    `db:"id"`
	Name string `db:"index"`
}

func (d DropUser) EidosBucketName() string {
	return "drop_me"
}

// ============================================================================
// Tests: BucketNamer with named types
// ============================================================================

func TestBucketNamerSaveAndOne(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type User struct {
		ID   int    `db:"id,increment"`
		Name string `db:"index"`
	}

	u := User{Name: "Alice"}
	err := db.Save(ctx, &u)
	require.NoError(t, err)
	require.Equal(t, 1, u.ID)

	var u2 User
	err = db.One(ctx, "ID", 1, &u2)
	require.NoError(t, err)
	require.Equal(t, "Alice", u2.Name)
	require.Equal(t, 1, u2.ID)
}

// ============================================================================
// Tests: Runtime-generated structs with explicit bucket via db.From
// ============================================================================

func TestDynamicStructSaveAndOneViaFrom(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Build a runtime struct: { ID int `db:"id,increment"`, Name string `db:"index"` }
	dynType := reflect.StructOf([]reflect.StructField{
		{
			Name: "ID",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"id,increment"`),
		},
		{
			Name: "Name",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"index"`),
		},
	})

	bucketName := "dyn_users"
	node := db.From(bucketName)

	// Save
	val := reflect.New(dynType)
	val.Elem().FieldByName("Name").SetString("DynamicAlice")
	err := node.Save(ctx, val.Interface())
	require.NoError(t, err)

	// Verify ID was set
	require.Equal(t, int64(1), val.Elem().FieldByName("ID").Int())

	// Read back via One
	userPtr := reflect.New(dynType).Interface()
	err = node.One(ctx, "ID", 1, userPtr)
	require.NoError(t, err)
}

func TestDynamicStructAllViaFrom(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	dynType := reflect.StructOf([]reflect.StructField{
		{
			Name: "ID",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"id,increment"`),
		},
		{
			Name: "Name",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"index"`),
		},
		{
			Name: "Score",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"index"`),
		},
	})

	bucketName := "dyn_players"
	node := db.From(bucketName)

	// Save multiple records
	for i := 0; i < 5; i++ {
		val := reflect.New(dynType)
		val.Elem().FieldByName("Name").SetString(fmt.Sprintf("Player%d", i))
		val.Elem().FieldByName("Score").SetInt(int64(i * 100))
		err := node.Save(ctx, val.Interface())
		require.NoError(t, err)
	}

	// Copy
	sliceType := reflect.SliceOf(reflect.PointerTo(dynType))
	resultsVal := reflect.New(sliceType)

	err := node.All(ctx, resultsVal.Interface())
	require.NoError(t, err)

	slice := resultsVal.Elem()
	require.Equal(t, 5, slice.Len())

	// Verify data
	elem := slice.Index(0).Elem()
	require.Equal(t, int64(1), elem.FieldByName("ID").Int())
	require.Equal(t, "Player0", elem.FieldByName("Name").String())
}

func TestDynamicStructFindByIndexViaFrom(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	dynType := reflect.StructOf([]reflect.StructField{
		{
			Name: "ID",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"id,increment"`),
		},
		{
			Name: "Name",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"index"`),
		},
		{
			Name: "Score",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"index"`),
		},
	})

	bucketName := "dyn_athletes"
	node := db.From(bucketName)

	// Save records
	scores := []int64{100, 200, 100, 300, 200}
	for i, score := range scores {
		val := reflect.New(dynType)
		val.Elem().FieldByName("Name").SetString(fmt.Sprintf("Athlete%d", i))
		val.Elem().FieldByName("Score").SetInt(score)
		err := node.Save(ctx, val.Interface())
		require.NoError(t, err)
	}

	// Find by indexed field (Score = 100)
	sliceType := reflect.SliceOf(reflect.PointerTo(dynType))
	resultsVal := reflect.New(sliceType)

	err := node.Find(ctx, "Score", 100, resultsVal.Interface())
	require.NoError(t, err)

	slice := resultsVal.Elem()
	require.Equal(t, 2, slice.Len())
}

// ============================================================================
// Tests: Multitenancy / Data Isolation
// ============================================================================

func TestDynamicStructBucketIsolation(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	dynType := reflect.StructOf([]reflect.StructField{
		{
			Name: "ID",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"id,increment"`),
		},
		{
			Name: "Value",
			Type: reflect.TypeOf(""),
		},
	})

	bucketA := db.From("tenant_a")
	bucketB := db.From("tenant_b")

	// Save to bucket A
	valA := reflect.New(dynType)
	valA.Elem().FieldByName("Value").SetString("data_from_a")
	err := bucketA.Save(ctx, valA.Interface())
	require.NoError(t, err)

	// Save to bucket B
	valB := reflect.New(dynType)
	valB.Elem().FieldByName("Value").SetString("data_from_b")
	err = bucketB.Save(ctx, valB.Interface())
	require.NoError(t, err)

	// Read from bucket A
	sliceType := reflect.SliceOf(reflect.PointerTo(dynType))
	resultsA := reflect.New(sliceType)
	err = bucketA.All(ctx, resultsA.Interface())
	require.NoError(t, err)
	require.Equal(t, 1, resultsA.Elem().Len())
	require.Equal(t, "data_from_a", resultsA.Elem().Index(0).Elem().FieldByName("Value").String())

	// Read from bucket B
	resultsB := reflect.New(sliceType)
	err = bucketB.All(ctx, resultsB.Interface())
	require.NoError(t, err)
	require.Equal(t, 1, resultsB.Elem().Len())
	require.Equal(t, "data_from_b", resultsB.Elem().Index(0).Elem().FieldByName("Value").String())
}

// ============================================================================
// Tests: Dynamic structs with Select (combined queries)
// ============================================================================

func TestDynamicStructSelectCombined(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	dynType := reflect.StructOf([]reflect.StructField{
		{
			Name: "ID",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"id,increment"`),
		},
		{
			Name: "Category",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"index"`),
		},
		{
			Name: "Price",
			Type: reflect.TypeOf(0),
			Tag:  reflect.StructTag(`db:"index"`),
		},
	})

	bucketName := "dyn_products"
	node := db.From(bucketName)

	// Save products
	type productSpec struct {
		category string
		price    int64
	}
	products := []productSpec{
		{"book", 10},
		{"book", 20},
		{"game", 50},
		{"book", 15},
		{"game", 60},
	}
	for _, p := range products {
		val := reflect.New(dynType)
		val.Elem().FieldByName("Category").SetString(p.category)
		val.Elem().FieldByName("Price").SetInt(p.price)
		err := node.Save(ctx, val.Interface())
		require.NoError(t, err)
	}

	// Count all
	count, err := node.Count(ctx, reflect.New(dynType).Interface())
	require.NoError(t, err)
	require.Equal(t, 5, count)

	// Copy
	sliceType := reflect.SliceOf(reflect.PointerTo(dynType))
	results := reflect.New(sliceType)
	err = node.All(ctx, results.Interface())
	require.NoError(t, err)
	require.Equal(t, 5, results.Elem().Len())
}

// ============================================================================
// Tests: DeleteStruct with dynamic types
// ============================================================================

func TestDynamicStructDelete(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	dynType := reflect.StructOf([]reflect.StructField{
		{
			Name: "ID",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"id"`),
		},
		{
			Name: "Data",
			Type: reflect.TypeOf(""),
		},
	})

	bucketName := "dyn_deletable"
	node := db.From(bucketName)

	val := reflect.New(dynType)
	val.Elem().FieldByName("ID").SetString("key-1")
	val.Elem().FieldByName("Data").SetString("some data")
	err := node.Save(ctx, val.Interface())
	require.NoError(t, err)

	// Delete
	err = node.DeleteStruct(ctx, val.Interface())
	require.NoError(t, err)

	// Verify deleted
	err = node.One(ctx, "ID", "key-1", reflect.New(dynType).Interface())
	require.ErrorIs(t, err, ErrNotFound)
}

// ============================================================================
// Tests: Init with dynamic types
// ============================================================================

func TestDynamicStructInit(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	dynType := reflect.StructOf([]reflect.StructField{
		{
			Name: "ID",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"id"`),
		},
		{
			Name: "Email",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"unique"`),
		},
		{
			Name: "Group",
			Type: reflect.TypeOf(""),
			Tag:  reflect.StructTag(`db:"index"`),
		},
	})

	bucketName := "dyn_init_test"
	n := db.From(bucketName)

	proto := reflect.New(dynType).Interface()
	err := n.Init(ctx, proto)
	require.NoError(t, err)

	// Verify bucket exists
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b := db.Node.(*node).getBucket(tx, bucketName)
		require.NotNil(t, b)

		// Check index buckets were created
		require.NotNil(t, b.Bucket([]byte(indexPrefix+"Email")))
		require.NotNil(t, b.Bucket([]byte(indexPrefix+"Group")))
		return nil
	})
	require.NoError(t, err)
}

// ============================================================================
// Tests: Save to explicit bucket using db.From
// ============================================================================

func TestExplicitBucketFromSave(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "eidos")
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "eidos.db")
	ctx := context.Background()
	bDB, err := bolt.Open(file, 0600, nil)
	require.NoError(t, err)
	defer bDB.Close()
	db, err := Open(ctx, bDB, Config{})
	require.NoError(t, err)
	defer db.Close()

	type User struct {
		ID   int    `db:"id,increment"`
		Name string `db:"index"`
	}

	// Save with explicit bucket using db.From
	node := db.From("explicit_users")
	u := User{Name: "ExplicitAlice"}
	err = node.Save(ctx, &u)
	require.NoError(t, err)
	require.Equal(t, 1, u.ID)

	// Read back from the correct bucket
	var u2 User
	err = node.One(ctx, "ID", 1, &u2)
	require.NoError(t, err)
	require.Equal(t, "ExplicitAlice", u2.Name)

	// Verify it's NOT in the default bucket
	err = db.One(ctx, "ID", 1, &u2)
	require.ErrorIs(t, err, ErrNotFound)
}

// ============================================================================
// Tests: BucketNamer for automatic name resolution
// ============================================================================

func TestBucketNamerInterface(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "eidos")
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "eidos.db")
	ctx := context.Background()
	bDB, err := bolt.Open(file, 0600, nil)
	require.NoError(t, err)
	defer bDB.Close()
	db, err := Open(ctx, bDB, Config{})
	require.NoError(t, err)
	defer db.Close()

	u := CustomBucketUser{Name: "BucketUser"}
	err = db.Save(ctx, &u)
	require.NoError(t, err)

	// Read back using the same node (root db) - BucketNamer resolves the bucket
	var u2 CustomBucketUser
	err = db.One(ctx, "ID", 1, &u2)
	require.NoError(t, err)
	require.Equal(t, "BucketUser", u2.Name)

	// Verify it's NOT accessible via the type name "CustomBucketUser" bucket
	err = db.From("CustomBucketUser").One(ctx, "ID", 1, &u2)
	require.ErrorIs(t, err, ErrNotFound)
}

// ============================================================================
// Tests: Update and UpdateField with BucketNamer
// ============================================================================

func TestBucketNamerUpdate(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "eidos")
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "eidos.db")
	ctx := context.Background()
	bDB, err := bolt.Open(file, 0600, nil)
	require.NoError(t, err)
	defer bDB.Close()
	db, err := Open(ctx, bDB, Config{})
	require.NoError(t, err)
	defer db.Close()

	u := NamerUser{Name: "Original", Email: "original@test.com"}
	err = db.Save(ctx, &u)
	require.NoError(t, err)

	// Update using the root db (BucketNamer resolves the bucket)
	err = db.Update(ctx, &NamerUser{ID: 1, Name: "Updated"})
	require.NoError(t, err)

	// UpdateField
	err = db.UpdateField(ctx, &NamerUser{ID: 1}, "Email", "updated@test.com")
	require.NoError(t, err)

	// Verify using the root db
	var u2 NamerUser
	err = db.One(ctx, "ID", 1, &u2)
	require.NoError(t, err)
	require.Equal(t, "Updated", u2.Name)
	require.Equal(t, "updated@test.com", u2.Email)
}

// ============================================================================
// Tests: Drop with BucketNamer
// ============================================================================
func TestBucketNamerDrop(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "eidos")
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "eidos.db")
	ctx := context.Background()
	bDB, err := bolt.Open(file, 0600, nil)
	require.NoError(t, err)
	defer bDB.Close()
	db, err := Open(ctx, bDB, Config{})
	require.NoError(t, err)
	defer db.Close()

	u := DropUser{ID: 1, Name: "ToDrop"}
	err = db.Save(ctx, &u)
	require.NoError(t, err)

	// Drop resolves bucket name via BucketNamer
	err = db.Drop(ctx, &DropUser{})
	require.NoError(t, err)

	// Verify bucket is gone
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b := db.Node.(*node).getBucket(tx, "drop_me")
		require.Nil(t, b)
		return nil
	})
	require.NoError(t, err)
}

// ============================================================================
// Regression: newListSink must resolve BucketNamer before static type name.
// This proves Copy/Find on named value-receiver BucketNamer types work.
// ============================================================================

func TestBucketNamerListSinkResolution(t *testing.T) {
	file := filepath.Join(t.TempDir(), "eidos.db")
	ctx := context.Background()
	bDB, err := bolt.Open(file, 0600, nil)
	require.NoError(t, err)
	defer bDB.Close()
	db, err := Open(ctx, bDB, Config{})
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// Use NamerUser: Go type name is "NamerUser", EidosBucketName is "namer_users".
	users := []NamerUser{
		{Name: "Alice", Email: "alice@test.com"},
		{Name: "Bob", Email: "bob@test.com"},
	}
	for i := range users {
		err := db.Save(ctx, &users[i])
		require.NoError(t, err)
	}

	// Copy: must read both records from the custom bucket.
	var all []NamerUser
	err = db.All(ctx, &all)
	require.NoError(t, err)
	require.Len(t, all, 2)

	byName := make(map[string]NamerUser, 2)
	for _, u := range all {
		byName[u.Name] = u
	}
	require.Contains(t, byName, "Alice")
	require.Contains(t, byName, "Bob")
	require.Equal(t, "alice@test.com", byName["Alice"].Email)
	require.Equal(t, "bob@test.com", byName["Bob"].Email)

	// Find on indexed field must read records from the custom bucket.
	var aliceResults []NamerUser
	err = db.Find(ctx, "Name", "Alice", &aliceResults)
	require.NoError(t, err)
	require.Len(t, aliceResults, 1)
	require.Equal(t, "Alice", aliceResults[0].Name)

	// Prove the Go type-name bucket is not being used.
	// With the fix: db.Copy should read from "namer_users" (via BucketNamer).
	// Without the fix: db.Copy would read from "NamerUser" and return empty.
	// So db.From("NamerUser") reading should be empty regardless (data is not stored there).
	goTypeNode := db.From("NamerUser")
	var goTypeAll []NamerUser
	err = goTypeNode.All(ctx, &goTypeAll)
	require.NoError(t, err)
	require.Len(t, goTypeAll, 0, "Go type-name bucket must not contain the data")

	// Unique lookup also works (routed through a different sink path).
	var bob NamerUser
	err = db.One(ctx, "Email", "bob@test.com", &bob)
	require.NoError(t, err)
	require.Equal(t, "Bob", bob.Name)
}
