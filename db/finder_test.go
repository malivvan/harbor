package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/malivvan/harbor/db/index"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestFind(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		w := User{Name: "John", ID: i + 1, Slug: fmt.Sprintf("John%d", i+1)}

		if i%2 == 0 {
			w.Group = "staff"
		} else {
			w.Group = "normal"
		}

		err := db.Save(ctx, &w)
		require.NoError(t, err)
	}

	err := db.Find(ctx, "Name", "John", &User{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSlicePtrNeeded)

	err = db.Find(ctx, "Name", "John", &[]struct {
		Name string
		ID   int
	}{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoName)

	notTheRightUsers := []UniqueNameUser{}

	err = db.Find(ctx, "Name", "John", &notTheRightUsers)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	users := []User{}

	err = db.Find(ctx, "unexportedField", "John", &users)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "harbor find:"),
		"expected prefix 'harbor find:', got %q", err.Error())
	require.True(t, strings.HasSuffix(err.Error(), "field unexportedField not found"))

	err = db.Find(ctx, "DateOfBirth", "John", &users)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.Find(ctx, "Group", "staff", &users)
	require.NoError(t, err)
	require.Len(t, users, 50)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 99, users[49].ID)

	err = db.Find(ctx, "Group", "staff", &users, Reverse())
	require.NoError(t, err)
	require.Len(t, users, 50)
	require.Equal(t, 99, users[0].ID)
	require.Equal(t, 1, users[49].ID)

	err = db.Find(ctx, "Group", "admin", &users)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.Find(ctx, "Name", "John", users)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSlicePtrNeeded)

	err = db.Find(ctx, "Name", "John", &users)
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 100, users[99].ID)

	err = db.Find(ctx, "Name", "John", &users, Reverse())
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 100, users[0].ID)
	require.Equal(t, 1, users[99].ID)

	users = []User{}
	err = db.Find(ctx, "Slug", "John10", &users)
	require.NoError(t, err)
	require.Len(t, users, 1)
	require.Equal(t, 10, users[0].ID)

	users = []User{}
	err = db.Find(ctx, "Name", nil, &users)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.Find(ctx, "Name", "John", &users, Limit(10), Skip(20))
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 21, users[0].ID)
	require.Equal(t, 30, users[9].ID)

	err = db.Find(ctx, "Age", 10, &users)
	require.NoError(t, err)
}

func TestFindNil(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type User struct {
		ID        int        `db:"increment"`
		CreatedAt *time.Time `db:"index"`
		DeletedAt *time.Time `db:"unique"`
	}

	t1 := time.Now()
	for i := 0; i < 10; i++ {
		now := time.Now()
		var u User

		if i%2 == 0 {
			u.CreatedAt = &t1
			u.DeletedAt = &now
		}

		err := db.Save(ctx, &u)
		require.NoError(t, err)
	}

	var users []User
	err := db.Find(ctx, "CreatedAt", nil, &users)
	require.NoError(t, err)
	require.Len(t, users, 5)

	users = nil
	err = db.Find(ctx, "CreatedAt", t1, &users)
	require.NoError(t, err)
	require.Len(t, users, 5)

	users = nil
	err = db.Find(ctx, "DeletedAt", nil, &users)
	require.NoError(t, err)
	require.Len(t, users, 5)
}

func TestFindIntIndex(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type Score struct {
		ID    int    `db:"increment"`
		Score uint64 `db:"index"`
	}

	for i := 0; i < 10; i++ {
		w := Score{Score: uint64(i % 3)}
		err := db.Save(ctx, &w)
		require.NoError(t, err)
	}

	var scores []Score
	err := db.Find(ctx, "Score", 2, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 3)
	require.Equal(t, []Score{
		{ID: 3, Score: 2},
		{ID: 6, Score: 2},
		{ID: 9, Score: 2},
	}, scores)
}

func TestAllByIndex(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		w := User{Name: "John", ID: i + 1, Slug: fmt.Sprintf("John%d", i+1), DateOfBirth: time.Now().Add(-time.Duration(i*10) * time.Minute)}
		err := db.Save(ctx, &w)
		require.NoError(t, err)
	}

	err := db.AllByIndex(ctx, "", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSlicePtrNeeded)

	var users []User

	err = db.AllByIndex(ctx, "Unknown field", &users)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.AllByIndex(ctx, "DateOfBirth", &users)
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 100, users[0].ID)
	require.Equal(t, 1, users[99].ID)

	err = db.AllByIndex(ctx, "Name", &users)
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 100, users[99].ID)

	y := UniqueNameUser{Name: "Jake", ID: 200}
	err = db.Save(ctx, &y)
	require.NoError(t, err)

	var y2 []UniqueNameUser
	err = db.AllByIndex(ctx, "ID", &y2)
	require.NoError(t, err)
	require.Len(t, y2, 1)

	n := NestedID{}
	n.ID = "100"
	n.Name = "John"

	err = db.Save(ctx, &n)
	require.NoError(t, err)

	var n2 []NestedID
	err = db.AllByIndex(ctx, "ID", &n2)
	require.NoError(t, err)
	require.Len(t, n2, 1)

	err = db.AllByIndex(ctx, "Name", &users, Limit(10))
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 10, users[9].ID)

	err = db.AllByIndex(ctx, "Name", &users, Limit(200))
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 100, users[99].ID)

	err = db.AllByIndex(ctx, "Name", &users, Limit(-10))
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 100, users[99].ID)

	err = db.AllByIndex(ctx, "Name", &users, Skip(200))
	require.NoError(t, err)
	require.Len(t, users, 0)

	err = db.AllByIndex(ctx, "Name", &users, Skip(-10))
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 100, users[99].ID)

	err = db.AllByIndex(ctx, "ID", &users)
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 100, users[99].ID)

	err = db.AllByIndex(ctx, "ID", &users, Limit(10))
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 10, users[9].ID)

	err = db.AllByIndex(ctx, "ID", &users, Skip(10))
	require.NoError(t, err)
	require.Len(t, users, 90)
	require.Equal(t, 11, users[0].ID)
	require.Equal(t, 100, users[89].ID)

	err = db.AllByIndex(ctx, "Name", &users, Limit(10), Skip(10))
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 11, users[0].ID)
	require.Equal(t, 20, users[9].ID)

	err = db.AllByIndex(ctx, "Name", &users, Limit(10), Skip(10), Reverse())
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 90, users[0].ID)
	require.Equal(t, 81, users[9].ID)

	err = db.AllByIndex(ctx, "Age", &users, Limit(10))
	require.NoError(t, err)
	require.Len(t, users, 10)
}

func TestAll(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		w := User{Name: "John", ID: i + 1, Slug: fmt.Sprintf("John%d", i+1), DateOfBirth: time.Now().Add(-time.Duration(i*10) * time.Minute)}
		err := db.Save(ctx, &w)
		require.NoError(t, err)
	}

	var users []User

	err := db.All(ctx, &users)
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 100, users[99].ID)

	err = db.All(ctx, &users, Reverse())
	require.NoError(t, err)
	require.Len(t, users, 100)
	require.Equal(t, 100, users[0].ID)
	require.Equal(t, 1, users[99].ID)

	var users2 []*User

	err = db.All(ctx, &users2)
	require.NoError(t, err)
	require.Len(t, users2, 100)
	require.Equal(t, 1, users2[0].ID)
	require.Equal(t, 100, users2[99].ID)

	err = db.Save(ctx, &NestedID{
		ToEmbed: ToEmbed{ID: "id1"},
		Name:    "John",
	})
	require.NoError(t, err)

	err = db.Save(ctx, &NestedID{
		ToEmbed: ToEmbed{ID: "id2"},
		Name:    "Mike",
	})
	require.NoError(t, err)

	db.Save(ctx, &NestedID{
		ToEmbed: ToEmbed{ID: "id3"},
		Name:    "Steve",
	})
	require.NoError(t, err)

	var nested []NestedID
	err = db.All(ctx, &nested)
	require.NoError(t, err)
	require.Len(t, nested, 3)

	err = db.All(ctx, &users, Limit(10), Skip(10))
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 11, users[0].ID)
	require.Equal(t, 20, users[9].ID)

	err = db.All(ctx, &users, Limit(0), Skip(0))
	require.NoError(t, err)
	require.Len(t, users, 0)
}

func TestCount(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		w := User{Name: "John", ID: i + 1, Slug: fmt.Sprintf("John%d", i+1), DateOfBirth: time.Now().Add(-time.Duration(i*10) * time.Minute)}
		err := db.Save(ctx, &w)
		require.NoError(t, err)
	}

	count, err := db.Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 100, count)

	w := User{Name: "John", ID: 101, Slug: fmt.Sprintf("John%d", 101), DateOfBirth: time.Now().Add(-time.Duration(101*10) * time.Minute)}
	err = db.Save(ctx, &w)
	require.NoError(t, err)

	count, err = db.Count(ctx, &User{})
	require.NoError(t, err)
	require.Equal(t, 101, count)

	err = db.WriteTransaction(ctx, func(txn Node) error {
		_, cerr := txn.Count(ctx, User{})
		require.ErrorIs(t, cerr, ErrStructPtrNeeded)

		c, cerr := txn.Count(ctx, &User{})
		require.NoError(t, cerr)
		require.Equal(t, 101, c)

		w2 := User{Name: "John", ID: 102, Slug: fmt.Sprintf("John%d", 102), DateOfBirth: time.Now().Add(-time.Duration(101*10) * time.Minute)}
		cerr = txn.Save(ctx, &w2)
		require.NoError(t, cerr)

		c, cerr = txn.Count(ctx, &User{})
		require.NoError(t, cerr)
		require.Equal(t, 102, c)

		return nil
	})
	require.NoError(t, err)
}

func TestCountEmpty(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	user := &User{}
	err := db.Init(ctx, user)
	require.NoError(t, err)

	count, err := db.Count(ctx, user)
	require.Zero(t, count)
	require.NoError(t, err)
}

func TestOne(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	u := UniqueNameUser{Name: "John", ID: 10}
	err := db.Save(ctx, &u)
	require.NoError(t, err)

	v := UniqueNameUser{}
	err = db.One(ctx, "Name", "John", &v)
	require.NoError(t, err)
	require.Equal(t, u, v)

	for i := 0; i < 10; i++ {
		w := IndexedNameUser{Name: "John", ID: i + 1, Group: "staff"}
		err = db.Save(ctx, &w)
		require.NoError(t, err)
	}

	var x IndexedNameUser
	err = db.One(ctx, "Name", "John", &x)
	require.NoError(t, err)
	require.Equal(t, "John", x.Name)
	require.Equal(t, 1, x.ID)
	require.Zero(t, x.age)
	require.True(t, x.DateOfBirth.IsZero())

	err = db.One(ctx, "Name", "Mike", &x)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.One(ctx, "", nil, &x)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.One(ctx, "", "Mike", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrStructPtrNeeded)

	err = db.One(ctx, "", nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrStructPtrNeeded)

	err = db.One(ctx, "Group", "staff", &x)
	require.NoError(t, err)
	require.Equal(t, 1, x.ID)

	err = db.One(ctx, "Score", 5, &x)
	require.NoError(t, err)
	require.Equal(t, 5, x.ID)

	err = db.One(ctx, "Group", "admin", &x)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	y := UniqueNameUser{Name: "Jake", ID: 200}
	err = db.Save(ctx, &y)
	require.NoError(t, err)

	var y2 UniqueNameUser
	err = db.One(ctx, "ID", 200, &y2)
	require.NoError(t, err)
	require.Equal(t, y, y2)

	n := NestedID{}
	n.ID = "100"
	n.Name = "John"

	err = db.Save(ctx, &n)
	require.NoError(t, err)

	var n2 NestedID
	err = db.One(ctx, "ID", "100", &n2)
	require.NoError(t, err)
	require.Equal(t, n, n2)
}

func TestOneNotWritable(t *testing.T) {
	dir, _ := os.MkdirTemp(os.TempDir(), "harbor")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "harbor.db")
	ctx := context.Background()

	bDB, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	db, err := Open(ctx, bDB, Config{})
	require.NoError(t, err)

	err = db.Save(ctx, &User{ID: 10, Name: "John"})
	require.NoError(t, err)

	require.NoError(t, db.Close())
	require.NoError(t, bDB.Close())

	roBDB, err := bolt.Open(path, 0660, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer roBDB.Close()
	db, err = Open(ctx, roBDB, Config{})
	require.NoError(t, err)
	defer db.Close()

	err = db.Save(ctx, &User{ID: 20, Name: "John"})
	require.Error(t, err)

	var u User
	err = db.One(ctx, "ID", 10, &u)
	require.NoError(t, err)
	require.Equal(t, 10, u.ID)
	require.Equal(t, "John", u.Name)

	err = db.One(ctx, "Name", "John", &u)
	require.NoError(t, err)
	require.Equal(t, 10, u.ID)
	require.Equal(t, "John", u.Name)
}

func TestRange(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		w := User{
			Name:        "John",
			ID:          i + 1,
			Slug:        fmt.Sprintf("John%03d", i+1),
			DateOfBirth: time.Now().Add(-time.Duration(i) * time.Hour),
			Group:       fmt.Sprintf("Group%03d", i%5),
		}
		err := db.Save(ctx, &w)
		require.NoError(t, err)
		z := User{Name: fmt.Sprintf("Zach%03d", i+1), ID: i + 101, Slug: fmt.Sprintf("Zach%03d", i+1)}
		err = db.Save(ctx, &z)
		require.NoError(t, err)
	}

	min := "John010"
	max := "John020"
	var users []User

	err := db.Range(ctx, "Slug", min, max, users)
	require.ErrorIs(t, err, ErrSlicePtrNeeded)

	err = db.Range(ctx, "Slug", min, max, &users)
	require.NoError(t, err)
	require.Len(t, users, 11)
	require.Equal(t, "John010", users[0].Slug)
	require.Equal(t, "John020", users[10].Slug)

	err = db.Range(ctx, "Slug", min, max, &users, Reverse())
	require.NoError(t, err)
	require.Len(t, users, 11)
	require.Equal(t, "John020", users[0].Slug)
	require.Equal(t, "John010", users[10].Slug)

	min = "Zach010"
	max = "Zach020"
	users = nil
	err = db.Range(ctx, "Name", min, max, &users)
	require.NoError(t, err)
	require.Len(t, users, 11)
	require.Equal(t, "Zach010", users[0].Name)
	require.Equal(t, "Zach020", users[10].Name)

	err = db.Range(ctx, "Name", min, max, &users, Reverse())
	require.NoError(t, err)
	require.Len(t, users, 11)
	require.Equal(t, "Zach020", users[0].Name)
	require.Equal(t, "Zach010", users[10].Name)

	err = db.Range(ctx, "Name", min, max, &User{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSlicePtrNeeded)

	notTheRightUsers := []UniqueNameUser{}

	err = db.Range(ctx, "Name", min, max, &notTheRightUsers)
	require.NoError(t, err)
	require.Equal(t, 0, len(notTheRightUsers))

	users = nil

	err = db.Range(ctx, "Age", min, max, &users)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.Range(ctx, "Age", 2, 5, &users)
	require.NoError(t, err)
	require.Len(t, users, 4)

	dateMin := time.Now().Add(-time.Duration(50) * time.Hour)
	dateMax := dateMin.Add(time.Duration(3) * time.Hour)
	err = db.Range(ctx, "DateOfBirth", dateMin, dateMax, &users)
	require.NoError(t, err)
	require.Len(t, users, 3)
	require.Equal(t, "John050", users[0].Slug)
	require.Equal(t, "John048", users[2].Slug)

	err = db.Range(ctx, "Slug", "John010", "John040", &users, Limit(10), Skip(20))
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 30, users[0].ID)
	require.Equal(t, 39, users[9].ID)

	err = db.Range(ctx, "Slug", "John010", "John040", &users, Limit(10), Skip(20), Reverse())
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 20, users[0].ID)
	require.Equal(t, 11, users[9].ID)

	err = db.Range(ctx, "Group", "Group002", "Group004", &users)
	require.NoError(t, err)
	require.Len(t, users, 60)
}

func TestPrefix(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 50; i++ {
		w := User{
			ID: i + 1,
		}

		if i%5 == 0 {
			w.Name = "John"
			w.Group = "group100"
		} else {
			w.Name = "Jack"
			w.Group = "group200"
		}

		err := db.Save(ctx, &w)
		require.NoError(t, err)
	}

	var users []User
	err := db.Prefix(ctx, "Name", "Jo", users)
	require.ErrorIs(t, err, ErrSlicePtrNeeded)

	// Using indexes
	err = db.Prefix(ctx, "Name", "Jo", &users)
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 46, users[9].ID)

	err = db.Prefix(ctx, "Name", "Ja", &users)
	require.NoError(t, err)
	require.Len(t, users, 40)
	require.Equal(t, 2, users[0].ID)
	require.Equal(t, 50, users[39].ID)

	err = db.Prefix(ctx, "Name", "Ja", &users, Limit(10), Skip(20), Reverse())
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 25, users[0].ID)
	require.Equal(t, 14, users[9].ID)

	// Using Select
	err = db.Prefix(ctx, "Group", "group1", &users)
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 1, users[0].ID)
	require.Equal(t, 46, users[9].ID)

	err = db.Prefix(ctx, "Group", "group2", &users)
	require.NoError(t, err)
	require.Len(t, users, 40)
	require.Equal(t, 2, users[0].ID)
	require.Equal(t, 50, users[39].ID)

	err = db.Prefix(ctx, "Group", "group2", &users, Limit(10), Skip(20), Reverse())
	require.NoError(t, err)
	require.Len(t, users, 10)
	require.Equal(t, 25, users[0].ID)
	require.Equal(t, 14, users[9].ID)

	// Bad value
	err = db.Prefix(ctx, "Group", "group3", &users)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestPrefixWithID(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type User struct {
		ID string
	}

	require.NoError(t, db.Save(ctx, &User{ID: "1"}))
	require.NoError(t, db.Save(ctx, &User{ID: "10"}))

	var users []User

	require.NoError(t, db.Prefix(ctx, "ID", "1", &users))
	require.Len(t, users, 2)
}

// ---------------------------------------------------------------------------
// finderStepContext — deterministic local context that cancels after N calls
// to Err(). Thread-safe, no timers, no goroutines.
// ---------------------------------------------------------------------------

type finderStepContext struct {
	mu       sync.Mutex
	calls    int
	cancelAt int
}

func (c *finderStepContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *finderStepContext) Done() <-chan struct{}       { return nil }
func (c *finderStepContext) Value(key interface{}) interface{} {
	return nil
}
func (c *finderStepContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls >= c.cancelAt {
		return context.Canceled
	}
	return nil
}

// Calls returns how many times Err() has been invoked.
func (c *finderStepContext) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// ---------------------------------------------------------------------------
// codecErrorCodec — wraps a delegate, unmarshals into to, then returns an
// error. Used to prove destination is preserved on codec error.
// ---------------------------------------------------------------------------

var errCodec = errors.New("codec error sentinel")

type codecErrorCodec struct {
	delegate Codec
}

func (c *codecErrorCodec) Name() string { return c.delegate.Name() }

func (c *codecErrorCodec) Marshal(v interface{}) ([]byte, error) {
	return c.delegate.Marshal(v)
}

func (c *codecErrorCodec) Unmarshal(raw []byte, to interface{}) error {
	// Unmarshal into the temporary first, then return a sentinel error.
	if err := c.delegate.Unmarshal(raw, to); err != nil {
		return err
	}
	return errCodec
}

// ---------------------------------------------------------------------------
// 12.1 One
// ---------------------------------------------------------------------------

func TestOne_IndexedCancellationDuringUnmarshalPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed a record with a normal codec.
	err := db.Save(context.Background(), &UniqueNameUser{ID: 1, Name: "john", Age: 10})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnUnmarshal(func() { cancel() })

	n := db.WithCodec(cc).(*node)
	dest := UniqueNameUser{ID: 999, Name: "SENTINEL", Age: 999}
	err = n.One(ctx, "Name", "john", &dest)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
	require.Equal(t, UniqueNameUser{ID: 999, Name: "SENTINEL", Age: 999}, dest,
		"destination must be preserved on cancellation")
}

func TestOne_IndexedCodecErrorPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed a record.
	err := db.Save(context.Background(), &UniqueNameUser{ID: 1, Name: "john", Age: 10})
	require.NoError(t, err)

	// Use the error codec so Unmarshal writes then returns an error.
	n := db.WithCodec(&codecErrorCodec{delegate: JSONCodec}).(*node)
	dest := UniqueNameUser{ID: 999, Name: "SENTINEL", Age: 999}
	err = n.One(context.Background(), "Name", "john", &dest)
	require.True(t, errors.Is(err, errCodec), "expected errCodec, got %v", err)
	// With codecErrorCodec, the delegate unmarshals successfully first,
	// so the temporary was filled. But since the error is returned,
	// the destination must still be the sentinel.
	require.Equal(t, UniqueNameUser{ID: 999, Name: "SENTINEL", Age: 999}, dest,
		"destination must be preserved on codec error")
}

func TestOne_ByIDCancellationDuringUnmarshalPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed via ID path.
	err := db.Save(context.Background(), &UniqueNameUser{ID: 42, Name: "by-id", Age: 10})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnUnmarshal(func() { cancel() })

	n := db.WithCodec(cc).(*node)
	dest := UniqueNameUser{ID: 999, Name: "SENTINEL", Age: 999}
	err = n.One(ctx, "ID", 42, &dest)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
	require.Equal(t, UniqueNameUser{ID: 999, Name: "SENTINEL", Age: 999}, dest,
		"destination must be preserved on cancellation (ID path)")
}

// ---------------------------------------------------------------------------
// 12.2 Find
// ---------------------------------------------------------------------------

func TestFind_IndexedCancellationDuringMaterializationPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed multiple records with Name indexed.
	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &IndexedNameUser{
			ID: i, Name: "test-find", Score: i * 10,
		})
		require.NoError(t, err)
	}

	// Cancel on the second unmarshal during materialization.
	ctx, cancel := context.WithCancel(context.Background())
	var unmarshalCalls int
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnUnmarshal(func() {
		unmarshalCalls++
		if unmarshalCalls >= 2 {
			cancel()
		}
	})

	n := db.WithCodec(cc).(*node)

	// Initialize destination with a sentinel non-empty slice.
	sentinel := IndexedNameUser{ID: 999, Name: "SENTINEL", Score: 999}
	dest := []IndexedNameUser{sentinel}
	err := n.Find(ctx, "Name", "test-find", &dest)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
	// The sentinel slice must be preserved — no partial results.
	require.Equal(t, []IndexedNameUser{sentinel}, dest,
		"destination must be preserved on cancellation during materialization")
	require.GreaterOrEqual(t, unmarshalCalls, 2,
		"must prove more than one checkpoint was traversed")
}

func TestFind_IndexedIndexCancellationPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed records.
	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &IndexedNameUser{
			ID: i, Name: "idx-cancel", Score: i * 10,
		})
		require.NoError(t, err)
	}

	// Public Find and readTx consume five checks before entering the helper.
	// The helper then checks entry, bucket and getIndex (calls 6-8), so call 9
	// is the first check inside idx.Copy.
	fsc := &finderStepContext{cancelAt: 9}
	n := db.Node.(*node)

	sentinel := IndexedNameUser{ID: 999, Name: "SENTINEL", Score: 999}
	dest := []IndexedNameUser{sentinel}
	err := n.Find(fsc, "Name", "idx-cancel", &dest)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []IndexedNameUser{sentinel}, dest,
		"destination must be preserved on index cancellation")
	require.GreaterOrEqual(t, fsc.Calls(), 9,
		"cancellation must occur inside idx.Copy")
}

func TestFind_IndexedSuccessReplacesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed records.
	for i := 1; i <= 3; i++ {
		err := db.Save(context.Background(), &IndexedNameUser{
			ID: i, Name: "success", Score: i * 10,
		})
		require.NoError(t, err)
	}

	// Initialize with a non-empty sentinel slice to prove replacement.
	sentinel := IndexedNameUser{ID: 999, Name: "SENTINEL", Score: 999}
	dest := []IndexedNameUser{sentinel}
	err := db.Find(context.Background(), "Name", "success", &dest)
	require.NoError(t, err)
	require.Len(t, dest, 3)
	require.Equal(t, 1, dest[0].ID)
	require.Equal(t, "success", dest[0].Name)
	require.Equal(t, 3, dest[2].ID)
	// Must not contain the sentinel — destination was replaced, not concatenated.
	for _, d := range dest {
		require.NotEqual(t, 999, d.ID, "sentinel must not appear in result")
	}
}

// ---------------------------------------------------------------------------
// 12.3 AllByIndex
// ---------------------------------------------------------------------------

func TestAllByIndex_CancellationDuringDecodePreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed records with DateOfBirth indexed (User struct has DateOfBirth index).
	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID:          i,
			Name:        "abx",
			DateOfBirth: time.Now().Add(-time.Duration(i) * time.Hour),
			Slug:        fmt.Sprintf("abx-%d", i),
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var unmarshalCalls int
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnUnmarshal(func() {
		unmarshalCalls++
		if unmarshalCalls >= 2 {
			cancel()
		}
	})

	n := db.WithCodec(cc).(*node)

	sentinel := User{ID: 999, Name: "SENTINEL"}
	dest := []User{sentinel}
	err := n.AllByIndex(ctx, "DateOfBirth", &dest)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
	require.Equal(t, []User{sentinel}, dest,
		"destination must be preserved on cancellation during decode")
	require.GreaterOrEqual(t, unmarshalCalls, 2,
		"must prove more than one checkpoint was traversed")
}

func TestAllByIndex_IndexCancellationPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID:          i,
			Name:        "allbyidx",
			DateOfBirth: time.Now().Add(-time.Duration(i) * time.Hour),
			Slug:        fmt.Sprintf("allbyidx-%d", i),
		})
		require.NoError(t, err)
	}

	// Public AllByIndex and readTx consume four checks. The helper consumes
	// entry, bucket, field and getIndex checks (calls 5-8), so call 9 is the
	// first check inside idx.AllRecords.
	fsc := &finderStepContext{cancelAt: 9}
	n := db.Node.(*node)

	sentinel := User{ID: 999, Name: "SENTINEL"}
	dest := []User{sentinel}
	err := n.AllByIndex(fsc, "DateOfBirth", &dest)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []User{sentinel}, dest,
		"destination must be preserved on index cancellation")
	require.GreaterOrEqual(t, fsc.Calls(), 9,
		"cancellation must occur inside idx.AllRecords")
}

func TestAllByIndex_CodecErrorPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID:          i,
			Name:        "codecerr",
			DateOfBirth: time.Now().Add(-time.Duration(i) * time.Hour),
			Slug:        fmt.Sprintf("codecerr-%d", i),
		})
		require.NoError(t, err)
	}

	n := db.WithCodec(&codecErrorCodec{delegate: JSONCodec}).(*node)

	sentinel := User{ID: 999, Name: "SENTINEL"}
	dest := []User{sentinel}
	err := n.AllByIndex(context.Background(), "DateOfBirth", &dest)
	require.True(t, errors.Is(err, errCodec), "expected errCodec, got %v", err)
	// Destination must remain the sentinel — no partial results.
	require.Equal(t, []User{sentinel}, dest,
		"destination must be preserved on codec error")
}

// ---------------------------------------------------------------------------
// 12.4 Range
// ---------------------------------------------------------------------------

func TestRange_IndexedCancellationDuringMaterializationPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed users with Slug unique indexed.
	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID:   i,
			Name: "range-test",
			Slug: "A" + string(rune('A'+i)),
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var unmarshalCalls int
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnUnmarshal(func() {
		unmarshalCalls++
		if unmarshalCalls >= 2 {
			cancel()
		}
	})

	n := db.WithCodec(cc).(*node)

	sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
	dest := []User{sentinel}
	err := n.Range(ctx, "Slug", "AB", "AF", &dest)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
	require.Equal(t, []User{sentinel}, dest,
		"destination must be preserved on cancellation during materialization")
	require.GreaterOrEqual(t, unmarshalCalls, 2,
		"must prove more than one checkpoint was traversed")
}

func TestRange_IndexedIndexCancellationPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID:   i,
			Name: "range-idx",
			Slug: "B" + string(rune('A'+i)),
		})
		require.NoError(t, err)
	}

	// Public Range and readTx consume six checks. The helper consumes entry,
	// bucket and getIndex checks (calls 7-9), so call 10 is the first check
	// inside idx.Range.
	fsc := &finderStepContext{cancelAt: 10}
	n := db.Node.(*node)

	sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
	dest := []User{sentinel}
	err := n.Range(fsc, "Slug", "BA", "BF", &dest)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []User{sentinel}, dest,
		"destination must be preserved on index cancellation")
	require.GreaterOrEqual(t, fsc.Calls(), 10,
		"cancellation must occur inside idx.Range")
}

func TestRange_IndexedEmptyResultPublishesEmptySlice(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// No records saved — the bucket for User will not exist.
	// Range on an indexed field with no bucket returns nil and empty slice.
	sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
	dest := []User{sentinel}
	err := db.Range(context.Background(), "Slug", "A", "Z", &dest)
	require.NoError(t, err)
	require.Len(t, dest, 0,
		"empty result must publish an empty slice")
}

// ---------------------------------------------------------------------------
// 12.5 Prefix
// ---------------------------------------------------------------------------

func TestPrefix_IndexedCancellationDuringMaterializationPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	// Seed users with Name indexed. Use unique Slug per record.
	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID: i, Name: "prefix-test", Slug: fmt.Sprintf("ptest-%d", i),
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var unmarshalCalls int
	cc := &cancellationCodec{delegate: JSONCodec}
	cc.setOnUnmarshal(func() {
		unmarshalCalls++
		if unmarshalCalls >= 2 {
			cancel()
		}
	})

	n := db.WithCodec(cc).(*node)

	sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
	dest := []User{sentinel}
	err := n.Prefix(ctx, "Name", "prefix", &dest)
	require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
	require.Equal(t, []User{sentinel}, dest,
		"destination must be preserved on cancellation during materialization")
	require.GreaterOrEqual(t, unmarshalCalls, 2,
		"must prove more than one checkpoint was traversed")
}

func TestPrefix_IndexedIndexCancellationPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID: i, Name: "pref-ix", Slug: fmt.Sprintf("pref-%d", i),
		})
		require.NoError(t, err)
	}

	// Public Prefix and readTx consume five checks. The helper consumes entry,
	// bucket and getIndex checks (calls 6-8), so call 9 is the first check
	// inside idx.Prefix.
	fsc := &finderStepContext{cancelAt: 9}
	n := db.Node.(*node)

	sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
	dest := []User{sentinel}
	err := n.Prefix(fsc, "Name", "pref", &dest)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []User{sentinel}, dest,
		"destination must be preserved on index cancellation")
	require.GreaterOrEqual(t, fsc.Calls(), 9,
		"cancellation must occur inside idx.Prefix")
}

func TestPrefix_IndexedSuccessPublishesCompleteResult(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		err := db.Save(context.Background(), &User{
			ID: i, Name: "pref-ok", Slug: fmt.Sprintf("pok-%d", i),
		})
		require.NoError(t, err)
	}

	sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
	dest := []User{sentinel}
	err := db.Prefix(context.Background(), "Name", "pref", &dest)
	require.NoError(t, err)
	require.Len(t, dest, 5)
	// Must not contain sentinel.
	for _, d := range dest {
		require.NotEqual(t, 999, d.ID, "sentinel must not appear in result")
	}
}

// ---------------------------------------------------------------------------
// 12.6 Options — cancellation during option stops before IO
// ---------------------------------------------------------------------------

func TestFinders_CancellationDuringOptionStopsBeforeIO(t *testing.T) {
	t.Run("Find", func(t *testing.T) {
		db, cleanup := createDB(t)
		defer cleanup()

		err := db.Save(context.Background(), &IndexedNameUser{
			ID: 1, Name: "opt-cancel", Score: 10,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		sentinel := IndexedNameUser{ID: 999, Name: "SENTINEL", Score: 999}
		dest := []IndexedNameUser{sentinel}
		err = db.Find(ctx, "Name", "opt-cancel", &dest, func(opts *index.Options) {
			cancel()
		})
		require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
		require.Equal(t, []IndexedNameUser{sentinel}, dest,
			"destination must be preserved after option cancellation")
	})

	t.Run("AllByIndex", func(t *testing.T) {
		db, cleanup := createDB(t)
		defer cleanup()

		for i := 1; i <= 3; i++ {
			err := db.Save(context.Background(), &User{
				ID: i, Name: "abx-opt", DateOfBirth: time.Now(),
				Slug: fmt.Sprintf("abx-opt-%d", i),
			})
			require.NoError(t, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		sentinel := User{ID: 999, Name: "SENTINEL"}
		dest := []User{sentinel}
		err := db.AllByIndex(ctx, "DateOfBirth", &dest, func(opts *index.Options) {
			cancel()
		})
		require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
		require.Equal(t, []User{sentinel}, dest,
			"destination must be preserved after option cancellation")
	})

	t.Run("Range", func(t *testing.T) {
		db, cleanup := createDB(t)
		defer cleanup()

		err := db.Save(context.Background(), &User{
			ID: 1, Name: "rng-opt", Slug: "ro1",
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
		dest := []User{sentinel}
		err = db.Range(ctx, "Slug", "A", "Z", &dest, func(opts *index.Options) {
			cancel()
		})
		require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
		require.Equal(t, []User{sentinel}, dest,
			"destination must be preserved after option cancellation")
	})

	t.Run("Prefix", func(t *testing.T) {
		db, cleanup := createDB(t)
		defer cleanup()

		err := db.Save(context.Background(), &User{
			ID: 1, Name: "popt", Slug: "popt",
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		sentinel := User{ID: 999, Name: "SENTINEL", Slug: "SENTINEL"}
		dest := []User{sentinel}
		err = db.Prefix(ctx, "Name", "p", &dest, func(opts *index.Options) {
			cancel()
		})
		require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
		require.Equal(t, []User{sentinel}, dest,
			"destination must be preserved after option cancellation")
	})

	t.Run("Copy", func(t *testing.T) {
		db, cleanup := createDB(t)
		defer cleanup()

		for i := 1; i <= 3; i++ {
			err := db.Save(context.Background(), &User{
				ID: i, Name: "all-opt", Slug: fmt.Sprintf("all-opt-%d", i),
			})
			require.NoError(t, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		sentinel := User{ID: 999, Name: "SENTINEL"}
		dest := []User{sentinel}
		err := db.All(ctx, &dest, func(opts *index.Options) {
			cancel()
		})
		require.True(t, errors.Is(err, context.Canceled), "expected Canceled, got %v", err)
		require.Equal(t, []User{sentinel}, dest,
			"destination must be preserved after option cancellation")
	})
}

func TestOne_IndexedRuntimeStructStillWorks(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	dynamicType := reflect.StructOf([]reflect.StructField{
		{Name: "ID", Type: reflect.TypeOf(0), Tag: reflect.StructTag(`db:"id"`)},
		{Name: "Name", Type: reflect.TypeOf(""), Tag: reflect.StructTag(`db:"index"`)},
	})

	n := db.From("runtime_one_indexed")
	record := reflect.New(dynamicType)
	record.Elem().FieldByName("ID").SetInt(100)
	record.Elem().FieldByName("Name").SetString("runtime-test")
	require.NoError(t, n.Save(ctx, record.Interface()))

	destination := reflect.New(dynamicType)
	require.NoError(t, n.One(ctx, "Name", "runtime-test", destination.Interface()))
	require.Equal(t, int64(100), destination.Elem().FieldByName("ID").Int())
	require.Equal(t, "runtime-test", destination.Elem().FieldByName("Name").String())
}
