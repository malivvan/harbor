package q_test

import (
	"context"
	"fmt"
	"log"

	"time"

	"os"

	"path/filepath"
	"strings"

	tempest "github.com/malivvan/tempest/store/record"
	"github.com/malivvan/tempest/store/record/q"
	bolt "go.etcd.io/bbolt"
)

func ExampleRe() {
	dir, db, bdb := prepareDB()
	defer os.RemoveAll(dir)
	defer db.Close()
	defer bdb.Close()

	ctx := context.Background()

	var users []User

	// Find all users with name that starts with the letter D.
	if err := db.Select(q.Re("Name", "^D")).Find(ctx, &users); err != nil {
		log.Println("error: Select failed:", err)
		return
	}

	// Donald and Dilbert
	fmt.Println("Found", len(users), "users.")

	// Output:
	// Found 2 users.
}

type User struct {
	ID        int    `db:"id,increment"`
	Group     string `db:"index"`
	Email     string `db:"unique"`
	Name      string
	Age       int       `db:"index"`
	CreatedAt time.Time `db:"index"`
}

func prepareDB() (string, *tempest.Store, *bolt.DB) {
	dir, err := os.MkdirTemp(os.TempDir(), "tempest")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	bdb, err := bolt.Open(filepath.Join(dir, "tempest.db"), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	db, err := tempest.Open(ctx, bdb, tempest.Config{})
	if err != nil {
		log.Fatal(err)
	}

	for i, name := range []string{"John", "Norm", "Donald", "Eric", "Dilbert"} {
		email := strings.ToLower(name + "@provider.com")
		user := User{
			Group:     "staff",
			Email:     email,
			Name:      name,
			Age:       21 + i,
			CreatedAt: time.Now(),
		}
		err = db.Save(ctx, &user)

		if err != nil {
			log.Fatal(err)
		}
	}

	return dir, db, bdb
}
