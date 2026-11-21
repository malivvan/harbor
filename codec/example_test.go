package codec_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/malivvan/storm"
	"github.com/malivvan/storm/codec"
	"github.com/malivvan/storm/codec/gob"
	"github.com/malivvan/storm/codec/json"
	bolt "go.etcd.io/bbolt"
)

func Example() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "storm-codecs")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Println("remove temporary directory:", err)
		}
	}()

	codecs := []struct {
		name  string
		codec codec.MarshalUnmarshaler
	}{
		{name: "gob", codec: gob.Codec},
		{name: "json", codec: json.Codec},
	}

	for _, candidate := range codecs {
		bdb, err := bolt.Open(filepath.Join(dir, candidate.name+".db"), 0600, nil)
		if err != nil {
			log.Fatal(err)
		}

		db, err := storm.Open(ctx, bdb, nil, candidate.codec)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%T\n", db.Codec())
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
		if err := bdb.Close(); err != nil {
			log.Fatal(err)
		}
	}

	// Output:
	// *gob.gobCodec
	// *json.jsonCodec
}
