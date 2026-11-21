package storm

import (
	"os"
	"time"

	"github.com/malivvan/storm/codec"
	bolt "go.etcd.io/bbolt"
)

// testOpenOption configures createDB. It mirrors the removed Open options for
// test convenience only.
type testOpenOption func(*testOpenOptions)

type testOpenOptions struct {
	root        []string
	codec       codec.MarshalUnmarshaler
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
func testCodec(c codec.MarshalUnmarshaler) testOpenOption {
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
