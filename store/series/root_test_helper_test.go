package series

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

// openStore keeps the old root-argument shape for tests while the production
// API takes the root in Config.
func openStore(ctx context.Context, bdb *bolt.DB, root []byte, cfg Config) (*Store, error) {
	if len(root) > 0 {
		cfg.Root = [][]byte{root}
	}
	return Open(ctx, bdb, cfg)
}
