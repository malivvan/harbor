package snapshot

// meter.go — the optional metering seam and the storage-usage report.
//
// Metering is deliberately an interface with no implementation here: what "bytes
// stored" means for billing is the application's business (an account, a plan, a
// price). A standalone deployment wires no meter and nothing is reported.

import (
	"context"
	"strings"
)

// Meterer reports snapshot storage growth to a billing or metering seam.
//
// It is optional. Implementations must be cheap and non-blocking: the snapshot
// path calls this inline and never waits for a result.
type Meterer interface {
	// MeterSnapshotBytes reports that bytes were added to snapshot storage for
	// accountID.
	MeterSnapshotBytes(ctx context.Context, accountID string, bytes int64)
}

// Usage is a breakdown of the bytes snapshot storage occupies.
type Usage struct {
	BlobBytes     int64 `json:"blob_bytes"`
	BlobCount     int   `json:"blob_count"`
	ManifestBytes int64 `json:"manifest_bytes"`
	IndexBytes    int64 `json:"index_bytes"`
	IndexCount    int   `json:"index_count"`
	TotalBytes    int64 `json:"total_bytes"`
}

// StorageUsage measures the exact bytes held under the snapshot artifact prefix,
// broken down by kind. This is the figure to report to an owner, and the one a
// Meterer normally reports.
func (s *Snapshotter) StorageUsage(ctx context.Context) (*Usage, error) {
	objs, err := s.store.List(ctx, s.snapPfx)
	if err != nil {
		return nil, err
	}
	u := &Usage{}
	blobs := s.snapPfx + "blobs/"
	mans := s.snapPfx + "manifests/"
	idxs := s.snapPfx + "index/"
	for _, o := range objs {
		switch {
		case strings.HasPrefix(o.Key, blobs):
			u.BlobBytes += o.Size
			u.BlobCount++
		case strings.HasPrefix(o.Key, mans):
			u.ManifestBytes += o.Size
		case strings.HasPrefix(o.Key, idxs):
			u.IndexBytes += o.Size
			u.IndexCount++
		}
		u.TotalBytes += o.Size
	}
	return u, nil
}
