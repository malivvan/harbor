package graphs

import "context"

// Copy copies every live record and every closed version from src to dst. It is
// the migration path between storage layouts: a graph can be read from the
// legacy record-backed layout and written into the independent graphs store
// without losing history.
//
// Copy is idempotent: records are upserted by id, so running it again rewrites
// the same values. The source is read once and the destination written once, so
// a failure leaves the destination unchanged (the write transaction rolls
// back).
func Copy(ctx context.Context, src, dst *Graph) error {
	if src == nil || dst == nil {
		return ErrInvalidRequest
	}
	if src.storage == nil || dst.storage == nil {
		return ErrInvalidRequest
	}

	var nodes []*NodeRecord
	var nodeVersions []*NodeVersion
	var edges []*EdgeRecord
	var edgeVersions []*EdgeVersion
	if err := src.storage.Read(ctx, func(r Reader) error {
		var err error
		if nodes, err = r.Nodes(ctx); err != nil {
			return err
		}
		if nodeVersions, err = r.NodeVersions(ctx); err != nil {
			return err
		}
		if edges, err = r.Edges(ctx); err != nil {
			return err
		}
		edgeVersions, err = r.EdgeVersions(ctx)
		return err
	}); err != nil {
		return err
	}

	return dst.storage.Write(ctx, func(w Writer) error {
		for _, record := range nodes {
			if err := w.PutNode(ctx, record); err != nil {
				return err
			}
		}
		for _, version := range nodeVersions {
			if err := w.PutNodeVersion(ctx, version); err != nil {
				return err
			}
		}
		for _, record := range edges {
			if err := w.PutEdge(ctx, record); err != nil {
				return err
			}
		}
		for _, version := range edgeVersions {
			if err := w.PutEdgeVersion(ctx, version); err != nil {
				return err
			}
		}
		return nil
	})
}
