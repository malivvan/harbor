package graphs

import (
	"context"
	"errors"
	"fmt"
)

// PurgeNode erases a node, its closed versions, every live edge touching it and
// those edges' closed versions. It is the erasure path used by cross-store
// Forget; DeleteNode only retracts.
func (g *Graph) PurgeNode(ctx context.Context, nodeID string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if nodeID == "" {
		return fmt.Errorf("%w: node id is required", ErrInvalidRequest)
	}
	return g.write(ctx, func(w Writer) error {
		removed := false
		if node, err := w.Node(ctx, nodeID); err == nil && node != nil {
			if err := w.DeleteNode(ctx, nodeID); err != nil && !errors.Is(err, ErrRecordNotFound) {
				return err
			}
			removed = true
		} else if err != nil && !errors.Is(err, ErrRecordNotFound) {
			return err
		}
		versions, err := w.NodeVersionsByNode(ctx, nodeID)
		if err != nil {
			return err
		}
		for _, version := range versions {
			if err := w.DeleteNodeVersion(ctx, version.VersionID); err != nil && !errors.Is(err, ErrRecordNotFound) {
				return err
			}
			removed = true
		}
		edges, err := w.EdgesTouching(ctx, nodeID)
		if err != nil {
			return err
		}
		for _, edge := range edges {
			if err := w.DeleteEdge(ctx, edge.ID); err != nil && !errors.Is(err, ErrRecordNotFound) {
				return err
			}
			removed = true
		}
		edgeVersions, err := w.EdgeVersionsTouching(ctx, nodeID)
		if err != nil {
			return err
		}
		for _, version := range edgeVersions {
			if err := w.DeleteEdgeVersion(ctx, version.VersionID); err != nil && !errors.Is(err, ErrRecordNotFound) {
				return err
			}
			removed = true
		}
		if !removed {
			return fmt.Errorf("%w: %q", ErrNodeNotFound, nodeID)
		}
		return nil
	})
}

// PurgeEdge erases an edge and every closed version of it.
func (g *Graph) PurgeEdge(ctx context.Context, edgeID string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if edgeID == "" {
		return fmt.Errorf("%w: edge id is required", ErrInvalidRequest)
	}
	return g.write(ctx, func(w Writer) error {
		removed := false
		if edge, err := w.Edge(ctx, edgeID); err == nil && edge != nil {
			if err := w.DeleteEdge(ctx, edgeID); err != nil && !errors.Is(err, ErrRecordNotFound) {
				return err
			}
			removed = true
		} else if err != nil && !errors.Is(err, ErrRecordNotFound) {
			return err
		}
		versions, err := w.EdgeVersionsByEdge(ctx, edgeID)
		if err != nil {
			return err
		}
		for _, version := range versions {
			if err := w.DeleteEdgeVersion(ctx, version.VersionID); err != nil && !errors.Is(err, ErrRecordNotFound) {
				return err
			}
			removed = true
		}
		if !removed {
			return fmt.Errorf("%w: %q", ErrEdgeNotFound, edgeID)
		}
		return nil
	})
}
