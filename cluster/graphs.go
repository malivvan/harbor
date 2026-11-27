package cluster

import (
	"context"

	"github.com/malivvan/tempest/store/graphs"
)

// Graphs is the replicated write view of the property-graph sub-store.
type Graphs struct{ n *Node }

type graphNameArg struct {
	Name string `json:"name"`
}

type graphNodeArg struct {
	Name string       `json:"name"`
	Node *graphs.Node `json:"node"`
}

type graphNodeIDArg struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

type graphEdgeArg struct {
	Name string       `json:"name"`
	Edge *graphs.Edge `json:"edge"`
}

type graphEdgeIDArg struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

// Graphs returns the replicated property-graph sub-store.
func (n *Node) Graphs() *Graphs {
	if n == nil {
		return nil
	}
	return &Graphs{n: n}
}

// Create replicates graphs.Store.Open, creating the named graph when it does
// not exist. Opening an existing graph is a no-op.
func (g *Graphs) Create(name string) error {
	if g == nil || g.n == nil {
		return ErrClosed
	}
	_, err := g.n.apply(opGraphCreate, graphNameArg{Name: name})
	return err
}

// Drop replicates graphs.Store.Drop, removing a graph and all its records.
func (g *Graphs) Drop(name string) error {
	if g == nil || g.n == nil {
		return ErrClosed
	}
	_, err := g.n.apply(opGraphDrop, graphNameArg{Name: name})
	return err
}

// UpsertNode replicates Graph.UpsertNode.
func (g *Graphs) UpsertNode(name string, node *graphs.Node) error {
	if g == nil || g.n == nil {
		return ErrClosed
	}
	_, err := g.n.apply(opGraphUpsertNode, graphNodeArg{Name: name, Node: node})
	return err
}

// DeleteNode replicates Graph.DeleteNode.
func (g *Graphs) DeleteNode(name, id string) error {
	if g == nil || g.n == nil {
		return ErrClosed
	}
	_, err := g.n.apply(opGraphDeleteNode, graphNodeIDArg{Name: name, ID: id})
	return err
}

// UpsertEdge replicates Graph.UpsertEdge.
func (g *Graphs) UpsertEdge(name string, edge *graphs.Edge) error {
	if g == nil || g.n == nil {
		return ErrClosed
	}
	_, err := g.n.apply(opGraphUpsertEdge, graphEdgeArg{Name: name, Edge: edge})
	return err
}

// DeleteEdge replicates Graph.DeleteEdge.
func (g *Graphs) DeleteEdge(name, id string) error {
	if g == nil || g.n == nil {
		return ErrClosed
	}
	_, err := g.n.apply(opGraphDeleteEdge, graphEdgeIDArg{Name: name, ID: id})
	return err
}

func (f *fsm) applyGraphCreate(ctx context.Context, a graphNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	_, err := st.Graphs().Open(a.Name)
	return err
}

func (f *fsm) applyGraphDrop(ctx context.Context, a graphNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Graphs().Drop(ctx, a.Name)
}

func (f *fsm) applyGraphUpsertNode(ctx context.Context, a graphNodeArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	g, err := st.Graphs().Open(a.Name)
	if err != nil {
		return err
	}
	return g.UpsertNode(ctx, a.Node)
}

func (f *fsm) applyGraphDeleteNode(ctx context.Context, a graphNodeIDArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	g, err := st.Graphs().Open(a.Name)
	if err != nil {
		return err
	}
	return g.DeleteNode(ctx, a.ID)
}

func (f *fsm) applyGraphUpsertEdge(ctx context.Context, a graphEdgeArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	g, err := st.Graphs().Open(a.Name)
	if err != nil {
		return err
	}
	return g.UpsertEdge(ctx, a.Edge)
}

func (f *fsm) applyGraphDeleteEdge(ctx context.Context, a graphEdgeIDArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	g, err := st.Graphs().Open(a.Name)
	if err != nil {
		return err
	}
	return g.DeleteEdge(ctx, a.ID)
}
