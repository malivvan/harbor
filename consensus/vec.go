package consensus

import (
	"context"

	"github.com/malivvan/harbor/vec"
)

// Vec is the replicated write view of the vec sub-store.
type Vec struct{ n *Node }

type vecCreateArg struct {
	Name      string       `json:"name"`
	VectorLen int          `json:"vector_len"`
	Metric    vec.Distance `json:"metric"`
	UseHNSW   bool         `json:"use_hnsw"`
}

type vecUpsertArg struct {
	Collection string            `json:"collection"`
	Points     []vec.PointStruct `json:"points"`
}

type vecDeleteArg struct {
	Collection string   `json:"collection"`
	IDs        []string `json:"ids"`
}

type vecDropArg struct {
	Name string `json:"name"`
}

// Vec returns the replicated vec sub-store.
func (n *Node) Vec() *Vec {
	if n == nil {
		return nil
	}
	return &Vec{n: n}
}

// CreateCollection replicates vec.Store.NewCollection.
func (v *Vec) CreateCollection(name string, vectorLen int, metric vec.Distance, useHNSW bool) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecCreateCollection, vecCreateArg{Name: name, VectorLen: vectorLen, Metric: metric, UseHNSW: useHNSW})
	return err
}

// Upsert replicates vec.Store.UpsertPoints.
func (v *Vec) Upsert(collection string, points []vec.PointStruct) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecUpsert, vecUpsertArg{Collection: collection, Points: points})
	return err
}

// Delete replicates vec.Store.DeletePoints.
func (v *Vec) Delete(collection string, ids []string) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecDelete, vecDeleteArg{Collection: collection, IDs: ids})
	return err
}

// DropCollection replicates vec.Store.DropCollection.
func (v *Vec) DropCollection(name string) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecDropCollection, vecDropArg{Name: name})
	return err
}

func (f *fsm) applyVecCreate(ctx context.Context, a vecCreateArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	_, err := st.Vec().NewCollection(a.Name, a.VectorLen, a.Metric, a.UseHNSW)
	return err
}

func (f *fsm) applyVecUpsert(ctx context.Context, a vecUpsertArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vec().UpsertPoints(a.Collection, a.Points)
}

func (f *fsm) applyVecDelete(ctx context.Context, a vecDeleteArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vec().DeletePoints(a.Collection, a.IDs)
}

func (f *fsm) applyVecDrop(ctx context.Context, a vecDropArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vec().DropCollection(a.Name)
}
