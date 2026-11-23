package consensus

import (
	"context"

	"github.com/malivvan/harbor/store/vector"
)

// Vec is the replicated write view of the vec sub-store.
type Vec struct{ n *Node }

type vecCreateArg struct {
	Name      string          `json:"name"`
	VectorLen int             `json:"vector_len"`
	Metric    vector.Distance `json:"metric"`
	UseHNSW   bool            `json:"use_hnsw"`
}

type vecUpsertArg struct {
	Collection string               `json:"collection"`
	Points     []vector.PointStruct `json:"points"`
}

type vecDeleteArg struct {
	Collection string   `json:"collection"`
	IDs        []string `json:"ids"`
}

type vecDropArg struct {
	Name string `json:"name"`
}

// vecNameArg is the payload for operations that only name a collection.
type vecNameArg struct {
	Name string `json:"name"`
}

// vecCreateOptionsArg is the payload for CreateCollectionWithOptions.
type vecCreateOptionsArg struct {
	Options vector.CollectionOptions `json:"options"`
}

// Vec returns the replicated vec sub-store.
func (n *Node) Vec() *Vec {
	if n == nil {
		return nil
	}
	return &Vec{n: n}
}

// CreateCollection replicates vector.Store.NewCollection.
func (v *Vec) CreateCollection(name string, vectorLen int, metric vector.Distance, useHNSW bool) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecCreateCollection, vecCreateArg{Name: name, VectorLen: vectorLen, Metric: metric, UseHNSW: useHNSW})
	return err
}

// CreateCollectionWithOptions replicates vector.Store.NewCollectionWithOptions,
// including the per-collection flush threshold and HNSW parameters.
func (v *Vec) CreateCollectionWithOptions(ctx context.Context, opts vector.CollectionOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecCreateCollectionOptions, vecCreateOptionsArg{Options: opts})
	return err
}

// EnsureCollection replicates vec.Store.EnsureCollection, creating the
// collection when it does not exist.
func (v *Vec) EnsureCollection(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecEnsureCollection, vecNameArg{Name: name})
	return err
}

// Flush replicates vec.Store.Flush. Because vector writes are buffered, this
// is the log point at which every replica drains its buffer, so a snapshot or
// read taken afterwards sees the same points on every node.
func (v *Vec) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVecFlush, nil)
	return err
}

// Upsert replicates vec.Store.UpsertPoints.
func (v *Vec) Upsert(collection string, points []vector.PointStruct) error {
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
	_, err := st.Vector().NewCollection(a.Name, a.VectorLen, a.Metric, a.UseHNSW)
	return err
}

func (f *fsm) applyVecCreateOptions(ctx context.Context, a vecCreateOptionsArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	_, err := st.Vector().NewCollectionWithOptions(a.Options)
	return err
}

func (f *fsm) applyVecEnsure(ctx context.Context, a vecNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().EnsureCollection(a.Name)
}

func (f *fsm) applyVecFlush(ctx context.Context) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().Flush()
}

func (f *fsm) applyVecUpsert(ctx context.Context, a vecUpsertArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().UpsertPoints(a.Collection, a.Points)
}

func (f *fsm) applyVecDelete(ctx context.Context, a vecDeleteArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().DeletePoints(a.Collection, a.IDs)
}

func (f *fsm) applyVecDrop(ctx context.Context, a vecDropArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().DropCollection(a.Name)
}
