package consensus

import (
	"context"

	"github.com/malivvan/harbor/store/vector"
)

// Vector is the replicated write view of the vector sub-store.
type Vector struct{ n *Node }

type vectorCreateArg struct {
	Name      string          `json:"name"`
	VectorLen int             `json:"vector_len"`
	Metric    vector.Distance `json:"metric"`
	UseHNSW   bool            `json:"use_hnsw"`
}

type vectorUpsertArg struct {
	Collection string               `json:"collection"`
	Points     []vector.PointStruct `json:"points"`
}

type vectorDeleteArg struct {
	Collection string   `json:"collection"`
	IDs        []string `json:"ids"`
}

type vectorDropArg struct {
	Name string `json:"name"`
}

// vectorNameArg is the payload for operations that only name a collection.
type vectorNameArg struct {
	Name string `json:"name"`
}

// vectorCreateOptionsArg is the payload for CreateCollectionWithOptions.
type vectorCreateOptionsArg struct {
	Options vector.CollectionOptions `json:"options"`
}

// Vector returns the replicated vector sub-store.
func (n *Node) Vector() *Vector {
	if n == nil {
		return nil
	}
	return &Vector{n: n}
}

// CreateCollection replicates vector.Store.NewCollection.
func (v *Vector) CreateCollection(name string, vectorLen int, metric vector.Distance, useHNSW bool) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVectorCreateCollection, vectorCreateArg{Name: name, VectorLen: vectorLen, Metric: metric, UseHNSW: useHNSW})
	return err
}

// CreateCollectionWithOptions replicates vector.Store.NewCollectionWithOptions,
// including the per-collection flush threshold and HNSW parameters.
func (v *Vector) CreateCollectionWithOptions(ctx context.Context, opts vector.CollectionOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVectorCreateCollectionOptions, vectorCreateOptionsArg{Options: opts})
	return err
}

// EnsureCollection replicates vec.Store.EnsureCollection, creating the
// collection when it does not exist.
func (v *Vector) EnsureCollection(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVectorEnsureCollection, vectorNameArg{Name: name})
	return err
}

// Flush replicates vec.Store.Flush. Because vector writes are buffered, this
// is the log point at which every replica drains its buffer, so a snapshot or
// read taken afterwards sees the same points on every node.
func (v *Vector) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVectorFlush, nil)
	return err
}

// Upsert replicates vec.Store.UpsertPoints.
func (v *Vector) Upsert(collection string, points []vector.PointStruct) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVectorUpsert, vectorUpsertArg{Collection: collection, Points: points})
	return err
}

// Delete replicates vec.Store.DeletePoints.
func (v *Vector) Delete(collection string, ids []string) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVectorDelete, vectorDeleteArg{Collection: collection, IDs: ids})
	return err
}

// DropCollection replicates vec.Store.DropCollection.
func (v *Vector) DropCollection(name string) error {
	if v == nil || v.n == nil {
		return ErrClosed
	}
	_, err := v.n.apply(opVectorDropCollection, vectorDropArg{Name: name})
	return err
}

func (f *fsm) applyVectorCreate(ctx context.Context, a vectorCreateArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	_, err := st.Vector().NewCollection(a.Name, a.VectorLen, a.Metric, a.UseHNSW)
	return err
}

func (f *fsm) applyVectorCreateOptions(ctx context.Context, a vectorCreateOptionsArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	_, err := st.Vector().NewCollectionWithOptions(a.Options)
	return err
}

func (f *fsm) applyVectorEnsure(ctx context.Context, a vectorNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().EnsureCollection(a.Name)
}

func (f *fsm) applyVectorFlush(ctx context.Context) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().Flush()
}

func (f *fsm) applyVectorUpsert(ctx context.Context, a vectorUpsertArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().UpsertPoints(a.Collection, a.Points)
}

func (f *fsm) applyVectorDelete(ctx context.Context, a vectorDeleteArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().DeletePoints(a.Collection, a.IDs)
}

func (f *fsm) applyVectorDrop(ctx context.Context, a vectorDropArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Vector().DropCollection(a.Name)
}
