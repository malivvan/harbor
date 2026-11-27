package cluster

import (
	"bytes"
	"context"
)

// Objects is the replicated write view of the flat object store.
type Objects struct{ n *Node }

type objectPutArg struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Data   []byte `json:"data"`
}

type objectKeyArg struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type objectMoveArg struct {
	Bucket string `json:"bucket"`
	Src    string `json:"src"`
	Dst    string `json:"dst"`
}

type objectBucketArg struct {
	Bucket string `json:"bucket"`
}

// Objects returns the replicated object sub-store.
func (n *Node) Objects() *Objects {
	if n == nil {
		return nil
	}
	return &Objects{n: n}
}

// Put replicates object.Store.Put. The whole payload travels in the command,
// because the object store writes complete objects anyway.
func (o *Objects) Put(bucket, key string, data []byte) error {
	if o == nil || o.n == nil {
		return ErrClosed
	}
	_, err := o.n.apply(opObjectPut, objectPutArg{Bucket: bucket, Key: key, Data: data})
	return err
}

// Delete replicates object.Store.Delete.
func (o *Objects) Delete(bucket, key string) error {
	if o == nil || o.n == nil {
		return ErrClosed
	}
	_, err := o.n.apply(opObjectDelete, objectKeyArg{Bucket: bucket, Key: key})
	return err
}

// Move replicates object.Store.Move.
func (o *Objects) Move(bucket, src, dst string) error {
	if o == nil || o.n == nil {
		return ErrClosed
	}
	_, err := o.n.apply(opObjectMove, objectMoveArg{Bucket: bucket, Src: src, Dst: dst})
	return err
}

// DropBucket replicates object.Store.DropBucket.
func (o *Objects) DropBucket(bucket string) error {
	if o == nil || o.n == nil {
		return ErrClosed
	}
	_, err := o.n.apply(opObjectDrop, objectBucketArg{Bucket: bucket})
	return err
}

func (f *fsm) applyObjectPut(ctx context.Context, a objectPutArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	_, err := st.Objects().Put(ctx, a.Bucket, a.Key, bytes.NewReader(a.Data), int64(len(a.Data)), "")
	return err
}

func (f *fsm) applyObjectDelete(ctx context.Context, a objectKeyArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Objects().Delete(ctx, a.Bucket, a.Key)
}

func (f *fsm) applyObjectMove(ctx context.Context, a objectMoveArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Objects().Move(ctx, a.Bucket, a.Src, a.Dst)
}

func (f *fsm) applyObjectDrop(ctx context.Context, a objectBucketArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Objects().DropBucket(ctx, a.Bucket)
}
