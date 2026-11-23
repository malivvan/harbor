package consensus

import (
	"context"

	"github.com/malivvan/harbor/ts"
)

// TS is the replicated write view of the ts sub-store.
type TS struct{ n *Node }

type tsInsertArg struct {
	Series string   `json:"series"`
	Point  ts.Point `json:"point"`
}

type tsInsertManyArg struct {
	Series string    `json:"series"`
	Points []ts.Point `json:"points"`
}

type tsRangeArg struct {
	Series string `json:"series"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type tsNameArg struct {
	Series string `json:"series"`
}

type tsLabelsArg struct {
	Series string            `json:"series"`
	Labels map[string]string `json:"labels"`
}

type tsRetentionArg struct {
	Series    string       `json:"series"`
	Retention ts.Retention `json:"retention"`
}

// TS returns the replicated ts sub-store.
func (n *Node) TS() *TS {
	if n == nil {
		return nil
	}
	return &TS{n: n}
}

// Insert replicates ts.Series.Insert.
func (t *TS) Insert(ctx context.Context, series string, p ts.Point) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSInsert, tsInsertArg{Series: series, Point: p})
	return err
}

// InsertMany replicates ts.Series.InsertMany.
func (t *TS) InsertMany(ctx context.Context, series string, pts []ts.Point) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSInsertMany, tsInsertManyArg{Series: series, Points: pts})
	return err
}

// DeleteRange replicates ts.Series.DeleteRange and returns the number of points
// removed.
func (t *TS) DeleteRange(ctx context.Context, series string, start, end int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resp, err := t.n.apply(opTSDeleteRange, tsRangeArg{Series: series, Start: start, End: end})
	if err != nil {
		return 0, err
	}
	n, _ := resp.(int)
	return n, nil
}

// Drop replicates ts.Series.Drop.
func (t *TS) Drop(ctx context.Context, series string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSDrop, tsNameArg{Series: series})
	return err
}

// SetLabels replicates ts.Series.SetLabels.
func (t *TS) SetLabels(ctx context.Context, series string, labels map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSSetLabels, tsLabelsArg{Series: series, Labels: labels})
	return err
}

// SetRetention replicates ts.Series.SetRetention.
func (t *TS) SetRetention(ctx context.Context, series string, r ts.Retention) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSSetRetention, tsRetentionArg{Series: series, Retention: r})
	return err
}

// DeleteSeries replicates ts.Store.DeleteSeries.
func (t *TS) DeleteSeries(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSDeleteSeries, tsNameArg{Series: name})
	return err
}

func (f *fsm) applyTSInsert(ctx context.Context, a tsInsertArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.TS().Series(a.Series).Insert(ctx, a.Point)
}

func (f *fsm) applyTSInsertMany(ctx context.Context, a tsInsertManyArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.TS().Series(a.Series).InsertMany(ctx, a.Points)
}

func (f *fsm) applyTSDeleteRange(ctx context.Context, a tsRangeArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	n, err := st.TS().Series(a.Series).DeleteRange(ctx, a.Start, a.End)
	if err != nil {
		return err
	}
	return n
}

func (f *fsm) applyTSDrop(ctx context.Context, a tsNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.TS().Series(a.Series).Drop(ctx)
}

func (f *fsm) applyTSSetLabels(ctx context.Context, a tsLabelsArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.TS().Series(a.Series).SetLabels(ctx, a.Labels)
}

func (f *fsm) applyTSSetRetention(ctx context.Context, a tsRetentionArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.TS().Series(a.Series).SetRetention(ctx, a.Retention)
}

func (f *fsm) applyTSDeleteSeries(ctx context.Context, a tsNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.TS().DeleteSeries(ctx, a.Series)
}
