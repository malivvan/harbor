package consensus

import (
	"context"

	"github.com/malivvan/harbor/store/series"
)

// TS is the replicated write view of the ts sub-store.
type TS struct{ n *Node }

type tsInsertArg struct {
	Series string       `json:"series"`
	Point  series.Point `json:"point"`
	// Now is the leader's clock at proposal time, in unix nanoseconds. It is
	// pinned into the command so every replica enforces retention with the same
	// reference time instead of each consulting its own wall clock.
	Now int64 `json:"now"`
}

type tsInsertManyArg struct {
	Series string         `json:"series"`
	Points []series.Point `json:"points"`
	Now    int64          `json:"now"`
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
	Series    string           `json:"series"`
	Retention series.Retention `json:"retention"`
}

// TS returns the replicated ts sub-store.
func (n *Node) TS() *TS {
	if n == nil {
		return nil
	}
	return &TS{n: n}
}

// Insert replicates ts.Series.Insert. The leader pins its clock into the
// command so every replica enforces retention with the same reference time.
func (t *TS) Insert(ctx context.Context, series string, p series.Point) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSInsert, tsInsertArg{Series: series, Point: p, Now: t.n.nowNanos()})
	return err
}

// InsertMany replicates ts.Series.InsertMany. The leader pins its clock into
// the command so every replica enforces retention with the same reference time.
func (t *TS) InsertMany(ctx context.Context, series string, pts []series.Point) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opTSInsertMany, tsInsertManyArg{Series: series, Points: pts, Now: t.n.nowNanos()})
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
func (t *TS) SetRetention(ctx context.Context, series string, r series.Retention) error {
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
	return st.Series().Series(a.Series).InsertAt(ctx, a.Point, a.Now)
}

func (f *fsm) applyTSInsertMany(ctx context.Context, a tsInsertManyArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).InsertManyAt(ctx, a.Points, a.Now)
}

func (f *fsm) applyTSDeleteRange(ctx context.Context, a tsRangeArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	n, err := st.Series().Series(a.Series).DeleteRange(ctx, a.Start, a.End)
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
	return st.Series().Series(a.Series).Drop(ctx)
}

func (f *fsm) applyTSSetLabels(ctx context.Context, a tsLabelsArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).SetLabels(ctx, a.Labels)
}

func (f *fsm) applyTSSetRetention(ctx context.Context, a tsRetentionArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).SetRetention(ctx, a.Retention)
}

func (f *fsm) applyTSDeleteSeries(ctx context.Context, a tsNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().DeleteSeries(ctx, a.Series)
}
