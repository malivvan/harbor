package consensus

import (
	"context"

	"github.com/malivvan/eidos-db/store/series"
)

// Series is the replicated write view of the series sub-store.
type Series struct{ n *Node }

type seriesInsertArg struct {
	Series string       `json:"series"`
	Point  series.Point `json:"point"`
	// Now is the leader's clock at proposal time, in unix nanoseconds. It is
	// pinned into the command so every replica enforces retention with the same
	// reference time instead of each consulting its own wall clock.
	Now int64 `json:"now"`
}

type seriesInsertManyArg struct {
	Series string         `json:"series"`
	Points []series.Point `json:"points"`
	Now    int64          `json:"now"`
}

type seriesRangeArg struct {
	Series string `json:"series"`
	Start  int64  `json:"start"`
	End    int64  `json:"end"`
}

type seriesNameArg struct {
	Series string `json:"series"`
}

type seriesLabelsArg struct {
	Series string            `json:"series"`
	Labels map[string]string `json:"labels"`
}

type seriesRetentionArg struct {
	Series    string           `json:"series"`
	Retention series.Retention `json:"retention"`
}

// Series returns the replicated series sub-store.
func (n *Node) Series() *Series {
	if n == nil {
		return nil
	}
	return &Series{n: n}
}

// Insert replicates ts.Series.Insert. The leader pins its clock into the
// command so every replica enforces retention with the same reference time.
func (t *Series) Insert(ctx context.Context, series string, p series.Point) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opSeriesInsert, seriesInsertArg{Series: series, Point: p, Now: t.n.nowNanos()})
	return err
}

// InsertMany replicates ts.Series.InsertMany. The leader pins its clock into
// the command so every replica enforces retention with the same reference time.
func (t *Series) InsertMany(ctx context.Context, series string, pts []series.Point) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opSeriesInsertMany, seriesInsertManyArg{Series: series, Points: pts, Now: t.n.nowNanos()})
	return err
}

// DeleteRange replicates ts.Series.DeleteRange and returns the number of points
// removed.
func (t *Series) DeleteRange(ctx context.Context, series string, start, end int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	resp, err := t.n.apply(opSeriesDeleteRange, seriesRangeArg{Series: series, Start: start, End: end})
	if err != nil {
		return 0, err
	}
	n, _ := resp.(int)
	return n, nil
}

// Drop replicates ts.Series.Drop.
func (t *Series) Drop(ctx context.Context, series string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opSeriesDrop, seriesNameArg{Series: series})
	return err
}

// SetLabels replicates ts.Series.SetLabels.
func (t *Series) SetLabels(ctx context.Context, series string, labels map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opSeriesSetLabels, seriesLabelsArg{Series: series, Labels: labels})
	return err
}

// SetRetention replicates ts.Series.SetRetention.
func (t *Series) SetRetention(ctx context.Context, series string, r series.Retention) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opSeriesSetRetention, seriesRetentionArg{Series: series, Retention: r})
	return err
}

// DeleteSeries replicates ts.Store.DeleteSeries.
func (t *Series) DeleteSeries(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := t.n.apply(opSeriesDeleteSeries, seriesNameArg{Series: name})
	return err
}

func (f *fsm) applySeriesInsert(ctx context.Context, a seriesInsertArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).InsertAt(ctx, a.Point, a.Now)
}

func (f *fsm) applySeriesInsertMany(ctx context.Context, a seriesInsertManyArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).InsertManyAt(ctx, a.Points, a.Now)
}

func (f *fsm) applySeriesDeleteRange(ctx context.Context, a seriesRangeArg) any {
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

func (f *fsm) applySeriesDrop(ctx context.Context, a seriesNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).Drop(ctx)
}

func (f *fsm) applySeriesSetLabels(ctx context.Context, a seriesLabelsArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).SetLabels(ctx, a.Labels)
}

func (f *fsm) applySeriesSetRetention(ctx context.Context, a seriesRetentionArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().Series(a.Series).SetRetention(ctx, a.Retention)
}

func (f *fsm) applySeriesDeleteSeries(ctx context.Context, a seriesNameArg) any {
	st := f.node.storeHandle()
	if st == nil {
		return ErrClosed
	}
	return st.Series().DeleteSeries(ctx, a.Series)
}
