package record

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/malivvan/eidos-db/store/record/q"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type Score struct {
	ID    int `db:"increment"`
	Value int
}

func prepareScoreDB(t *testing.T) (*Store, func()) {
	db, cleanup := createDB(t)

	ctx := context.Background()

	for i := 0; i < 20; i++ {
		err := db.Save(ctx, &Score{
			Value: i,
		})
		require.NoError(t, err)
	}

	return db, cleanup
}

func TestSelectFind(t *testing.T) {
	db, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx := context.Background()

	var scores []Score
	var scoresPtr []*Score

	err := db.Select(q.Eq("Value", 5)).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 1)
	require.Equal(t, 5, scores[0].Value)

	err = db.Select(q.Eq("Value", 5)).Find(ctx, &scoresPtr)
	require.NoError(t, err)
	require.Len(t, scoresPtr, 1)
	require.Equal(t, 5, scoresPtr[0].Value)

	err = db.Select(
		q.Or(
			q.Eq("Value", 5),
			q.Eq("Value", 6),
		),
	).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	require.Equal(t, 5, scores[0].Value)
	require.Equal(t, 6, scores[1].Value)

	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 6)
	require.Equal(t, 0, scores[0].Value)
	require.Equal(t, 1, scores[1].Value)
	require.Equal(t, 2, scores[2].Value)
	require.Equal(t, 5, scores[3].Value)
	require.Equal(t, 18, scores[4].Value)
	require.Equal(t, 19, scores[5].Value)

	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Reverse().Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 6)
	require.Equal(t, 19, scores[0].Value)
	require.Equal(t, 18, scores[1].Value)
	require.Equal(t, 5, scores[2].Value)
	require.Equal(t, 2, scores[3].Value)
	require.Equal(t, 1, scores[4].Value)
	require.Equal(t, 0, scores[5].Value)
}

func TestSelectFindSkip(t *testing.T) {
	db, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx := context.Background()

	var scores []Score

	err := db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(4).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	require.Equal(t, 18, scores[0].Value)
	require.Equal(t, 19, scores[1].Value)

	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(-10).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 6)
	require.Equal(t, 0, scores[0].Value)

	scores = nil
	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(1000).Find(ctx, &scores)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)
	require.Len(t, scores, 0)
}

func TestSelectFindLimit(t *testing.T) {
	db, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx := context.Background()

	var scores []Score

	err := db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Limit(2).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	require.Equal(t, 0, scores[0].Value)
	require.Equal(t, 1, scores[1].Value)

	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Limit(-10).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 6)
	require.Equal(t, 0, scores[0].Value)

	scores = nil
	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Limit(0).Find(ctx, &scores)
	require.NoError(t, err, "Limit(0) is an empty success")
	require.Len(t, scores, 0)
}

func TestSelectFindLimitSkip(t *testing.T) {
	db, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx := context.Background()

	var scores []Score

	err := db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Limit(2).Skip(2).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	require.Equal(t, 2, scores[0].Value)
	require.Equal(t, 5, scores[1].Value)

	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Limit(2).Skip(5).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 1)
	require.Equal(t, 19, scores[0].Value)
}

func TestSelectFindOrderBy(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type T struct {
		ID  int `db:"increment"`
		Str string
		Int int
		Rnd int
	}

	strs := []string{"e", "b", "d", "a", "c", "d"}
	ints := []int{2, 3, 5, 4, 2, 1}
	for i := 0; i < 6; i++ {
		record := T{
			Str: strs[i],
			Int: ints[i],
		}
		if i == 3 {
			record.Rnd = 3
		}

		err := db.Save(ctx, &record)
		require.NoError(t, err)
	}

	var list []T
	err := db.Select().OrderBy("ID").Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 6)
	for i, j := 0, 0; i < 6; i, j = i+1, j+1 {
		if i == 2 {
			j--
		}
		require.Equal(t, i+1, list[i].ID)
	}

	list = nil
	err = db.Select().OrderBy("Str").Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 6)
	for i, j := 0, 0; i < 6; i, j = i+1, j+1 {
		if i == 4 {
			j--
		}
		require.Equal(t, string([]byte{'a' + byte(j)}), list[i].Str)
	}

	list = nil
	err = db.Select().OrderBy("Int").Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 6)
	for i, j := 0, 0; i < 6; i, j = i+1, j+1 {
		if i == 2 {
			j--
		}
		require.Equal(t, j+1, list[i].Int)
	}

	list = nil
	err = db.Select().OrderBy("Rnd").Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 6)
	require.Equal(t, 1, list[0].ID)
	require.Equal(t, 2, list[1].ID)
	require.Equal(t, 3, list[2].ID)
	require.Equal(t, 5, list[3].ID)
	require.Equal(t, 6, list[4].ID)
	require.Equal(t, 4, list[5].ID)

	list = nil
	err = db.Select().OrderBy("Int").Reverse().Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 6)
	for i, j := 0, 0; i < 6; i, j = i+1, j+1 {
		if i == 4 {
			j--
		}
		require.Equal(t, 5-j, list[i].Int)
	}

	list = nil
	err = db.Select().OrderBy("Int").Reverse().Limit(2).Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 2)
	for i := 0; i < 2; i++ {
		require.Equal(t, 5-i, list[i].Int)
	}

	list = nil
	err = db.Select().OrderBy("Int").Reverse().Skip(2).Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 4)
	for i, j := 0, 0; i < 3; i, j = i+1, j+1 {
		if i == 2 {
			j--
		}
		require.Equal(t, 3-j, list[i].Int)
	}

	list = nil
	err = db.Select().OrderBy("Int").Reverse().Skip(5).Limit(2).Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, 1, list[0].Int)

	list = nil
	err = db.Select().OrderBy("Str", "Int").Find(ctx, &list)
	require.NoError(t, err)
	require.Len(t, list, 6)
	require.Equal(t, "a", list[0].Str)
	require.Equal(t, 4, list[0].Int)
	require.Equal(t, "b", list[1].Str)
	require.Equal(t, 3, list[1].Int)
	require.Equal(t, "c", list[2].Str)
	require.Equal(t, 2, list[2].Int)
	require.Equal(t, "d", list[3].Str)
	require.Equal(t, 1, list[3].Int)
	require.Equal(t, "d", list[4].Str)
	require.Equal(t, 5, list[4].Int)
	require.Equal(t, "e", list[5].Str)
	require.Equal(t, 2, list[5].Int)
}

func TestSelectFirst(t *testing.T) {
	db, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx := context.Background()

	var score Score

	err := db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(2).First(ctx, &score)
	require.NoError(t, err)
	require.Equal(t, 2, score.Value)

	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(1).Reverse().First(ctx, &score)
	require.NoError(t, err)
	require.Equal(t, 18, score.Value)
}

func TestSelectFirstOrderBy(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type T struct {
		ID  int `db:"increment"`
		Str string
		Int int
	}

	strs := []string{"e", "b", "a", "c", "d"}
	ints := []int{2, 3, 1, 4, 5}
	for i := 0; i < 5; i++ {
		err := db.Save(ctx, &T{
			Str: strs[i],
			Int: ints[i],
		})
		require.NoError(t, err)
	}

	var record T
	err := db.Select().OrderBy("ID").First(ctx, &record)
	require.NoError(t, err)
	require.Equal(t, 1, record.ID)

	err = db.Select().OrderBy("Str").First(ctx, &record)
	require.NoError(t, err)
	require.Equal(t, "a", record.Str)

	err = db.Select().OrderBy("Int").First(ctx, &record)
	require.NoError(t, err)
	require.Equal(t, 1, record.Int)

	err = db.Select().OrderBy("Int").Reverse().First(ctx, &record)
	require.NoError(t, err)
	require.Equal(t, 5, record.Int)

	err = db.Select().OrderBy("Int").Reverse().Limit(2).First(ctx, &record)
	require.NoError(t, err)
	require.Equal(t, 5, record.Int)

	err = db.Select().OrderBy("Int").Reverse().Skip(2).First(ctx, &record)
	require.NoError(t, err)
	require.Equal(t, 3, record.Int)

	err = db.Select().OrderBy("Int").Reverse().Skip(4).Limit(2).First(ctx, &record)
	require.NoError(t, err)
	require.Equal(t, 1, record.Int)
}

func TestSelectDelete(t *testing.T) {
	db, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx := context.Background()

	err := db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(2).Delete(ctx, &Score{})
	require.NoError(t, err)

	var scores []Score
	err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Find(ctx, &scores)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	require.Equal(t, 0, scores[0].Value)
	require.Equal(t, 1, scores[1].Value)

	for i := 0; i < 10; i++ {
		w := User{ID: i + 1, Name: fmt.Sprintf("John%d", i+1)}
		err = db.Save(ctx, &w)
		require.NoError(t, err)
	}

	err = db.Select(q.Gte("ID", 5)).Delete(ctx, &User{})
	require.NoError(t, err)

	var user User
	err = db.One(ctx, "Name", "John6", &user)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)

	err = db.One(ctx, "Name", "John4", &user)
	require.NoError(t, err)
}

func TestSelectCount(t *testing.T) {
	db, cleanup := prepareScoreDB(t)
	defer cleanup()

	ctx := context.Background()

	total, err := db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Count(ctx, &Score{})
	require.NoError(t, err)
	require.Equal(t, 6, total)

	total, err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(2).Count(ctx, &Score{})
	require.NoError(t, err)
	require.Equal(t, 4, total)

	total, err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(2).Limit(2).Count(ctx, &Score{})
	require.NoError(t, err)
	require.Equal(t, 2, total)

	total, err = db.Select(q.Or(
		q.Eq("Value", 5),
		q.Or(
			q.Lte("Value", 2),
			q.Gte("Value", 18),
		),
	)).Skip(5).Limit(2).Count(ctx, &Score{})
	require.NoError(t, err)
	require.Equal(t, 1, total)
}

func TestSelectRaw(t *testing.T) {
	db, cleanup := createDB(t, testCodec(JSONCodec))
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 20; i++ {
		err := db.Save(ctx, &Score{
			Value: i,
		})
		require.NoError(t, err)
	}

	list, err := db.Select().Bucket("Score").Raw(ctx, &Score{})
	require.NoError(t, err)
	require.Len(t, list, 20)

	list, err = db.Select().Bucket("Score").Skip(18).Limit(5).Raw(ctx, &Score{})
	require.NoError(t, err)
	require.Len(t, list, 2)

	i := 0
	err = db.Select().Bucket("Score").Skip(18).Limit(5).RawEach(ctx, &Score{}, func(k []byte, v []byte) error {
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, i, 2)
}

func TestSelectEach(t *testing.T) {
	db, cleanup := createDB(t, testCodec(JSONCodec))
	defer cleanup()

	ctx := context.Background()

	for i := 0; i < 20; i++ {
		err := db.Save(ctx, &Score{
			Value: i,
		})
		require.NoError(t, err)
	}

	i := 0
	err := db.Select().Each(ctx, new(Score), func(record interface{}) error {
		s, ok := record.(*Score)
		require.True(t, ok)
		require.Equal(t, i, s.Value)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 20, i)

	i = 0
	err = db.Select().Skip(18).Limit(5).Each(ctx, new(Score), func(record interface{}) error {
		s, ok := record.(*Score)
		require.True(t, ok)
		require.Equal(t, i+18, s.Value)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, i)
}

// ============================================================================
// Test helpers: deterministic context cancellation without hooks
// ============================================================================

// queryStepContext wraps a cancellable context and provides an observable
// step counter. When the counter reaches cancelAt, the context is cancelled.
//
// Thread-safe. No timers, no goroutines.
type queryStepContext struct {
	mu       sync.Mutex
	counter  int
	cancelAt int
	cancel   context.CancelFunc
	ctx      context.Context
}

func newQueryStepContext(parent context.Context) *queryStepContext {
	ctx, cancel := context.WithCancel(parent)
	return &queryStepContext{
		ctx:    ctx,
		cancel: cancel,
	}
}

// cancelAfter sets the step count at which the context will be cancelled.
// The cancellation occurs inside step() when counter reaches cancelAt.
func (q *queryStepContext) cancelAfter(n int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cancelAt = n
}

// step increments the counter. If counter >= cancelAt, the context is
// cancelled. Returns checkContext(q.ctx) — nil while the context is
// still alive, or ctx.Err() after cancellation.
func (q *queryStepContext) step() error {
	q.mu.Lock()
	q.counter++
	if q.cancelAt > 0 && q.counter >= q.cancelAt {
		q.cancel()
	}
	q.mu.Unlock()
	return checkContext(q.ctx)
}

// context returns the wrapped context.Context.
func (q *queryStepContext) context() context.Context {
	return q.ctx
}

// count returns the current step counter value.
func (q *queryStepContext) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.counter
}

// stepMatcher wraps a q.Matcher and calls queryStepContext.step()
// before delegating to the inner matcher. When step() returns an
// error (context cancelled), that error is returned immediately
// without calling the inner matcher.
//
// This implements q.Matcher without modifying package q.
type stepMatcher struct {
	qsc   *queryStepContext
	inner q.Matcher
}

type queryErrStepContext struct {
	context.Context
	mu       sync.Mutex
	calls    int
	cancelAt int
}

func (c *queryErrStepContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls >= c.cancelAt {
		return context.Canceled
	}
	return c.Context.Err()
}

func (c *queryErrStepContext) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

type cancelInLessContext struct {
	context.Context
	mu        sync.Mutex
	lessCalls int
}

func (c *cancelInLessContext) Err() error {
	pcs := make([]uintptr, 16)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".(*sorter).Less") {
			c.mu.Lock()
			c.lessCalls++
			c.mu.Unlock()
			return context.Canceled
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

func (c *cancelInLessContext) LessCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lessCalls
}

func (m *stepMatcher) Match(data interface{}) (bool, error) {
	if err := m.qsc.step(); err != nil {
		return false, err
	}
	if m.inner == nil {
		return true, nil
	}
	return m.inner.Match(data)
}

// ============================================================================
// Cursor and destination safety
// ============================================================================

// TestQueryFind_CancellationDuringDecodePreservesDestination verifies that
// cancellation during materialization prevents Find from publishing partial
// results and leaves the caller's destination slice untouched.
func TestQueryFind_CancellationDuringDecodePreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	var scores []Score
	scores = append(scores, Score{ID: 999, Value: 999})

	cancelCtx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: db.Codec()}
	decodeCount := 0
	cc.setOnUnmarshal(func() {
		decodeCount++
		if decodeCount == 3 {
			cancel()
		}
	})

	err := db.WithCodec(cc).Select().Find(cancelCtx, &scores)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Len(t, scores, 1)
	require.Equal(t, Score{ID: 999, Value: 999}, scores[0])
}

// TestQueryFirst_CancellationDuringDecodePreservesDestination verifies that
// cancellation during decode leaves the caller's destination struct intact.
func TestQueryFirst_CancellationDuringDecodePreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &Score{Value: 42}))

	var score Score
	score.ID = 999
	score.Value = 999

	cancelCtx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: db.Codec()}
	cc.setOnUnmarshal(cancel)

	err := db.WithCodec(cc).Select().First(cancelCtx, &score)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, 999, score.ID)
	require.Equal(t, 999, score.Value)
}

// TestQueryFirst_CodecErrorPreservesDestination verifies that a codec
// error during decode leaves the caller's destination struct intact.
func TestQueryFirst_CodecErrorPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &Score{Value: 1}))

	// Insert garbage bytes that sort before the valid record
	// so that the codec error is encountered on the first key.
	// The valid record has ID=1 encoded as big-endian int64,
	// so a single 0x00 byte sorts before it.
	require.NoError(t, db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("Score"))
		require.NotNil(t, b)
		return b.Put([]byte{0x00}, []byte("{this is not valid json"))
	}))

	var score Score
	score.ID = 999
	score.Value = 999

	err := db.Select().First(ctx, &score)
	require.Error(t, err)
	// The destination must remain unchanged.
	require.Equal(t, 999, score.ID)
	require.Equal(t, 999, score.Value)
}

// TestQueryFind_CancellationDuringMatcherPreservesDestination verifies
// that cancellation triggered during matcher evaluation leaves the
// destination untouched.
func TestQueryFind_CancellationDuringMatcherPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	qsc := newQueryStepContext(context.Background())
	// Cancel after the 3rd matcher call.
	qsc.cancelAfter(3)

	var scores []Score
	scores = append(scores, Score{ID: 999, Value: 999})

	err := db.Select(&stepMatcher{qsc: qsc}).Find(qsc.context(), &scores)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Len(t, scores, 1)
	require.Equal(t, Score{ID: 999, Value: 999}, scores[0])
}

// ============================================================================
// Count and Raw
// ============================================================================

// TestQueryCount_CancellationReturnsZero verifies that Count returns
// (0, err) on cancellation, never a partial counter.
func TestQueryCount_CancellationReturnsZero(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cc := &cancellationCodec{delegate: db.Codec()}
	decodeCount := 0
	cc.setOnUnmarshal(func() {
		decodeCount++
		if decodeCount == 3 {
			cancel()
		}
	})

	// Count without matchers no longer decodes, so use a matcher to keep a
	// decode in the loop and cancel from the codec mid-scan.
	cnt, err := db.WithCodec(cc).Select(q.True()).Count(cancelCtx, &Score{})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, 0, cnt)
}

// TestQueryRaw_CancellationReturnsNil verifies that Raw returns
// (nil, err) on cancellation, never partial results.
func TestQueryRaw_CancellationReturnsNil(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	stepCtx := &queryErrStepContext{
		Context:  context.Background(),
		cancelAt: 12,
	}

	results, err := db.Select().Bucket("Score").Raw(stepCtx, &Score{})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Nil(t, results)
	require.GreaterOrEqual(t, stepCtx.Calls(), 12)
}

// TestQueryRaw_ReturnsDefensiveCopies verifies that Raw returns
// defensive copies. Mutating returned bytes does not affect a
// subsequent read.
func TestQueryRaw_ReturnsDefensiveCopies(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	results, err := db.Select().Bucket("Score").Raw(ctx, &Score{})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// Mutate the first returned slice.
	results[0][0] = 0xFF

	// Read again — should be unaffected.
	results2, err := db.Select().Bucket("Score").Raw(ctx, &Score{})
	require.NoError(t, err)
	require.NotEmpty(t, results2)

	// Verify original value is not corrupted (compare with first read
	// before mutation — we check that the first byte is not 0xFF).
	require.NotEqual(t, byte(0xFF), results2[0][0])
}

// ============================================================================
// Callback cancellation and precedence
// ============================================================================

// TestQueryRawEach_CancellationStopsFurtherCallbacks verifies that
// cancellation prevents further callbacks from executing. Previously
// executed callbacks are not undone.
func TestQueryRawEach_CancellationStopsFurtherCallbacks(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	qsc := newQueryStepContext(context.Background())
	// Cancel after 4 callbacks.
	qsc.cancelAfter(4)

	callCount := 0
	err := db.Select().Bucket("Score").RawEach(qsc.context(), &Score{}, func(k, v []byte) error {
		callCount++
		return qsc.step()
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	// The 4th callback triggers cancellation; the 5th is never called.
	// Actually: step checks before returning, and if cancelled,
	// the next iteration's checkContext catches it. Callback 4
	// completes successfully (step returns nil), then the next
	// iteration sees cancelled context.
	require.Equal(t, 4, callCount)
}

// TestQueryRawEach_CallbackErrorTakesPrecedence verifies that a
// callback error takes precedence over context cancellation.
func TestQueryRawEach_CallbackErrorTakesPrecedence(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	seedCtx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(seedCtx, &Score{Value: i}))
	}

	ctx, cancel := context.WithCancel(context.Background())
	callbackErr := errors.New("callback failure")

	err := db.Select().Bucket("Score").RawEach(ctx, &Score{}, func(k, v []byte) error {
		cancel()
		return callbackErr
	})
	require.ErrorIs(t, err, callbackErr)
	require.NotErrorIs(t, err, context.Canceled)
}

// TestQueryRawEach_NilCallbackRejected verifies that a nil callback
// is rejected with ErrNilParam.
func TestQueryRawEach_NilCallbackRejected(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	err := db.Select().Bucket("Score").RawEach(ctx, &Score{}, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNilParam))
}

// TestQueryEach_CancellationStopsFurtherCallbacks verifies that
// cancellation prevents further Each callbacks.
func TestQueryEach_CancellationStopsFurtherCallbacks(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i + 100}))
	}

	qsc := newQueryStepContext(context.Background())
	qsc.cancelAfter(4)

	callCount := 0
	err := db.Select().Each(qsc.context(), new(Score), func(record interface{}) error {
		callCount++
		return qsc.step()
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, 4, callCount)
}

// TestQueryEach_CallbackErrorTakesPrecedence verifies that a callback
// error takes precedence in Each.
func TestQueryEach_CallbackErrorTakesPrecedence(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	seedCtx := context.Background()
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(seedCtx, &Score{Value: i}))
	}

	ctx, cancel := context.WithCancel(context.Background())
	callbackErr := errors.New("callback failure")

	err := db.Select().Each(ctx, new(Score), func(record interface{}) error {
		cancel()
		return callbackErr
	})
	require.ErrorIs(t, err, callbackErr)
	require.NotErrorIs(t, err, context.Canceled)
}

// TestQueryEach_NilCallbackRejected verifies that a nil callback
// is rejected with ErrNilParam.
func TestQueryEach_NilCallbackRejected(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	err := db.Select().Each(ctx, new(Score), nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNilParam))
}

// ============================================================================
// Delete rollback
// ============================================================================

// TestQueryDelete_CancellationRollsBackRecordsAndIndexes verifies that
// cancellation during Delete rolls back all mutations via Bolt's Update
// transaction rollback.
func TestQueryDelete_CancellationRollsBackRecordsAndIndexes(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	// Use a type with an index so we can verify index integrity.
	type IndexedScore struct {
		ID    int    `db:"id,increment"`
		Value int    `db:"index"`
		Name  string `db:"unique"`
	}
	require.NoError(t, db.Init(ctx, &IndexedScore{}))

	// Insert records with unique names.
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i, name := range names {
		require.NoError(t, db.Save(ctx, &IndexedScore{Value: i * 10, Name: name}))
	}

	// Verify records exist.
	var all []IndexedScore
	require.NoError(t, db.All(ctx, &all))
	require.Len(t, all, 5)

	// Cancel after 2 deletes. Since Bolt.Update rolls back on error,
	// all records and indexes should be preserved.
	qsc := newQueryStepContext(context.Background())
	qsc.cancelAfter(2)

	// Use a matcher that calls step() during scan.
	err := db.Select(&stepMatcher{qsc: qsc}).Delete(qsc.context(), &IndexedScore{})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))

	// Copy records must still exist.
	var after []IndexedScore
	require.NoError(t, db.All(ctx, &after))
	require.Len(t, after, 5)

	// Unique names must still be occupied.
	for _, name := range names {
		var found IndexedScore
		require.NoError(t, db.One(ctx, "Name", name, &found))
		require.Equal(t, name, found.Name)
	}

	// A fresh query must work correctly.
	var fresh []IndexedScore
	require.NoError(t, db.Select().Find(ctx, &fresh))
	require.Len(t, fresh, 5)
}

// ============================================================================
// Sorting cancellation and errors
// ============================================================================

// TestQueryFind_CancellationDuringSortPreservesDestination verifies that
// cancellation detected during sort (via Less checkContext) preserves
// the destination and returns context.Canceled.
func TestQueryFind_CancellationDuringSortPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type Sortable struct {
		ID    int `db:"id,increment"`
		Value int
	}
	for i := 0; i < 20; i++ {
		require.NoError(t, db.Save(ctx, &Sortable{Value: i}))
	}

	// This context remains valid during scanning and cancels only when
	// checkContext is called from sorter.Less.
	sortCtx := &cancelInLessContext{Context: context.Background()}

	var results []Sortable
	results = append(results, Sortable{ID: 999, Value: 999})

	err := db.Select().OrderBy("Value").Find(sortCtx, &results)
	require.ErrorIs(t, err, context.Canceled)
	require.Greater(t, sortCtx.LessCalls(), 0)
	require.Len(t, results, 1)
	require.Equal(t, Sortable{ID: 999, Value: 999}, results[0])

	// A new query with ordering should still work — no goroutine leak.
	var ok []Sortable
	require.NoError(t, db.Select().OrderBy("Value").Find(ctx, &ok))
	require.Len(t, ok, 20)
}

// TestQueryFind_SortErrorPreservesDestination verifies that an ordering
// error (missing field) preserves the destination.
func TestQueryFind_SortErrorPreservesDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type Simple struct {
		ID    int `db:"id,increment"`
		Value int
	}
	for i := 0; i < 5; i++ {
		require.NoError(t, db.Save(ctx, &Simple{Value: i}))
	}

	var results []Simple
	results = append(results, Simple{ID: 999, Value: 999})

	err := db.Select().OrderBy("MissingField").Find(ctx, &results)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNotFound))
	require.Len(t, results, 1)
	require.Equal(t, Simple{ID: 999, Value: 999}, results[0])
}

// TestQueryFind_OrderedSuccessPublishesCompleteResult verifies that
// an ordered query publishes the full sorted result on success.
func TestQueryFind_OrderedSuccessPublishesCompleteResult(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()

	type Item struct {
		ID    int `db:"id,increment"`
		Value int
	}
	for i := 0; i < 10; i++ {
		require.NoError(t, db.Save(ctx, &Item{Value: (i * 7) % 10}))
	}

	var results []Item
	require.NoError(t, db.Select().OrderBy("Value").Find(ctx, &results))
	require.Len(t, results, 10)
	// Verify ascending order by Value.
	for i := 1; i < len(results); i++ {
		require.LessOrEqual(t, results[i-1].Value, results[i].Value)
	}
}

// ============================================================================
// Limit zero
// ============================================================================

// TestQueryLimitZero_CancelledContextDoesNotPublishDestination verifies
// that Limit(0) with a cancelled context does not mutate the destination.
func TestQueryLimitZero_CancelledContextDoesNotPublishDestination(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &Score{Value: 42}))

	var scores []Score
	scores = append(scores, Score{ID: 999, Value: 999})

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := db.Select().Limit(0).Find(cancelCtx, &scores)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Len(t, scores, 1)
	require.Equal(t, Score{ID: 999, Value: 999}, scores[0])
}

// ============================================================================
// Transaction-bound cancellation
// ============================================================================

// TestQueryCancellationInsideWriteTransactionRollsBack verifies that
// cancellation during a Query inside WriteTransaction rolls back all
// changes (the save before the query and the query's own effects).
func TestQueryCancellationInsideWriteTransactionRollsBack(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, db.Save(ctx, &Score{Value: 1}))

	cancelCtx, cancel := context.WithCancel(context.Background())

	// Use WriteTransaction: save a record, then run a query that
	// will be cancelled.
	err := db.WriteTransaction(cancelCtx, func(node Node) error {
		// Save inside transaction — should be rolled back.
		if err := node.Save(cancelCtx, &Score{Value: 100}); err != nil {
			return err
		}

		// Cancel the context now.
		cancel()

		// This query should fail with context.Canceled.
		return node.Select().Find(cancelCtx, new([]Score))
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))

	// Verify the save was rolled back — only the original record exists.
	var all []Score
	require.NoError(t, db.All(ctx, &all))
	require.Len(t, all, 1)
	require.Equal(t, 1, all[0].Value)
}

// ============================================================================
// Nil callback validation with a cancelled context
// ============================================================================

// TestQueryRawEach_CancelledContextTakesPrecedenceOverNilCallback verifies
// that public entry context validation occurs before callback validation.
func TestQueryRawEach_CancelledContextTakesPrecedenceOverNilCallback(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := db.Select().Bucket("Score").RawEach(cancelCtx, &Score{}, nil)
	require.Error(t, err)
	// Context check comes first, so we get context.Canceled.
	require.True(t, errors.Is(err, context.Canceled))
}

// TestQueryRaw_AppliesMatchers verifies that Raw/RawEach decode values when a
// sample kind is supplied, so matchers are not silently ignored.
func TestQueryRaw_AppliesMatchers(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	all, err := db.Select().Bucket("Score").Raw(ctx, &Score{})
	require.NoError(t, err)
	require.Len(t, all, 5)

	matched, err := db.Select(q.Eq("Value", 2)).Bucket("Score").Raw(ctx, &Score{})
	require.NoError(t, err)
	require.Len(t, matched, 1)

	count := 0
	err = db.Select(q.Gt("Value", 1)).Bucket("Score").RawEach(ctx, &Score{}, func(k, v []byte) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

// TestQueryDelete_RemovesEveryMatchAtOnce verifies that a delete query removes
// all matches even though the bucket cursor is walked: deletions are applied
// only after the scan completes.
func TestQueryDelete_RemovesEveryMatchAtOnce(t *testing.T) {
	db, cleanup := createDB(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		require.NoError(t, db.Save(ctx, &Score{Value: i}))
	}

	require.NoError(t, db.Select(q.Lt("Value", 30)).Delete(ctx, &Score{}))
	require.NoError(t, db.Select().Bucket("Score").Delete(ctx, &Score{}))

	var left []Score
	require.NoError(t, db.All(ctx, &left))
	require.Empty(t, left)
}
