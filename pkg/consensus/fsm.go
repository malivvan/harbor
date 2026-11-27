package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/malivvan/eidos-db/pkg/metrics"
)

// fsm adapts a Node's eidos-db store to raft.FSM. Apply dispatches commands to
// the four stores; Snapshot serializes the eidos-db store; Restore replaces it.
type fsm struct {
	node *Node

	// applyMu serializes Apply against Snapshot so a snapshot never observes a
	// half-applied command. raft already serializes Apply calls; this also
	// excludes the snapshot read that runs outside raft's FSM lock.
	applyMu sync.Mutex
}

var _ raft.FSM = (*fsm)(nil)

// Apply applies one committed log entry to the local eidos-db store. It returns
// the operation result (or an error) as the raft future's response.
//
// A corrupt, incompatible, or manually edited log entry is reported as an
// ApplyError instead of panicking: the FSM is a replicated state machine, so
// terminating the process would take down a node that is otherwise healthy.
// The error carries the log index and operation so an operator can pinpoint
// the offending entry. Panic recovery stays as an outer safety net for
// unexpected failures in the store layer.
func (f *fsm) Apply(l *raft.Log) any {
	f.applyMu.Lock()
	defer f.applyMu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			f.node.logf("[ERROR] consensus: panic applying log index %d: %v", l.Index, r)
		}
	}()

	// Apply lag is the delay between the entry being appended to the raft log
	// and this node applying it, which is what a follower read must wait out.
	started := time.Now()

	c, err := decodeCommand(l.Data)
	if err != nil {
		f.observeApply(started, err)
		return &ApplyError{Index: l.Index, Err: err}
	}
	resp := f.applyCommand(l.Index, c)
	f.observeApply(started, asError(resp))
	return resp
}

// observeApply reports one applied log entry to the configured observer: how
// long it took to apply and whether it succeeded.
func (f *fsm) observeApply(started time.Time, err error) {
	if f == nil || f.node == nil || f.node.obs == nil {
		return
	}
	result := metrics.ResultOK
	if err != nil {
		result = metrics.ResultError
	}
	labels := metrics.Labels{
		metrics.LabelStore:  "consensus",
		metrics.LabelOp:     "apply",
		metrics.LabelResult: result,
	}
	metrics.Histogram(f.node.obs, metrics.MetricConsensusApplyLag, float64(time.Since(started)), labels)
	metrics.Counter(f.node.obs, metrics.MetricConsensusApplyTotal, 1, labels)
}

// asError returns resp as an error when it is one, and nil otherwise.
func asError(resp any) error {
	if err, ok := resp.(error); ok {
		return err
	}
	return nil
}

// Snapshot returns a serialized snapshot of the eidos-db store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.applyMu.Lock()
	defer f.applyMu.Unlock()

	data, err := f.node.serializeStore()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the eidos-db store with the snapshot contents.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.applyMu.Lock()
	defer f.applyMu.Unlock()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	return f.node.restoreStore(data)
}

func (f *fsm) applyCommand(index uint64, c Command) any {
	ctx := context.Background()
	switch c.Op {
	// db
	case opRecordSave, opRecordUpdate:
		var a recordChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyRecordUpsert(ctx, a, c.Op == opRecordUpdate)
	case opRecordUpdateField:
		var a recordFieldChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyRecordUpdateField(ctx, a)
	case opRecordDelete:
		var a recordChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyRecordDelete(ctx, a)
	case opRecordDrop:
		var a recordChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyRecordDrop(ctx, a)
	case opRecordSet:
		var a recordKVArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyRecordSet(ctx, a)
	case opRecordDeleteKey:
		var a recordKVArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyRecordDeleteKey(ctx, a)

	// ts
	case opSeriesInsert:
		var a seriesInsertArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applySeriesInsert(ctx, a)
	case opSeriesInsertMany:
		var a seriesInsertManyArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applySeriesInsertMany(ctx, a)
	case opSeriesDeleteRange:
		var a seriesRangeArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applySeriesDeleteRange(ctx, a)
	case opSeriesDrop:
		var a seriesNameArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applySeriesDrop(ctx, a)
	case opSeriesSetLabels:
		var a seriesLabelsArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applySeriesSetLabels(ctx, a)
	case opSeriesSetRetention:
		var a seriesRetentionArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applySeriesSetRetention(ctx, a)
	case opSeriesDeleteSeries:
		var a seriesNameArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applySeriesDeleteSeries(ctx, a)

	// vec
	case opVectorCreateCollection:
		var a vectorCreateArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVectorCreate(ctx, a)
	case opVectorCreateCollectionOptions:
		var a vectorCreateOptionsArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVectorCreateOptions(ctx, a)
	case opVectorEnsureCollection:
		var a vectorNameArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVectorEnsure(ctx, a)
	case opVectorFlush:
		return f.applyVectorFlush(ctx)
	case opVectorUpsert:
		var a vectorUpsertArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVectorUpsert(ctx, a)
	case opVectorDelete:
		var a vectorDeleteArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVectorDelete(ctx, a)
	case opVectorDropCollection:
		var a vectorDropArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVectorDrop(ctx, a)

	// fs
	case opFileMkdir:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileMkdir(a)
	case opFileMkdirAll:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileMkdirAll(a)
	case opFileTruncate:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileTruncate(a)
	case opFileChmod:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileChmod(a)
	case opFileChtimes:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileChtimes(a)
	case opFileChown:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileChown(a)
	case opFileSymlink:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileSymlink(a)
	case opFileRemove:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileRemove(a)
	case opFileRename:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileRename(a)
	case opFileRemoveAll:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileRemoveAll(a)
	case opFileWrite:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileWrite(a)
	case opFileDeleteFileSystem:
		var a filePathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFileDeleteFileSystem(a)

	default:
		return &ApplyError{
			Index: index,
			Op:    c.Op,
			Err:   fmt.Errorf("unknown command op"),
		}
	}
}

// decodeArg unmarshals a command argument, reporting a malformed payload as a
// structured apply failure instead of panicking.
func decodeArg(index uint64, c Command, v any) error {
	if err := json.Unmarshal(c.Arg, v); err != nil {
		return &ApplyError{
			Index: index,
			Op:    c.Op,
			Err:   fmt.Errorf("decode %T argument: %w", v, err),
		}
	}
	return nil
}
