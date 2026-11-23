package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/malivvan/harbor/pkg/metrics"
)

// fsm adapts a Node's harbor store to raft.FSM. Apply dispatches commands to
// the four stores; Snapshot serializes the harbor database; Restore replaces it.
type fsm struct {
	node *Node

	// applyMu serializes Apply against Snapshot so a snapshot never observes a
	// half-applied command. raft already serializes Apply calls; this also
	// excludes the snapshot read that runs outside raft's FSM lock.
	applyMu sync.Mutex
}

var _ raft.FSM = (*fsm)(nil)

// Apply applies one committed log entry to the local harbor store. It returns
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

// Snapshot returns a serialized snapshot of the harbor database.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.applyMu.Lock()
	defer f.applyMu.Unlock()

	data, err := f.node.serializeStore()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the harbor database with the snapshot contents.
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
	case opDBSave, opDBUpdate:
		var a dbChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyDBUpsert(ctx, a, c.Op == opDBUpdate)
	case opDBUpdateField:
		var a dbFieldChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyDBUpdateField(ctx, a)
	case opDBDelete:
		var a dbChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyDBDelete(ctx, a)
	case opDBDrop:
		var a dbChange
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyDBDrop(ctx, a)
	case opDBSet:
		var a dbKVArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyDBSet(ctx, a)
	case opDBDeleteKey:
		var a dbKVArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyDBDeleteKey(ctx, a)

	// ts
	case opTSInsert:
		var a tsInsertArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyTSInsert(ctx, a)
	case opTSInsertMany:
		var a tsInsertManyArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyTSInsertMany(ctx, a)
	case opTSDeleteRange:
		var a tsRangeArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyTSDeleteRange(ctx, a)
	case opTSDrop:
		var a tsNameArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyTSDrop(ctx, a)
	case opTSSetLabels:
		var a tsLabelsArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyTSSetLabels(ctx, a)
	case opTSSetRetention:
		var a tsRetentionArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyTSSetRetention(ctx, a)
	case opTSDeleteSeries:
		var a tsNameArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyTSDeleteSeries(ctx, a)

	// vec
	case opVecCreateCollection:
		var a vecCreateArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVecCreate(ctx, a)
	case opVecCreateCollectionOptions:
		var a vecCreateOptionsArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVecCreateOptions(ctx, a)
	case opVecEnsureCollection:
		var a vecNameArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVecEnsure(ctx, a)
	case opVecFlush:
		return f.applyVecFlush(ctx)
	case opVecUpsert:
		var a vecUpsertArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVecUpsert(ctx, a)
	case opVecDelete:
		var a vecDeleteArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVecDelete(ctx, a)
	case opVecDropCollection:
		var a vecDropArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyVecDrop(ctx, a)

	// fs
	case opFSMkdir:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSMkdir(a)
	case opFSMkdirAll:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSMkdirAll(a)
	case opFSTruncate:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSTruncate(a)
	case opFSChmod:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSChmod(a)
	case opFSChtimes:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSChtimes(a)
	case opFSChown:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSChown(a)
	case opFSSymlink:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSSymlink(a)
	case opFSRemove:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSRemove(a)
	case opFSRename:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSRename(a)
	case opFSRemoveAll:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSRemoveAll(a)
	case opFSWrite:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSWrite(a)
	case opFSDeleteFS:
		var a fsPathArg
		if err := decodeArg(index, c, &a); err != nil {
			return err
		}
		return f.applyFSDeleteFS(a)

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
