package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
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
func (f *fsm) Apply(l *raft.Log) any {
	f.applyMu.Lock()
	defer f.applyMu.Unlock()

	c, err := decodeCommand(l.Data)
	if err != nil {
		// An undecodable entry means a corrupt or tampered log, which cannot be
		// recovered from silently.
		panic(err)
	}
	return f.applyCommand(c)
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

func (f *fsm) applyCommand(c Command) any {
	ctx := context.Background()
	switch c.Op {
	// db
	case opDBSave, opDBUpdate:
		var a dbChange
		mustDecode(c.Arg, &a)
		return f.applyDBUpsert(ctx, a, c.Op == opDBUpdate)
	case opDBUpdateField:
		var a dbFieldChange
		mustDecode(c.Arg, &a)
		return f.applyDBUpdateField(ctx, a)
	case opDBDelete:
		var a dbChange
		mustDecode(c.Arg, &a)
		return f.applyDBDelete(ctx, a)
	case opDBDrop:
		var a dbChange
		mustDecode(c.Arg, &a)
		return f.applyDBDrop(ctx, a)

	// ts
	case opTSInsert:
		var a tsInsertArg
		mustDecode(c.Arg, &a)
		return f.applyTSInsert(ctx, a)
	case opTSInsertMany:
		var a tsInsertManyArg
		mustDecode(c.Arg, &a)
		return f.applyTSInsertMany(ctx, a)
	case opTSDeleteRange:
		var a tsRangeArg
		mustDecode(c.Arg, &a)
		return f.applyTSDeleteRange(ctx, a)
	case opTSDrop:
		var a tsNameArg
		mustDecode(c.Arg, &a)
		return f.applyTSDrop(ctx, a)
	case opTSSetLabels:
		var a tsLabelsArg
		mustDecode(c.Arg, &a)
		return f.applyTSSetLabels(ctx, a)
	case opTSSetRetention:
		var a tsRetentionArg
		mustDecode(c.Arg, &a)
		return f.applyTSSetRetention(ctx, a)
	case opTSDeleteSeries:
		var a tsNameArg
		mustDecode(c.Arg, &a)
		return f.applyTSDeleteSeries(ctx, a)

	// vec
	case opVecCreateCollection:
		var a vecCreateArg
		mustDecode(c.Arg, &a)
		return f.applyVecCreate(ctx, a)
	case opVecUpsert:
		var a vecUpsertArg
		mustDecode(c.Arg, &a)
		return f.applyVecUpsert(ctx, a)
	case opVecDelete:
		var a vecDeleteArg
		mustDecode(c.Arg, &a)
		return f.applyVecDelete(ctx, a)
	case opVecDropCollection:
		var a vecDropArg
		mustDecode(c.Arg, &a)
		return f.applyVecDrop(ctx, a)

	// fs
	case opFSMkdir:
		var a fsPathArg
		mustDecode(c.Arg, &a)
		return f.applyFSMkdir(a)
	case opFSRemove:
		var a fsPathArg
		mustDecode(c.Arg, &a)
		return f.applyFSRemove(a)
	case opFSRename:
		var a fsPathArg
		mustDecode(c.Arg, &a)
		return f.applyFSRename(a)
	case opFSRemoveAll:
		var a fsPathArg
		mustDecode(c.Arg, &a)
		return f.applyFSRemoveAll(a)
	case opFSWrite:
		var a fsPathArg
		mustDecode(c.Arg, &a)
		return f.applyFSWrite(a)
	case opFSDeleteFS:
		var a fsPathArg
		mustDecode(c.Arg, &a)
		return f.applyFSDeleteFS(a)

	default:
		panic(fmt.Sprintf("consensus: unknown command op %q", c.Op))
	}
}

func mustDecode(raw json.RawMessage, v any) {
	if err := json.Unmarshal(raw, v); err != nil {
		panic(fmt.Sprintf("consensus: decode %T argument: %v", v, err))
	}
}
