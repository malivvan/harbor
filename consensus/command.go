package consensus

import (
	"encoding/json"
	"fmt"
)

// Command is one replicated operation. It is the unit of the raft log: the
// leader encodes a write as a Command, applies it through raft, and every
// node's FSM dispatches Op to the store that owns it.
type Command struct {
	// Op identifies the operation, one of the op* constants.
	Op string `json:"op"`

	// Arg is the operation-specific, deterministic payload.
	Arg json.RawMessage `json:"arg,omitempty"`
}

// Operation identifiers. Each is handled by the FSM dispatcher.
const (
	// db store
	opDBSave        = "db.save"
	opDBUpdate      = "db.update"
	opDBUpdateField = "db.update_field"
	opDBDelete      = "db.delete"
	opDBDrop        = "db.drop"
	opDBSet         = "db.set"
	opDBDeleteKey   = "db.delete_key"

	// ts store
	opTSInsert       = "ts.insert"
	opTSInsertMany   = "ts.insert_many"
	opTSDeleteRange  = "ts.delete_range"
	opTSDrop         = "ts.drop"
	opTSSetLabels    = "ts.set_labels"
	opTSSetRetention = "ts.set_retention"
	opTSDeleteSeries = "ts.delete_series"

	// vec store
	opVecCreateCollection        = "vec.create_collection"
	opVecCreateCollectionOptions = "vec.create_collection_options"
	opVecEnsureCollection        = "vec.ensure_collection"
	opVecUpsert                  = "vec.upsert"
	opVecDelete                  = "vec.delete"
	opVecDropCollection          = "vec.drop_collection"
	opVecFlush                   = "vec.flush"

	// fs store
	opFSMkdir     = "fs.mkdir"
	opFSMkdirAll  = "fs.mkdir_all"
	opFSRemove    = "fs.remove"
	opFSRename    = "fs.rename"
	opFSRemoveAll = "fs.remove_all"
	opFSWrite     = "fs.write"
	opFSTruncate  = "fs.truncate"
	opFSChmod     = "fs.chmod"
	opFSChtimes   = "fs.chtimes"
	opFSChown     = "fs.chown"
	opFSSymlink   = "fs.symlink"
	opFSDeleteFS  = "fs.delete_fs"
)

func encodeCommand(op string, arg any) ([]byte, error) {
	raw, err := json.Marshal(arg)
	if err != nil {
		return nil, fmt.Errorf("consensus: encode %s argument: %w", op, err)
	}
	return json.Marshal(Command{Op: op, Arg: raw})
}

func decodeCommand(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("consensus: decode command: %w", err)
	}
	if c.Op == "" {
		return Command{}, fmt.Errorf("consensus: decode command: missing op")
	}
	return c, nil
}
