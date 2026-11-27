package cluster

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
	// record store
	opRecordSave        = "record.save"
	opRecordUpdate      = "record.update"
	opRecordUpdateField = "record.update_field"
	opRecordDelete      = "record.delete"
	opRecordDrop        = "record.drop"
	opRecordSet         = "record.set"
	opRecordDeleteKey   = "record.delete_key"

	// series store
	opSeriesInsert       = "series.insert"
	opSeriesInsertMany   = "series.insert_many"
	opSeriesDeleteRange  = "series.delete_range"
	opSeriesDrop         = "series.drop"
	opSeriesSetLabels    = "series.set_labels"
	opSeriesSetRetention = "series.set_retention"
	opSeriesDeleteSeries = "series.delete_series"

	// vector store
	opVectorCreateCollection        = "vector.create_collection"
	opVectorCreateCollectionOptions = "vector.create_collection_options"
	opVectorEnsureCollection        = "vector.ensure_collection"
	opVectorUpsert                  = "vector.upsert"
	opVectorDelete                  = "vector.delete"
	opVectorDropCollection          = "vector.drop_collection"
	opVectorFlush                   = "vector.flush"

	// search store
	opSearchCreate = "search.create"
	opSearchUpsert = "search.upsert"
	opSearchDelete = "search.delete"
	opSearchDrop   = "search.drop"

	// graphs store
	opGraphCreate     = "graph.create"
	opGraphDrop       = "graph.drop"
	opGraphUpsertNode = "graph.upsert_node"
	opGraphDeleteNode = "graph.delete_node"
	opGraphUpsertEdge = "graph.upsert_edge"
	opGraphDeleteEdge = "graph.delete_edge"

	// object store
	opObjectPut    = "object.put"
	opObjectDelete = "object.delete"
	opObjectMove   = "object.move"
	opObjectDrop   = "object.drop"

	// fstree store
	opFstreeMkdir            = "fstree.mkdir"
	opFstreeMkdirAll         = "fstree.mkdir_all"
	opFstreeRemove           = "fstree.remove"
	opFstreeRename           = "fstree.rename"
	opFstreeRemoveAll        = "fstree.remove_all"
	opFstreeWrite            = "fstree.write"
	opFstreeTruncate         = "fstree.truncate"
	opFstreeChmod            = "fstree.chmod"
	opFstreeChtimes          = "fstree.chtimes"
	opFstreeChown            = "fstree.chown"
	opFstreeSymlink          = "fstree.symlink"
	opFstreeDeleteFileSystem = "fstree.delete_fs"
)

func encodeCommand(op string, arg any) ([]byte, error) {
	raw, err := json.Marshal(arg)
	if err != nil {
		return nil, fmt.Errorf("cluster: encode %s argument: %w", op, err)
	}
	return json.Marshal(Command{Op: op, Arg: raw})
}

func decodeCommand(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("cluster: decode command: %w", err)
	}
	if c.Op == "" {
		return Command{}, fmt.Errorf("cluster: decode command: missing op")
	}
	return c, nil
}
