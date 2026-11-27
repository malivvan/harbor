package tempest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/malivvan/tempest/store/fstree"
	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/series"
	"github.com/malivvan/tempest/store/vector"
)

// ImportOptions controls Import.
type ImportOptions struct {
	// RecordTypes lists the db record types the stream may contain. Each value
	// is a pointer to a struct; the exported type name must match. A stream
	// containing an unregistered type fails instead of being silently skipped.
	RecordTypes []any

	// DryRun validates the container, its checksum and every record without
	// writing to the destination. dest may be nil in this mode.
	//
	// Because the checksum covers the whole payload and is verified at the
	// footer, a corrupted stream can apply its earlier records before the
	// mismatch is detected. Run a DryRun first when the source is untrusted.
	DryRun bool

	// Progress, when non-nil, is called after each record is validated/applied.
	Progress func(Progress)

	// SearchTokenizer supplies the tokenizer for an exported index whose
	// persisted tokenizer is "custom": a custom tokenizer cannot be
	// reconstructed from its name. It is ignored for built-in tokenizers, and
	// an import of a custom index without it fails (fail closed) instead of
	// approximating the tokenizer.
	SearchTokenizer search.Tokenizer
}

// ImportSummary describes a completed import.
type ImportSummary struct {
	// Records is the number of records applied (or validated in a dry run).
	Records int64
	// Bytes is the number of container bytes read.
	Bytes int64
	// Version is the container format version.
	Version int
	// SourceFlags is the encryption selection of the exporting store, for
	// diagnostics only.
	SourceFlags uint16
	// DryRun reports whether anything was written.
	DryRun bool
}

// Import reads an Export stream and applies it to dest.
//
// Records are applied in stream order through the same store methods a caller
// would use, so the destination's own encryption, codecs and validation apply.
// That is what makes Import the migration path between encryption modes: export
// a plaintext store and import it into an encrypted one (or the reverse).
//
// The footer's SHA-256 checksum is verified after the last record. A truncated
// stream, an unknown format or version, or a checksum mismatch fails with the
// corresponding ErrExport* sentinel.
func Import(ctx context.Context, dest *Store, r io.Reader, opts ImportOptions) (ImportSummary, error) {
	if r == nil {
		return ImportSummary{}, fmt.Errorf("%w: reader is required", ErrInvalidOptions)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ImportSummary{}, err
	}

	im := &importer{
		ctx:         ctx,
		dest:        dest,
		opts:        opts,
		dryRun:      opts.DryRun,
		types:       make(map[string]reflect.Type, len(opts.RecordTypes)),
		fsystems:    make(map[string]*fstree.FS),
		collections: make(map[string]*vector.Collection),
		indexMeta:   make(map[string]search.IndexMeta),
		indexDocs:   make(map[string][]search.DocEntry),
		indexTerms:  make(map[string][]search.TermEntry),
	}
	for _, sample := range opts.RecordTypes {
		t, err := recordType(sample)
		if err != nil {
			return ImportSummary{}, err
		}
		im.types[t.String()] = t
	}

	if !opts.DryRun {
		if dest == nil {
			return ImportSummary{}, ErrClosed
		}
		dest.mu.RLock()
		defer dest.mu.RUnlock()
		if dest.bs == nil {
			return ImportSummary{}, ErrClosed
		}
	}

	reader := bufio.NewReader(r)
	hasher := sha256.New()
	summary := ImportSummary{DryRun: opts.DryRun}
	var (
		read    int64
		records int64
		header  bool
	)

	for {
		if err := ctx.Err(); err != nil {
			return ImportSummary{}, err
		}
		line, err := reader.ReadBytes('\n')
		if len(line) == 0 && err == io.EOF {
			return ImportSummary{}, ErrExportTruncated
		}
		if err != nil && err != io.EOF {
			return ImportSummary{}, fmt.Errorf("tempest: import: read: %w", err)
		}
		atEOF := err == io.EOF
		read += int64(len(line))

		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			return ImportSummary{}, fmt.Errorf("%w: empty line", ErrExportFormat)
		}

		var head struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(trimmed, &head); err != nil {
			return ImportSummary{}, fmt.Errorf("%w: %v", ErrExportFormat, err)
		}

		if head.Kind == exportKindFooter {
			var footer exportRecord
			if err := json.Unmarshal(trimmed, &footer); err != nil {
				return ImportSummary{}, fmt.Errorf("%w: %v", ErrExportFormat, err)
			}
			if !header {
				return ImportSummary{}, fmt.Errorf("%w: footer before header", ErrExportFormat)
			}
			if footer.Records != records {
				return ImportSummary{}, fmt.Errorf("%w: footer counts %d records, stream has %d", ErrExportFormat, footer.Records, records)
			}
			if footer.Checksum != hex.EncodeToString(hasher.Sum(nil)) {
				return ImportSummary{}, ErrExportChecksum
			}
			if err := im.finish(); err != nil {
				return ImportSummary{}, err
			}
			summary.Records = records
			summary.Bytes = read
			return summary, nil
		}

		hasher.Write(line)

		var rec exportRecord
		if err := json.Unmarshal(trimmed, &rec); err != nil {
			return ImportSummary{}, fmt.Errorf("%w: %v", ErrExportFormat, err)
		}

		if rec.Kind == exportKindHeader {
			if header {
				return ImportSummary{}, fmt.Errorf("%w: duplicate header", ErrExportFormat)
			}
			if rec.Format != exportFormat {
				return ImportSummary{}, ErrExportFormat
			}
			if rec.Version != exportVersion {
				return ImportSummary{}, fmt.Errorf("%w: version %d", ErrExportVersion, rec.Version)
			}
			header = true
			summary.Version = rec.Version
			summary.SourceFlags = rec.Flags
			continue
		}

		if !header {
			return ImportSummary{}, fmt.Errorf("%w: record before header", ErrExportFormat)
		}
		records++
		if err := im.apply(rec); err != nil {
			return ImportSummary{}, err
		}
		if opts.Progress != nil {
			opts.Progress(Progress{Section: rec.Kind, Records: records, Bytes: read})
		}
		if atEOF {
			return ImportSummary{}, ErrExportTruncated
		}
	}
}

// importer carries the state of one Import.
type importer struct {
	ctx         context.Context
	dest        *Store
	opts        ImportOptions
	dryRun      bool
	types       map[string]reflect.Type
	fsystems    map[string]*fstree.FS
	collections map[string]*vector.Collection
	// Search records arrive as index, then docs, then terms, so they are
	// collected and materialized in finish. Maps are keyed by index name.
	indexMeta  map[string]search.IndexMeta
	indexDocs  map[string][]search.DocEntry
	indexTerms map[string][]search.TermEntry
}

func (im *importer) apply(rec exportRecord) error {
	switch rec.Kind {
	case exportKindDBRecord:
		return im.applyDBRecord(rec)
	case exportKindFSDir:
		return im.applyFSDir(rec)
	case exportKindFSFile:
		return im.applyFSFile(rec)
	case exportKindFSSymlink:
		return im.applyFSSymlink(rec)
	case exportKindTSPoints:
		return im.applyTSPoints(rec)
	case exportKindTSLabels:
		return im.applyTSLabels(rec)
	case exportKindTSRetention:
		return im.applyTSRetention(rec)
	case exportKindVecCollection:
		return im.applyVecCollection(rec)
	case exportKindVecPoints:
		return im.applyVecPoints(rec)
	case exportKindSearchIndex:
		return im.applySearchIndex(rec)
	case exportKindSearchDocs:
		return im.applySearchDocs(rec)
	case exportKindSearchTerms:
		return im.applySearchTerms(rec)
	case exportKindGraphDocument:
		return im.applyGraphDocument(rec)
	case exportKindObject:
		return im.applyObject(rec)
	default:
		return fmt.Errorf("%w: unknown record kind %q", ErrExportFormat, rec.Kind)
	}
}

func (im *importer) applyDBRecord(rec exportRecord) error {
	t, ok := im.types[rec.Type]
	if !ok {
		return fmt.Errorf("%w: db record type %q is not registered for import", ErrInvalidOptions, rec.Type)
	}
	if len(rec.Data) == 0 {
		return fmt.Errorf("%w: db.record without data", ErrExportFormat)
	}
	ptr := reflect.New(t).Interface()
	if err := json.Unmarshal(rec.Data, ptr); err != nil {
		return fmt.Errorf("tempest: import db record %s: %w", rec.Type, err)
	}
	if im.dryRun {
		return nil
	}
	return im.dest.Record().Save(im.ctx, ptr)
}

func (im *importer) fsFor(name string) (*fstree.FS, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: fs record without container", ErrExportFormat)
	}
	if filesystem, ok := im.fsystems[name]; ok {
		return filesystem, nil
	}
	filesystem, err := im.dest.File().Open(name)
	if err != nil {
		return nil, fmt.Errorf("tempest: import fs %q: %w", name, err)
	}
	im.fsystems[name] = filesystem
	return filesystem, nil
}

func (im *importer) applyFSDir(rec exportRecord) error {
	if rec.Container == "" || rec.Path == "" {
		return fmt.Errorf("%w: fs.dir without container or path", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	filesystem, err := im.fsFor(rec.Container)
	if err != nil {
		return err
	}
	if err := filesystem.MkdirAll(rec.Path, os.FileMode(rec.Mode).Perm()); err != nil {
		return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, rec.Path, err)
	}
	if err := filesystem.Chtimes(rec.Path, rec.Atime, rec.Mtime); err != nil {
		return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, rec.Path, err)
	}
	return nil
}

func (im *importer) applyFSFile(rec exportRecord) error {
	if rec.Container == "" || rec.Path == "" {
		return fmt.Errorf("%w: fs.fstree without container or path", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	filesystem, err := im.fsFor(rec.Container)
	if err != nil {
		return err
	}
	if parent := path.Dir(rec.Path); parent != "/" && parent != "." {
		if err := filesystem.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, parent, err)
		}
	}
	file, err := filesystem.OpenFile(rec.Path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(rec.Mode).Perm())
	if err != nil {
		return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, rec.Path, err)
	}
	if _, err := file.Write(rec.Data); err != nil {
		_ = file.Close()
		return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, rec.Path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, rec.Path, err)
	}
	if err := filesystem.Chtimes(rec.Path, rec.Atime, rec.Mtime); err != nil {
		return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, rec.Path, err)
	}
	return nil
}

func (im *importer) applyFSSymlink(rec exportRecord) error {
	if rec.Container == "" || rec.Path == "" || rec.Link == "" {
		return fmt.Errorf("%w: fs.symlink without container, path or target", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	filesystem, err := im.fsFor(rec.Container)
	if err != nil {
		return err
	}
	if err := filesystem.Symlink(rec.Link, rec.Path); err != nil {
		return fmt.Errorf("tempest: import fs %q %s: %w", rec.Container, rec.Path, err)
	}
	return nil
}

func (im *importer) applyTSPoints(rec exportRecord) error {
	if rec.Container == "" || len(rec.Points) == 0 {
		return fmt.Errorf("%w: ts.points without series or points", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	if err := im.dest.Series().Series(rec.Container).InsertMany(im.ctx, rec.Points); err != nil {
		return fmt.Errorf("tempest: import series %q: %w", rec.Container, err)
	}
	return nil
}

func (im *importer) applyTSLabels(rec exportRecord) error {
	if rec.Container == "" {
		return fmt.Errorf("%w: ts.labels without series", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	if err := im.dest.Series().Series(rec.Container).SetLabels(im.ctx, rec.Labels); err != nil {
		return fmt.Errorf("tempest: import series %q: %w", rec.Container, err)
	}
	return nil
}

func (im *importer) applyTSRetention(rec exportRecord) error {
	if rec.Container == "" {
		return fmt.Errorf("%w: ts.retention without series", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	retention := series.Retention{MaxAge: time.Duration(rec.MaxAge), MaxPoints: rec.MaxPoints}
	if err := im.dest.Series().Series(rec.Container).SetRetention(im.ctx, retention); err != nil {
		return fmt.Errorf("tempest: import series %q: %w", rec.Container, err)
	}
	return nil
}

func (im *importer) applyVecCollection(rec exportRecord) error {
	options := vector.CollectionOptions{
		Name:      rec.Container,
		VectorLen: rec.VectorLen,
		Metric:    vector.Distance(rec.Metric),
		UseHNSW:   rec.UseHNSW,
	}
	if rec.HNSW != nil {
		options.HNSWParams = *rec.HNSW
	}
	if err := options.Validate(); err != nil {
		return fmt.Errorf("tempest: import collection %q: %w", rec.Container, err)
	}
	if im.dryRun {
		return nil
	}
	collection, err := im.dest.Vector().NewCollectionWithOptions(options)
	if err != nil {
		return fmt.Errorf("tempest: import collection %q: %w", rec.Container, err)
	}
	im.collections[rec.Container] = collection
	return nil
}

func (im *importer) applyVecPoints(rec exportRecord) error {
	if rec.Container == "" || len(rec.Vectors) == 0 {
		return fmt.Errorf("%w: vec.points without collection or points", ErrExportFormat)
	}
	batch, err := decodeVectorBatch(rec.Vectors)
	if err != nil {
		return fmt.Errorf("tempest: import collection %q: %w", rec.Container, err)
	}
	if im.dryRun {
		return nil
	}
	collection, ok := im.collections[rec.Container]
	if !ok {
		return fmt.Errorf("%w: vector points for unknown collection %q", ErrExportFormat, rec.Container)
	}
	for i := range batch {
		batch[i].Version = 0 // store-managed
	}
	if err := collection.Upsert(batch); err != nil {
		return fmt.Errorf("tempest: import collection %q: %w", rec.Container, err)
	}
	return nil
}

// finish flushes imported vector collections so the records are durable even if
// the store stays open, then materializes the collected search indexes.
func (im *importer) finish() error {
	if im.dryRun {
		return nil
	}
	for name, collection := range im.collections {
		if err := collection.Flush(); err != nil {
			return fmt.Errorf("tempest: import collection %q: %w", name, err)
		}
	}
	for name, meta := range im.indexMeta {
		if err := im.dest.Search().RestoreIndex(im.ctx, name, search.Config{
			Tokenizer: im.opts.SearchTokenizer,
		}, meta, im.indexDocs[name], im.indexTerms[name]); err != nil {
			return fmt.Errorf("tempest: import search %q: %w", name, err)
		}
	}
	return nil
}

func (im *importer) applySearchIndex(rec exportRecord) error {
	if rec.Name == "" {
		return fmt.Errorf("%w: fts.index without name", ErrExportFormat)
	}
	if rec.K1 < 0 || rec.B < 0 || rec.B > 1 {
		return fmt.Errorf("%w: fts.index %q has invalid BM25 parameters", ErrExportFormat, rec.Name)
	}
	if im.dryRun {
		return nil
	}
	im.indexMeta[rec.Name] = search.IndexMeta{
		K1:        rec.K1,
		B:         rec.B,
		Tokenizer: rec.Tokenizer,
		DocCount:  rec.DocCount,
		TotalLen:  rec.TotalLen,
	}
	return nil
}

func (im *importer) applySearchDocs(rec exportRecord) error {
	if rec.Name == "" || len(rec.Docs) == 0 {
		return fmt.Errorf("%w: fts.docs without index or documents", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	im.indexDocs[rec.Name] = append(im.indexDocs[rec.Name], rec.Docs...)
	return nil
}

func (im *importer) applySearchTerms(rec exportRecord) error {
	if rec.Name == "" || len(rec.Terms) == 0 {
		return fmt.Errorf("%w: fts.terms without index or terms", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	im.indexTerms[rec.Name] = append(im.indexTerms[rec.Name], rec.Terms...)
	return nil
}

func (im *importer) applyGraphDocument(rec exportRecord) error {
	if rec.Name == "" {
		return fmt.Errorf("%w: graph.document without name", ErrExportFormat)
	}
	if rec.Nodes == nil && rec.Edges == nil {
		return fmt.Errorf("%w: graph.document %q carries no nodes or edges", ErrExportFormat, rec.Name)
	}
	if im.dryRun {
		return nil
	}
	g, err := im.dest.Graphs().Open(rec.Name)
	if err != nil {
		return fmt.Errorf("tempest: import graph %q: %w", rec.Name, err)
	}
	result, err := g.ImportDocument(im.ctx, &graphs.Document{Nodes: rec.Nodes, Edges: rec.Edges})
	if err != nil {
		return fmt.Errorf("tempest: import graph %q: %w", rec.Name, err)
	}
	if result.FailedCount > 0 {
		return fmt.Errorf("tempest: import graph %q: %d of %d records failed",
			rec.Name, result.FailedCount, result.SuccessCount+result.FailedCount)
	}
	return nil
}

func (im *importer) applyObject(rec exportRecord) error {
	if rec.Bucket == "" || rec.Key == "" {
		return fmt.Errorf("%w: object.put without bucket or key", ErrExportFormat)
	}
	if im.dryRun {
		return nil
	}
	if _, err := im.dest.Objects().Put(im.ctx, rec.Bucket, rec.Key, bytes.NewReader(rec.Data), int64(len(rec.Data)), ""); err != nil {
		return fmt.Errorf("tempest: import object %q/%q: %w", rec.Bucket, rec.Key, err)
	}
	return nil
}

// decodeVectorBatch decodes an exported vector batch. json.Number is converted
// back to int64 (or float64 when it does not fit), so payload integers survive
// the JSON round trip.
func decodeVectorBatch(raw json.RawMessage) ([]vector.PointStruct, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var batch []vector.PointStruct
	if err := decoder.Decode(&batch); err != nil {
		return nil, fmt.Errorf("decode vector batch: %w", err)
	}
	for i := range batch {
		for key, value := range batch[i].Payload {
			batch[i].Payload[key] = normalizeJSONNumbers(value)
		}
	}
	return batch, nil
}

// normalizeJSONNumbers converts json.Number values (and numbers nested in maps
// and slices) back to int64 or float64.
func normalizeJSONNumbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case map[string]any:
		for key, item := range v {
			v[key] = normalizeJSONNumbers(item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = normalizeJSONNumbers(item)
		}
		return v
	default:
		return value
	}
}
