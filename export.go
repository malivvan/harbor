package harbor

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"reflect"
	"sort"
	"time"

	"github.com/malivvan/harbor/store/fstree"
	"github.com/malivvan/harbor/store/record"
	"github.com/malivvan/harbor/store/series"
	"github.com/malivvan/harbor/store/vector"
)

// Export format constants. The container is newline-delimited JSON: one header
// line, one line per record, and a footer with the record count and a SHA-256
// checksum of the header and record lines.
const (
	exportFormat  = "harbor-export"
	exportVersion = 1

	// exportBatch bounds how many time-series or vector points share one
	// record, so lines and progress callbacks stay at a useful granularity.
	exportBatch = 500
)

// Record kinds written by Export and accepted by Import.
const (
	exportKindHeader        = "header"
	exportKindFooter        = "footer"
	exportKindDBRecord      = "db.record"
	exportKindFSDir         = "fs.dir"
	exportKindFSFile        = "fs.file"
	exportKindFSSymlink     = "fs.symlink"
	exportKindTSPoints      = "ts.points"
	exportKindTSLabels      = "ts.labels"
	exportKindTSRetention   = "ts.retention"
	exportKindVecCollection = "vec.collection"
	exportKindVecPoints     = "vec.points"
)

// Errors returned by Export and Import.
var (
	// ErrExportFormat reports a stream that is not a Harbor export: a bad
	// header, an unknown record kind, or an unexpected ordering.
	ErrExportFormat = errors.New("harbor: unrecognized export format")

	// ErrExportVersion reports an export written by an incompatible version.
	ErrExportVersion = errors.New("harbor: unsupported export version")

	// ErrExportChecksum reports a checksum mismatch: the container was modified
	// after it was written.
	ErrExportChecksum = errors.New("harbor: export checksum mismatch")

	// ErrExportTruncated reports a stream that ended before its footer.
	ErrExportTruncated = errors.New("harbor: export is truncated")
)

// Progress describes how far an export or import has advanced.
type Progress struct {
	// Section is the record kind being processed, for example "fs.file".
	Section string
	// Records is the number of records processed so far, including this one.
	Records int64
	// Bytes is the number of payload bytes read or written so far (header and
	// records, excluding the footer).
	Bytes int64
}

// ExportOptions controls Export.
type ExportOptions struct {
	// RecordTypes lists the db record types to export. Each value is a pointer
	// to (or a value of) a struct mapped with the db store's tags; every record
	// of that type is written. Other db buckets, including raw key/value
	// buckets, are not exported.
	RecordTypes []any

	// Progress, when non-nil, is called after each record is written.
	Progress func(Progress)
}

// ExportSummary describes a completed export.
type ExportSummary struct {
	// Records is the number of records, excluding the header and footer.
	Records int64
	// Bytes is the total size of the container.
	Bytes int64
	// Checksum is the hex-encoded SHA-256 of the payload (header and records).
	Checksum string
}

// exportRecord is the wire form of one line. Header and footer use the first
// fields; a record kind uses the fields relevant to it.
type exportRecord struct {
	Kind      string             `json:"kind"`
	Format    string             `json:"format,omitempty"`
	Version   int                `json:"version,omitempty"`
	Flags     uint16             `json:"flags,omitempty"`
	Records   int64              `json:"records,omitempty"`
	Checksum  string             `json:"sha256,omitempty"`
	Container string             `json:"container,omitempty"`
	Type      string             `json:"record_type,omitempty"`
	Path      string             `json:"path,omitempty"`
	Link      string             `json:"link,omitempty"`
	Mode      uint32             `json:"mode,omitempty"`
	Atime     time.Time          `json:"atime,omitempty"`
	Mtime     time.Time          `json:"mtime,omitempty"`
	Data      []byte             `json:"data,omitempty"`
	Labels    map[string]string  `json:"labels,omitempty"`
	MaxAge    int64              `json:"max_age,omitempty"`
	MaxPoints int                `json:"max_points,omitempty"`
	Points    []series.Point     `json:"points,omitempty"`
	VectorLen int                `json:"vector_len,omitempty"`
	Metric    string             `json:"metric,omitempty"`
	UseHNSW   bool               `json:"use_hnsw,omitempty"`
	HNSW      *vector.HNSWParams `json:"hnsw,omitempty"`
	Vectors   json.RawMessage    `json:"vectors,omitempty"`
}

// countingWriter counts the bytes that reach the destination.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// Export writes a versioned, checksummed logical snapshot of every store to w.
//
// Unlike Backup, which copies the database file byte for byte, Export writes
// logical records: the db records of the supplied types, every named file
// system's tree, every time series and every vector collection. Import applies
// those records to another store, so the container can migrate between
// encryption modes and between on-disk formats. Each record is
// newline-delimited JSON; the footer carries the record count and a SHA-256
// checksum of the payload.
//
// Buffered vector points are flushed first, so points the store accepted but
// did not yet write are included. db records are encoded with encoding/json, so
// a record type that JSON cannot represent fails the export. File ownership is
// not exported: the portable FileInfo surface has no owner, and permissions are
// advisory on virtual backends.
func Export(ctx context.Context, src *Store, w io.Writer, opts ExportOptions) (ExportSummary, error) {
	if w == nil {
		return ExportSummary{}, fmt.Errorf("%w: writer is required", ErrInvalidOptions)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ExportSummary{}, err
	}
	if src == nil {
		return ExportSummary{}, ErrClosed
	}

	// Read the flags before taking the store lock: Flags takes the same lock,
	// and a waiting writer would make a nested read lock deadlock.
	flags := src.Flags()

	src.mu.RLock()
	defer src.mu.RUnlock()
	if src.bs == nil {
		return ExportSummary{}, ErrClosed
	}
	if src.vector != nil {
		if err := src.vector.Flush(); err != nil && !errors.Is(err, vector.ErrClosed) {
			return ExportSummary{}, fmt.Errorf("harbor: export: flush vec store: %w", err)
		}
	}

	counted := &countingWriter{w: w}
	buffered := bufio.NewWriter(counted)
	hasher := sha256.New()
	e := &exporter{
		ctx:     ctx,
		src:     src,
		out:     buffered,
		payload: io.MultiWriter(buffered, hasher),
		opts:    opts,
	}

	if err := e.writeHeader(flags); err != nil {
		return ExportSummary{}, err
	}
	for _, export := range []func() error{e.exportDB, e.exportFS, e.exportTS, e.exportVec} {
		if err := export(); err != nil {
			return ExportSummary{}, err
		}
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	footer, err := json.Marshal(exportRecord{Kind: exportKindFooter, Records: e.records, Checksum: sum})
	if err != nil {
		return ExportSummary{}, fmt.Errorf("harbor: export: encode footer: %w", err)
	}
	if _, err := buffered.Write(append(footer, '\n')); err != nil {
		return ExportSummary{}, fmt.Errorf("harbor: export: write footer: %w", err)
	}
	if err := buffered.Flush(); err != nil {
		return ExportSummary{}, fmt.Errorf("harbor: export: write: %w", err)
	}

	return ExportSummary{Records: e.records, Bytes: counted.n, Checksum: sum}, nil
}

// exporter carries the state of one Export.
type exporter struct {
	ctx     context.Context
	src     *Store
	out     *bufio.Writer
	payload io.Writer
	opts    ExportOptions
	records int64
	bytes   int64
}

func (e *exporter) writeHeader(flags uint16) error {
	line, err := json.Marshal(exportRecord{
		Kind:    exportKindHeader,
		Format:  exportFormat,
		Version: exportVersion,
		Flags:   flags,
	})
	if err != nil {
		return fmt.Errorf("harbor: export: encode header: %w", err)
	}
	line = append(line, '\n')
	if _, err := e.payload.Write(line); err != nil {
		return fmt.Errorf("harbor: export: write header: %w", err)
	}
	e.bytes += int64(len(line))
	return nil
}

func (e *exporter) writeRecord(rec exportRecord) error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("harbor: export: encode %s record: %w", rec.Kind, err)
	}
	line = append(line, '\n')
	if _, err := e.payload.Write(line); err != nil {
		return fmt.Errorf("harbor: export: write: %w", err)
	}
	e.records++
	e.bytes += int64(len(line))
	if e.opts.Progress != nil {
		e.opts.Progress(Progress{Section: rec.Kind, Records: e.records, Bytes: e.bytes})
	}
	return nil
}

// recordType validates a record sample and returns its struct type.
func recordType(sample any) (reflect.Type, error) {
	if sample == nil {
		return nil, fmt.Errorf("%w: record type is required", ErrInvalidOptions)
	}
	t := reflect.TypeOf(sample)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: record type %s is not a struct", ErrInvalidOptions, t)
	}
	return t, nil
}

func (e *exporter) exportDB() error {
	for _, sample := range e.opts.RecordTypes {
		t, err := recordType(sample)
		if err != nil {
			return err
		}
		records := reflect.New(reflect.SliceOf(reflect.PointerTo(t)))
		if err := e.src.Record().Select().Find(e.ctx, records.Interface()); err != nil && !errors.Is(err, record.ErrNotFound) {
			return fmt.Errorf("harbor: export db %s: %w", t, err)
		}
		slice := records.Elem()
		for i := 0; i < slice.Len(); i++ {
			if err := e.ctx.Err(); err != nil {
				return err
			}
			data, err := json.Marshal(slice.Index(i).Interface())
			if err != nil {
				return fmt.Errorf("harbor: export db record %s: %w", t, err)
			}
			if err := e.writeRecord(exportRecord{Kind: exportKindDBRecord, Type: t.String(), Data: data}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *exporter) exportFS() error {
	names, err := e.src.File().FileSystems()
	if err != nil {
		return fmt.Errorf("harbor: export fs: %w", err)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := e.ctx.Err(); err != nil {
			return err
		}
		vfs, err := e.src.File().Open(name)
		if err != nil {
			return fmt.Errorf("harbor: export fs %q: %w", name, err)
		}
		if err := e.exportFSTree(name, vfs, "/"); err != nil {
			return err
		}
	}
	return nil
}

func (e *exporter) exportFSTree(container string, vfs *fstree.FS, dir string) error {
	entries, err := vfs.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("harbor: export fs %q %s: %w", container, dir, err)
	}
	for _, entry := range entries {
		if err := e.ctx.Err(); err != nil {
			return err
		}
		p := path.Join(dir, entry.Name())
		info, err := vfs.Lstat(p)
		if err != nil {
			return fmt.Errorf("harbor: export fs %q %s: %w", container, p, err)
		}
		mtime := info.ModTime()
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := vfs.Readlink(p)
			if err != nil {
				return fmt.Errorf("harbor: export fs %q %s: %w", container, p, err)
			}
			if err := e.writeRecord(exportRecord{Kind: exportKindFSSymlink, Container: container, Path: p, Link: target}); err != nil {
				return err
			}
		case info.IsDir():
			if err := e.writeRecord(exportRecord{
				Kind:      exportKindFSDir,
				Container: container,
				Path:      p,
				Mode:      uint32(info.Mode()),
				Atime:     mtime,
				Mtime:     mtime,
			}); err != nil {
				return err
			}
			if err := e.exportFSTree(container, vfs, p); err != nil {
				return err
			}
		default:
			data, err := vfs.ReadFile(p)
			if err != nil {
				return fmt.Errorf("harbor: export fs %q %s: %w", container, p, err)
			}
			if err := e.writeRecord(exportRecord{
				Kind:      exportKindFSFile,
				Container: container,
				Path:      p,
				Mode:      uint32(info.Mode()),
				Atime:     mtime,
				Mtime:     mtime,
				Data:      data,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *exporter) exportTS() error {
	seriesInfos, err := e.src.Series().ListSeries(e.ctx)
	if err != nil {
		return fmt.Errorf("harbor: export ts: %w", err)
	}
	for _, info := range seriesInfos {
		if err := e.ctx.Err(); err != nil {
			return err
		}
		ser := e.src.Series().Series(info.Name)
		var batch []series.Point
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			err := e.writeRecord(exportRecord{Kind: exportKindTSPoints, Container: info.Name, Points: batch})
			batch = nil
			return err
		}
		err := ser.RangeEach(e.ctx, math.MinInt64, math.MaxInt64, func(p series.Point) error {
			batch = append(batch, p)
			if len(batch) >= exportBatch {
				return flush()
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("harbor: export series %q: %w", info.Name, err)
		}
		if err := flush(); err != nil {
			return err
		}
		// Points precede metadata so an import applies retention only after the
		// exported points are present.
		if len(info.Labels) > 0 {
			if err := e.writeRecord(exportRecord{Kind: exportKindTSLabels, Container: info.Name, Labels: info.Labels}); err != nil {
				return err
			}
		}
		if info.Retention.MaxAge != 0 || info.Retention.MaxPoints != 0 {
			if err := e.writeRecord(exportRecord{
				Kind:      exportKindTSRetention,
				Container: info.Name,
				MaxAge:    int64(info.Retention.MaxAge),
				MaxPoints: info.Retention.MaxPoints,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *exporter) exportVec() error {
	names, err := e.src.Vector().ListCollections()
	if err != nil {
		return fmt.Errorf("harbor: export vec: %w", err)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := e.ctx.Err(); err != nil {
			return err
		}
		meta, err := e.src.Vector().LoadCollectionMeta(name)
		if err != nil {
			return fmt.Errorf("harbor: export collection %q: %w", name, err)
		}
		if err := e.writeRecord(exportRecord{
			Kind:      exportKindVecCollection,
			Container: name,
			VectorLen: meta.VectorLen,
			Metric:    string(meta.Metric),
			UseHNSW:   meta.UseHNSW,
			HNSW:      &meta.HNSWParams,
		}); err != nil {
			return err
		}

		points, err := e.src.Vector().LoadCollection(name)
		if err != nil {
			return fmt.Errorf("harbor: export collection %q: %w", name, err)
		}
		ids := make([]string, 0, len(points))
		for id := range points {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		var batch []vector.PointStruct
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			raw, err := json.Marshal(batch)
			if err != nil {
				return fmt.Errorf("harbor: export collection %q: %w", name, err)
			}
			batch = nil
			return e.writeRecord(exportRecord{Kind: exportKindVecPoints, Container: name, Vectors: raw})
		}
		for _, id := range ids {
			if err := e.ctx.Err(); err != nil {
				return err
			}
			point := *points[id]
			point.Version = 0 // store-managed; the destination assigns its own
			batch = append(batch, point)
			if len(batch) >= exportBatch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if err := flush(); err != nil {
			return err
		}
	}
	return nil
}
