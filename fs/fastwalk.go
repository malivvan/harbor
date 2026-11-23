package fs

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/malivvan/harbor/internal/absfs"
	"github.com/malivvan/harbor/metrics"
)

// FastWalkFunc is the callback function type for FastWalk.
// It receives fs.DirEntry instead of os.FileInfo for better performance
// (avoids stat calls when only type information is needed).
type FastWalkFunc func(path string, d fs.DirEntry, err error) error

// ErrTraverseLink is a sentinel error that can be returned from FastWalkFunc
// to indicate that a symlink should be traversed as a directory.
// The caller is responsible for cycle detection.
var ErrTraverseLink = errors.New("traverse symlink, assuming target is a directory")

// FastWalkConfig holds configuration options for FastWalk.
type FastWalkConfig struct {
	// NumWorkers sets the number of concurrent workers.
	// If 0 or negative, defaults to max(4, runtime.NumCPU()).
	NumWorkers int

	// QueueSize bounds how many directories may be buffered for traversal and
	// how many work items may be in flight. A smaller value applies
	// backpressure sooner; a larger value lets a fast backend keep more work
	// queued. Zero selects NumWorkers (after the default is applied).
	QueueSize int

	// FollowSymlinks determines whether symlinks should be followed.
	// When true, symlinks to directories will be traversed.
	// The caller must handle potential cycles.
	FollowSymlinks bool

	// Ctx cancels the traversal. When it is cancelled FastWalk stops scheduling
	// new directories and returns Ctx.Err(). A callback that is already running
	// is not interrupted; cancellation is observed at the next boundary. A nil
	// Ctx means context.Background().
	Ctx context.Context

	// Metrics, when non-nil, receives a queue-depth gauge and a callback-latency
	// histogram. A nil observer disables instrumentation.
	Metrics metrics.Observer
}

// FastWalk walks the file tree rooted at root concurrently, calling fn for each file.
// It is significantly faster than Walk because:
//   - Runs multiple goroutines concurrently to traverse directories in parallel
//   - Avoids unnecessary stat calls by using DirEntry type information
//
// When the underlying filesystem is osfs, the platform-optimized ReadDir
// implementation is used automatically (e.g., getdents with d_type on Linux).
//
// The fn must be safe for concurrent use as it may be called from multiple goroutines.
// If fn returns filepath.SkipDir, the directory is skipped.
// If fn returns ErrTraverseLink for a symlink, it will be traversed as a directory.
func FastWalk(filer absfs.Filer, root string, fn FastWalkFunc) error {
	return FastWalkWithConfig(filer, root, nil, fn)
}

// FastWalkWithConfig is like FastWalk but accepts a configuration.
func FastWalkWithConfig(filer absfs.Filer, root string, config *FastWalkConfig, fn FastWalkFunc) error {
	return genericFastWalk(filer, root, config, fn)
}

// FastWalkContext is FastWalk with a context that cancels the traversal. It is
// a convenience wrapper around FastWalkWithConfig.
func FastWalkContext(ctx context.Context, filer absfs.Filer, root string, fn FastWalkFunc) error {
	return FastWalkWithConfig(filer, root, &FastWalkConfig{Ctx: ctx}, fn)
}

// genericFastWalk implements concurrent directory walking for any absfs.Filer.
func genericFastWalk(filer absfs.Filer, root string, config *FastWalkConfig, fn FastWalkFunc) error {
	numWorkers := 4
	if config != nil && config.NumWorkers > 0 {
		numWorkers = config.NumWorkers
	} else if n := runtime.NumCPU(); n > numWorkers {
		numWorkers = n
	}

	// QueueSize bounds both channels, so a slow callback applies backpressure
	// to directory reads instead of letting the queue grow without bound.
	queueSize := numWorkers
	if config != nil && config.QueueSize > 0 {
		queueSize = config.QueueSize
	}

	ctx := context.Background()
	var obs metrics.Observer
	if config != nil {
		if config.Ctx != nil {
			ctx = config.Ctx
		}
		obs = config.Metrics
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Wait for all workers to finish
	var wg sync.WaitGroup
	defer wg.Wait()

	w := &genericWalker{
		filer:          filer,
		fn:             fn,
		followSymlinks: config != nil && config.FollowSymlinks,
		ctx:            ctx,
		obs:            obs,
		enqueuec:       make(chan fastWalkItem, queueSize),
		workc:          make(chan fastWalkItem, queueSize),
		donec:          make(chan struct{}),
		resc:           make(chan error, numWorkers),
	}
	defer close(w.donec)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go w.doWork(&wg)
	}

	todo := []fastWalkItem{{dir: root}}
	out := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		workc := w.workc
		var workItem fastWalkItem
		if len(todo) == 0 {
			workc = nil
		} else {
			workItem = todo[len(todo)-1]
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case workc <- workItem:
			todo = todo[:len(todo)-1]
			out++
		case it := <-w.enqueuec:
			todo = append(todo, it)
		case err := <-w.resc:
			out--
			if err != nil {
				return err
			}
			if out == 0 && len(todo) == 0 {
				// Safe to quit, but check for race with enqueue
				select {
				case it := <-w.enqueuec:
					todo = append(todo, it)
				default:
					return nil
				}
			}
		}
	}
}

type genericWalker struct {
	filer          absfs.Filer
	fn             FastWalkFunc
	followSymlinks bool
	ctx            context.Context
	obs            metrics.Observer
	donec          chan struct{}
	workc          chan fastWalkItem
	enqueuec       chan fastWalkItem
	resc           chan error
}

type fastWalkItem struct {
	dir          string
	callbackDone bool
}

func (w *genericWalker) enqueue(it fastWalkItem) {
	select {
	case w.enqueuec <- it:
	case <-w.donec:
	case <-w.ctx.Done():
	}
	if w.obs != nil {
		metrics.Gauge(w.obs, metrics.MetricWalkQueueDepth, float64(len(w.enqueuec)), metrics.Labels{
			metrics.LabelStore: "fs",
			metrics.LabelOp:    "walk",
		})
	}
}

// call invokes the user callback, recording its latency when an observer is
// configured.
func (w *genericWalker) call(path string, d fs.DirEntry, err error) error {
	if w.obs == nil {
		return w.fn(path, d, err)
	}
	start := time.Now()
	cbErr := w.fn(path, d, err)
	metrics.Histogram(w.obs, metrics.MetricWalkCallbackDuration, float64(time.Since(start)), metrics.Labels{
		metrics.LabelStore: "fs",
		metrics.LabelOp:    "walk",
	})
	return cbErr
}

func (w *genericWalker) doWork(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-w.donec:
			return
		case <-w.ctx.Done():
			return
		case it := <-w.workc:
			select {
			case <-w.donec:
				return
			case <-w.ctx.Done():
				return
			case w.resc <- w.walk(it.dir, !it.callbackDone):
			}
		}
	}
}

func (w *genericWalker) walk(root string, runUserCallback bool) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}

	// Check if root is a file or directory
	info, err := w.filer.Stat(root)
	if err != nil {
		return w.call(root, nil, err)
	}

	if runUserCallback {
		err := w.call(root, &fakeDirEntry{name: filepath.Base(root), isDir: info.IsDir(), info: info}, nil)
		if err == filepath.SkipDir {
			return nil
		}
		if err != nil {
			return err
		}
	}

	// If it's a file, we're done
	if !info.IsDir() {
		return nil
	}

	return w.readDir(root)
}

func (w *genericWalker) readDir(dirName string) error {
	// Use Filer.ReadDir for optimized directory reading
	entries, err := w.filer.ReadDir(dirName)
	if err != nil {
		// Handle permission errors gracefully
		if isPermissionError(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if err := w.ctx.Err(); err != nil {
			return err
		}

		// Skip . and ..
		if entry.Name() == "." || entry.Name() == ".." {
			continue
		}

		// Join path correctly (avoid "//file" when dirName is "/")
		var joined string
		if dirName == "/" {
			joined = "/" + entry.Name()
		} else {
			joined = dirName + "/" + entry.Name()
		}

		if entry.IsDir() {
			// Call callback for directory first
			err := w.call(joined, entry, nil)
			if err == filepath.SkipDir {
				continue
			}
			if err != nil {
				return err
			}
			// Enqueue directory for traversal
			w.enqueue(fastWalkItem{dir: joined, callbackDone: true})
			continue
		}

		err := w.call(joined, entry, nil)
		if entry.Type()&fs.ModeSymlink != 0 {
			if err == filepath.SkipDir {
				continue
			}
			if err == ErrTraverseLink || w.followSymlinks {
				w.enqueue(fastWalkItem{dir: joined, callbackDone: true})
				continue
			}
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// isPermissionError checks if an error is a permission denied error
func isPermissionError(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "permission denied" || err.Error() == "access is denied"
}

// fakeDirEntry is a minimal implementation of fs.DirEntry for directory callbacks
type fakeDirEntry struct {
	name  string
	isDir bool
	info  fs.FileInfo // Optional: full FileInfo if available
}

func (f *fakeDirEntry) Name() string { return f.name }
func (f *fakeDirEntry) IsDir() bool  { return f.isDir }
func (f *fakeDirEntry) Type() fs.FileMode {
	if f.isDir {
		return fs.ModeDir
	}
	return 0
}
func (f *fakeDirEntry) Info() (fs.FileInfo, error) {
	if f.info != nil {
		return f.info, nil
	}
	return nil, errors.New("FileInfo not available")
}
