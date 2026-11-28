package tempest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/malivvan/tempest/store/graphs"
	"github.com/malivvan/tempest/store/record"
	"github.com/malivvan/tempest/store/search"
	"github.com/malivvan/tempest/store/vector"
	bolt "go.etcd.io/bbolt"
)

const linksBucketName = "__tempest_links__"

// LinkSpec describes how one entity id maps into a sub-store. The record
// variant carries a sample Type so the store can construct a keyed struct for
// DeleteStruct; the other stores carry a Name or Bucket.
type LinkSpec struct {
	// Store is "record", "vector", "search", "graphs", "object" or "fstree".
	Store string
	// Type is a sample struct for the record store (pointer or value).
	Type any `json:"-"`
	// IDField names the record primary-key field; empty selects "ID".
	IDField string
	// Name is the vector collection, search index, graph or fstree name.
	Name string
	// Bucket is the object bucket.
	Bucket string
	// Prefix is an optional fstree path prefix.
	Prefix string
}

type persistedLink struct {
	Store   string `json:"store"`
	IDField string `json:"id_field,omitempty"`
	Name    string `json:"name,omitempty"`
	Bucket  string `json:"bucket,omitempty"`
	Prefix  string `json:"prefix,omitempty"`
}

// ForgetOptions selects the links used by Forget. When Links is empty the
// store's registered links are used.
type ForgetOptions struct {
	Links []LinkSpec
}

// ForgetReport records what a Forget call removed.
type ForgetReport struct {
	// Deleted counts removed rows by "store:name".
	Deleted map[string]int
	// Missing lists links that held no row for the id.
	Missing []string
}

// OrphanRef is one secondary-store row with no matching record.
type OrphanRef struct {
	Store string
	Name  string
	ID    string
}

// OrphansOptions controls whether Orphans also removes what it finds.
type OrphansOptions struct {
	Remove bool
}

// OrphanReport lists rows that have no matching record link.
type OrphanReport struct {
	Orphans []OrphanRef
	// Removed is the number of orphans deleted when Remove was set.
	Removed int
}

// ErrInvalidLink reports an unusable LinkSpec.
var ErrInvalidLink = errors.New("tempest: invalid link specification")

// RegisterLink adds a link to the in-memory registry and persists its
// serialisable fields. The sample Type is not persisted; register record links
// again after reopening a store before calling Forget or Orphans.
func (s *Store) RegisterLink(ctx context.Context, spec LinkSpec) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.bs == nil {
		return ErrClosed
	}
	if err := validateLink(spec); err != nil {
		return err
	}
	s.linksMu.Lock()
	s.links = append(s.links, spec)
	s.linksMu.Unlock()

	raw, err := json.Marshal(persistedLink{
		Store:   spec.Store,
		IDField: spec.IDField,
		Name:    spec.Name,
		Bucket:  spec.Bucket,
		Prefix:  spec.Prefix,
	})
	if err != nil {
		return err
	}
	return s.bs.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(linksBucketName))
		if err != nil {
			return err
		}
		return b.Put([]byte(fmt.Sprintf("%d", time.Now().UnixNano())), raw)
	})
}

func validateLink(spec LinkSpec) error {
	switch spec.Store {
	case "record":
		if spec.Type == nil {
			return fmt.Errorf("%w: record link needs a Type", ErrInvalidLink)
		}
	case "vector", "search", "graphs", "fstree":
		if spec.Name == "" {
			return fmt.Errorf("%w: %s link needs a Name", ErrInvalidLink, spec.Store)
		}
	case "object":
		if spec.Bucket == "" {
			return fmt.Errorf("%w: object link needs a Bucket", ErrInvalidLink)
		}
	default:
		return fmt.Errorf("%w: unknown store %q", ErrInvalidLink, spec.Store)
	}
	return nil
}

// Forget deletes every linked row for id inside one cross-store transaction.
// Missing rows are reported, not treated as errors.
func (s *Store) Forget(ctx context.Context, id string, opts ForgetOptions) (*ForgetReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidLink)
	}
	if s == nil {
		return nil, ErrClosed
	}
	links := opts.Links
	if len(links) == 0 {
		s.linksMu.RLock()
		links = append([]LinkSpec(nil), s.links...)
		s.linksMu.RUnlock()
	}
	if len(links) == 0 {
		return &ForgetReport{Deleted: map[string]int{}}, nil
	}
	for _, link := range links {
		if err := validateLink(link); err != nil {
			return nil, err
		}
	}

	report := &ForgetReport{Deleted: map[string]int{}}
	err := s.WriteTransaction(ctx, func(tx *Tx) error {
		for _, link := range links {
			key := link.Store
			if link.Name != "" {
				key += ":" + link.Name
			} else if link.Bucket != "" {
				key += ":" + link.Bucket
			}
			removed, err := forgetLink(ctx, tx, link, id)
			if err != nil {
				return err
			}
			if removed {
				report.Deleted[key]++
			} else {
				report.Missing = append(report.Missing, key)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Refresh the live vector index and fstree inode cache after the bound
	// transaction commits; their in-memory state is not the transaction view.
	for _, link := range links {
		switch link.Store {
		case "vector":
			if col, err := s.Vector().Collection(link.Name); err == nil {
				_, _ = col.Delete([]string{id}, nil)
			}
		case "fstree":
			if fsys, err := s.Fstree().Open(link.Name); err == nil {
				target := id
				if !strings.HasPrefix(target, "/") {
					target = "/" + target
				}
				if link.Prefix != "" {
					target = path.Join(link.Prefix, target)
				}
				_ = fsys.RemoveAll(target)
			}
		}
	}
	return report, nil
}

func forgetLink(ctx context.Context, tx *Tx, link LinkSpec, id string) (bool, error) {
	switch link.Store {
	case "record":
		sample, err := recordSample(link, id)
		if err != nil {
			return false, err
		}
		if err := tx.Record().DeleteStruct(ctx, sample); err != nil {
			if errors.Is(err, record.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		if _, err := tx.Record().PurgeHistory(ctx, sample); err != nil && !errors.Is(err, record.ErrHistoryDisabled) {
			return false, err
		}
		return true, nil
	case "vector":
		if _, err := tx.Vector().GetPoint(link.Name, id); err != nil {
			return false, nil
		}
		if err := tx.Vector().DeletePoints(link.Name, []string{id}); err != nil {
			return false, err
		}
		return true, nil
	case "search":
		idx, err := tx.Search().Open(link.Name, search.Config{})
		if err != nil {
			return false, nil
		}
		if err := idx.Delete(ctx, id); err != nil {
			return false, err
		}
		return true, nil
	case "graphs":
		g, err := tx.Graphs().Open(link.Name)
		if err != nil {
			return false, nil
		}
		removed := false
		if err := g.PurgeNode(ctx, id); err == nil {
			removed = true
		} else if !errors.Is(err, graphs.ErrNodeNotFound) {
			return false, err
		}
		if err := g.PurgeEdge(ctx, id); err == nil {
			removed = true
		} else if !errors.Is(err, graphs.ErrEdgeNotFound) {
			return false, err
		}
		return removed, nil
	case "object":
		if err := tx.Objects().Delete(ctx, link.Bucket, id); err != nil {
			return false, nil
		}
		return true, nil
	case "fstree":
		fsys, err := tx.Fstree().Open(link.Name)
		if err != nil {
			return false, nil
		}
		target := id
		if !strings.HasPrefix(target, "/") {
			target = "/" + target
		}
		if link.Prefix != "" {
			target = path.Join(link.Prefix, target)
		}
		if err := fsys.RemoveAll(target); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, fmt.Errorf("%w: unknown store %q", ErrInvalidLink, link.Store)
	}
}

func recordSample(link LinkSpec, id string) (any, error) {
	typ := reflect.TypeOf(link.Type)
	if typ == nil {
		return nil, fmt.Errorf("%w: record link has no Type", ErrInvalidLink)
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: record Type must be a struct", ErrInvalidLink)
	}
	ptr := reflect.New(typ)
	fieldName := link.IDField
	if fieldName == "" {
		fieldName = "ID"
	}
	field := ptr.Elem().FieldByName(fieldName)
	if !field.IsValid() || !field.CanSet() {
		return nil, fmt.Errorf("%w: %s has no settable field %q", ErrInvalidLink, typ.Name(), fieldName)
	}
	switch field.Kind() {
	case reflect.String:
		field.SetString(id)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			return nil, err
		}
		field.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return nil, err
		}
		field.SetUint(n)
	default:
		return nil, fmt.Errorf("%w: unsupported id field kind %s", ErrInvalidLink, field.Kind())
	}
	return ptr.Interface(), nil
}

// Orphans reports secondary-store rows whose id has no live record in the
// first registered record link. It is read-only; call Forget to remove them.
func (s *Store) Orphans(ctx context.Context, options ...OrphansOptions) (*OrphanReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil {
		return nil, ErrClosed
	}
	s.linksMu.RLock()
	links := append([]LinkSpec(nil), s.links...)
	s.linksMu.RUnlock()

	var recordLink *LinkSpec
	for i := range links {
		if links[i].Store == "record" {
			recordLink = &links[i]
			break
		}
	}
	if recordLink == nil {
		return nil, fmt.Errorf("%w: Orphans needs a record link", ErrInvalidLink)
	}

	report := &OrphanReport{}
	err := s.ReadTransaction(ctx, func(tx *Tx) error {
		known, err := recordIDs(ctx, tx, *recordLink)
		if err != nil {
			return err
		}
		for _, link := range links {
			if link.Store == "record" {
				continue
			}
			ids, err := linkIDs(ctx, tx, link)
			if err != nil {
				return err
			}
			for _, id := range ids {
				if !known[id] {
					report.Orphans = append(report.Orphans, OrphanRef{Store: link.Store, Name: link.Name, ID: id})
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var opts OrphansOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.Remove {
		for _, orphan := range report.Orphans {
			if _, err := s.Forget(ctx, orphan.ID, ForgetOptions{Links: links}); err != nil {
				return report, err
			}
			report.Removed++
		}
	}
	return report, nil
}

func recordIDs(ctx context.Context, tx *Tx, link LinkSpec) (map[string]bool, error) {
	typ := reflect.TypeOf(link.Type)
	if typ == nil {
		return nil, fmt.Errorf("%w: record link has no Type", ErrInvalidLink)
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	slice := reflect.New(reflect.SliceOf(typ))
	if err := tx.Record().All(ctx, slice.Interface()); err != nil && !errors.Is(err, record.ErrNotFound) {
		return nil, err
	}
	fieldName := link.IDField
	if fieldName == "" {
		fieldName = "ID"
	}
	known := map[string]bool{}
	for i := 0; i < slice.Elem().Len(); i++ {
		field := slice.Elem().Index(i).FieldByName(fieldName)
		if field.IsValid() {
			known[fmt.Sprint(field.Interface())] = true
		}
	}
	return known, nil
}

func linkIDs(ctx context.Context, tx *Tx, link LinkSpec) ([]string, error) {
	switch link.Store {
	case "vector":
		col, err := tx.Vector().Collection(link.Name)
		if err != nil {
			return nil, nil
		}
		var ids []string
		_ = col.ForEachPoint(nil, func(point vector.PointStruct) error {
			ids = append(ids, point.ID)
			return nil
		})
		return ids, nil
	case "search":
		idx, err := tx.Search().Open(link.Name, search.Config{})
		if err != nil {
			return nil, nil
		}
		docs, err := idx.Docs(ctx)
		if err != nil {
			return nil, err
		}
		ids := make([]string, 0, len(docs))
		for _, doc := range docs {
			ids = append(ids, doc.ID)
		}
		return ids, nil
	case "graphs":
		g, err := tx.Graphs().Open(link.Name)
		if err != nil {
			return nil, nil
		}
		nodes, err := g.GetAllNodes(ctx, nil)
		if err != nil {
			return nil, err
		}
		ids := make([]string, 0, len(nodes))
		for _, node := range nodes {
			ids = append(ids, node.ID)
		}
		return ids, nil
	case "object":
		infos, err := tx.Objects().List(ctx, link.Bucket, "")
		if err != nil {
			return nil, err
		}
		ids := make([]string, 0, len(infos))
		for _, info := range infos {
			ids = append(ids, info.Key)
		}
		return ids, nil
	default:
		return nil, nil
	}
}
