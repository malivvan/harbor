package record

import (
	"errors"
	"fmt"

	"github.com/malivvan/harbor/internal/crypto"
	"github.com/malivvan/harbor/store/record/index"
)

// Errors
var (
	// ErrNoID is returned when no ID field or id tag is found in the struct.
	ErrNoID = errors.New("missing struct tag id or ID field")

	// ErrZeroID is returned when the ID field is a zero value.
	ErrZeroID = errors.New("id field must not be a zero value")

	// ErrBadType is returned when a method receives an unexpected value type.
	ErrBadType = errors.New("provided data must be a struct or a pointer to struct")

	// ErrAlreadyExists is returned when trying to set an existing value on a field that has a unique index.
	ErrAlreadyExists = index.ErrAlreadyExists

	// ErrNilParam is returned when the specified param is expected to be not nil.
	ErrNilParam = index.ErrNilParam

	// ErrNilContext is returned when a nil context.Context is passed to an operation that requires a context.
	ErrNilContext = index.ErrNilContext

	// ErrUnknownTag is returned when an unexpected tag is specified.
	ErrUnknownTag = errors.New("unknown tag")

	// ErrIdxNotFound is returned when the specified index is not found.
	ErrIdxNotFound = errors.New("index not found")

	// ErrSlicePtrNeeded is returned when an unexpected value is given, instead of a pointer to slice.
	ErrSlicePtrNeeded = errors.New("provided target must be a pointer to slice")

	// ErrStructPtrNeeded is returned when an unexpected value is given, instead of a pointer to struct.
	ErrStructPtrNeeded = errors.New("provided target must be a pointer to struct")

	// ErrPtrNeeded is returned when an unexpected value is given, instead of a pointer.
	ErrPtrNeeded = errors.New("provided target must be a pointer to a valid variable")

	// ErrNoName is returned when the specified struct has no name.
	ErrNoName = errors.New("provided target must have a name")

	// ErrNotFound is returned when the specified record is not saved in the bucket.
	ErrNotFound = index.ErrNotFound

	// ErrIncompatibleValue is returned when trying to set a value with a different type than the chosen field
	ErrIncompatibleValue = errors.New("incompatible value")

	// ErrImmutableID is returned when UpdateField targets the primary key.
	// Changing a primary key would leave the old record and its index entries
	// behind; delete and re-save instead.
	ErrImmutableID = errors.New("the primary key cannot be updated")

	// ErrSampleRequired is returned by Raw/RawEach/Iter when a nil sample is
	// used together with matchers or ordering, because the query cannot decode
	// the records needed to evaluate them.
	ErrSampleRequired = errors.New("a sample record is required for matchers or ordering")

	// ErrStreamOrdered is returned by Iter when the query has an OrderBy
	// clause. Ordered results require materializing the whole result set, so
	// they cannot be streamed.
	ErrStreamOrdered = errors.New("streaming queries do not support OrderBy")

	// ErrDifferentCodec is returned when using a codec different than the first codec used with the bucket.
	ErrDifferentCodec = errors.New("the selected codec is incompatible with this bucket")

	// ErrIncompatibleVersion is returned when the on-disk format version is
	// not the one this build understands. A first release refuses unknown
	// (including newer) versions instead of misinterpreting their data.
	ErrIncompatibleVersion = errors.New("incompatible on-disk format version")

	// ErrTxFromDifferentDB is returned when Attach receives a bbolt transaction
	// that belongs to a different database.
	ErrTxFromDifferentDB = errors.New("transaction belongs to a different database")

	// ErrAuthFailed is returned when encrypted data fails authentication.
	ErrAuthFailed = crypto.ErrAuthFailed

	// ErrInvalidKey is returned for a missing or malformed encryption key.
	ErrInvalidKey = crypto.ErrInvalidKey

	// ErrInvalidConfig is returned for an inconsistent encryption config.
	ErrInvalidConfig = crypto.ErrInvalidConfig

	// ErrWrongMode is returned when a store is opened with an encryption mode
	// that does not match what is stored on disk.
	ErrWrongMode = crypto.ErrWrongMode

	// ErrProjectionUnsupported reports Query.Project combined with matchers or
	// ordering, which need fields the projection may remove.
	ErrProjectionUnsupported = errors.New("db: projection is not supported with matchers or ordering")

	// ErrDanglingReference reports a save whose reference field names a record
	// that does not exist.
	ErrDanglingReference = errors.New("db: referenced record does not exist")

	// ErrReferenceViolation reports a delete or drop blocked by a restrict
	// reference.
	ErrReferenceViolation = errors.New("db: referenced record is still in use")

	// ErrReferenceTargetMissing reports a reference to a bucket that does not
	// exist.
	ErrReferenceTargetMissing = errors.New("db: reference target bucket does not exist")

	// ErrReferenceCycle reports a cascade deeper than the guard allows.
	ErrReferenceCycle = errors.New("db: reference cascade is too deep")

	// ErrEncryptedOrdering is returned by ordered or prefix lookups on a store
	// that encrypts keys: ciphertext ordering is meaningless, so these queries
	// fail loud instead of silently returning wrong results.
	ErrEncryptedOrdering = crypto.ErrEncryptedOrdering
)

// wrapError adds operation context to an error while preserving classification.
// It returns nil when err is nil.
func wrapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("harbor %s: %w", operation, err)
}
