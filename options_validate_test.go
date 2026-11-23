package harbor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/malivvan/harbor/store/system"
)

// TestOptionsValidate covers the creation-time option checks exposed by
// Options.Validate and enforced by Open.
func TestOptionsValidate(t *testing.T) {
	var nilOptions *Options
	require.ErrorIs(t, nilOptions.Validate(), ErrInvalidOptions)

	require.ErrorIs(t, (&Options{}).Validate(), ErrInvalidPassword)

	valid := testOptions("pw")
	require.NoError(t, valid.Validate())

	partial := testOptions("pw")
	partial.KDFThreads = 0
	require.ErrorIs(t, partial.Validate(), ErrInvalidOptions)

	conflict := testOptions("pw")
	conflict.Plaintext = true
	conflict.Encrypt = EncryptAll
	require.ErrorIs(t, conflict.Validate(), ErrInvalidOptions)

	unknownEnc := testOptions("pw")
	unknownEnc.Encrypt = 1 << 15
	require.ErrorIs(t, unknownEnc.Validate(), ErrInvalidOptions)

	unknownAudit := testOptions("pw")
	unknownAudit.Audit = 1 << 15
	require.ErrorIs(t, unknownAudit.Validate(), ErrInvalidOptions)

	beyondLimits := testOptions("pw")
	beyondLimits.KDFTime = system.MaxKDFTime + 1
	require.ErrorIs(t, beyondLimits.Validate(), ErrInvalidOptions)

	plaintext := testOptions("pw")
	plaintext.Plaintext = true
	require.NoError(t, plaintext.Validate())
}

func TestOpenRejectsUnknownAuditFlags(t *testing.T) {
	opts := testOptions("pw")
	opts.Audit = 1 << 15
	_, err := Open(context.Background(), filepath.Join(t.TempDir(), "harbor.db"), 0o600, opts)
	require.ErrorIs(t, err, ErrInvalidOptions)
}

// TestOptionsValidateMatchesOpen ties the public validation to Open for a valid
// and an invalid configuration.
func TestOptionsValidateMatchesOpen(t *testing.T) {
	dir := t.TempDir()

	valid := testOptions("pw")
	require.NoError(t, valid.Validate())
	store, err := Open(context.Background(), filepath.Join(dir, "valid.db"), 0o600, valid)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	invalid := testOptions("pw")
	invalid.KDFTime = system.MaxKDFTime + 1
	require.ErrorIs(t, invalid.Validate(), ErrInvalidOptions)
	_, err = Open(context.Background(), filepath.Join(dir, "invalid.db"), 0o600, invalid)
	require.ErrorIs(t, err, ErrInvalidOptions)
}
