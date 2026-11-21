package storm

import "github.com/malivvan/storm/index"

// FindOption customizes a finder query.
type FindOption func(*index.Options)

// Limit sets the maximum number of records to return
func Limit(limit int) FindOption {
	return func(opts *index.Options) {
		opts.Limit = limit
	}
}

// Skip sets the number of records to skip
func Skip(offset int) FindOption {
	return func(opts *index.Options) {
		opts.Skip = offset
	}
}

// Reverse will return the results in descending order
func Reverse() FindOption {
	return func(opts *index.Options) {
		opts.Reverse = true
	}
}
