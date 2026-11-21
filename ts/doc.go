// Package ts provides a time series store backed by bbolt.
//
// A Store manages named series under an optional root bucket. Each series is an
// ordered sequence of timestamp/value points stored in fixed-width records, so
// bbolt key order is chronological order. Series labels are stored as JSON
// metadata in a reserved sub-bucket.
package ts
