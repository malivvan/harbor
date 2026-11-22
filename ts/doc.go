// Package ts provides a persistent time-series store backed by bbolt.
//
// A [Store] owns named [Series]. Each series stores timestamped [Point] values
// in chronological key order, so range scans and aggregations do not need an
// in-memory index. Series also carry labels and an optional [Retention] policy
// that is enforced on insert and can be applied explicitly with
// [Series.EnforceRetention] or [Store.EnforceRetention].
//
// A zero [Config] creates a plaintext store. Setting Config.Secret together
// with one of the encryption flags enables AEAD encryption; Config.BlockDuration
// groups points into sealed blocks for high-volume encrypted series. The store
// borrows the caller's *bolt.DB and never closes it.
package ts
