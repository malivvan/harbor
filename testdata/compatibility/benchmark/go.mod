module github.com/malivvan/storm/testdata/compatibility/benchmark

go 1.24.0

replace github.com/malivvan/storm => ../../..

require (
	github.com/malivvan/storm v5.3.0
	github.com/malivvan/storm v6.0.0-00010101000000-000000000000
	go.etcd.io/bbolt v1.4.3
)

require golang.org/x/sys v0.39.0 // indirect
