module github.com/malivvan/storm/testdata/compatibility/roundtrip

go 1.24.0

replace github.com/malivvan/storm => ../../..

require (
	github.com/malivvan/storm v5.3.0
	github.com/malivvan/storm v6.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.12.1
)

require (
	go.etcd.io/bbolt v1.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sys v0.39.0 // indirect
)
