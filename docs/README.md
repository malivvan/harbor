# Harbor documentation

Harbor is an encrypted, bbolt-backed storage toolkit with record, file,
time-series and vector stores. Start with the [project README](../README.md)
for the pitch and a five-minute example, then pick the guide that matches what
you are building.

## Start here

1. [Project README](../README.md) — features, install, quick start, package map.
2. [Getting started](getting-started.md) — open a store and use all four
   engines: records, files, time series and vectors.
3. [Encryption](encryption.md) — key derivation, flags, rotation, threat model.
4. [Architecture](architecture.md) — package map, on-disk layouts, concurrency,
   compatibility.

## Package guides

| Guide | Covers |
|-------|--------|
| [db/README.md](../db/README.md) | Models and tags, CRUD, finders, matcher queries, transactions, `Attach`, key/value access, codecs, encryption. |
| [fs/README.md](../fs/README.md) | Named file systems, `absfs` compatibility, symlinks, walking, snapshots, external content, chunked encryption. |
| [ts/README.md](../ts/README.md) | Series, inserts, ordered ranges, aggregation, labels, retention, encrypted blocks. |
| [vec/README.md](../vec/README.md) | Collections, Flat/HNSW indexes, filters, quantization, buffered writes, encryption. |
| [fs/s3fs/README.md](../fs/s3fs/README.md) | S3-compatible `absfs.Filer`, configuration, credentials, limitations. |

## Choose a reading path

- **Application developer using one database file:** [getting-started.md](getting-started.md),
  then the guides for the stores you use.
- **Records, indexes and transactions:** [db/README.md](../db/README.md).
- **Files, snapshots or external content:** [fs/README.md](../fs/README.md) and
  [fs/s3fs/README.md](../fs/s3fs/README.md).
- **Metrics and time series:** [ts/README.md](../ts/README.md).
- **Embeddings and similarity search:** [vec/README.md](../vec/README.md).
- **Security review or operations:** [encryption.md](encryption.md) and
  [architecture.md](architecture.md).

## API reference

Generated documentation is published on pkg.go.dev:

- [github.com/malivvan/harbor](https://pkg.go.dev/github.com/malivvan/harbor)
- [github.com/malivvan/harbor/db](https://pkg.go.dev/github.com/malivvan/harbor/db)
- [github.com/malivvan/harbor/fs](https://pkg.go.dev/github.com/malivvan/harbor/fs)
- [github.com/malivvan/harbor/ts](https://pkg.go.dev/github.com/malivvan/harbor/ts)
- [github.com/malivvan/harbor/vec](https://pkg.go.dev/github.com/malivvan/harbor/vec)

Every public package has a package-level overview in its doc comment, and every
exported identifier is documented. Compile-checked examples live next to the
code where they are most useful (for the `db` package, see
`db/examples_test.go`).

## Development

```sh
test -z "$(gofmt -l .)"
go vet ./...
go build ./...
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
```

CI runs these checks on every push to `master` and every pull request. See
[.github/workflows/ci.yml](../.github/workflows/ci.yml).

## Documentation conventions

- Code examples are written against the current public API; package guides are
  the source of truth for usage, and doc comments are the source of truth for
  behavior.
- Encryption claims are scoped: "protects data at rest" is not the same as
  "protects a running process". See [encryption.md](encryption.md) for the
  threat model.
- When behavior changes, update the package guide and the affected code example
  in the same change.
