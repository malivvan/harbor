# Contributing

Thanks for helping improve Harbor.

## Development

Requirements: Go 1.27.4 or newer.

```sh
go build ./...
go test ./...
gofmt -l .
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...
```

CI runs the same commands, plus the race detector and the benchmarks.

## Guidelines

- Keep changes focused and add a regression test for every bug fix.
- Public API changes need a short rationale in the commit message and a
  documentation update.
- On-disk format changes need a version bump and a compatibility note: the
  project refuses unknown format versions.
- Do not reformat unrelated files. `gofmt` and `go vet` must be clean.
- Commit messages follow `area: imperative summary` (for example
  `fix(ts): keep counts consistent after DeleteRange`).

## Security

Do not report vulnerabilities in public issues; see [SECURITY.md](SECURITY.md).
