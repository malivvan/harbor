# Dependency audit

**Module:** `github.com/malivvan/storm`
**Public API package:** `github.com/malivvan/storm/db`
**Audit date:** 2026-11-21
**Go directive:** `go 1.24.0`

## Direct dependencies

| Module | Version | Usage | Notes |
|--------|---------|-------|-------|
| `go.etcd.io/bbolt` | `v1.4.3` | Storage engine | Required runtime dependency |
| `github.com/stretchr/testify` | `v1.12.1` | Test-only | Used only from `_test.go` files |

## Indirect dependencies

| Module | Version | Reason |
|--------|---------|--------|
| `go.yaml.in/yaml/v3` | `v3.0.5` | Test dependency graph |
| `golang.org/x/sys` | `v0.39.0` | Transitive through bbolt and tests |

## Codecs

- JSON: standard library.
- Gob: standard library.
- AES: standard library (`crypto/aes`, `crypto/cipher`).

The module has no third-party codec dependencies.

## Nested modules and fixtures

- The module has no nested Go modules.
- All compatibility fixtures previously used for old-version testing have been
  removed. There is no legacy on-disk compatibility surface.

## Validation

```sh
gofmt -l .
go build ./...
go vet ./...
go mod verify
go test -count=1 -timeout 180s ./...
go test -race -count=1 -timeout 300s ./...
```

No vulnerability scan is claimed. Run `govulncheck` in CI or locally when
available before making a security claim.
