# Post-implementation tasks

## Status

Per-series retention for the `ts` time series store is implemented, tested, and
documented.

## Remaining required work

None.

## Verification completed

```sh
gofmt -l ts                                             # clean
go vet ./...                                            # pass
go build ./...                                          # pass
go test -count=1 -timeout 180s ./...                    # pass
go test -race -count=1 -timeout 300s ./ts/...           # pass
```

## Commits

- `8888c06` — `feat(ts): add per-series retention`
- Documentation commit containing this file — README, CHANGELOG, and PLAN.md

## Optional follow-ups

- Add a background retention scheduler that periodically calls
  `Store.EnforceRetention`.
- Expose retention metrics (points pruned, policy, last enforcement time).
- Consider a store-level default policy if applications need uniform retention.
- Add fuzz coverage for malformed series metadata JSON.
- Consider an atomic label+retention metadata update API if callers need to
  change both under one transaction without two round trips.
