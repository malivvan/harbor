# store

`store` manages the process-wide Harbor database: one directory, one password file,
one open handle.

A Harbor deployment normally has exactly one database and one password, and every
subsystem in the process should use the same handle rather than opening the file
again. This package owns that convention.

```go
import (
    "context"

    "github.com/malivvan/harbor/store"
)

func main() {
    ctx := context.Background()

    st, err := store.Open(ctx, store.Config{Dir: "/var/lib/myapp"})
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    _ = st.DB()
    _ = st.FS()
    _ = st.Vec()
}
```

## Password file

When `Config.Password` is empty, the password is 32 random bytes, hex encoded
(64 characters) and stored at `<Dir>/harbor.key` with mode `0600`. The file is
created with `O_EXCL`, so two processes racing to initialise the same directory
agree on one key instead of overwriting each other. The value is never logged.

Losing the key file loses the data: back it up with the database.

Set `Config.Password` to supply the password from somewhere else (a secret
manager, an environment variable). No key file is written in that case.

## Reopening when the directory changes

`Manager.Open` compares the configured directory with the one the current store
belongs to. If they differ, that store is closed and the new directory's database
is opened. In a server the directory is resolved once, so this never fires; it
exists so a test harness can point each test at a fresh directory and get a
genuinely fresh database instead of a stale handle.

## Two shapes

| Use | API |
|---|---|
| One default store for the process | `store.Open(ctx, cfg)`, `store.Store()`, `store.DB()`, `store.FS()`, `store.Vec()`, `store.Close()` |
| Independent stores (tests, multiple tenants in one process) | `store.NewManager(cfg)` then `Manager.Open` and the same accessors |

## Config

| Field | Default | Meaning |
|---|---|---|
| `Dir` | — (required) | Directory holding the database and password files |
| `DBFile` | `harbor.db` | Database file name |
| `KeyFile` | `harbor.key` | Password file name |
| `Mode` | `0600` | Permission for both files |
| `Password` | read/create `KeyFile` | Explicit password |
| `Options` | — | Extra `harbor.Options` (KDF cost, encryption flags, metrics) |

`Config.Password` always wins over `Options.Password`; the manager fills that in.
