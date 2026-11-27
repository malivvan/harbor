# s3fs — an S3-backed `absfs.Filer`

[![Go Reference](https://pkg.go.dev/badge/github.com/malivvan/tempest/store/fstree/s3fs.svg)](https://pkg.go.dev/github.com/malivvan/tempest/store/fstree/s3fs)
[![CI](https://github.com/malivvan/tempest/actions/workflows/ci.yml/badge.svg)](https://github.com/malivvan/tempest/actions/workflows/ci.yml)

`s3fs` implements the module's `absfs.Filer` interface on top of
S3-compatible object storage. It signs requests locally with AWS Signature
Version 4, so it has no third-party dependencies: `net/http` is the only
transport.

`FileSystem` implements the module's `absfs.Filer` interface. It does **not**
implement the full `absfs.FileSystem` interface (working directory and
whole-tree operations are absent), so it cannot be passed directly to
`file.Store.OpenWithContentFS`, which requires a `FileSystem`. Using S3 as an
external content backend requires an in-module adapter that wraps this `Filer`
with `absfs.ExtendFiler`.

## Configuration

```go
type Config struct {
	Bucket       string       // required
	Region       string       // default: AWS_REGION, then us-east-1
	Endpoint     string       // e.g. "http://localhost:9000"
	UsePathStyle bool         // force path-style addressing for the AWS endpoint
	AccessKey    string       // static credentials; falls back to the environment
	SecretKey    string
	SessionToken string
	HTTPClient   *http.Client // default: http.DefaultClient
	PartSize     int64        // multipart part size; default: s3fs.DefaultPartSize
}
```

`New` returns an error when `Bucket` is empty. `s3fs.DefaultPartSize` is 5 MiB,
the minimum S3 part size for all parts except the last.

Credentials are resolved in this order:

1. `Config.AccessKey` / `Config.SecretKey` (plus optional `SessionToken`).
2. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.
3. The shared credentials file (`~/.aws/credentials`, or
   `AWS_SHARED_CREDENTIALS_FILE`) using `AWS_PROFILE`.

The region comes from `Config.Region`, then `AWS_REGION` /
`AWS_DEFAULT_REGION`, then the shared config file, then `us-east-1`.

Custom endpoints (MinIO, LocalStack, R2, ...) use path-style addressing
automatically. Set `UsePathStyle` to force it for the default AWS endpoint.

## Usage

`FileSystem` implements `absfs.Filer`; use `OpenFile` rather than `Create`:

```go
package main

import (
	"log"
	"os"

	"github.com/malivvan/tempest/store/fstree/s3fs"
)

func main() {
	vfs, err := s3fs.New(&s3fs.Config{
		Bucket: "my-bucket",
		Region: "us-east-1",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Write a file. Content is buffered in memory and uploaded on Close.
	f, err := vfs.OpenFile("path/to/file.txt", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte("Hello, S3!")); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	// Read it back.
	data, err := vfs.ReadFile("path/to/file.txt")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(string(data))

	// Delete it.
	if err := vfs.Remove("path/to/file.txt"); err != nil {
		log.Fatal(err)
	}
}
```

### Custom endpoint

```go
vfs, err := s3fs.New(&s3fs.Config{
	Bucket:    "my-bucket",
	Region:    "us-west-2",
	Endpoint:  "http://localhost:9000",
	AccessKey: "access-key",
	SecretKey: "secret-key",
	// PartSize: 8 * 1024 * 1024,
})
```

### Contexts and custom clients

```go
vfs = vfs.WithContext(ctx)

// Inject a custom transport or a mock S3Client (useful for tests).
client := s3fs.NewWithClient(myS3Client, "my-bucket")
client = s3fs.NewWithClientAndContext(ctx, myS3Client, "my-bucket")
```

## Behavior

- Directories are zero-byte objects with a trailing slash.
- Writes are buffered in memory and uploaded when the `File` is closed.
  Content larger than `PartSize` uses a multipart upload; the final buffer is
  uploaded as the last part and the upload is completed.
- `Rename` is a copy followed by a delete; it is not atomic.
- `Seek` is supported for the in-memory buffer, but S3 is object storage, not
  a random-access block device.
- The file system and individual files are safe for concurrent use.
- Missing objects are reported as `os.ErrNotExist`; other non-2xx responses are
  `*s3fs.APIError` with the status code, S3 error code and message.

## Limitations

S3 has no POSIX permissions, ownership, or timestamps:

- `Chmod`, `Chown` and `Chtimes` return a not-implemented error.
- Directory listings and `Stat` are derived from object keys and prefixes.
- Atomic rename and append semantics do not exist in S3; concurrent writers to
  the same object last-writer-wins.

## Errors

| Error | Meaning |
|-------|---------|
| `s3fs.ErrNotFound` | The object or bucket does not exist. |
| `s3fs.APIError` | A non-2xx response with an S3 error document. |
| `os.ErrNotExist` | Mapped from `ErrNotFound` by the file system methods. |

## License

MIT — see [../../LICENSE](../../../LICENSE).
