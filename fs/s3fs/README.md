# S3FS - S3 FileSystem

[![Go Reference](https://pkg.go.dev/badge/github.com/absfs/s3fs.svg)](https://pkg.go.dev/github.com/absfs/s3fs)
[![Go Report Card](https://goreportcard.com/badge/github.com/absfs/s3fs)](https://goreportcard.com/report/github.com/absfs/s3fs)
[![CI](https://github.com/absfs/s3fs/actions/workflows/ci.yml/badge.svg)](https://github.com/absfs/s3fs/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

The `s3fs` package implements an `absfs.Filer` for S3-compatible object storage. It provides file operations on S3 buckets using the AWS SDK v2.

## Features

- **S3-compatible storage**: Works with AWS S3 and compatible services
- **Standard interface**: Implements `absfs.Filer` for seamless integration
- **Basic operations**: Read, write, delete files in S3 buckets

## Install

```bash
go get github.com/absfs/s3fs
```

## Example Usage

### Basic Usage

```go
package main

import (
    "log"
    "os"

    "github.com/malivvan/storm/fs/s3fs"
)

func main() {
    // Create S3 filesystem
    fs, err := s3fs.New(&s3fs.Config{
        Bucket: "my-bucket",
        Region: "us-east-1",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Write a file
    f, _ := fs.OpenFile("path/to/file.txt", os.O_CREATE|os.O_WRONLY, 0644)
    f.Write([]byte("Hello, S3!"))
    f.Close()

    // Read a file
    f, _ = fs.OpenFile("path/to/file.txt", os.O_RDONLY, 0)
    defer f.Close()

    buf := make([]byte, 1024)
    n, _ := f.Read(buf)
    log.Printf("Read: %s", buf[:n])

    // Delete a file
    fs.Remove("path/to/file.txt")
}
```

### With a custom endpoint (MinIO, LocalStack, R2, ...)

```go
package main

import (
    "log"

    "github.com/malivvan/storm/fs/s3fs"
)

func main() {
    fs, err := s3fs.New(&s3fs.Config{
        Bucket:    "my-bucket",
        Region:    "us-west-2",
        Endpoint:  "http://localhost:9000", // custom endpoints use path-style addressing
        AccessKey: "access-key",
        SecretKey: "secret-key",
        // PartSize: 8 * 1024 * 1024, // optional multipart part size
    })
    if err != nil {
        log.Fatal(err)
    }

    // Use filesystem
    _ = fs
}
```

## Limitations

- **Chmod, Chtimes, Chown**: Not supported (S3 doesn't have POSIX permissions)
- **Directories**: Represented as zero-byte objects with trailing slash
- **Seeking**: Limited support (S3 is object storage, not a traditional filesystem)
- **Atomic operations**: Some operations (like Rename) require copy+delete

## Authentication

Credentials are resolved in this order:
1. `Config.AccessKey` / `Config.SecretKey` (plus optional `Config.SessionToken`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
3. Shared credentials file (`~/.aws/credentials`, or `AWS_SHARED_CREDENTIALS_FILE`), using `AWS_PROFILE`

The region comes from `Config.Region`, then `AWS_REGION` / `AWS_DEFAULT_REGION`, then the shared config file, and finally `us-east-1`.

Requests are signed locally with AWS Signature Version 4, so the package has no third-party dependencies.

## absfs

Check out the [`absfs`](https://github.com/malivvan/storm/internal/absfs) repo for more information about the abstract filesystem interface and features like filesystem composition.

## LICENSE

This project is governed by the MIT License. See [LICENSE](https://github.com/absfs/s3fs/blob/master/LICENSE)
