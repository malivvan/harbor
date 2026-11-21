package s3fs

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned (possibly wrapped) when an object or bucket does not
// exist. It is mapped to os.ErrNotExist by the FileSystem methods.
var ErrNotFound = errors.New("s3fs: not found")

// Object describes an entry returned by ListObjectsV2.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// CompletedPart identifies a part that was uploaded as part of a multipart
// upload.
type CompletedPart struct {
	ETag       string
	PartNumber int32
}

// CompletedMultipartUpload lists the parts that make up a completed multipart
// upload.
type CompletedMultipartUpload struct {
	Parts []CompletedPart
}

// GetObjectInput is the input for [S3Client.GetObject].
type GetObjectInput struct {
	Bucket string
	Key    string
	Range  string // optional HTTP Range header value, e.g. "bytes=0-99"
}

// GetObjectOutput is the output of [S3Client.GetObject].
type GetObjectOutput struct {
	Body          io.ReadCloser
	ContentLength int64
	LastModified  time.Time
	ContentType   string
}

// PutObjectInput is the input for [S3Client.PutObject]. Body is consumed
// entirely before the request is sent.
type PutObjectInput struct {
	Bucket      string
	Key         string
	Body        io.Reader
	ContentType string
}

// PutObjectOutput is the output of [S3Client.PutObject].
type PutObjectOutput struct {
	ETag string
}

// DeleteObjectInput is the input for [S3Client.DeleteObject].
type DeleteObjectInput struct {
	Bucket string
	Key    string
}

// DeleteObjectOutput is the output of [S3Client.DeleteObject].
type DeleteObjectOutput struct{}

// CopyObjectInput is the input for [S3Client.CopyObject]. CopySource must be in
// "/bucket/key" form.
type CopyObjectInput struct {
	Bucket     string
	Key        string
	CopySource string
}

// CopyObjectOutput is the output of [S3Client.CopyObject].
type CopyObjectOutput struct {
	ETag string
}

// HeadObjectInput is the input for [S3Client.HeadObject].
type HeadObjectInput struct {
	Bucket string
	Key    string
}

// HeadObjectOutput is the output of [S3Client.HeadObject].
type HeadObjectOutput struct {
	ContentLength int64
	LastModified  time.Time
	ContentType   string
}

// ListObjectsV2Input is the input for [S3Client.ListObjectsV2].
type ListObjectsV2Input struct {
	Bucket            string
	Prefix            string
	ContinuationToken string
	MaxKeys           int32
}

// ListObjectsV2Output is the output of [S3Client.ListObjectsV2].
type ListObjectsV2Output struct {
	Contents              []Object
	IsTruncated           bool
	KeyCount              int32
	NextContinuationToken string
}

// CreateMultipartUploadInput is the input for
// [S3Client.CreateMultipartUpload].
type CreateMultipartUploadInput struct {
	Bucket      string
	Key         string
	ContentType string
}

// CreateMultipartUploadOutput is the output of
// [S3Client.CreateMultipartUpload].
type CreateMultipartUploadOutput struct {
	UploadID string
}

// UploadPartInput is the input for [S3Client.UploadPart]. Body is consumed
// entirely before the request is sent.
type UploadPartInput struct {
	Bucket     string
	Key        string
	UploadID   string
	PartNumber int32
	Body       io.Reader
}

// UploadPartOutput is the output of [S3Client.UploadPart].
type UploadPartOutput struct {
	ETag string
}

// CompleteMultipartUploadInput is the input for
// [S3Client.CompleteMultipartUpload].
type CompleteMultipartUploadInput struct {
	Bucket          string
	Key             string
	UploadID        string
	MultipartUpload *CompletedMultipartUpload
}

// CompleteMultipartUploadOutput is the output of
// [S3Client.CompleteMultipartUpload].
type CompleteMultipartUploadOutput struct {
	ETag string
}

// AbortMultipartUploadInput is the input for [S3Client.AbortMultipartUpload].
type AbortMultipartUploadInput struct {
	Bucket   string
	Key      string
	UploadID string
}

// AbortMultipartUploadOutput is the output of [S3Client.AbortMultipartUpload].
type AbortMultipartUploadOutput struct{}

// S3Client defines the subset of the S3 API used by [FileSystem]. The default
// implementation is an HTTP client created by [New]; [MockS3Client] is an
// in-memory implementation for tests.
type S3Client interface {
	GetObject(ctx context.Context, input *GetObjectInput) (*GetObjectOutput, error)
	PutObject(ctx context.Context, input *PutObjectInput) (*PutObjectOutput, error)
	DeleteObject(ctx context.Context, input *DeleteObjectInput) (*DeleteObjectOutput, error)
	CopyObject(ctx context.Context, input *CopyObjectInput) (*CopyObjectOutput, error)
	HeadObject(ctx context.Context, input *HeadObjectInput) (*HeadObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *ListObjectsV2Input) (*ListObjectsV2Output, error)
	CreateMultipartUpload(ctx context.Context, input *CreateMultipartUploadInput) (*CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, input *UploadPartInput) (*UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *CompleteMultipartUploadInput) (*CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *AbortMultipartUploadInput) (*AbortMultipartUploadOutput, error)
}

// APIError is returned by the HTTP client for non-2xx responses other than
// missing objects.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	if e.Code == "" && e.Message == "" {
		return "s3fs: request failed with status " + httpStatusText(e.StatusCode)
	}
	if e.Message == "" {
		return "s3fs: " + e.Code + " (status " + httpStatusText(e.StatusCode) + ")"
	}
	if e.Code == "" {
		return "s3fs: " + e.Message + " (status " + httpStatusText(e.StatusCode) + ")"
	}
	return "s3fs: " + e.Code + ": " + e.Message + " (status " + httpStatusText(e.StatusCode) + ")"
}
