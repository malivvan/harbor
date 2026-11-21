package s3fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// MockS3Client is an in-memory mock implementation of S3Client for testing.
type MockS3Client struct {
	mu       sync.RWMutex
	objects  map[string]*mockObject // key -> object
	uploads  map[string]*mockUpload // uploadID -> upload
	uploadID int                    // counter for generating upload IDs

	// Error injection for testing failure scenarios
	GetObjectErr    error
	PutObjectErr    error
	DeleteObjectErr error
	CopyObjectErr   error
	HeadObjectErr   error
	ListObjectsErr  error

	// Per-key error injection for DeleteObject (checked before global DeleteObjectErr)
	DeleteObjectKeyErrs map[string]error

	// DefaultMaxKeys overrides the default 1000 max keys for ListObjectsV2.
	// Set to a small value in tests to force pagination behavior.
	DefaultMaxKeys int32

	// Call tracking for assertions
	GetObjectCalls    []string
	PutObjectCalls    []string
	DeleteObjectCalls []string
	CopyObjectCalls   []copyObjectCall
	HeadObjectCalls   []string
	ListObjectsCalls  []string
}

type mockUpload struct {
	key   string
	parts map[int32][]byte // partNumber -> data
}

type mockObject struct {
	data         []byte
	lastModified time.Time
	contentType  string
	metadata     map[string]string
}

type copyObjectCall struct {
	Source      string
	Destination string
}

// NewMockS3Client creates a new mock S3 client for testing.
func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		objects: make(map[string]*mockObject),
	}
}

// Reset clears all objects and error injections.
func (m *MockS3Client) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objects = make(map[string]*mockObject)
	m.uploads = nil
	m.uploadID = 0
	m.GetObjectErr = nil
	m.PutObjectErr = nil
	m.DeleteObjectErr = nil
	m.DeleteObjectKeyErrs = nil
	m.CopyObjectErr = nil
	m.HeadObjectErr = nil
	m.ListObjectsErr = nil
	m.GetObjectCalls = nil
	m.PutObjectCalls = nil
	m.DeleteObjectCalls = nil
	m.CopyObjectCalls = nil
	m.HeadObjectCalls = nil
	m.ListObjectsCalls = nil
}

// PutTestObject adds a test object directly to the mock store.
func (m *MockS3Client) PutTestObject(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objects[key] = &mockObject{
		data:         data,
		lastModified: time.Now(),
		contentType:  "application/octet-stream",
	}
}

// GetTestObject retrieves a test object from the mock store.
func (m *MockS3Client) GetTestObject(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	obj, ok := m.objects[key]
	if !ok {
		return nil, false
	}
	return obj.data, true
}

// HasObject checks if an object exists in the mock store.
func (m *MockS3Client) HasObject(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.objects[key]
	return ok
}

// ObjectCount returns the number of objects in the mock store.
func (m *MockS3Client) ObjectCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.objects)
}

// GetObject implements S3Client.
func (m *MockS3Client) GetObject(ctx context.Context, input *GetObjectInput) (*GetObjectOutput, error) {
	m.mu.Lock()
	m.GetObjectCalls = append(m.GetObjectCalls, input.Key)
	m.mu.Unlock()

	if m.GetObjectErr != nil {
		return nil, m.GetObjectErr
	}

	m.mu.RLock()
	obj, ok := m.objects[input.Key]
	m.mu.RUnlock()

	if !ok {
		return nil, mockNotFound(input.Key)
	}

	data := obj.data
	contentLength := int64(len(data))

	// Handle range requests
	if input.Range != "" {
		start, end, err := parseRange(input.Range, int64(len(obj.data)))
		if err != nil {
			return nil, fmt.Errorf("invalid range: %w", err)
		}
		data = obj.data[start : end+1]
		contentLength = int64(len(data))
	}

	return &GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: contentLength,
		LastModified:  obj.lastModified,
		ContentType:   obj.contentType,
	}, nil
}

// PutObject implements S3Client.
func (m *MockS3Client) PutObject(ctx context.Context, input *PutObjectInput) (*PutObjectOutput, error) {
	m.mu.Lock()
	m.PutObjectCalls = append(m.PutObjectCalls, input.Key)
	m.mu.Unlock()

	if m.PutObjectErr != nil {
		return nil, m.PutObjectErr
	}

	data, err := readAll(input.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.objects[input.Key] = &mockObject{
		data:         data,
		lastModified: time.Now(),
		contentType:  input.ContentType,
	}
	m.mu.Unlock()

	return &PutObjectOutput{}, nil
}

// DeleteObject implements S3Client.
func (m *MockS3Client) DeleteObject(ctx context.Context, input *DeleteObjectInput) (*DeleteObjectOutput, error) {
	m.mu.Lock()
	m.DeleteObjectCalls = append(m.DeleteObjectCalls, input.Key)
	m.mu.Unlock()

	if keyErr, ok := m.DeleteObjectKeyErrs[input.Key]; ok {
		return nil, keyErr
	}
	if m.DeleteObjectErr != nil {
		return nil, m.DeleteObjectErr
	}

	m.mu.Lock()
	delete(m.objects, input.Key)
	m.mu.Unlock()

	return &DeleteObjectOutput{}, nil
}

// CopyObject implements S3Client.
func (m *MockS3Client) CopyObject(ctx context.Context, input *CopyObjectInput) (*CopyObjectOutput, error) {
	m.mu.Lock()
	m.CopyObjectCalls = append(m.CopyObjectCalls, copyObjectCall{Source: input.CopySource, Destination: input.Key})
	m.mu.Unlock()

	if m.CopyObjectErr != nil {
		return nil, m.CopyObjectErr
	}

	// Extract the source key from the CopySource (format: /bucket/key or bucket/key)
	sourceKey := strings.TrimPrefix(input.CopySource, "/")
	// Remove bucket prefix if present
	if parts := strings.SplitN(sourceKey, "/", 2); len(parts) == 2 {
		sourceKey = parts[1]
	}

	m.mu.RLock()
	obj, ok := m.objects[sourceKey]
	m.mu.RUnlock()

	if !ok {
		return nil, mockNotFound(sourceKey)
	}

	m.mu.Lock()
	m.objects[input.Key] = &mockObject{
		data:         append([]byte(nil), obj.data...),
		lastModified: time.Now(),
		contentType:  obj.contentType,
		metadata:     obj.metadata,
	}
	m.mu.Unlock()

	return &CopyObjectOutput{}, nil
}

// HeadObject implements S3Client.
func (m *MockS3Client) HeadObject(ctx context.Context, input *HeadObjectInput) (*HeadObjectOutput, error) {
	m.mu.Lock()
	m.HeadObjectCalls = append(m.HeadObjectCalls, input.Key)
	m.mu.Unlock()

	if m.HeadObjectErr != nil {
		return nil, m.HeadObjectErr
	}

	m.mu.RLock()
	obj, ok := m.objects[input.Key]
	m.mu.RUnlock()

	if !ok {
		return nil, mockNotFound(input.Key)
	}

	return &HeadObjectOutput{
		ContentLength: int64(len(obj.data)),
		LastModified:  obj.lastModified,
		ContentType:   obj.contentType,
	}, nil
}

// ListObjectsV2 implements S3Client with pagination support via ContinuationToken.
func (m *MockS3Client) ListObjectsV2(ctx context.Context, input *ListObjectsV2Input) (*ListObjectsV2Output, error) {
	m.mu.Lock()
	m.ListObjectsCalls = append(m.ListObjectsCalls, input.Prefix)
	m.mu.Unlock()

	if m.ListObjectsErr != nil {
		return nil, m.ListObjectsErr
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect and sort matching keys for deterministic pagination
	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, input.Prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	// Handle ContinuationToken (the token is the last key from previous page)
	startIdx := 0
	if input.ContinuationToken != "" {
		for i, key := range keys {
			if key > input.ContinuationToken {
				startIdx = i
				break
			}
			// If we reach the end without finding a key > token, no results
			if i == len(keys)-1 {
				startIdx = len(keys)
			}
		}
	}

	maxKeys := int32(1000)
	if m.DefaultMaxKeys > 0 {
		maxKeys = m.DefaultMaxKeys
	}
	if input.MaxKeys > 0 {
		maxKeys = input.MaxKeys
	}

	var contents []Object
	endIdx := startIdx
	for i := startIdx; i < len(keys) && int32(len(contents)) < maxKeys; i++ {
		obj := m.objects[keys[i]]
		contents = append(contents, Object{
			Key:          keys[i],
			Size:         int64(len(obj.data)),
			LastModified: obj.lastModified,
		})
		endIdx = i
	}

	isTruncated := endIdx < len(keys)-1 && len(contents) > 0
	var nextToken string
	if isTruncated {
		nextToken = keys[endIdx]
	}

	return &ListObjectsV2Output{
		Contents:              contents,
		IsTruncated:           isTruncated,
		KeyCount:              int32(len(contents)),
		NextContinuationToken: nextToken,
	}, nil
}

// CreateMultipartUpload implements S3Client.
func (m *MockS3Client) CreateMultipartUpload(ctx context.Context, input *CreateMultipartUploadInput) (*CreateMultipartUploadOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.uploadID++
	id := fmt.Sprintf("upload-%d", m.uploadID)

	if m.uploads == nil {
		m.uploads = make(map[string]*mockUpload)
	}
	m.uploads[id] = &mockUpload{
		key:   input.Key,
		parts: make(map[int32][]byte),
	}

	return &CreateMultipartUploadOutput{UploadID: id}, nil
}

// UploadPart implements S3Client.
func (m *MockS3Client) UploadPart(ctx context.Context, input *UploadPartInput) (*UploadPartOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	upload, ok := m.uploads[input.UploadID]
	if !ok {
		return nil, fmt.Errorf("no such upload: %s", input.UploadID)
	}

	data, err := readAll(input.Body)
	if err != nil {
		return nil, err
	}

	upload.parts[input.PartNumber] = data

	return &UploadPartOutput{ETag: fmt.Sprintf("etag-part-%d", input.PartNumber)}, nil
}

// CompleteMultipartUpload implements S3Client.
func (m *MockS3Client) CompleteMultipartUpload(ctx context.Context, input *CompleteMultipartUploadInput) (*CompleteMultipartUploadOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	upload, ok := m.uploads[input.UploadID]
	if !ok {
		return nil, fmt.Errorf("no such upload: %s", input.UploadID)
	}

	// Assemble parts in order
	var assembled []byte
	for i := int32(1); i <= int32(len(upload.parts)); i++ {
		data, ok := upload.parts[i]
		if !ok {
			return nil, fmt.Errorf("missing part %d", i)
		}
		assembled = append(assembled, data...)
	}

	// Store as final object
	m.objects[upload.key] = &mockObject{
		data:         assembled,
		lastModified: time.Now(),
		contentType:  "application/octet-stream",
	}

	delete(m.uploads, input.UploadID)
	return &CompleteMultipartUploadOutput{}, nil
}

// AbortMultipartUpload implements S3Client.
func (m *MockS3Client) AbortMultipartUpload(ctx context.Context, input *AbortMultipartUploadInput) (*AbortMultipartUploadOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.uploads, input.UploadID)
	return &AbortMultipartUploadOutput{}, nil
}

// mockNotFound returns the error used for missing objects.
func mockNotFound(key string) error {
	return fmt.Errorf("%w: %s", ErrNotFound, key)
}

// parseRange parses an HTTP range header value like "bytes=0-99".
func parseRange(rangeStr string, size int64) (start, end int64, err error) {
	if !strings.HasPrefix(rangeStr, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	rangeStr = strings.TrimPrefix(rangeStr, "bytes=")
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	if parts[0] != "" {
		_, err = fmt.Sscanf(parts[0], "%d", &start)
		if err != nil {
			return 0, 0, err
		}
	}

	if parts[1] != "" {
		_, err = fmt.Sscanf(parts[1], "%d", &end)
		if err != nil {
			return 0, 0, err
		}
	} else {
		end = size - 1
	}

	if start < 0 || end >= size || start > end {
		return 0, 0, fmt.Errorf("range out of bounds")
	}

	return start, end, nil
}

// Ensure MockS3Client satisfies S3Client interface
var _ S3Client = (*MockS3Client)(nil)
