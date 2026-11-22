package s3fs

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// emptyPayloadHash is the SHA-256 of an empty payload.
const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// httpS3Client is a minimal S3 REST client implementing [S3Client]. It signs
// requests with AWS Signature Version 4 and speaks just enough of the S3
// protocol for the operations used by [FileSystem].
type httpS3Client struct {
	httpClient *http.Client
	region     string
	endpoint   *url.URL
	pathStyle  bool
	creds      credentials
}

// newHTTPClient builds an [httpS3Client] from cfg.
func newHTTPClient(cfg *Config) (*httpS3Client, error) {
	creds, err := resolveCredentials(cfg)
	if err != nil {
		return nil, err
	}

	region := resolveRegion(cfg)

	var endpoint *url.URL
	pathStyle := cfg.UsePathStyle
	if cfg.Endpoint != "" {
		endpoint, err = url.Parse(cfg.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("s3fs: invalid endpoint %q: %w", cfg.Endpoint, err)
		}
		if endpoint.Scheme == "" || endpoint.Host == "" {
			return nil, fmt.Errorf("s3fs: invalid endpoint %q: must include scheme and host", cfg.Endpoint)
		}
		// Custom endpoints (MinIO, LocalStack, R2, ...) are addressed
		// path-style by default.
		pathStyle = true
	} else {
		endpoint = &url.URL{Scheme: "https", Host: "s3." + region + ".amazonaws.com"}
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &httpS3Client{
		httpClient: httpClient,
		region:     region,
		endpoint:   endpoint,
		pathStyle:  pathStyle,
		creds:      creds,
	}, nil
}

// do builds, signs and executes a single S3 request.
func (c *httpS3Client) do(ctx context.Context, method, bucket, key string, query url.Values, headers map[string]string, body []byte) (*http.Response, error) {
	host := c.endpoint.Host
	basePath := strings.TrimSuffix(c.endpoint.EscapedPath(), "/")

	var canonPath string
	if c.pathStyle {
		canonPath = basePath + "/" + awsEncode(bucket, false)
		if key != "" {
			canonPath += "/" + awsEncode(key, false)
		}
	} else {
		host = bucket + "." + host
		canonPath = basePath + "/"
		if key != "" {
			canonPath += awsEncode(key, false)
		}
	}

	canonQuery := encodeQuery(query)
	rawURL := c.endpoint.Scheme + "://" + host + canonPath
	if canonQuery != "" {
		rawURL += "?" + canonQuery
	}

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("User-Agent", "storm-s3fs/1.0")
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	payloadHash := emptyPayloadHash
	if len(body) > 0 {
		payloadHash = sha256Hex(body)
	}
	c.sign(req, canonPath, canonQuery, payloadHash, time.Now())

	return c.httpClient.Do(req)
}

// sign adds AWS Signature Version 4 headers to req.
func (c *httpS3Client) sign(req *http.Request, canonPath, canonQuery, payloadHash string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := amzDate[:8]

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	// SigV4 requires every x-amz-* header that is sent to be signed. Build the
	// list from the request instead of a fixed set: operation-specific headers
	// such as X-Amz-Copy-Source would otherwise be sent unsigned, which real S3
	// and current MinIO reject.
	if c.creds.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", c.creds.sessionToken)
	}
	signed := map[string]struct{}{"host": {}}
	for name := range req.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") {
			signed[lower] = struct{}{}
		}
	}
	signedHeaderNames := make([]string, 0, len(signed))
	for name := range signed {
		signedHeaderNames = append(signedHeaderNames, name)
	}
	sort.Strings(signedHeaderNames)

	var canonicalHeaders strings.Builder
	for _, name := range signedHeaderNames {
		value := req.Header.Get(name)
		if name == "host" {
			value = req.URL.Host
		}
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.Join(strings.Fields(value), " "))
		canonicalHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(signedHeaderNames, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonPath,
		canonQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := dateStamp + "/" + c.region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonicalRequest))

	kDate := hmacSHA256([]byte("AWS4"+c.creds.secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, c.region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.creds.accessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func (c *httpS3Client) GetObject(ctx context.Context, input *GetObjectInput) (*GetObjectOutput, error) {
	var headers map[string]string
	if input.Range != "" {
		headers = map[string]string{"Range": input.Range}
	}
	resp, err := c.do(ctx, http.MethodGet, input.Bucket, input.Key, nil, headers, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		closeBody(resp)
		return nil, notFound(input.Bucket, input.Key)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		err := responseError(resp)
		closeBody(resp)
		return nil, err
	}
	lastModified, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return &GetObjectOutput{
		Body:          resp.Body,
		ContentLength: resp.ContentLength,
		LastModified:  lastModified,
		ContentType:   resp.Header.Get("Content-Type"),
	}, nil
}

func (c *httpS3Client) PutObject(ctx context.Context, input *PutObjectInput) (*PutObjectOutput, error) {
	body, err := readAll(input.Body)
	if err != nil {
		return nil, err
	}
	var headers map[string]string
	if input.ContentType != "" {
		headers = map[string]string{"Content-Type": input.ContentType}
	}
	resp, err := c.do(ctx, http.MethodPut, input.Bucket, input.Key, nil, headers, body)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if !isSuccess(resp.StatusCode) {
		return nil, responseError(resp)
	}
	return &PutObjectOutput{ETag: resp.Header.Get("ETag")}, nil
}

func (c *httpS3Client) DeleteObject(ctx context.Context, input *DeleteObjectInput) (*DeleteObjectOutput, error) {
	resp, err := c.do(ctx, http.MethodDelete, input.Bucket, input.Key, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	// S3 returns 204 for successful deletes and 404 for missing keys; both are
	// treated as success.
	if !isSuccess(resp.StatusCode) && resp.StatusCode != http.StatusNotFound {
		return nil, responseError(resp)
	}
	return &DeleteObjectOutput{}, nil
}

func (c *httpS3Client) CopyObject(ctx context.Context, input *CopyObjectInput) (*CopyObjectOutput, error) {
	source := strings.TrimPrefix(input.CopySource, "/")
	sourceBucket, sourceKey, ok := strings.Cut(source, "/")
	if !ok {
		return nil, fmt.Errorf("s3fs: invalid copy source %q: must be /bucket/key", input.CopySource)
	}
	headers := map[string]string{
		"X-Amz-Copy-Source": "/" + awsEncode(sourceBucket, false) + "/" + awsEncode(sourceKey, false),
	}
	resp, err := c.do(ctx, http.MethodPut, input.Bucket, input.Key, nil, headers, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if !isSuccess(resp.StatusCode) {
		return nil, responseError(resp)
	}

	// CopyObject can return 200 with an <Error> document in the body.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if apiErr := errorFromBody(body, resp.StatusCode); apiErr != nil {
		return nil, apiErr
	}
	var result struct {
		ETag string `xml:"ETag"`
	}
	_ = xml.Unmarshal(body, &result)
	return &CopyObjectOutput{ETag: result.ETag}, nil
}

func (c *httpS3Client) HeadObject(ctx context.Context, input *HeadObjectInput) (*HeadObjectOutput, error) {
	resp, err := c.do(ctx, http.MethodHead, input.Bucket, input.Key, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if resp.StatusCode == http.StatusNotFound {
		return nil, notFound(input.Bucket, input.Key)
	}
	if !isSuccess(resp.StatusCode) {
		return nil, responseError(resp)
	}
	lastModified, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return &HeadObjectOutput{
		ContentLength: resp.ContentLength,
		LastModified:  lastModified,
		ContentType:   resp.Header.Get("Content-Type"),
	}, nil
}

func (c *httpS3Client) ListObjectsV2(ctx context.Context, input *ListObjectsV2Input) (*ListObjectsV2Output, error) {
	query := url.Values{"list-type": {"2"}}
	if input.Prefix != "" {
		query.Set("prefix", input.Prefix)
	}
	if input.ContinuationToken != "" {
		query.Set("continuation-token", input.ContinuationToken)
	}
	if input.MaxKeys > 0 {
		query.Set("max-keys", strconv.Itoa(int(input.MaxKeys)))
	}

	resp, err := c.do(ctx, http.MethodGet, input.Bucket, "", query, nil, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if !isSuccess(resp.StatusCode) {
		return nil, responseError(resp)
	}

	var result struct {
		IsTruncated           bool   `xml:"IsTruncated"`
		KeyCount              int32  `xml:"KeyCount"`
		NextContinuationToken string `xml:"NextContinuationToken"`
		Contents              []struct {
			Key          string `xml:"Key"`
			Size         int64  `xml:"Size"`
			LastModified string `xml:"LastModified"`
		} `xml:"Contents"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("s3fs: decoding ListObjectsV2 response: %w", err)
	}

	output := &ListObjectsV2Output{
		IsTruncated:           result.IsTruncated,
		KeyCount:              result.KeyCount,
		NextContinuationToken: result.NextContinuationToken,
		Contents:              make([]Object, 0, len(result.Contents)),
	}
	for _, item := range result.Contents {
		lastModified, _ := time.Parse(time.RFC3339, item.LastModified)
		output.Contents = append(output.Contents, Object{
			Key:          item.Key,
			Size:         item.Size,
			LastModified: lastModified,
		})
	}
	return output, nil
}

func (c *httpS3Client) CreateMultipartUpload(ctx context.Context, input *CreateMultipartUploadInput) (*CreateMultipartUploadOutput, error) {
	var headers map[string]string
	if input.ContentType != "" {
		headers = map[string]string{"Content-Type": input.ContentType}
	}
	resp, err := c.do(ctx, http.MethodPost, input.Bucket, input.Key, url.Values{"uploads": {""}}, headers, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if !isSuccess(resp.StatusCode) {
		return nil, responseError(resp)
	}
	var result struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("s3fs: decoding CreateMultipartUpload response: %w", err)
	}
	return &CreateMultipartUploadOutput{UploadID: result.UploadID}, nil
}

func (c *httpS3Client) UploadPart(ctx context.Context, input *UploadPartInput) (*UploadPartOutput, error) {
	body, err := readAll(input.Body)
	if err != nil {
		return nil, err
	}
	query := url.Values{
		"partNumber": {strconv.Itoa(int(input.PartNumber))},
		"uploadId":   {input.UploadID},
	}
	resp, err := c.do(ctx, http.MethodPut, input.Bucket, input.Key, query, nil, body)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if !isSuccess(resp.StatusCode) {
		return nil, responseError(resp)
	}
	return &UploadPartOutput{ETag: resp.Header.Get("ETag")}, nil
}

func (c *httpS3Client) CompleteMultipartUpload(ctx context.Context, input *CompleteMultipartUploadInput) (*CompleteMultipartUploadOutput, error) {
	type completePart struct {
		PartNumber int32  `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}
	payload := struct {
		XMLName xml.Name       `xml:"CompleteMultipartUpload"`
		Parts   []completePart `xml:"Part"`
	}{}
	if input.MultipartUpload != nil {
		for _, part := range input.MultipartUpload.Parts {
			payload.Parts = append(payload.Parts, completePart{PartNumber: part.PartNumber, ETag: part.ETag})
		}
	}
	body, err := xml.Marshal(payload)
	if err != nil {
		return nil, err
	}

	query := url.Values{"uploadId": {input.UploadID}}
	headers := map[string]string{"Content-Type": "application/xml"}
	resp, err := c.do(ctx, http.MethodPost, input.Bucket, input.Key, query, headers, body)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if !isSuccess(resp.StatusCode) {
		return nil, responseError(resp)
	}

	// CompleteMultipartUpload can return 200 with an <Error> document.
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if apiErr := errorFromBody(data, resp.StatusCode); apiErr != nil {
		return nil, apiErr
	}
	var result struct {
		ETag string `xml:"ETag"`
	}
	_ = xml.Unmarshal(data, &result)
	return &CompleteMultipartUploadOutput{ETag: result.ETag}, nil
}

func (c *httpS3Client) AbortMultipartUpload(ctx context.Context, input *AbortMultipartUploadInput) (*AbortMultipartUploadOutput, error) {
	query := url.Values{"uploadId": {input.UploadID}}
	resp, err := c.do(ctx, http.MethodDelete, input.Bucket, input.Key, query, nil, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	if !isSuccess(resp.StatusCode) && resp.StatusCode != http.StatusNotFound {
		return nil, responseError(resp)
	}
	return &AbortMultipartUploadOutput{}, nil
}

// --- helpers ---

func notFound(bucket, key string) error {
	return fmt.Errorf("%w: %s/%s", ErrNotFound, bucket, key)
}

func isSuccess(status int) bool {
	return status >= 200 && status < 300
}

func readAll(r io.Reader) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	return io.ReadAll(r)
}

func closeBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// responseError converts a non-2xx response into an *APIError.
func responseError(resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if apiErr := errorFromBody(data, resp.StatusCode); apiErr != nil {
		return apiErr
	}
	return &APIError{StatusCode: resp.StatusCode}
}

// errorFromBody parses an S3 <Error> document, returning nil if body is not one.
func errorFromBody(body []byte, status int) error {
	if len(body) == 0 {
		return nil
	}
	var doc struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil
	}
	if doc.XMLName.Local != "Error" {
		return nil
	}
	return &APIError{StatusCode: status, Code: doc.Code, Message: doc.Message}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

// awsEncode percent-encodes value as required by AWS: unreserved characters
// (A-Z a-z 0-9 - _ . ~) and, unless encodeSlash is set, "/" are left alone.
func awsEncode(value string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		ch := value[i]
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9',
			ch == '-', ch == '_', ch == '.', ch == '~':
			b.WriteByte(ch)
		case ch == '/' && !encodeSlash:
			b.WriteByte(ch)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[ch>>4])
			b.WriteByte(hexDigits[ch&0x0F])
		}
	}
	return b.String()
}

// encodeQuery renders a canonical, sorted query string using AWS encoding.
func encodeQuery(query url.Values) string {
	if len(query) == 0 {
		return ""
	}
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var parts []string
	for _, key := range keys {
		values := append([]string(nil), query[key]...)
		sort.Strings(values)
		for _, value := range values {
			parts = append(parts, awsEncode(key, true)+"="+awsEncode(value, true))
		}
	}
	return strings.Join(parts, "&")
}

func httpStatusText(status int) string {
	if text := http.StatusText(status); text != "" {
		return strconv.Itoa(status) + " " + text
	}
	return strconv.Itoa(status)
}
