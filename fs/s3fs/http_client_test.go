package s3fs

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testAccessKey = "AKIDEXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	testBucket    = "test-bucket"
)

// fakeS3 is a tiny S3-compatible HTTP server used to exercise the real HTTP
// client. It verifies SigV4 signatures and implements the operations s3fs uses.
type fakeS3 struct {
	mu        sync.Mutex
	objects   map[string][]byte
	multipart map[string]map[int32][]byte
	uploadSeq int
	secret    string
}

func newFakeS3(t *testing.T, secret string) (*fakeS3, *httptest.Server) {
	t.Helper()
	f := &fakeS3{
		objects:   make(map[string][]byte),
		multipart: make(map[string]map[int32][]byte),
		secret:    secret,
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := verifySigV4(r, f.secret); err != nil {
		http.Error(w, `<?xml version="1.0"?><Error><Code>SignatureDoesNotMatch</Code><Message>`+err.Error()+`</Message></Error>`, http.StatusForbidden)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if bucket != testBucket {
		http.Error(w, `<Error><Code>NoSuchBucket</Code></Error>`, http.StatusNotFound)
		return
	}

	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		f.list(w, q.Get("prefix"))
	case r.Method == http.MethodPost && q.Has("uploads"):
		f.startMultipart(w, key)
	case r.Method == http.MethodPut && q.Get("partNumber") != "":
		f.uploadPart(w, r, q.Get("uploadId"), q.Get("partNumber"))
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		f.completeMultipart(w, r, key, q.Get("uploadId"))
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		f.mu.Lock()
		delete(f.multipart, q.Get("uploadId"))
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		f.object(w, r, key)
	}
}

func (f *fakeS3) object(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodHead:
		f.mu.Lock()
		data, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		f.mu.Lock()
		data, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)

	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	case http.MethodPut:
		if source := r.Header.Get("X-Amz-Copy-Source"); source != "" {
			src, err := url.PathUnescape(strings.TrimPrefix(source, "/"))
			if err != nil {
				http.Error(w, "bad copy source", http.StatusBadRequest)
				return
			}
			if _, srcKey, ok := strings.Cut(src, "/"); ok {
				src = srcKey
			}
			f.mu.Lock()
			data, ok := f.objects[src]
			if ok {
				f.objects[key] = append([]byte(nil), data...)
			}
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprintf(w, `<CopyObjectResult><ETag>"copy-etag"</ETag></CopyObjectResult>`)
			return
		}
		data, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.objects[key] = data
		f.mu.Unlock()
		w.Header().Set("ETag", `"put-etag"`)
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeS3) list(w http.ResponseWriter, prefix string) {
	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	type content struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		Size         int    `xml:"Size"`
	}
	result := struct {
		XMLName     xml.Name  `xml:"ListBucketResult"`
		Name        string    `xml:"Name"`
		Prefix      string    `xml:"Prefix"`
		KeyCount    int       `xml:"KeyCount"`
		IsTruncated bool      `xml:"IsTruncated"`
		Contents    []content `xml:"Contents"`
	}{Name: testBucket, Prefix: prefix, KeyCount: len(keys)}
	for _, key := range keys {
		result.Contents = append(result.Contents, content{
			Key:          key,
			LastModified: time.Unix(1700000000, 0).UTC().Format(time.RFC3339),
			Size:         len(f.objects[key]),
		})
	}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

func (f *fakeS3) startMultipart(w http.ResponseWriter, key string) {
	f.mu.Lock()
	f.uploadSeq++
	id := "upload-" + strconv.Itoa(f.uploadSeq)
	f.multipart[id] = make(map[int32][]byte)
	f.mu.Unlock()

	fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`,
		testBucket, key, id)
}

func (f *fakeS3) uploadPart(w http.ResponseWriter, r *http.Request, uploadID, partNumber string) {
	n, err := strconv.Atoi(partNumber)
	if err != nil {
		http.Error(w, "bad part number", http.StatusBadRequest)
		return
	}
	data, _ := io.ReadAll(r.Body)

	f.mu.Lock()
	parts, ok := f.multipart[uploadID]
	if ok {
		parts[int32(n)] = data
	}
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, n))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) completeMultipart(w http.ResponseWriter, r *http.Request, key, uploadID string) {
	var doc struct {
		Parts []struct {
			PartNumber int32  `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&doc); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	parts, ok := f.multipart[uploadID]
	var assembled []byte
	if ok {
		for _, part := range doc.Parts {
			assembled = append(assembled, parts[part.PartNumber]...)
		}
		f.objects[key] = assembled
		delete(f.multipart, uploadID)
	}
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fmt.Fprintf(w, `<CompleteMultipartUploadResult><Key>%s</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`, key)
}

// verifySigV4 independently recomputes the request signature from the bytes
// actually received by the server.
func verifySigV4(r *http.Request, secret string) error {
	auth := r.Header.Get("Authorization")
	const prefix = "AWS4-HMAC-SHA256 "
	if !strings.HasPrefix(auth, prefix) {
		return fmt.Errorf("missing or malformed Authorization header")
	}
	var credential, signedHeaders, signature string
	for _, field := range strings.Split(strings.TrimPrefix(auth, prefix), ", ") {
		switch {
		case strings.HasPrefix(field, "Credential="):
			credential = strings.TrimPrefix(field, "Credential=")
		case strings.HasPrefix(field, "SignedHeaders="):
			signedHeaders = strings.TrimPrefix(field, "SignedHeaders=")
		case strings.HasPrefix(field, "Signature="):
			signature = strings.TrimPrefix(field, "Signature=")
		}
	}
	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 {
		return fmt.Errorf("malformed credential %q", credential)
	}

	var canonicalHeaders strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		value := r.Header.Get(name)
		if name == "host" {
			value = r.Host
		}
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.Join(strings.Fields(value), " "))
		canonicalHeaders.WriteByte('\n')
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return fmt.Errorf("missing X-Amz-Content-Sha256")
	}
	canonicalRequest := strings.Join([]string{
		r.Method,
		r.URL.EscapedPath(),
		r.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	amzDate := r.Header.Get("X-Amz-Date")
	scope := strings.Join(credParts[1:], "/")
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256HexTest([]byte(canonicalRequest))

	kDate := hmacTest([]byte("AWS4"+secret), credParts[1])
	kRegion := hmacTest(kDate, credParts[2])
	kService := hmacTest(kRegion, credParts[3])
	kSigning := hmacTest(kService, credParts[4])
	expected := hex.EncodeToString(hmacTest(kSigning, stringToSign))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func sha256HexTest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacTest(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(data))
	return h.Sum(nil)
}

func newHTTPTestFS(t *testing.T) (*FileSystem, *fakeS3) {
	t.Helper()
	fake, srv := newFakeS3(t, testSecretKey)
	fs, err := New(&Config{
		Bucket:    testBucket,
		Region:    "us-east-1",
		Endpoint:  srv.URL,
		AccessKey: testAccessKey,
		SecretKey: testSecretKey,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return fs, fake
}

func TestHTTPClientRoundTrip(t *testing.T) {
	fs, fake := newHTTPTestFS(t)

	f, err := fs.OpenFile("dir/file.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("hello s3")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake.mu.Lock()
	stored := string(fake.objects["dir/file.txt"])
	fake.mu.Unlock()
	if stored != "hello s3" {
		t.Fatalf("stored = %q, want %q", stored, "hello s3")
	}

	if err := fs.Mkdir("dir", 0755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := fs.Mkdir("dir", 0755); err == nil {
		t.Fatal("Mkdir on existing directory should fail")
	}
	if err := fs.Mkdir("other", 0755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	entries, err := fs.ReadDir("")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "dir,other" {
		t.Fatalf("ReadDir names = %v, want [dir other]", names)
	}

	if err := fs.Rename("dir/file.txt", "dir/moved.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	fake.mu.Lock()
	_, oldExists := fake.objects["dir/file.txt"]
	_, newExists := fake.objects["dir/moved.txt"]
	fake.mu.Unlock()
	if oldExists || !newExists {
		t.Fatalf("rename did not move the object (old=%v new=%v)", oldExists, newExists)
	}

	if err := fs.Remove("dir/moved.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := fs.Stat("dir/moved.txt"); !os.IsNotExist(err) {
		t.Fatalf("Stat after remove = %v, want os.ErrNotExist", err)
	}
}

func TestHTTPClientMultipart(t *testing.T) {
	fake, srv := newFakeS3(t, testSecretKey)
	fs, err := New(&Config{
		Bucket:    testBucket,
		Region:    "us-east-1",
		Endpoint:  srv.URL,
		AccessKey: testAccessKey,
		SecretKey: testSecretKey,
		PartSize:  4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	f, err := fs.OpenFile("big.bin", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	payload := []byte("abcdefghij") // 4 + 4 + 2
	if _, err := f.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake.mu.Lock()
	stored := string(fake.objects["big.bin"])
	fake.mu.Unlock()
	if !bytes.Equal([]byte(stored), payload) {
		t.Fatalf("stored = %q, want %q", stored, payload)
	}
}

func TestHTTPClientRejectsBadSignature(t *testing.T) {
	_, srv := newFakeS3(t, testSecretKey)
	fs, err := New(&Config{
		Bucket:    testBucket,
		Region:    "us-east-1",
		Endpoint:  srv.URL,
		AccessKey: testAccessKey,
		SecretKey: "wrong-secret",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := fs.ReadDir(""); err == nil {
		t.Fatal("expected signature failure")
	}
}

func TestAWSEncode(t *testing.T) {
	cases := map[string]string{
		"simple":        "simple",
		"a b":           "a%20b",
		"a+b":           "a%2Bb",
		"dir/file.txt":  "dir/file.txt",
		"dir/fïle.txt":  "dir/f%C3%AFle.txt",
		"tilde~_-.9AZz": "tilde~_-.9AZz",
	}
	for in, want := range cases {
		if got := awsEncode(in, false); got != want {
			t.Errorf("awsEncode(%q) = %q, want %q", in, got, want)
		}
	}
	if got := awsEncode("a/b", true); got != "a%2Fb" {
		t.Errorf("awsEncode(a/b, true) = %q, want a%%2Fb", got)
	}
}

func TestEncodeQuerySorts(t *testing.T) {
	got := encodeQuery(url.Values{
		"uploadId":   {"u 1"},
		"partNumber": {"2"},
	})
	want := "partNumber=2&uploadId=u%201"
	if got != want {
		t.Fatalf("encodeQuery = %q, want %q", got, want)
	}
}
