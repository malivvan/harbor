package s3fs

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/malivvan/harbor/internal/absfs"
	"github.com/stretchr/testify/require"
)

func TestTruncate(t *testing.T) {
	fsys, mock := newTestFS()
	mock.PutTestObject("file", []byte("abcdef"))

	require.NoError(t, fsys.Truncate("file", 3))
	data, ok := mock.GetTestObject("file")
	require.True(t, ok)
	require.Equal(t, "abc", string(data))

	require.NoError(t, fsys.Truncate("file", 5))
	data, ok = mock.GetTestObject("file")
	require.True(t, ok)
	require.Equal(t, []byte{'a', 'b', 'c', 0, 0}, data)

	require.ErrorIs(t, fsys.Truncate("file", -1), os.ErrInvalid)
	require.ErrorIs(t, fsys.Truncate("file", maxObjectSize+1), os.ErrInvalid)
	require.Error(t, fsys.Truncate("missing", 1))
	require.Error(t, fsys.Truncate("../escape", 1))
}

func TestFileInfoDirEntry(t *testing.T) {
	file := &fileInfo{name: "f", size: 3, modTime: time.Unix(1, 0)}
	require.Equal(t, "f", file.Name())
	require.Equal(t, int64(3), file.Size())
	require.Equal(t, os.FileMode(0644), file.Mode())
	require.Equal(t, time.Unix(1, 0), file.ModTime())
	require.False(t, file.IsDir())
	require.Nil(t, file.Sys())
	require.Equal(t, fs.FileMode(0), file.Type())
	info, err := file.Info()
	require.NoError(t, err)
	require.Same(t, file, info)

	dir := &fileInfo{name: "d", isDir: true}
	require.Equal(t, os.ModeDir|0755, dir.Mode())
	require.True(t, dir.IsDir())
	require.Equal(t, fs.ModeDir, dir.Type())
}

func TestSubFSWrappers(t *testing.T) {
	fsys, mock := newTestFS()
	mock.PutTestObject("dir/", nil)
	mock.PutTestObject("dir/sub/", nil)
	mock.PutTestObject("dir/a.txt", []byte("A"))
	mock.PutTestObject("dir/sub/b.txt", []byte("B"))
	mock.PutTestObject("dir/old.txt", []byte("O"))

	sub := &subFS{parent: fsys, prefix: "dir"}
	require.Equal(t, "dir/a.txt", sub.fullPath("a.txt"))
	require.Equal(t, "dir", sub.fullPath(""))
	require.Equal(t, "a.txt", (&subFS{parent: fsys}).fullPath("a.txt"))

	data, err := sub.ReadFile("a.txt")
	require.NoError(t, err)
	require.Equal(t, "A", string(data))

	info, err := sub.Stat("a.txt")
	require.NoError(t, err)
	require.Equal(t, "a.txt", info.Name())

	entries, err := sub.ReadDir("")
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	f, err := sub.OpenFile("a.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, sub.Mkdir("newdir", 0755))
	require.NoError(t, sub.Rename("old.txt", "renamed.txt"))
	require.NoError(t, sub.Remove("renamed.txt"))
	require.ErrorIs(t, sub.Chmod("a.txt", 0600), absfs.ErrNotImplemented)
	require.ErrorIs(t, sub.Chtimes("a.txt", time.Now(), time.Now()), absfs.ErrNotImplemented)
	require.ErrorIs(t, sub.Chown("a.txt", 0, 0), absfs.ErrNotImplemented)

	nested, err := sub.Sub("sub")
	require.NoError(t, err)
	got, err := fs.ReadFile(nested, "b.txt")
	require.NoError(t, err)
	require.Equal(t, "B", string(got))

	rootSub, err := fsys.Sub("dir")
	require.NoError(t, err)
	got, err = fs.ReadFile(rootSub, "a.txt")
	require.NoError(t, err)
	require.Equal(t, "A", string(got))

	// An invalid sub path is rejected by sanitizePath.
	_, err = fsys.Sub("../escape")
	require.Error(t, err)
}

func TestParseRange(t *testing.T) {
	start, end, err := parseRange("bytes=0-9", 10)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(9), end)

	start, end, err = parseRange("bytes=5-", 10)
	require.NoError(t, err)
	require.Equal(t, int64(5), start)
	require.Equal(t, int64(9), end)

	start, end, err = parseRange("bytes=-5", 10)
	require.NoError(t, err)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(5), end)

	for _, bad := range []string{"0-9", "bytes=1", "bytes=a-b", "bytes=5-2", "bytes=0-99"} {
		_, _, err := parseRange(bad, 10)
		require.Error(t, err, bad)
	}
}

func TestHTTPStatusTextAndAPIError(t *testing.T) {
	require.Equal(t, "200 OK", httpStatusText(200))
	require.Equal(t, "599", httpStatusText(599))

	require.Equal(t, "s3fs: request failed with status 500 Internal Server Error",
		(&APIError{StatusCode: 500}).Error())
	require.Equal(t, "s3fs: Code (status 500 Internal Server Error)",
		(&APIError{StatusCode: 500, Code: "Code"}).Error())
	require.Equal(t, "s3fs: message (status 500 Internal Server Error)",
		(&APIError{StatusCode: 500, Message: "message"}).Error())
	require.Equal(t, "s3fs: Code: message (status 500 Internal Server Error)",
		(&APIError{StatusCode: 500, Code: "Code", Message: "message"}).Error())
}

func TestCredentialsResolution(t *testing.T) {
	// Explicit configuration.
	c, err := resolveCredentials(&Config{AccessKey: "ak", SecretKey: "sk", SessionToken: "st"})
	require.NoError(t, err)
	require.Equal(t, credentials{accessKey: "ak", secretKey: "sk", sessionToken: "st"}, c)

	_, err = resolveCredentials(&Config{AccessKey: "ak"})
	require.Error(t, err)

	// Environment.
	t.Setenv("AWS_ACCESS_KEY_ID", "env-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
	t.Setenv("AWS_SESSION_TOKEN", "env-st")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "")
	c, err = resolveCredentials(&Config{})
	require.NoError(t, err)
	require.Equal(t, credentials{accessKey: "env-ak", secretKey: "env-sk", sessionToken: "env-st"}, c)

	// Shared credentials file, selected by profile.
	dir := t.TempDir()
	credFile := filepath.Join(dir, "credentials")
	require.NoError(t, os.WriteFile(credFile, []byte(`
# comment
[default]
aws_access_key_id = file-ak
aws_secret_access_key = file-sk

[profile]
aws_access_key_id = profile-ak
aws_secret_access_key = profile-sk
aws_session_token = profile-st
bad line without equals
`), 0600))
	configFile := filepath.Join(dir, "config")
	require.NoError(t, os.WriteFile(configFile, []byte(`
[profile profile]
region = eu-west-1
[default]
region = us-west-2
`), 0600))

	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credFile)
	t.Setenv("AWS_CONFIG_FILE", configFile)
	t.Setenv("AWS_PROFILE", "profile")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")

	c, err = resolveCredentials(&Config{})
	require.NoError(t, err)
	require.Equal(t, credentials{accessKey: "profile-ak", secretKey: "profile-sk", sessionToken: "profile-st"}, c)
	require.Equal(t, "eu-west-1", resolveRegion(&Config{}))
	require.Equal(t, configFile, sharedConfigPath())

	// The default profile uses the plain [default] section.
	t.Setenv("AWS_PROFILE", "default")
	require.Equal(t, "us-west-2", sharedFileRegion())
	t.Setenv("AWS_PROFILE", "profile")

	// Region precedence.
	require.Equal(t, "explicit", resolveRegion(&Config{Region: "explicit"}))
	t.Setenv("AWS_REGION", "env-region")
	require.Equal(t, "env-region", resolveRegion(&Config{}))
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "default-region")
	require.Equal(t, "default-region", resolveRegion(&Config{}))
	t.Setenv("AWS_DEFAULT_REGION", "")
	require.Equal(t, "eu-west-1", resolveRegion(&Config{}))
}

func TestReadINISectionEdgeCases(t *testing.T) {
	require.Empty(t, readINISection("", "x"))
	require.Empty(t, readINISection(filepath.Join(t.TempDir(), "missing"), "x"))

	path := filepath.Join(t.TempDir(), "ini")
	require.NoError(t, os.WriteFile(path, []byte("; comment\n[s]\nKEY = value\nnoequals\n"), 0600))
	values := readINISection(path, "s")
	require.Equal(t, "value", values["key"])
}

func TestAbortMultipart(t *testing.T) {
	fsys, mock := newTestFS()
	_, err := mock.AbortMultipartUpload(t.Context(), &AbortMultipartUploadInput{UploadID: "missing"})
	require.NoError(t, err)

	f := &File{
		fs:          fsys,
		key:         "k",
		multipartID: "upload-1",
		parts:       []CompletedPart{{PartNumber: 1, ETag: "e"}},
		partNum:     1,
	}
	f.abortMultipartLocked()
	require.Empty(t, f.multipartID)
	require.Nil(t, f.parts)
	require.Zero(t, f.partNum)

	// A file without an active upload is a no-op.
	f.abortMultipartLocked()
	require.Empty(t, f.multipartID)
}
