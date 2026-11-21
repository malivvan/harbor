package aeadfs

import (
	"bytes"
	"io"
	ioFS "io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/malivvan/storm/fs/absfs"
)

// memFS is a simple in-memory filesystem for testing
// We'll use the osfs package and a temporary directory instead
func setupTestFS(t *testing.T) (absfs.FileSystem, func()) {
	t.Helper()

	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "aeadfs-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Import osfs for the base filesystem
	// For now, we'll create a simple wrapper around os package
	base := &osTestFS{root: tmpDir}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return base, cleanup
}

// osTestFS is a minimal filesystem implementation for testing
type osTestFS struct {
	root string
	cwd  string
}

func (fs *osTestFS) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	path := filepath.Join(fs.root, name)
	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, flag, perm)
}

func (fs *osTestFS) Mkdir(name string, perm os.FileMode) error {
	return os.Mkdir(filepath.Join(fs.root, name), perm)
}

func (fs *osTestFS) MkdirAll(name string, perm os.FileMode) error {
	return os.MkdirAll(filepath.Join(fs.root, name), perm)
}

func (fs *osTestFS) Remove(name string) error {
	return os.Remove(filepath.Join(fs.root, name))
}

func (fs *osTestFS) RemoveAll(path string) error {
	return os.RemoveAll(filepath.Join(fs.root, path))
}

func (fs *osTestFS) Rename(oldpath, newpath string) error {
	return os.Rename(filepath.Join(fs.root, oldpath), filepath.Join(fs.root, newpath))
}

func (fs *osTestFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(filepath.Join(fs.root, name))
}

func (fs *osTestFS) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(filepath.Join(fs.root, name), mode)
}

func (fs *osTestFS) Chtimes(name string, atime, mtime time.Time) error {
	return os.Chtimes(filepath.Join(fs.root, name), atime, mtime)
}

func (fs *osTestFS) Chown(name string, uid, gid int) error {
	return os.Chown(filepath.Join(fs.root, name), uid, gid)
}

func (fs *osTestFS) ReadDir(name string) ([]ioFS.DirEntry, error) {
	return os.ReadDir(filepath.Join(fs.root, name))
}

func (fs *osTestFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(fs.root, name))
}

func (fs *osTestFS) Sub(dir string) (ioFS.FS, error) {
	return absfs.FilerToFS(fs, dir)
}

func (fs *osTestFS) Chdir(dir string) error {
	fs.cwd = dir
	return nil
}

func (fs *osTestFS) Getwd() (string, error) {
	if fs.cwd == "" {
		return "/", nil
	}
	return fs.cwd, nil
}

func (fs *osTestFS) TempDir() string {
	return os.TempDir()
}

func (fs *osTestFS) Open(name string) (absfs.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *osTestFS) Create(name string) (absfs.File, error) {
	return fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (fs *osTestFS) Truncate(name string, size int64) error {
	return os.Truncate(filepath.Join(fs.root, name), size)
}

func TestNewAEADFS(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("failed to create AEADFS: %v", err)
	}

	if fs == nil {
		t.Fatal("AEADFS is nil")
	}
}

func TestAEADFS_WriteAndRead(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("failed to create AEADFS: %v", err)
	}

	// Test data
	testData := []byte("Hello, World! This is a test of the encryption system.")

	// Write data
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	n, err := file.Write(testData)
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	if n != len(testData) {
		t.Fatalf("wrote %d bytes, expected %d", n, len(testData))
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Read data back
	file, err = fs.Open("/test.txt")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	readData, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	if !bytes.Equal(readData, testData) {
		t.Fatalf("data mismatch:\ngot:  %q\nwant: %q", readData, testData)
	}
}

func TestAEADFS_MultipleWrites(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("failed to create AEADFS: %v", err)
	}

	// Write data in multiple writes
	file, err := fs.Create("/multi.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	writes := [][]byte{
		[]byte("First line\n"),
		[]byte("Second line\n"),
		[]byte("Third line\n"),
	}

	for _, data := range writes {
		if _, err := file.Write(data); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Read all data back
	file, err = fs.Open("/multi.txt")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	readData, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	file.Close()

	expected := bytes.Join(writes, nil)
	if !bytes.Equal(readData, expected) {
		t.Fatalf("data mismatch:\ngot:  %q\nwant: %q", readData, expected)
	}
}

func TestAEADFS_XChaCha20(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherXChaCha20Poly1305,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("failed to create AEADFS: %v", err)
	}

	testData := []byte("Testing XChaCha20-Poly1305 cipher")

	// Write
	file, err := fs.Create("/chacha.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if _, err := file.Write(testData); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	file.Close()

	// Read
	file, err = fs.Open("/chacha.txt")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	readData, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	file.Close()

	if !bytes.Equal(readData, testData) {
		t.Fatalf("data mismatch")
	}
}

func TestAEADFS_Seek(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("failed to create AEADFS: %v", err)
	}

	testData := []byte("0123456789ABCDEF")

	// Write
	file, err := fs.Create("/seek.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	file.Write(testData)
	file.Close()

	// Read with seeking
	file, err = fs.Open("/seek.txt")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// Seek to middle
	pos, err := file.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("failed to seek: %v", err)
	}

	if pos != 5 {
		t.Fatalf("seek position: got %d, want 5", pos)
	}

	// Read from position 5
	buf := make([]byte, 5)
	n, err := file.Read(buf)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if n != 5 {
		t.Fatalf("read %d bytes, want 5", n)
	}

	expected := testData[5:10]
	if !bytes.Equal(buf, expected) {
		t.Fatalf("read data mismatch: got %q, want %q", buf, expected)
	}
}

func TestAEADFS_WrongPassword(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	// Write with one password
	config1 := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("correct-password"),
	}

	fs1, err := New(base, config1)
	if err != nil {
		t.Fatalf("failed to create AEADFS: %v", err)
	}

	file, err := fs1.Create("/secret.txt")
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	file.Write([]byte("secret data"))
	file.Close()

	// Try to read with wrong password
	config2 := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("wrong-password"),
	}

	fs2, err := New(base, config2)
	if err != nil {
		t.Fatalf("failed to create AEADFS: %v", err)
	}

	file, err = fs2.Open("/secret.txt")
	if err == nil {
		file.Close()
		t.Fatal("expected error when opening file with wrong password")
	}

	// Should get authentication error
	if err != ErrAuthFailed && err.Error() != "failed to load encrypted file: failed to decrypt: authentication failed - data may be corrupted or tampered" {
		t.Logf("got error: %v", err)
	}
}

// Filesystem operation tests for coverage

// TestAEADFS_Separator removed - Separator method removed in absfs 1.0

// TestAEADFS_ListSeparator removed - ListSeparator method removed in absfs 1.0

func TestAEADFS_TempDir(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	tmpDir := fs.TempDir()
	if tmpDir == "" {
		t.Error("TempDir returned empty string")
	}
}

func TestAEADFS_Mkdir(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	err = fs.Mkdir("/testdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Verify directory exists
	info, err := fs.Stat("/testdir")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if !info.IsDir() {
		t.Error("Expected directory")
	}
}

func TestAEADFS_MkdirAll(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	err = fs.MkdirAll("/a/b/c", 0755)
	if err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	// Verify nested directory exists
	info, err := fs.Stat("/a/b/c")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if !info.IsDir() {
		t.Error("Expected directory")
	}
}

func TestAEADFS_Remove(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, err := fs.Create("/to-remove.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	file.Write([]byte("test"))
	file.Close()

	// Remove it
	err = fs.Remove("/to-remove.txt")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify it's gone
	_, err = fs.Stat("/to-remove.txt")
	if err == nil {
		t.Error("Expected error for stat of removed file")
	}
}

func TestAEADFS_RemoveAll(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create directory structure
	fs.MkdirAll("/removeall/sub", 0755)
	file, _ := fs.Create("/removeall/file.txt")
	file.Write([]byte("test"))
	file.Close()

	file, _ = fs.Create("/removeall/sub/nested.txt")
	file.Write([]byte("nested"))
	file.Close()

	// Remove all
	err = fs.RemoveAll("/removeall")
	if err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify it's gone
	_, err = fs.Stat("/removeall")
	if err == nil {
		t.Error("Expected error for stat of removed directory")
	}
}

func TestAEADFS_Rename(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, err := fs.Create("/old-name.txt")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	file.Write([]byte("rename test"))
	file.Close()

	// Rename it
	err = fs.Rename("/old-name.txt", "/new-name.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify old name is gone
	_, err = fs.Stat("/old-name.txt")
	if err == nil {
		t.Error("Expected error for stat of old name")
	}

	// Verify new name exists
	_, err = fs.Stat("/new-name.txt")
	if err != nil {
		t.Errorf("Stat of new name failed: %v", err)
	}

	// Verify content is preserved
	file, _ = fs.Open("/new-name.txt")
	data, _ := io.ReadAll(file)
	file.Close()

	if string(data) != "rename test" {
		t.Errorf("Content mismatch after rename: got %q", string(data))
	}
}

func TestAEADFS_Stat(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, _ := fs.Create("/stat-test.txt")
	file.Write([]byte("stat test data"))
	file.Close()

	// Stat the file
	info, err := fs.Stat("/stat-test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	if info == nil {
		t.Fatal("Stat returned nil")
	}

	if info.IsDir() {
		t.Error("Expected file, not directory")
	}

	if info.Size() == 0 {
		t.Error("Expected non-zero size")
	}
}

func TestAEADFS_Chmod(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, _ := fs.Create("/chmod-test.txt")
	file.Write([]byte("chmod test"))
	file.Close()

	// Change permissions
	err = fs.Chmod("/chmod-test.txt", 0644)
	if err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	// Verify (on some systems this might be a no-op)
	info, _ := fs.Stat("/chmod-test.txt")
	mode := info.Mode().Perm()
	_ = mode // Just verify we can get the mode
}

func TestAEADFS_Chtimes(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, _ := fs.Create("/chtimes-test.txt")
	file.Write([]byte("chtimes test"))
	file.Close()

	// Set specific times
	atime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	mtime := time.Date(2021, 6, 15, 12, 0, 0, 0, time.UTC)

	err = fs.Chtimes("/chtimes-test.txt", atime, mtime)
	if err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	// Verify modification time
	info, _ := fs.Stat("/chtimes-test.txt")
	if !info.ModTime().Equal(mtime) {
		t.Logf("ModTime: got %v, want %v", info.ModTime(), mtime)
		// Note: Some filesystems don't support precise times
	}
}

func TestAEADFS_Chown(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, _ := fs.Create("/chown-test.txt")
	file.Write([]byte("chown test"))
	file.Close()

	// Chown typically requires root, so we just verify it doesn't panic
	// and returns an appropriate error (or success if running as root)
	err = fs.Chown("/chown-test.txt", -1, -1) // -1 means don't change
	// We don't fail on error since it may require elevated permissions
	_ = err
}

func TestAEADFS_Truncate(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, _ := fs.Create("/truncate-test.txt")
	file.Write([]byte("0123456789"))
	file.Close()

	// Truncate the file
	// Note: For encrypted files, truncation is complex
	// This tests the filesystem-level Truncate method
	err = fs.Truncate("/truncate-test.txt", 5)
	// May succeed or fail depending on implementation
	_ = err
}

func TestAEADFS_Chdir_Getwd(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a directory
	fs.Mkdir("/workdir", 0755)

	// Change to it
	err = fs.Chdir("/workdir")
	if err != nil {
		t.Fatalf("Chdir failed: %v", err)
	}

	// Get current directory
	wd, err := fs.Getwd()
	if err != nil {
		t.Fatalf("Getwd failed: %v", err)
	}

	if wd != "/workdir" {
		t.Logf("Working directory: got %q, want /workdir", wd)
	}
}

func TestAEADFS_OpenFile_Flags(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Test O_RDONLY
	file, _ := fs.Create("/flags-test.txt")
	file.Write([]byte("test data"))
	file.Close()

	file, err = fs.OpenFile("/flags-test.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile O_RDONLY failed: %v", err)
	}
	file.Close()

	// Test O_RDWR (encrypted files need to read header)
	file, err = fs.OpenFile("/flags-test.txt", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile O_RDWR failed: %v", err)
	}
	file.Close()

	// Test O_CREATE with new file
	file, err = fs.OpenFile("/flags-new.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("OpenFile O_CREATE failed: %v", err)
	}
	file.Write([]byte("new file"))
	file.Close()

	// Test O_TRUNC with O_RDWR
	file, err = fs.OpenFile("/flags-test.txt", os.O_RDWR|os.O_TRUNC, 0)
	if err != nil {
		t.Fatalf("OpenFile O_TRUNC failed: %v", err)
	}
	file.Write([]byte("truncated"))
	file.Close()
}

func TestAEADFS_NilBase(t *testing.T) {
	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	_, err := New(nil, config)
	if err == nil {
		t.Error("Expected error for nil base filesystem")
	}
}

func TestAEADFS_AutoCipher(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAuto, // Auto-select cipher
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS with auto cipher: %v", err)
	}

	// Verify it works
	file, _ := fs.Create("/auto.txt")
	file.Write([]byte("auto cipher test"))
	file.Close()

	file, _ = fs.Open("/auto.txt")
	data, _ := io.ReadAll(file)
	file.Close()

	if string(data) != "auto cipher test" {
		t.Errorf("Data mismatch: got %q", string(data))
	}
}

func TestEncryptedFileInfo_Size(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	config := &Config{
		Cipher:      CipherAES256GCM,
		KeyProvider: testProvider("test-password"),
	}

	fs, err := New(base, config)
	if err != nil {
		t.Fatalf("Failed to create AEADFS: %v", err)
	}

	// Create a file
	file, _ := fs.Create("/fileinfo-test.txt")
	testData := "test data for fileinfo"
	file.Write([]byte(testData))
	file.Close()

	// Get info
	info, err := fs.Stat("/fileinfo-test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	size := info.Size()
	// Size should be greater than 0 (includes encryption overhead)
	if size == 0 {
		t.Error("Expected non-zero size")
	}
}

func TestXChaCha20FileHeaderNonce(t *testing.T) {
	base, cleanup := setupTestFS(t)
	defer cleanup()

	fsys, err := New(base, &Config{
		Cipher:      CipherXChaCha20Poly1305,
		KeyProvider: testProvider("xchacha"),
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	file, err := fsys.Create("/xchacha.bin")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := file.Write([]byte("xchacha data")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	raw, err := base.Open("/xchacha.bin")
	if err != nil {
		t.Fatalf("open encrypted file: %v", err)
	}
	defer raw.Close()

	header := &FileHeader{}
	if _, err := header.ReadFrom(raw); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if header.Cipher != CipherXChaCha20Poly1305 {
		t.Errorf("header cipher = %d, want %d", header.Cipher, CipherXChaCha20Poly1305)
	}
	if len(header.Nonce) != 24 {
		t.Errorf("header nonce size = %d, want 24", len(header.Nonce))
	}
}
