package s3fs

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

// BenchmarkMultipartWrite measures the multipart split and upload path against
// the in-memory S3 client at two payload sizes. The mock client is created
// fresh per iteration so state cannot accumulate across b.N.
func BenchmarkMultipartWrite(b *testing.B) {
	const partSize = 64 << 10

	for _, size := range []int{256 << 10, 1 << 20} {
		payload := bytes.Repeat([]byte("x"), size)

		b.Run(fmt.Sprintf("size=%d/part=%d", size, partSize), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mock := NewMockS3Client()
				filesystem := NewWithClient(mock, "bench-bucket")
				filesystem.partSize = partSize

				f, err := filesystem.OpenFile("large.bin", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := f.Write(payload); err != nil {
					b.Fatal(err)
				}
				if err := f.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
