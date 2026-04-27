package packfile

import (
	"bytes"
	"io"
	"testing"
)

func TestCompressTo_Zstd(t *testing.T) {
	data := bytes.Repeat([]byte("hello world "), 1000)

	var buf bytes.Buffer
	err := CompressTo(bytes.NewReader(data), CompressionZstd, &buf)
	if err != nil {
		t.Fatalf("CompressTo error = %v", err)
	}

	// buf now contains compressed data
	var out bytes.Buffer
	err = UncompressTo(&buf, CompressionZstd, &out)
	if err != nil {
		t.Fatalf("UncompressTo error = %v", err)
	}

	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("uncompressed data mismatch, got %d bytes, want %d", out.Len(), len(data))
	}
}

func TestCompressTo_None(t *testing.T) {
	data := []byte("hello world")

	var buf bytes.Buffer
	err := CompressTo(bytes.NewReader(data), CompressionNone, &buf)
	if err != nil {
		t.Fatalf("CompressTo error = %v", err)
	}

	var out bytes.Buffer
	err = UncompressTo(&buf, CompressionNone, &out)
	if err != nil {
		t.Fatalf("UncompressTo error = %v", err)
	}

	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("uncompressed data mismatch")
	}
}

func TestCompressTo_Zstd_LargeData(t *testing.T) {
	// Test with larger data to ensure streaming works
	data := bytes.Repeat([]byte("x"), 1<<20) // 1MB

	var buf bytes.Buffer
	err := CompressTo(bytes.NewReader(data), CompressionZstd, &buf)
	if err != nil {
		t.Fatalf("CompressTo error = %v", err)
	}

	var out bytes.Buffer
	err = UncompressTo(&buf, CompressionZstd, &out)
	if err != nil {
		t.Fatalf("UncompressTo error = %v", err)
	}

	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("large data mismatch: got %d bytes, want %d", out.Len(), len(data))
	}
}

func TestUncompressTo_Unsupported(t *testing.T) {
	data := []byte("test")
	err := UncompressTo(bytes.NewReader(data), Compression(99), io.Discard)
	if err != ErrUnsupportedCompression {
		t.Errorf("UncompressTo(Compression(99)) = %v, want ErrUnsupportedCompression", err)
	}
}

func TestCompressTo_Unsupported(t *testing.T) {
	data := []byte("test")
	err := CompressTo(bytes.NewReader(data), Compression(99), io.Discard)
	if err != ErrUnsupportedCompression {
		t.Errorf("CompressTo(Compression(99)) = %v, want ErrUnsupportedCompression", err)
	}
}

func TestCompressTo_Zstd_Empty(t *testing.T) {
	data := []byte{}

	var buf bytes.Buffer
	err := CompressTo(bytes.NewReader(data), CompressionZstd, &buf)
	if err != nil {
		t.Fatalf("CompressTo error = %v", err)
	}

	var out bytes.Buffer
	err = UncompressTo(&buf, CompressionZstd, &out)
	if err != nil {
		t.Fatalf("UncompressTo error = %v", err)
	}

	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("empty data mismatch")
	}
}

func TestNewCompressionReader_Zstd(t *testing.T) {
	data := bytes.Repeat([]byte("hello world "), 100)

	var buf bytes.Buffer
	r, err := NewCompressionReader(bytes.NewReader(data), CompressionZstd)
	if err != nil {
		t.Fatalf("NewCompressionReader error = %v", err)
	}
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy error = %v", err)
	}

	// Decompress and verify
	var out bytes.Buffer
	if err := UncompressTo(&buf, CompressionZstd, &out); err != nil {
		t.Fatalf("UncompressTo error = %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("got %d bytes, want %d", out.Len(), len(data))
	}
}

func TestNewCompressionReader_None(t *testing.T) {
	data := []byte("hello world")
	r, err := NewCompressionReader(bytes.NewReader(data), CompressionNone)
	if err != nil {
		t.Fatalf("NewCompressionReader error = %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("io.ReadAll error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestNewCompressionReader_Unsupported(t *testing.T) {
	_, err := NewCompressionReader(bytes.NewReader([]byte("test")), Compression(99))
	if err != ErrUnsupportedCompression {
		t.Errorf("got %v, want ErrUnsupportedCompression", err)
	}
}

func TestNewDecompressionReader_None(t *testing.T) {
	data := []byte("hello world")
	r, err := NewDecompressionReader(bytes.NewReader(data), CompressionNone)
	if err != nil {
		t.Fatalf("NewDecompressionReader error = %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("io.ReadAll error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestNewDecompressionReader_Unsupported(t *testing.T) {
	_, err := NewDecompressionReader(bytes.NewReader([]byte("test")), Compression(99))
	if err != ErrUnsupportedCompression {
		t.Errorf("got %v, want ErrUnsupportedCompression", err)
	}
}

func TestNewDecompressionReader_Zstd(t *testing.T) {
	data := bytes.Repeat([]byte("hello world "), 100)

	// Compress first
	var buf bytes.Buffer
	if err := CompressTo(bytes.NewReader(data), CompressionZstd, &buf); err != nil {
		t.Fatalf("CompressTo error = %v", err)
	}

	// Use NewDecompressionReader
	r, err := NewDecompressionReader(bytes.NewReader(buf.Bytes()), CompressionZstd)
	if err != nil {
		t.Fatalf("NewDecompressionReader error = %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("io.ReadAll error = %v", err)
	}
	if closer, ok := r.(io.Closer); ok {
		closer.Close()
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %d bytes, want %d", len(got), len(data))
	}
}

