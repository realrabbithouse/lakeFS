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
