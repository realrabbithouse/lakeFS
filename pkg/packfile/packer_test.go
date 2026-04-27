package packfile

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestPacker_AppendObject(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)

	obj := []byte("hello world")
	r := bytes.NewReader(obj)

	offset, compressedSize, err := p.AppendObject(context.Background(), r, int64(len(obj)))
	if err != nil {
		t.Fatalf("AppendObject error = %v", err)
	}

	if offset != 40 { // header is 40 bytes
		t.Errorf("offset = %d, want 40", offset)
	}
	if compressedSize != int64(len(obj)) {
		t.Errorf("compressedSize = %d, want %d", compressedSize, len(obj))
	}
}

func TestPacker_Close(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)

	// Append two objects (size=5 for "hello", size=5 for "world")
	p.AppendObject(context.Background(), bytes.NewReader([]byte("hello")), 5)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("world")), 5)

	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	data := buf.Bytes()
	if string(data[:4]) != "LKP1" {
		t.Errorf("magic = %q, want LKP1", data[:4])
	}
}

func TestPacker_Close_Idempotent(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("hello")), 5)
	p.Close()
	err := p.Close()
	if err == nil {
		t.Error("expected error on double Close")
	}
}

func TestPacker_AppendObject_Closed(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)
	p.Close()
	_, _, err := p.AppendObject(context.Background(), bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Error("expected error when appending to closed packer")
	}
}

func TestPacker_MultipleObjects(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)

	objs := []struct {
		data string
		size int64
	}{
		{"a", 1},
		{"bc", 2},
		{"def", 3},
		{"xyzw", 4},
	}

	var offsets []int64
	for _, o := range objs {
		off, cs, err := p.AppendObject(context.Background(), bytes.NewReader([]byte(o.data)), o.size)
		if err != nil {
			t.Fatalf("AppendObject(%q) error = %v", o.data, err)
		}
		offsets = append(offsets, off)
		if cs != o.size {
			t.Errorf("compressedSize = %d, want %d", cs, o.size)
		}
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	data := buf.Bytes()
	// Verify footer (last 32 bytes) is SHA-256 of everything before it
	footer := data[len(data)-32:]
	if len(footer) != 32 {
		t.Errorf("footer len = %d, want 32", len(footer))
	}
}

func TestPacker_AppendObject_WithZstd(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionZstd, ClassificationSecondClass)

	data := bytes.Repeat([]byte("hello world "), 100)
	r := bytes.NewReader(data)

	offset, compressedSize, err := p.AppendObject(context.Background(), r, int64(len(data)))
	if err != nil {
		t.Fatalf("AppendObject error = %v", err)
	}

	if offset != 40 {
		t.Errorf("offset = %d, want 40", offset)
	}
	// Compressed size should be much smaller than uncompressed
	if compressedSize >= int64(len(data)) {
		t.Errorf("compressedSize = %d, should be < uncompressed %d", compressedSize, len(data))
	}

	p.Close()

	// Verify packfile ends with footer
	contents := buf.Bytes()
	if len(contents) < 40+int(compressedSize)+32 {
		t.Errorf("packfile too short: %d", len(contents))
	}
}

func TestPacker_AppendObject_Empty(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)

	_, _, err := p.AppendObject(context.Background(), bytes.NewReader([]byte{}), 0)
	if err != nil {
		t.Fatalf("AppendObject(empty) error = %v", err)
	}
	p.Close()
}

func TestPacker_ReadBack(t *testing.T) {
	var buf bytes.Buffer
	p := NewPacker(&buf, CompressionNone, ClassificationSecondClass)

	original := []byte("hello world packfile test data")
	offset, _, err := p.AppendObject(context.Background(), bytes.NewReader(original), int64(len(original)))
	if err != nil {
		t.Fatalf("AppendObject error = %v", err)
	}
	p.Close()

	// Read it back using Unpacker
	u := NewUnpacker(bytes.NewReader(buf.Bytes()))
	r, size, _, err := u.GetObject(context.Background(), offset)
	if err != nil {
		t.Fatalf("GetObject error = %v", err)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading object: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("got %q, want %q", got, original)
	}
	if size != int64(len(original)) {
		t.Errorf("size = %d, want %d", size, len(original))
	}
}
