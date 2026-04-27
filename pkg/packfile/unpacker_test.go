package packfile

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestUnpacker_GetObject(t *testing.T) {
	// Build a packfile with one object
	ws := newTestWriteSeeker()
	p := NewPacker(ws, CompressionNone, ClassificationSecondClass)
	data := []byte("hello world")
	offset, _, err := p.AppendObject(context.Background(), bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("AppendObject error = %v", err)
	}
	p.Close()

	// Read it back
	u := NewUnpacker(bytes.NewReader(ws.bytes()))
	h, r, size, compressedSize, err := u.GetObject(context.Background(), offset)
	if err != nil {
		t.Fatalf("GetObject error = %v", err)
	}
	if h == (ContentHash{}) {
		t.Error("content hash is zero")
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
	if size != int64(len(data)) {
		t.Errorf("size = %d, want %d", size, len(data))
	}
	if compressedSize != int64(len(data)) { // CompressionNone
		t.Errorf("compressedSize = %d, want %d", compressedSize, len(data))
	}
}

func TestUnpacker_Scan(t *testing.T) {
	ws := newTestWriteSeeker()
	p := NewPacker(ws, CompressionNone, ClassificationSecondClass)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("hello")), 5)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("world")), 5)
	p.Close()

	u := NewUnpacker(bytes.NewReader(ws.bytes()))
	iter := u.Scan(context.Background(), false)
	defer iter.Close()

	cnt := 0
	var contents []string
	for iter.Next() {
		_, _, _, _, r, err := iter.Object()
		if err != nil {
			t.Fatalf("iter.Object error = %v", err)
		}
		data, _ := io.ReadAll(r)
		contents = append(contents, string(data))
		cnt++
	}
	if cnt != 2 {
		t.Errorf("count = %d, want 2", cnt)
	}
	if len(contents) != 2 || contents[0] != "hello" || contents[1] != "world" {
		t.Errorf("contents = %v, want [hello world]", contents)
	}
}

func TestUnpacker_GetObject_Zstd(t *testing.T) {
	ws := newTestWriteSeeker()
	p := NewPacker(ws, CompressionZstd, ClassificationSecondClass)
	data := bytes.Repeat([]byte("hello world "), 100)
	offset, _, err := p.AppendObject(context.Background(), bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("AppendObject error = %v", err)
	}
	p.Close()

	u := NewUnpacker(bytes.NewReader(ws.bytes()))
	_, r, size, _, err := u.GetObject(context.Background(), offset)
	if err != nil {
		t.Fatalf("GetObject error = %v", err)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading object: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %d bytes, want %d", len(got), len(data))
	}
	if size != int64(len(data)) {
		t.Errorf("size = %d, want %d", size, len(data))
	}
}

func TestUnpacker_Scan_Empty(t *testing.T) {
	ws := newTestWriteSeeker()
	p := NewPacker(ws, CompressionNone, ClassificationSecondClass)
	p.Close()

	u := NewUnpacker(bytes.NewReader(ws.bytes()))
	iter := u.Scan(context.Background(), false)
	defer iter.Close()

	cnt := 0
	for iter.Next() {
		cnt++
	}
	if cnt != 0 {
		t.Errorf("count = %d, want 0", cnt)
	}
}

func TestUnpacker_GetObject_InvalidOffset(t *testing.T) {
	u := NewUnpacker(bytes.NewReader([]byte{}))
	_, _, _, _, err := u.GetObject(context.Background(), 100)
	if err == nil {
		t.Error("expected error for invalid offset")
	}
}

func TestUnpacker_Scan_MultipleObjects(t *testing.T) {
	ws := newTestWriteSeeker()
	p := NewPacker(ws, CompressionNone, ClassificationSecondClass)

	objs := []string{"a", "bc", "def", "xyzw"}
	for _, s := range objs {
		p.AppendObject(context.Background(), bytes.NewReader([]byte(s)), int64(len(s)))
	}
	p.Close()

	u := NewUnpacker(bytes.NewReader(ws.bytes()))
	iter := u.Scan(context.Background(), false)
	defer iter.Close()

	var contents []string
	for iter.Next() {
		_, _, _, _, r, err := iter.Object()
		if err != nil {
			t.Fatalf("iter.Object error = %v", err)
		}
		data, _ := io.ReadAll(r)
		contents = append(contents, string(data))
	}

	if len(contents) != len(objs) {
		t.Errorf("got %d objects, want %d", len(contents), len(objs))
	}
	for i, s := range objs {
		if contents[i] != s {
			t.Errorf("contents[%d] = %q, want %q", i, contents[i], s)
		}
	}
}

func TestUnpacker_Scan_IgnoreData(t *testing.T) {
	ws := newTestWriteSeeker()
	p := NewPacker(ws, CompressionNone, ClassificationSecondClass)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("hello")), 5)
	p.AppendObject(context.Background(), bytes.NewReader([]byte("world")), 5)
	p.Close()

	u := NewUnpacker(bytes.NewReader(ws.bytes()))
	iter := u.Scan(context.Background(), true) // ignoreData=true
	defer iter.Close()

	cnt := 0
	var hashes []ContentHash
	for iter.Next() {
		h, _, size, compressedSize, r, err := iter.Object()
		if err != nil {
			t.Fatalf("iter.Object error = %v", err)
		}
		if r != nil {
			t.Error("expected nil reader when ignoreData=true")
		}
		if size != 5 {
			t.Errorf("size = %d, want 5", size)
		}
		if compressedSize != 5 {
			t.Errorf("compressedSize = %d, want 5", compressedSize)
		}
		hashes = append(hashes, h)
		cnt++
	}
	if cnt != 2 {
		t.Errorf("count = %d, want 2", cnt)
	}
	if len(hashes) != 2 || hashes[0] == (ContentHash{}) || hashes[1] == (ContentHash{}) {
		t.Errorf("hashes not populated correctly")
	}
}
