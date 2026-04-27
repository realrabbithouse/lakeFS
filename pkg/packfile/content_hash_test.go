package packfile

import (
	"fmt"
	"testing"
)

func TestContentHash_Parse(t *testing.T) {
	// 64-char valid hex string (SHA-256 of "test")
	s := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	h, err := ParseContentHash(s)
	if err != nil {
		t.Fatalf("ParseContentHash(%q) error = %v", s, err)
	}
	if fmt.Sprintf("%x", h[:]) != s {
		t.Errorf("ParseContentHash(%q) = %x, want %x", s, h[:], s)
	}
}

func TestContentHash_ParseInvalid(t *testing.T) {
	_, err := ParseContentHash("not-valid")
	if err == nil {
		t.Error("expected error for invalid hash")
	}
}

func TestContentHash_ParseTooShort(t *testing.T) {
	_, err := ParseContentHash("abc123")
	if err == nil {
		t.Error("expected error for short hash")
	}
}

func TestContentHash_String(t *testing.T) {
	h, _ := ParseContentHash("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
	s := h.String()
	if len(s) != 64 {
		t.Errorf("String() len = %d, want 64", len(s))
	}
}

func TestContentHash_Equal(t *testing.T) {
	// SHA-256 of "test1" and "test2" — different hashes
	h1, _ := ParseContentHash("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
	h2, _ := ParseContentHash("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
	h3, _ := ParseContentHash("bf86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
	if !h1.Equal(h2) {
		t.Error("Equal: same hashes should be equal")
	}
	if h1.Equal(h3) {
		t.Error("Equal: different hashes should not be equal")
	}
}

func TestContentHash_Bytes(t *testing.T) {
	h, _ := ParseContentHash("a3f2b1c9d4e5f6789012345678901234567890abcdef1234567890abcdef123456")
	b := h.Bytes()
	if len(b) != 32 {
		t.Errorf("Bytes() len = %d, want 32", len(b))
	}
}

func TestStreamingHash(t *testing.T) {
	h := NewStreamingHash()
	h.Write([]byte("hello world"))
	sum := h.Sum()
	// SHA256 of "hello world" = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
	want := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if fmt.Sprintf("%x", sum[:]) != want {
		t.Errorf("Sum() = %x, want %s", sum[:], want)
	}
}

func TestStreamingHash_MultipleWrites(t *testing.T) {
	h := NewStreamingHash()
	h.Write([]byte("hello "))
	h.Write([]byte("world"))
	sum := h.Sum()
	// SHA256 of "hello world" = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
	want := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if fmt.Sprintf("%x", sum[:]) != want {
		t.Errorf("Sum() = %x, want %s", sum[:], want)
	}
}

func TestStreamingHash_Empty(t *testing.T) {
	h := NewStreamingHash()
	sum := h.Sum()
	// SHA256 of "" = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if fmt.Sprintf("%x", sum[:]) != want {
		t.Errorf("Sum() = %x, want %s", sum[:], want)
	}
}
