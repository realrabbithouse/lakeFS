package packfile

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
)

var ErrInvalidContentHash = errors.New("invalid content hash")

// ContentHash represents a SHA-256 content hash.
// For packfiles, the ContentHash IS the packfile_id (content-addressed).
type ContentHash [32]byte

// ParseContentHash parses a bare hex content hash string (64 hex chars).
func ParseContentHash(s string) (ContentHash, error) {
	if len(s) != 64 {
		return ContentHash{}, ErrInvalidContentHash
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return ContentHash{}, ErrInvalidContentHash
	}
	var h ContentHash
	copy(h[:], b)
	return h, nil
}

// String returns the content hash as lowercase hex string.
func (h ContentHash) String() string {
	return fmt.Sprintf("%x", h[:])
}

// Bytes returns the raw 32-byte hash.
func (h ContentHash) Bytes() []byte {
	return h[:]
}

// Equal compares two ContentHashes.
func (h ContentHash) Equal(other ContentHash) bool {
	return h == other
}

// BigThan returns true if h > other (lexicographic comparison).
func (h ContentHash) BigThan(other ContentHash) bool {
	for i := range h {
		if h[i] != other[i] {
			return h[i] > other[i]
		}
	}
	return false
}

// StreamingHash computes a ContentHash incrementally as data is written.
// Use during AppendObject to hash as data streams in.
type StreamingHash struct {
	h hash.Hash
}

// NewStreamingHash creates a new StreamingHash.
func NewStreamingHash() *StreamingHash {
	return &StreamingHash{h: sha256.New()}
}

// Write updates the hash with data.
func (sh *StreamingHash) Write(p []byte) (int, error) {
	return sh.h.Write(p)
}

// Sum returns the final ContentHash.
func (sh *StreamingHash) Sum() ContentHash {
	return ContentHash(sh.h.Sum(nil))
}
