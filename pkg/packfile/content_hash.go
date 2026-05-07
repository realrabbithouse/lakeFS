package packfile

import (
	"encoding/hex"
	"errors"
	"strings"
)

// ContentHash is a 32-byte SHA-256 content hash used as the content-addressed
// identity for objects within a packfile. The zero value is valid and represents
// all zeros (0000...0 with 64 hex chars).
type ContentHash [32]byte

// String returns the lowercase hex representation of the content hash (64 chars,
// no prefix). The zero value returns "0000...0".
func (h ContentHash) String() string {
	return hex.EncodeToString(h[:])
}

// ParseContentHash parses a 64-character hex string into a ContentHash.
// The string may contain uppercase or lowercase hex characters.
// Returns an error if the input is not exactly 64 hex characters.
func ParseContentHash(s string) (ContentHash, error) {
	var h ContentHash
	if len(s) != 64 {
		return ContentHash{}, errors.New("packfile: invalid content hash: invalid length")
	}
	decoded, err := hex.DecodeString(s)
	if err != nil {
		return ContentHash{}, errors.New("packfile: invalid content hash: invalid hex")
	}
	copy(h[:], decoded)
	return h, nil
}

// ContentHashFromBytes converts a 32-byte slice into a ContentHash.
// Returns an error if the slice is not exactly 32 bytes.
func ContentHashFromBytes(b []byte) (ContentHash, error) {
	var h ContentHash
	if len(b) != 32 {
		return ContentHash{}, errors.New("packfile: invalid content hash: must be 32 bytes")
	}
	copy(h[:], b)
	return h, nil
}

// PackfileID is a content-addressed identifier for a packfile.
// It is derived from the packfile's footer checksum (SHA-256 of header + data).
type PackfileID string

// ParsePackfileID parses a 64-character hex string into a PackfileID.
// Returns an error if the input is not exactly 64 hex characters.
func ParsePackfileID(s string) (PackfileID, error) {
	if len(s) != 64 {
		return "", errors.New("packfile: invalid packfile id: invalid length")
	}
	// Validate hex characters
	_, err := hex.DecodeString(s)
	if err != nil {
		return "", errors.New("packfile: invalid packfile id: invalid hex")
	}
	return PackfileID(s), nil
}

// UploadID is a session identifier for an in-progress upload.
// It is a ULID string - a monotonically increasing ID used as the upload token.
type UploadID string

// ParseUploadID validates a ULID string and returns an UploadID.
// Returns an error if the input is not a valid ULID format.
// Accepts both uppercase and lowercase ULIDs (case-insensitive).
func ParseUploadID(s string) (UploadID, error) {
	if len(s) != 26 {
		return "", errors.New("packfile: invalid upload id: must be 26 characters")
	}
	// ULID format: uppercase alphanumeric, Crockford base32 encoding
	// Accept both uppercase and lowercase by normalizing
	const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	normalized := strings.ToUpper(s)
	for _, c := range normalized {
		if c < 128 && !strings.Contains(ulidAlphabet, string(byte(c))) {
			return "", errors.New("packfile: invalid upload id: invalid ULID character")
		}
	}
	return UploadID(normalized), nil
}

// init performs self-validation of ContentHash parsing.
// This runs at package initialization time to catch any regression.
func init() {
	ch, err := ParseContentHash("0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		panic("packfile: ParseContentHash failed: " + err.Error())
	}
	if ch != (ContentHash{}) {
		panic("packfile: ContentHash zero value mismatch")
	}
}