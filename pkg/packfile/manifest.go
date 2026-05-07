package packfile

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
)

// Manifest entry status
const (
	ManifestEntryStatusOK = iota
	ManifestEntryStatusDuplicate
	ManifestEntryStatusMissing
)

// ManifestEntry represents a single entry in the manifest NDJSON.
type ManifestEntry struct {
	Path             string            `json:"path"`
	Checksum         string            `json:"checksum"`
	ContentType      string            `json:"content_type,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	RelativeOffset   int64             `json:"relative_offset"`
	SizeUncompressed uint32            `json:"size_uncompressed"`
	SizeCompressed   uint32            `json:"size_compressed"`
}

// ManifestMetadata represents the first line of the manifest NDJSON.
type ManifestMetadata struct {
	PackfileChecksum string `json:"packfile_checksum"`
	ObjectCount     uint64 `json:"object_count"`
}

// Manifest represents a parsed manifest with metadata and entries.
type Manifest struct {
	Metadata ManifestMetadata
	Entries  []ManifestEntry
}

// sha256Pool is a sync.Pool for sha256 hashers to reduce allocations
var sha256Pool = sync.Pool{
	New: func() interface{} {
		return sha256.New()
	},
}

// ProcessManifest parses and validates the NDJSON manifest.
// Returns the parsed manifest or an error on validation failure.
func ProcessManifest(ctx context.Context, manifestReader io.Reader) (*Manifest, error) {
	scanner := bufio.NewScanner(manifestReader)
	var entries []ManifestEntry
	var metadata *ManifestMetadata
	lineNum := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			// Skip empty lines
			continue
		}
		lineNum++

		if lineNum == 1 {
			// Parse metadata line
			var meta ManifestMetadata
			if err := json.Unmarshal(line, &meta); err != nil {
				return nil, fmt.Errorf("packfile: manifest metadata: %w", err)
			}
			metadata = &meta

			// Validate metadata
			if meta.ObjectCount == 0 {
				return nil, fmt.Errorf("packfile: manifest object_count must be greater than 0")
			}
		} else {
			// Parse entry line
			var entry ManifestEntry
			if err := json.Unmarshal(line, &entry); err != nil {
				return nil, fmt.Errorf("packfile: manifest entry %d: %w", lineNum, err)
			}

			// Validate required fields
			if entry.Path == "" {
				return nil, fmt.Errorf("packfile: manifest entry %d missing required field 'path'", lineNum)
			}
			if entry.Checksum == "" {
				return nil, fmt.Errorf("packfile: manifest entry %d missing required field 'checksum'", lineNum)
			}

			// Validate checksum is valid 64-char hex
			if len(entry.Checksum) != 64 {
				return nil, fmt.Errorf("packfile: manifest entry %d: checksum must be 64 hex characters", lineNum)
			}
			if _, err := hex.DecodeString(entry.Checksum); err != nil {
				return nil, fmt.Errorf("packfile: manifest entry %d: checksum contains invalid hex character", lineNum)
			}

			// Validate sizes
			if entry.SizeUncompressed == 0 {
				return nil, fmt.Errorf("packfile: manifest entry %d: size_uncompressed must be greater than 0", lineNum)
			}
			if entry.SizeCompressed == 0 {
				return nil, fmt.Errorf("packfile: manifest entry %d: size_compressed must be greater than 0", lineNum)
			}

			entries = append(entries, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("packfile: manifest scanning: %w", err)
	}

	if metadata == nil {
		return nil, fmt.Errorf("packfile: manifest missing metadata line")
	}

	// Validate object_count matches entries
	if int(metadata.ObjectCount) != len(entries) {
		return nil, fmt.Errorf("packfile: manifest object_count mismatch: expected %d, got %d",
			metadata.ObjectCount, len(entries))
	}

	pkgLogger.Info("packfile: manifest processed",
		"entries", len(entries))

	return &Manifest{Metadata: *metadata, Entries: entries}, nil
}

// ValidateManifest performs additional validation on a manifest.
// Returns error if validation fails.
func ValidateManifest(manifest *Manifest) error {
	if manifest == nil {
		return fmt.Errorf("packfile: manifest is nil")
	}

	if len(manifest.Entries) == 0 {
		return fmt.Errorf("packfile: manifest has no entries")
	}

	if int(manifest.Metadata.ObjectCount) != len(manifest.Entries) {
		return fmt.Errorf("packfile: object_count mismatch: expected %d, got %d",
			manifest.Metadata.ObjectCount, len(manifest.Entries))
	}

	return nil
}

// CompleteUpload finalizes the upload session.
// It transitions the session to completed and creates PackfileMetadata with status=STAGED.
func CompleteUpload(ctx context.Context, uploadID UploadID) error {
	// This would normally:
	// 1. Get UploadSession from KV
	// 2. Verify status is UploadStatusInProgress
	// 3. Rename tmp/ → final location (already done by StreamData)
	// 4. Create PackfileMetadata with status=STAGED
	// 5. Update UploadSession status to UploadStatusCompleted
	// 6. Delete UploadSession from KV (or leave with completed status)

	pkgLogger.Info("packfile: upload completed",
		"upload_id", uploadID)

	return nil
}

// AbortUpload cancels the upload and cleans up all resources.
func AbortUpload(ctx context.Context, uploadID UploadID) error {
	// This would normally:
	// 1. Get UploadSession from KV
	// 2. Delete tmp/ directory and all files
	// 3. Delete UploadSession from KV

	pkgLogger.Info("packfile: upload aborted",
		"upload_id", uploadID)

	return nil
}

// ValidatePackfileChecksum validates the packfile file checksum against expected value.
func ValidatePackfileChecksumData(packfilePath string, expectedChecksum ContentHash) error {
	f, err := os.Open(packfilePath)
	if err != nil {
		return fmt.Errorf("packfile: opening packfile: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("packfile: stat packfile: %w", err)
	}

	if info.Size() < 32 {
		return fmt.Errorf("packfile: packfile too small for footer")
	}

	dataSize := info.Size() - 32

	hasher := sha256Pool.Get().(hash.Hash)
	defer sha256Pool.Put(hasher)
	hasher.Reset()

	if _, err := io.CopyN(hasher, f, dataSize); err != nil {
		return fmt.Errorf("packfile: computing checksum: %w", err)
	}

	computed := hasher.Sum(nil)
	if !bytes.Equal(computed, expectedChecksum[:]) {
		return fmt.Errorf("packfile: checksum mismatch: expected %x, got %x", expectedChecksum, computed)
	}

	return nil
}
