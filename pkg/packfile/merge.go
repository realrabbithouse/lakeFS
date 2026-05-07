package packfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	// ErrMergeInProgress is returned when a merge is already in progress for the repository
	ErrMergeInProgress = errors.New("packfile: merge in progress, try again after retry-after duration")

	// ErrNonCommittedPackfile is returned when trying to merge a non-COMMITTED packfile
	ErrNonCommittedPackfile = errors.New("packfile: packfile is not in COMMITTED state")
)

// MergeResult contains the result of a merge operation
type MergeResult struct {
	NewPackfileID     PackfileID
	MergedCount       int // number of objects in new packfile
	DeduplicatedCount int // number of objects deduplicated
	SourceCount       int // total objects read from sources
}

// MergeInput contains the input parameters for a merge operation
type MergeInput struct {
	RepoID            string
	SourcePackfileIDs []PackfileID
	CompressionAlgo   uint8
	Classification    uint8 // FIRST_CLASS or SECOND_CLASS for new packfile
	ReaderCache       *ReaderCache
	StoragePath       string // base path for packfile storage
	LockManager       LockManager
}

// LockManager is the interface for distributed locking (no-op in single-node)
type LockManager interface {
	GetLock(repoID string) Lock
}

// Lock represents a distributed lock
type Lock interface {
	Acquire(ctx context.Context) error
	Release() error
	TryAcquire(ctx context.Context) (bool, error)
}

// NoOpLock is a lock that does nothing - for single-node mode
type NoOpLock struct{}

// Acquire always returns nil immediately
func (l *NoOpLock) Acquire(ctx context.Context) error {
	return nil
}

// Release does nothing
func (l *NoOpLock) Release() error {
	return nil
}

// TryAcquire always returns true immediately
func (l *NoOpLock) TryAcquire(ctx context.Context) (bool, error) {
	return true, nil
}

// NoOpLockManager is a lock manager that does nothing - for single-node mode
type NoOpLockManager struct{}

// GetLock returns a no-op lock
func (m *NoOpLockManager) GetLock(repoID string) Lock {
	return &NoOpLock{}
}

// IsRetryable returns true if the error indicates a retryable condition
func IsRetryable(err error) bool {
	return errors.Is(err, ErrMergeInProgress)
}

// Merge reads from source packfiles, deduplicates by content hash, and writes
// a new merged packfile with SSTable index.
// Returns the merged packfile metadata and merge statistics.
func Merge(ctx context.Context, input MergeInput) (*MergeResult, error) {
	if len(input.SourcePackfileIDs) == 0 {
		return nil, fmt.Errorf("packfile: no source packfiles provided")
	}

	// Default to no-op lock manager if not provided
	lockMgr := input.LockManager
	if lockMgr == nil {
		lockMgr = &NoOpLockManager{}
	}

	// Acquire lock
	lock := lockMgr.GetLock(input.RepoID)
	if err := lock.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("packfile: acquiring merge lock: %w", err)
	}
	defer lock.Release()

	pkgLogger.Info("packfile: merge started",
		"repo_id", input.RepoID,
		"sources", len(input.SourcePackfileIDs))

	// Phase 1: Read all source packfiles and deduplicate
	dedupMap, totalObjects, err := mergeDeduplicate(ctx, input.SourcePackfileIDs)
	if err != nil {
		return nil, fmt.Errorf("packfile: merging source packfiles: %w", err)
	}

	// Build sorted slice for writing (maintain deterministic order)
	objects := make([]*PackfileObject, 0, len(dedupMap))
	for _, obj := range dedupMap {
		objects = append(objects, obj)
	}
	sort.Slice(objects, func(i, j int) bool {
		return bytes.Compare(objects[i].ContentHash[:], objects[j].ContentHash[:]) < 0
	})

	deduplicatedCount := totalObjects - len(objects)
	pkgLogger.Info("packfile: merge deduplication complete",
		"repo_id", input.RepoID,
		"total_objects", totalObjects,
		"unique_objects", len(objects),
		"deduplicated", deduplicatedCount)

	// Phase 2: Write new merged packfile
	newPackfileID, err := writeMergedPackfile(ctx, input, objects)
	if err != nil {
		return nil, fmt.Errorf("packfile: writing merged packfile: %w", err)
	}

	pkgLogger.Info("packfile: merge completed",
		"repo_id", input.RepoID,
		"new_packfile_id", newPackfileID,
		"objects", len(objects),
		"deduplicated", deduplicatedCount)

	return &MergeResult{
		NewPackfileID:     newPackfileID,
		MergedCount:       len(objects),
		DeduplicatedCount: deduplicatedCount,
		SourceCount:       totalObjects,
	}, nil
}

// mergeDeduplicate reads all source packfiles and builds a deduplication map.
// Returns a map of content hash to PackfileObject (first-seen wins).
func mergeDeduplicate(ctx context.Context, sourcePackfileIDs []PackfileID) (map[ContentHash]*PackfileObject, int, error) {
	dedupMap := make(map[ContentHash]*PackfileObject)
	totalObjects := 0

	for _, packfileID := range sourcePackfileIDs {
		// Validate PackfileID to prevent path traversal
		if strings.Contains(string(packfileID), "..") {
			return nil, 0, fmt.Errorf("packfile: invalid packfileID contains path traversal: %s", packfileID)
		}

		// Packfile data is at {PackfileID}/data.pack
		packfilePath := filepath.Join(string(packfileID), "data.pack")

		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}

		// Open the packfile for reading
		f, err := os.Open(packfilePath)
		if err != nil {
			return nil, 0, fmt.Errorf("packfile: opening source packfile %s: %w", packfilePath, err)
		}

		// Parse header
		header, err := ParsePackfileHeader(f)
		if err != nil {
			f.Close()
			return nil, 0, fmt.Errorf("packfile: parsing source packfile header %s: %w", packfileID, err)
		}

		// Read all objects
		for i := uint64(0); i < header.ObjectCount; i++ {
			obj, err := ReadObject(f)
			if err != nil {
				f.Close()
				return nil, 0, fmt.Errorf("packfile: reading object %d from packfile %s: %w", i, packfileID, err)
			}

			// Verify content hash by recomputing
			data, _ := io.ReadAll(obj.Data)
			hasher := sha256.New()
			hasher.Write(data)
			var computedHash ContentHash
			copy(computedHash[:], hasher.Sum(nil))
			if computedHash != obj.ContentHash {
				f.Close()
				return nil, 0, fmt.Errorf("packfile: content hash mismatch for object %d in packfile %s", i, packfileID)
			}
			// Reset the reader for future use
			obj.Data = bytes.NewReader(data)

			totalObjects++

			// First-seen wins deduplication
			if _, exists := dedupMap[obj.ContentHash]; !exists {
				dedupMap[obj.ContentHash] = obj
			}
		}

		f.Close()
	}

	return dedupMap, totalObjects, nil
}

// writeMergedPackfile writes the deduplicated objects to a new packfile.
// Returns the new packfile ID.
func writeMergedPackfile(ctx context.Context, input MergeInput, objects []*PackfileObject) (PackfileID, error) {
	// Create temp file for writing
	tmpDir := filepath.Join(input.StoragePath, "_packfiles", input.RepoID)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", fmt.Errorf("packfile: creating temp directory: %w", err)
	}

	tmpFile := filepath.Join(tmpDir, fmt.Sprintf("merge-%d.tmp", time.Now().UnixNano()))
	f, err := os.Create(tmpFile)
	if err != nil {
		return "", fmt.Errorf("packfile: creating temp file: %w", err)
	}

	// Track whether rename succeeded for cleanup
	renamed := false
	finalPath := ""

	// Helper to clean up on error
	cleanup := func() {
		f.Close()
		if renamed && finalPath != "" {
			os.Remove(finalPath)
		} else if !renamed {
			os.Remove(tmpFile)
		}
	}

	// Calculate total sizes
	var totalSize, totalSizeCompressed uint64
	for _, obj := range objects {
		totalSize += uint64(obj.SizeUncompressed)
		totalSizeCompressed += uint64(obj.SizeCompressed)
	}

	// Write header
	header := &PackfileHeader{
		Magic:               [4]byte{'L', 'K', 'P', '1'},
		Version:             1,
		ChecksumAlgo:       ChecksumSHA256,
		CompressionAlgo:    input.CompressionAlgo,
		Classification:     input.Classification,
		ObjectCount:        uint64(len(objects)),
		TotalSize:          totalSize,
		TotalSizeCompressed: totalSizeCompressed,
	}

	if err := WritePackfileHeader(f, header); err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: writing header: %w", err)
	}

	// Write objects
	hasher := sha256.New()
	for _, obj := range objects {
		// Read data from the decompressed reader and write
		data, err := io.ReadAll(obj.Data)
		if err != nil {
			cleanup()
			return "", fmt.Errorf("packfile: reading object data: %w", err)
		}

		// Compress data if compression is enabled
		var writeData []byte
		switch input.CompressionAlgo {
		case CompressionZstd:
			writeData = data // TODO: implement actual zstd compression
		case CompressionGzip:
			writeData = data // TODO: implement actual gzip compression
		default:
			writeData = data
		}
		actualSizeCompressed := uint32(len(writeData))

		// Write object: size_uncompressed, size_compressed, content_hash, data
		var sizeUncompressedBuf [4]byte
		var sizeCompressedBuf [4]byte
		binary.BigEndian.PutUint32(sizeUncompressedBuf[:], obj.SizeUncompressed)
		binary.BigEndian.PutUint32(sizeCompressedBuf[:], actualSizeCompressed)

		if _, err := f.Write(sizeUncompressedBuf[:]); err != nil {
			cleanup()
			return "", fmt.Errorf("packfile: writing size_uncompressed: %w", err)
		}
		if _, err := f.Write(sizeCompressedBuf[:]); err != nil {
			cleanup()
			return "", fmt.Errorf("packfile: writing size_compressed: %w", err)
		}
		if _, err := f.Write(obj.ContentHash[:]); err != nil {
			cleanup()
			return "", fmt.Errorf("packfile: writing content_hash: %w", err)
		}
		if _, err := f.Write(writeData); err != nil {
			cleanup()
			return "", fmt.Errorf("packfile: writing data: %w", err)
		}

		// Update hasher for footer
		hasher.Write(sizeUncompressedBuf[:])
		hasher.Write(sizeCompressedBuf[:])
		hasher.Write(obj.ContentHash[:])
		hasher.Write(writeData)
	}

	// Write footer checksum
	footerChecksum := hasher.Sum(nil)
	if _, err := f.Write(footerChecksum); err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: writing footer checksum: %w", err)
	}

	// Close the file before renaming
	if err := f.Close(); err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: closing temp file: %w", err)
	}

	// Compute packfile ID from the completed file
	completedFile, err := os.Open(tmpFile)
	if err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: opening completed file: %w", err)
	}
	packfileID, err := ComputePackfileID(completedFile)
	completedFile.Close()
	if err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: computing packfile ID: %w", err)
	}

	// Rename to final location
	finalDir := filepath.Join(input.StoragePath, "_packfiles", input.RepoID, string(packfileID))
	if err := os.MkdirAll(finalDir, 0755); err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: creating final directory: %w", err)
	}
	finalPath = filepath.Join(finalDir, "data.pack")
	if err := os.Rename(tmpFile, finalPath); err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: renaming to final location: %w", err)
	}
	renamed = true

	// Build SSTable index - put it in the finalDir alongside the packfile
	_, _, err = BuildIndex(ctx, finalPath, finalDir, compressionAlgoToString(input.CompressionAlgo))
	if err != nil {
		cleanup()
		return "", fmt.Errorf("packfile: building SSTable index: %w", err)
	}

	return PackfileID(packfileID), nil
}

// compressionAlgoToString converts compression algorithm uint8 to string
func compressionAlgoToString(algo uint8) string {
	switch algo {
	case CompressionZstd:
		return "zstd"
	case CompressionGzip:
		return "gzip"
	case CompressionLz4:
		return "lz4"
	default:
		return "none"
	}
}
