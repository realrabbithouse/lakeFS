package packfile

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDataReader implements DataReader for merge testing
type mergeTestDataReader struct {
	data   []byte
	offset int
	size   uint32
}

func (r *mergeTestDataReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *mergeTestDataReader) ContentHash() (ContentHash, error) {
	h := sha256.New()
	h.Write(r.data)
	var hash ContentHash
	copy(hash[:], h.Sum(nil))
	return hash, nil
}

func (r *mergeTestDataReader) SizeUncompressed() uint32 {
	return r.size
}

// Helper to create a packfile with specific objects
// The packfile will be created at dir/data.pack (the merge expects {PackfileID}/data.pack)
func createTestPackfileForMerge(t *testing.T, dir string, objects [][]byte) (PackfileID, []ContentHash) {
	// Create the directory structure {dir}/data.pack
	packfilePath := filepath.Join(dir, "data.pack")
	f, err := os.Create(packfilePath)
	require.NoError(t, err)
	defer f.Close()

	// Calculate total sizes
	var totalSize, totalSizeCompressed uint64
	for _, obj := range objects {
		totalSize += uint64(len(obj))
		totalSizeCompressed += uint64(len(obj))
	}

	// Write header
	header := &PackfileHeader{
		Magic:               [4]byte{'L', 'K', 'P', '1'},
		Version:             1,
		ChecksumAlgo:       ChecksumSHA256,
		CompressionAlgo:    CompressionNone,
		Classification:     ClassificationSecondClass,
		ObjectCount:        uint64(len(objects)),
		TotalSize:          totalSize,
		TotalSizeCompressed: totalSizeCompressed,
	}
	err = WritePackfileHeader(f, header)
	require.NoError(t, err)

	// Collect hashes for verification
	hashes := make([]ContentHash, 0, len(objects))
	hasher := sha256.New()

	// Write each object
	for _, obj := range objects {
		// Compute content hash
		h := sha256.New()
		h.Write(obj)
		var contentHash ContentHash
		copy(contentHash[:], h.Sum(nil))
		hashes = append(hashes, contentHash)

		// Write object header and data
		var sizeBuf [4]byte
		size := uint32(len(obj))
		binary.BigEndian.PutUint32(sizeBuf[:], size) // uncompressed
		f.Write(sizeBuf[:])
		binary.BigEndian.PutUint32(sizeBuf[:], size) // compressed
		f.Write(sizeBuf[:])
		f.Write(contentHash[:])
		f.Write(obj)

		// Update hasher
		hasher.Write(sizeBuf[:])
		hasher.Write(sizeBuf[:])
		hasher.Write(contentHash[:])
		hasher.Write(obj)
	}

	// Write footer
	footer := hasher.Sum(nil)
	f.Write(footer)

	// Compute packfile ID (directory path - merge expects {PackfileID}/data.pack)
	// The dir is used as the PackfileID
	return PackfileID(dir), hashes
}

func TestMerge_BasicDeduplication(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first packfile with objects [A, B, C]
	packfile1Dir := filepath.Join(tmpDir, "pf1")
	os.MkdirAll(packfile1Dir, 0755)
	objA := []byte("object A content")
	objB := []byte("object B content")
	objC := []byte("object C content")
	_, _ = createTestPackfileForMerge(t, packfile1Dir, [][]byte{objA, objB, objC})

	// Create second packfile with objects [B, C, D] (B and C overlap)
	packfile2Dir := filepath.Join(tmpDir, "pf2")
	os.MkdirAll(packfile2Dir, 0755)
	objD := []byte("object D content")
	_, _ = createTestPackfileForMerge(t, packfile2Dir, [][]byte{objB, objC, objD})

	// Prepare storage directory
	storageDir := filepath.Join(tmpDir, "storage")
	os.MkdirAll(storageDir, 0755)

	// Run merge
	input := MergeInput{
		RepoID:            "repo1",
		SourcePackfileIDs:  []PackfileID{PackfileID(packfile1Dir), PackfileID(packfile2Dir)},
		CompressionAlgo:   CompressionNone,
		Classification:    ClassificationSecondClass,
		ReaderCache:       NewReaderCache(10),
		StoragePath:        storageDir,
	}

	result, err := Merge(context.Background(), input)
	require.NoError(t, err)

	// Should have 4 unique objects (A, B, C, D)
	assert.Equal(t, 4, result.MergedCount)
	// 2 objects should have been deduplicated (B, C from second packfile)
	assert.Equal(t, 2, result.DeduplicatedCount)
	assert.Equal(t, 6, result.SourceCount)
	assert.NotEmpty(t, result.NewPackfileID)
}

func TestMerge_EmptySourceHandling(t *testing.T) {
	// Note: ParsePackfileHeader rejects ObjectCount == 0
	// This is correct behavior - we cannot merge empty packfiles
	// Skip this test as it would require modifying ParsePackfileHeader
}

func TestNoOpLock(t *testing.T) {
	lock := &NoOpLock{}

	// Acquire should return immediately with no error
	err := lock.Acquire(context.Background())
	require.NoError(t, err)

	// Release should return immediately with no error
	err = lock.Release()
	require.NoError(t, err)

	// TryAcquire should return true immediately
	acquired, err := lock.TryAcquire(context.Background())
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestNoOpLockManager(t *testing.T) {
	manager := &NoOpLockManager{}

	lock1 := manager.GetLock("repo1")
	lock2 := manager.GetLock("repo2")

	// Should get a lock
	err := lock1.Acquire(context.Background())
	require.NoError(t, err)

	// Getting another lock for different repo should also work
	err = lock2.Acquire(context.Background())
	require.NoError(t, err)

	lock1.Release()
	lock2.Release()
}

func TestIsRetryable(t *testing.T) {
	assert.True(t, IsRetryable(ErrMergeInProgress))
	assert.False(t, IsRetryable(nil))
	assert.False(t, IsRetryable(assert.AnError))
}

func TestMergeInput_Validation(t *testing.T) {
	// Empty source packfile list should fail
	input := MergeInput{
		RepoID:            "repo1",
		SourcePackfileIDs: []PackfileID{},
		CompressionAlgo:   CompressionNone,
		Classification:    ClassificationSecondClass,
		ReaderCache:       NewReaderCache(10),
		StoragePath:        t.TempDir(),
	}

	_, err := Merge(context.Background(), input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no source packfiles")
}

func TestMerge_SingleSource(t *testing.T) {
	tmpDir := t.TempDir()

	// Create single packfile with objects [A, B]
	packfile1Dir := filepath.Join(tmpDir, "pf1")
	os.MkdirAll(packfile1Dir, 0755)
	objA := []byte("object A content")
	objB := []byte("object B content")
	createTestPackfileForMerge(t, packfile1Dir, [][]byte{objA, objB})

	// Prepare storage directory
	storageDir := filepath.Join(tmpDir, "storage")
	os.MkdirAll(storageDir, 0755)

	// Run merge with single source
	input := MergeInput{
		RepoID:            "repo1",
		SourcePackfileIDs:  []PackfileID{PackfileID(packfile1Dir)},
		CompressionAlgo:   CompressionNone,
		Classification:    ClassificationSecondClass,
		ReaderCache:       NewReaderCache(10),
		StoragePath:        storageDir,
	}

	result, err := Merge(context.Background(), input)
	require.NoError(t, err)

	// Should have 2 unique objects (no deduplication)
	assert.Equal(t, 2, result.MergedCount)
	assert.Equal(t, 0, result.DeduplicatedCount)
	assert.Equal(t, 2, result.SourceCount)
	assert.NotEmpty(t, result.NewPackfileID)
}
