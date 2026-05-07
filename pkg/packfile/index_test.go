package packfile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildIndexFromEntries(t *testing.T) {
	// Create temp index file
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.sst")

	// Create test entries
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000002"),
			Offset:           200,
			SizeUncompressed: 60,
			SizeCompressed:   50,
		},
	}

	// Build index
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	// Verify index file exists
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	// Open index and verify
	reader, err := OpenIndex(context.Background(), indexPath)
	require.NoError(t, err)
	defer reader.Close()

	// Lookup first entry
	offset, sizeUncomp, sizeComp, err := reader.Get(context.Background(), entries[0].ContentHash)
	require.NoError(t, err)
	assert.Equal(t, int64(100), offset)
	assert.Equal(t, uint32(50), sizeUncomp)
	assert.Equal(t, uint32(40), sizeComp)

	// Lookup second entry
	offset, sizeUncomp, sizeComp, err = reader.Get(context.Background(), entries[1].ContentHash)
	require.NoError(t, err)
	assert.Equal(t, int64(200), offset)
	assert.Equal(t, uint32(60), sizeUncomp)
	assert.Equal(t, uint32(50), sizeComp)
}

func TestBuildIndexFromEntries_Sorted(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.sst")

	// Create entries in reverse order - should still work because we sort
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000003"),
			Offset:           300,
			SizeUncompressed: 70,
			SizeCompressed:   60,
		},
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000002"),
			Offset:           200,
			SizeUncompressed: 60,
			SizeCompressed:   50,
		},
	}

	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	reader, err := OpenIndex(context.Background(), indexPath)
	require.NoError(t, err)
	defer reader.Close()

	// Verify all entries are present and correct
	for _, entry := range entries {
		offset, sizeUncomp, sizeComp, err := reader.Get(context.Background(), entry.ContentHash)
		require.NoError(t, err, "entry %s not found", entry.ContentHash.String())
		assert.Equal(t, entry.Offset, offset)
		assert.Equal(t, entry.SizeUncompressed, sizeUncomp)
		assert.Equal(t, entry.SizeCompressed, sizeComp)
	}
}

func TestIndexReader_Get_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.sst")

	// Create empty index
	err := BuildIndexFromEntries(context.Background(), []IndexEntry{}, indexPath, "none")
	require.NoError(t, err)

	reader, err := OpenIndex(context.Background(), indexPath)
	require.NoError(t, err)
	defer reader.Close()

	// Lookup non-existent entry
	_, _, _, err = reader.Get(context.Background(), ContentHash{})
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestBuildIndex_SmallPackfile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test packfile
	packfilePath := filepath.Join(tmpDir, "test.pack")
	err := createTestPackfile(packfilePath, 100) // 100 objects
	require.NoError(t, err)

	// Build index
	indexPath, count, err := BuildIndex(context.Background(), packfilePath, tmpDir, "none")
	require.NoError(t, err)

	assert.Equal(t, 100, count)
	assert.Equal(t, packfilePath+".sst", indexPath)

	// Verify index is readable
	reader, err := OpenIndex(context.Background(), indexPath)
	require.NoError(t, err)
	defer reader.Close()
}

func TestBuildIndex_PackfileWithObjects(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a packfile with known content hashes
	packfilePath := filepath.Join(tmpDir, "test.pack")
	entries, err := createPackfileWithEntries(packfilePath, []uint32{100, 200, 300})
	require.NoError(t, err)

	// Build index
	indexPath, count, err := BuildIndex(context.Background(), packfilePath, tmpDir, "none")
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify entries are in index
	reader, err := OpenIndex(context.Background(), indexPath)
	require.NoError(t, err)
	defer reader.Close()

	for _, entry := range entries {
		offset, sizeUncomp, sizeComp, err := reader.Get(context.Background(), entry.ContentHash)
		require.NoError(t, err)
		assert.Equal(t, entry.Offset, offset)
		assert.Equal(t, entry.SizeUncompressed, sizeUncomp)
		assert.Equal(t, entry.SizeCompressed, sizeComp)
	}
}

// Helper: createTestPackfile creates a packfile with N random-looking objects
func createTestPackfile(packfilePath string, numObjects int) error {
	header := &PackfileHeader{
		Magic:               [4]byte{'L', 'K', 'P', '1'},
		Version:             1,
		ChecksumAlgo:       ChecksumSHA256,
		CompressionAlgo:    CompressionNone,
		Classification:     ClassificationFirstClass,
		ObjectCount:        uint64(numObjects),
		TotalSize:          0,
		TotalSizeCompressed: 0,
	}

	f, err := os.Create(packfilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := WritePackfileHeader(f, header); err != nil {
		return err
	}

	for i := 0; i < numObjects; i++ {
		// Create object data
		data := make([]byte, 32+i%100) // varying size
		for j := range data {
			data[j] = byte(i + j)
		}

		obj := &testDataReader{
			data:   data,
			offset: 0,
			size:   uint32(len(data)),
		}

		if err := WriteObject(f, obj); err != nil {
			return err
		}
	}

	// Write footer checksum
	footer := make([]byte, 32)
	_, err = f.Write(footer)
	return err
}

// Helper: createPackfileWithEntries creates a packfile with specified entries
func createPackfileWithEntries(packfilePath string, sizes []uint32) ([]IndexEntry, error) {
	header := &PackfileHeader{
		Magic:               [4]byte{'L', 'K', 'P', '1'},
		Version:             1,
		ChecksumAlgo:       ChecksumSHA256,
		CompressionAlgo:    CompressionNone,
		Classification:     ClassificationFirstClass,
		ObjectCount:        uint64(len(sizes)),
		TotalSize:          0,
		TotalSizeCompressed: 0,
	}

	f, err := os.Create(packfilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := WritePackfileHeader(f, header); err != nil {
		return nil, err
	}

	entries := make([]IndexEntry, 0, len(sizes))
	var offset int64 = 40

	for i, size := range sizes {
		// Create object with known content
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i*17 + j) // deterministic pattern
		}

		obj := &testDataReader{
			data:   data,
			offset: 0,
			size:   size,
		}

		// Get the hash that will be computed
		hash, err := obj.ContentHash()
		if err != nil {
			return nil, err
		}

		if err := WriteObject(f, obj); err != nil {
			return nil, err
		}

		entries = append(entries, IndexEntry{
			ContentHash:      hash,
			Offset:           offset,
			SizeUncompressed: size,
			SizeCompressed:   size, // no compression
		})

		offset += 40 + int64(size)
	}

	// Write footer
	footer := make([]byte, 32)
	f.Write(footer)

	return entries, nil
}

// testDataReader implements DataReader for testing
type testDataReader struct {
	data   []byte
	offset int
	size   uint32
}

func (r *testDataReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *testDataReader) ContentHash() (ContentHash, error) {
	h := sha256.New()
	h.Write(r.data)
	var hash ContentHash
	copy(hash[:], h.Sum(nil))
	return hash, nil
}

func (r *testDataReader) SizeUncompressed() uint32 {
	return r.size
}

// Helper to create a test content hash from hex string
func contentHashFromHex(hexStr string) ContentHash {
	var hash ContentHash
	decoded, _ := hex.DecodeString(hexStr)
	copy(hash[:], decoded)
	return hash
}
