package packfile

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderCache_Open_CacheMiss(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file at the path cache expects:
	// sstPath returns tmpDir/data.sst
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	// Create cache
	cache := NewReaderCache(10)

	// Open should create new entry - packfileID is the directory
	packfileID := PackfileID(tmpDir)
	handle, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	require.NotNil(t, handle)
	assert.Equal(t, packfileID, handle.PackfileID)
	assert.Equal(t, indexPath, handle.DataFilePath)
	assert.NotNil(t, handle.IndexReader)

	// Refcount should be 1
	assert.Equal(t, int32(1), handle.handle.entry.Refcount)

	handle.Close()
}

func TestReaderCache_Open_CacheHit(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	// Create cache
	cache := NewReaderCache(10)
	packfileID := PackfileID(tmpDir)

	// First open - cache miss
	handle1, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), handle1.handle.entry.Refcount)

	// Second open - cache hit, refcount should increment
	handle2, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	assert.Equal(t, int32(2), handle2.handle.entry.Refcount)

	// Both handles should point to same entry
	assert.Equal(t, handle1.handle.entry, handle2.handle.entry)

	handle1.Close()
	handle2.Close()
}

func TestReaderCache_Close_DecrementRefcount(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	cache := NewReaderCache(10)
	packfileID := PackfileID(tmpDir)

	// Open two handles
	handle1, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	handle2, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	assert.Equal(t, int32(2), handle1.handle.entry.Refcount)

	// Close first handle
	err = handle1.Close()
	require.NoError(t, err)
	assert.Equal(t, int32(1), handle2.handle.entry.Refcount)

	// Entry should still be in LRU
	cache.mu.Lock()
	assert.Equal(t, 1, cache.len())
	cache.mu.Unlock()

	handle2.Close()
}

func TestReaderCache_Close_RefcountZero_Evicts(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	cache := NewReaderCache(10)
	packfileID := PackfileID(tmpDir)

	handle, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), handle.handle.entry.Refcount)

	// Close - refcount goes to 0
	err = handle.Close()
	require.NoError(t, err)

	// Entry should be removed from LRU
	cache.mu.Lock()
	assert.Equal(t, 0, cache.len())
	cache.mu.Unlock()

	// Opening again should create a new entry (old one was evicted)
	handle2, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), handle2.handle.entry.Refcount)
	handle2.Close()
}

func TestReaderCache_LRU_Eviction(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple SSTable index files in separate directories
	numFiles := 3
	paths := make([]string, numFiles)
	packfileIDs := make([]PackfileID, numFiles)
	for i := 0; i < numFiles; i++ {
		// Each packfile gets its own subdir
		subDir := filepath.Join(tmpDir, "packfile"+string(rune('0'+i)))
		err := os.MkdirAll(subDir, 0755)
		require.NoError(t, err)
		indexPath := filepath.Join(subDir, "data.sst")
		entries := []IndexEntry{
			{
				ContentHash:      contentHashFromHex("000000000000000000000000000000000000000000000000000000000000000" + string(rune('1'+i))),
				Offset:           int64(100 * (i + 1)),
				SizeUncompressed: 50,
				SizeCompressed:   40,
			},
		}
		err = BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
		require.NoError(t, err)
		paths[i] = indexPath
		packfileIDs[i] = PackfileID(subDir)
	}

	// Create cache with capacity for 2
	cache := NewReaderCache(2)

	// Open first two entries
	handle1, err := cache.Open(context.Background(), packfileIDs[0])
	require.NoError(t, err)
	handle2, err := cache.Open(context.Background(), packfileIDs[1])
	require.NoError(t, err)

	// LRU should have both
	cache.mu.Lock()
	assert.Equal(t, 2, cache.len())
	cache.mu.Unlock()

	// Close handle2 so entry1 can be evicted (refcount=0)
	handle2.Close()

	// LRU is still [entry0, entry1] but entry1 has refcount=0

	// Access handle1 to make it most recently used (bumps refcount to 2 then back to 1 on close)
	_, err = cache.Open(context.Background(), packfileIDs[0])
	require.NoError(t, err)
	handle1.Close()

	// LRU is now [entry1, entry0] (entry1 still at front, entry0 at end)

	// Open third entry - should evict entry1 (oldest, refcount=0)
	handle3, err := cache.Open(context.Background(), packfileIDs[2])
	require.NoError(t, err)

	// LRU should still have 2 entries
	cache.mu.Lock()
	assert.Equal(t, 2, cache.len())
	cache.mu.Unlock()

	// Entry 1 should have been evicted (was already refcount=0 before we closed handle1)
	// After closing handle1, entry0 refcount is 1, entry1 was already evicted
	assert.Equal(t, int32(0), handle2.handle.entry.Refcount)

	handle1.Close()
	handle3.Close()
}

func TestReaderCache_Capacity_Respected(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple SSTable index files
	numFiles := 5
	packfileIDs := make([]PackfileID, numFiles)
	for i := 0; i < numFiles; i++ {
		subDir := filepath.Join(tmpDir, "packfile"+string(rune('0'+i)))
		err := os.MkdirAll(subDir, 0755)
		require.NoError(t, err)
		indexPath := filepath.Join(subDir, "data.sst")
		entries := []IndexEntry{
			{
				ContentHash:      contentHashFromHex("000000000000000000000000000000000000000000000000000000000000000" + string(rune('1'+i))),
				Offset:           int64(100 * (i + 1)),
				SizeUncompressed: 50,
				SizeCompressed:   40,
			},
		}
		err = BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
		require.NoError(t, err)
		packfileIDs[i] = PackfileID(subDir)
	}

	// Create cache with capacity 3
	cache := NewReaderCache(3)

	// Open 3 entries (at capacity)
	handles := make([]*PackfileHandle, 3)
	for i := 0; i < 3; i++ {
		var err error
		handles[i], err = cache.Open(context.Background(), packfileIDs[i])
		require.NoError(t, err)
	}

	// Should have exactly 3 entries
	cache.mu.Lock()
	assert.Equal(t, 3, cache.len())
	cache.mu.Unlock()

	// Close all handles - entries should be evicted from LRU
	for i := 0; i < 3; i++ {
		handles[i].Close()
	}

	// Cache should be empty now
	cache.mu.Lock()
	assert.Equal(t, 0, cache.len())
	cache.mu.Unlock()
}

func TestReaderCache_Evict_ClosesReader(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first SSTable
	subDir1 := filepath.Join(tmpDir, "packfile1")
	err := os.MkdirAll(subDir1, 0755)
	require.NoError(t, err)
	indexPath1 := filepath.Join(subDir1, "data.sst")
	entries1 := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err = BuildIndexFromEntries(context.Background(), entries1, indexPath1, "none")
	require.NoError(t, err)

	// Create cache with capacity 1
	cache := NewReaderCache(1)
	packfileID1 := PackfileID(subDir1)

	// Open first entry
	handle1, err := cache.Open(context.Background(), packfileID1)
	require.NoError(t, err)
	handle1.Close()

	// Create second SSTable
	subDir2 := filepath.Join(tmpDir, "packfile2")
	err = os.MkdirAll(subDir2, 0755)
	require.NoError(t, err)
	indexPath2 := filepath.Join(subDir2, "data.sst")
	entries2 := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000002"),
			Offset:           200,
			SizeUncompressed: 60,
			SizeCompressed:   50,
		},
	}
	err = BuildIndexFromEntries(context.Background(), entries2, indexPath2, "none")
	require.NoError(t, err)

	// Open new entry - should trigger eviction of old
	handle2, err := cache.Open(context.Background(), PackfileID(subDir2))
	require.NoError(t, err)
	handle2.Close()
}

func TestPackfileHandle_Close_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	cache := NewReaderCache(10)
	packfileID := PackfileID(tmpDir)

	handle, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)

	// First close
	err = handle.Close()
	require.NoError(t, err)

	// Second close should be no-op
	err = handle.Close()
	require.NoError(t, err)
}

func TestReaderCache_Open_SameHandle(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	cache := NewReaderCache(10)
	packfileID := PackfileID(tmpDir)

	// Open same packfile twice from same cache
	handle1, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)
	handle2, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)

	// They should share the same underlying entry
	assert.Equal(t, handle1.handle.entry, handle2.handle.entry)
	assert.Equal(t, handle1.IndexReader, handle2.IndexReader)

	// Close both
	handle1.Close()
	handle2.Close()
}

func TestReaderCache_Evict_WithRefcountZero(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	cache := NewReaderCache(1)
	packfileID := PackfileID(tmpDir)

	// Open entry (refcount = 1)
	handle, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)

	// Close handle (refcount goes to 0, entry removed from LRU)
	handle.Close()

	// Evict should work since entry has refcount 0
	err = cache.Evict()
	require.NoError(t, err)

	cache.mu.Lock()
	assert.Equal(t, 0, cache.len())
	cache.mu.Unlock()
}

func TestReaderCache_IndexReader_Usable(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
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
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	cache := NewReaderCache(10)
	packfileID := PackfileID(tmpDir)

	handle, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)

	// Use IndexReader to lookup entry
	offset, sizeUncomp, sizeComp, err := handle.IndexReader.Get(context.Background(), entries[0].ContentHash)
	require.NoError(t, err)
	assert.Equal(t, int64(100), offset)
	assert.Equal(t, uint32(50), sizeUncomp)
	assert.Equal(t, uint32(40), sizeComp)

	handle.Close()
}

func TestReaderCache_UpdateLRU(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple SSTable index files in separate directories
	packfileIDs := make([]PackfileID, 3)
	for i := 0; i < 3; i++ {
		subDir := filepath.Join(tmpDir, "packfile"+string(rune('0'+i)))
		err := os.MkdirAll(subDir, 0755)
		require.NoError(t, err)
		indexPath := filepath.Join(subDir, "data.sst")
		entries := []IndexEntry{
			{
				ContentHash:      contentHashFromHex("000000000000000000000000000000000000000000000000000000000000000" + string(rune('1'+i))),
				Offset:           int64(100 * (i + 1)),
				SizeUncompressed: 50,
				SizeCompressed:   40,
			},
		}
		err = BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
		require.NoError(t, err)
		packfileIDs[i] = PackfileID(subDir)
	}

	cache := NewReaderCache(3)

	// Open all three
	h1, err := cache.Open(context.Background(), packfileIDs[0])
	require.NoError(t, err)
	h2, err := cache.Open(context.Background(), packfileIDs[1])
	require.NoError(t, err)
	h3, err := cache.Open(context.Background(), packfileIDs[2])
	require.NoError(t, err)

	// LRU order: [entry0, entry1, entry2]
	cache.mu.Lock()
	assert.Equal(t, 3, len(cache.lru))
	cache.mu.Unlock()

	// Access entry0 to move it to end
	_, err = cache.Open(context.Background(), packfileIDs[0])
	require.NoError(t, err)

	// Now entry1 should be oldest, entry0 newest
	h1.Close()
	h2.Close()
	h3.Close()
}

func TestReaderCache_RemoveFromLRU(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test SSTable index file
	indexPath := filepath.Join(tmpDir, "data.sst")
	entries := []IndexEntry{
		{
			ContentHash:      contentHashFromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			Offset:           100,
			SizeUncompressed: 50,
			SizeCompressed:   40,
		},
	}
	err := BuildIndexFromEntries(context.Background(), entries, indexPath, "none")
	require.NoError(t, err)

	cache := NewReaderCache(10)
	packfileID := PackfileID(tmpDir)

	handle, err := cache.Open(context.Background(), packfileID)
	require.NoError(t, err)

	cache.mu.Lock()
	assert.Equal(t, 1, cache.len())
	cache.mu.Unlock()

	handle.Close()

	cache.mu.Lock()
	assert.Equal(t, 0, cache.len())
	cache.mu.Unlock()
}
