package packfile

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/sstable"
)

// PackfileHandle is a reference to an open packfile with its SSTable reader.
// Must be closed when no longer needed.
type PackfileHandle struct {
	PackfileID   PackfileID
	IndexReader  *IndexReader
	DataFilePath string
	handle       *cacheHandle
}

// cacheHandle is internal - wraps the entry with a reference to the cache for closing
type cacheHandle struct {
	cache   *ReaderCache
	entry   *PackfileCacheEntry
	closed  bool
	mu      sync.Mutex
}

// Close decrements the reference count and closes the reader if refcount reaches zero.
// If refcount > 0, the entry remains in cache.
func (h *PackfileHandle) Close() error {
	h.handle.mu.Lock()
	defer h.handle.mu.Unlock()

	if h.handle.closed {
		return nil
	}
	h.handle.closed = true

	// Decrement refcount
	newCount := atomic.AddInt32(&h.handle.entry.Refcount, -1)
	pkgLogger.Info("packfile: cache close",
		"packfile_id", h.PackfileID,
		"refcount", newCount)

	if newCount == 0 {
		// Remove from LRU list (non-blocking, caller holds lock)
		h.handle.cache.removeFromLRU(h.handle.entry)
		// Also remove from entries map to prevent leak
		h.handle.cache.entries.Delete(h.handle.entry.PackfileID)
	}

	return nil
}

// ReaderCache provides L1 caching of SSTable readers with reference counting.
type ReaderCache struct {
	capacity int
	entries  sync.Map // map[PackfileID]*PackfileCacheEntry
	lru      []*PackfileCacheEntry // ordered by last access, for eviction
	mu       sync.Mutex
}

// PackfileCacheEntry holds a cached SSTable reader with reference counting.
type PackfileCacheEntry struct {
	PackfileID PackfileID
	Reader     *sstable.Reader
	Refcount   int32
	LastAccess time.Time
	FilePath   string
}

// NewReaderCache creates a new L1 cache with the given capacity.
func NewReaderCache(capacity int) *ReaderCache {
	return &ReaderCache{
		capacity: capacity,
	}
}

// Open returns a PackfileHandle for the given packfile.
// If the packfile is already in cache, returns the cached reader (refcount incremented).
// If not in cache, opens from disk and adds to cache.
func (c *ReaderCache) Open(ctx context.Context, packfileID PackfileID) (*PackfileHandle, error) {
	// Check if already in cache
	entry, ok := c.entries.Load(packfileID)
	if ok {
		// Cache hit
		cachedEntry := entry.(*PackfileCacheEntry)

		// Increment refcount
		newCount := atomic.AddInt32(&cachedEntry.Refcount, 1)

		// Re-check entry is still in cache (might have been evicted concurrently)
		if _, ok := c.entries.Load(packfileID); !ok {
			// Entry was evicted between Load and our increment - need to open fresh
			// Decrement the refcount we just incremented
			atomic.AddInt32(&cachedEntry.Refcount, -1)
			// Fall through to miss path
		} else {
			// Update LRU - move to end
			c.mu.Lock()
			c.updateLRU(cachedEntry)
			c.mu.Unlock()

			pkgLogger.Info("packfile: cache open",
				"packfile_id", packfileID,
				"hit", true,
				"refcount", newCount)

			return &PackfileHandle{
				PackfileID:   packfileID,
				IndexReader:  &IndexReader{reader: cachedEntry.Reader},
				DataFilePath: cachedEntry.FilePath,
				handle: &cacheHandle{
					cache: c,
					entry: cachedEntry,
				},
			}, nil
		}
	}

	// Cache miss - open from disk
	filePath := c.sstPath(packfileID)
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("packfile: opening SSTable file: %w", err)
	}
	reader, err := sstable.NewReader(f, sstable.ReaderOptions{})
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("packfile: opening SSTable reader: %w", err)
	}

	// Create new entry
	newEntry := &PackfileCacheEntry{
		PackfileID: packfileID,
		Reader:     reader,
		Refcount:   1, // Initial reference
		LastAccess: time.Now(),
		FilePath:   filePath,
	}

	// Use LoadOrStore to handle concurrent opens of the same packfileID
	// If another goroutine stored an entry first, close our reader and use theirs
	c.mu.Lock()
	actualEntry, loaded := c.entries.LoadOrStore(packfileID, newEntry)
	if loaded {
		// Another goroutine beat us - close our reader and use theirs
		reader.Close()
		f.Close()
		actualEntry.(*PackfileCacheEntry).Refcount++ // Increment their refcount
		entry = actualEntry
		c.updateLRU(actualEntry.(*PackfileCacheEntry))
	}
	// Check capacity and evict if needed (only when we added a new entry)
	if !loaded && c.len() >= c.capacity {
		c.evictLocked()
	}
	// Add to LRU only if we are the ones who stored
	if !loaded {
		c.lru = append(c.lru, newEntry)
	}
	c.mu.Unlock()

	if loaded {
		pkgLogger.Info("packfile: cache open",
			"packfile_id", packfileID,
			"hit", true,
			"refcount", atomic.LoadInt32(&actualEntry.(*PackfileCacheEntry).Refcount))
	} else {
		pkgLogger.Info("packfile: cache open",
			"packfile_id", packfileID,
			"hit", false,
			"refcount", int32(1))
	}

	if loaded {
		return &PackfileHandle{
			PackfileID:   packfileID,
			IndexReader:  &IndexReader{reader: actualEntry.(*PackfileCacheEntry).Reader},
			DataFilePath: actualEntry.(*PackfileCacheEntry).FilePath,
			handle: &cacheHandle{
				cache: c,
				entry: actualEntry.(*PackfileCacheEntry),
			},
		}, nil
	}
	return &PackfileHandle{
		PackfileID:   packfileID,
		IndexReader:  &IndexReader{reader: reader},
		DataFilePath: filePath,
		handle: &cacheHandle{
			cache: c,
			entry: newEntry,
		},
	}, nil
}

// sstPath returns the path to the SSTable index file for a packfile.
func (c *ReaderCache) sstPath(packfileID PackfileID) string {
	id := string(packfileID)
	// Reject path traversal attempts
	if strings.Contains(id, "..") {
		// Path traversal - this should not happen with valid PackfileIDs
		return ""
	}
	// Assuming packfiles are stored at <storage>/_packfiles/<repo_id>/<packfile_id>/data.sst
	// The index file is at data.sst
	return filepath.Join(id, "data.sst")
}

// len returns the number of entries in the cache (must hold mu).
func (c *ReaderCache) len() int {
	return len(c.lru)
}

// updateLRU moves an entry to the end of the LRU list (most recently used).
// Must hold mu.
func (c *ReaderCache) updateLRU(entry *PackfileCacheEntry) {
	for i, e := range c.lru {
		if e == entry {
			// Remove from current position
			c.lru = append(c.lru[:i], c.lru[i+1:]...)
			// Add to end
			c.lru = append(c.lru, entry)
			entry.LastAccess = time.Now()
			return
		}
	}
}

// removeFromLRU removes an entry from the LRU list.
// Must hold mu. Entry must have refcount == 0.
func (c *ReaderCache) removeFromLRU(entry *PackfileCacheEntry) {
	for i, e := range c.lru {
		if e == entry {
			c.lru = append(c.lru[:i], c.lru[i+1:]...)
			return
		}
	}
}

// evictLocked evicts the oldest entries until capacity is respected.
// Must hold mu. Closes readers when refcount reaches 0.
func (c *ReaderCache) evictLocked() {
	for c.len() >= c.capacity {
		if len(c.lru) == 0 {
			return
		}

		// Get oldest entry (front of LRU)
		entry := c.lru[0]

		// If refcount > 0, wait for it to reach 0
		iterations := 0
		for atomic.LoadInt32(&entry.Refcount) > 0 {
			// Yield to allow other goroutines to make progress
			runtime.Gosched()
			iterations++
			// Safety limit to prevent infinite spin (1M iterations ~ a few seconds)
			if iterations > 1000000 {
				// Skip this entry and move on (it will be retried on next eviction)
				return
			}
		}

		// Now refcount == 0, safe to evict
		c.lru = c.lru[1:] // Remove from front
		c.entries.Delete(entry.PackfileID)

		// Close the reader
		if err := entry.Reader.Close(); err != nil {
			pkgLogger.Info("packfile: reader close error",
				"packfile_id", entry.PackfileID,
				"error", err.Error())
			// Continue even on error - file handle leak is worse
		}

		pkgLogger.Info("packfile: cache eviction",
			"packfile_id", entry.PackfileID,
			"reason", "capacity")
	}
}

// Evict removes entries until capacity is respected.
// Called when cache is full and a new entry needs to be added.
// Blocks until refcount reaches zero for each evicted entry.
func (c *ReaderCache) Evict() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictLocked()
	return nil
}
