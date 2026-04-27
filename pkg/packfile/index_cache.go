package packfile

import (
	"fmt"

	"github.com/cockroachdb/pebble/sstable"
	lru "github.com/hnlq715/golang-lru"
)

// IndexCache provides LRU caching of open SSTable readers for packfile indices.
// It avoids repeated file opens and SSTable parsing when accessing the same packfile's index.
type IndexCache struct {
	cache *lru.Cache
}

// NewIndexCache creates a new index cache with the specified maximum number of entries.
// When entries are evicted, the corresponding SSTable readers are closed.
func NewIndexCache(maxEntries int) (*IndexCache, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("max entries must be positive, got %d", maxEntries)
	}

	evictedFunc := func(k, v interface{}) {
		if reader, ok := v.(*sstable.Reader); ok {
			_ = reader.Close()
		}
	}

	cache, err := lru.NewWithEvict(maxEntries, evictedFunc)
	if err != nil {
		return nil, fmt.Errorf("creating LRU cache: %w", err)
	}
	return &IndexCache{cache: cache}, nil
}

// Get returns an open SSTable reader for the given packfile ID, or nil if not cached.
func (c *IndexCache) Get(packfileID string) *sstable.Reader {
	if c == nil || c.cache == nil {
		return nil
	}
	val, ok := c.cache.Get(packfileID)
	if !ok {
		return nil
	}
	return val.(*sstable.Reader)
}

// Put stores an SSTable reader in the cache. If the cache is full, the least
// recently used reader will be evicted and closed.
func (c *IndexCache) Put(packfileID string, reader *sstable.Reader) {
	if c == nil || c.cache == nil {
		return
	}
	c.cache.Add(packfileID, reader)
}

// Close closes all cached SSTable readers and clears the cache.
func (c *IndexCache) Close() error {
	if c == nil || c.cache == nil {
		return nil
	}

	c.cache.Purge()
	return nil
}