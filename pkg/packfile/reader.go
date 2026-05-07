package packfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/treeverse/lakefs/pkg/block"
)

var (
	// ErrPackfileUnavailable is returned when a packfile cannot be found in any tier
	ErrPackfileUnavailable = fmt.Errorf("packfile unavailable in any storage tier")
)

// PackfileReader provides random access to packfile objects via content hash lookup.
// It implements a 3-tier cache hierarchy: L1 (memory), L2 (local disk), L3 (object storage).
type PackfileReader struct {
	cache   *ReaderCache
	adapter block.Adapter
}

// NewPackfileReader creates a new PackfileReader with the given L1 cache and block adapter.
func NewPackfileReader(cache *ReaderCache, adapter block.Adapter) *PackfileReader {
	return &PackfileReader{
		cache:   cache,
		adapter: adapter,
	}
}

// Open returns a PackfileHandle for the given storage namespace and packfile ID.
// It checks in order: L1 cache, L2 local disk, L3 object storage.
func (r *PackfileReader) Open(ctx context.Context, storageNamespace string, packfileID PackfileID) (*PackfileHandle, error) {
	// Try L1 cache first
	handle, err := r.cache.Open(ctx, packfileID)
	if err == nil {
		// L1 hit - open data file if not already open
		if handle.DataFile == nil {
			dataPath := r.dataPath(storageNamespace, packfileID)
			f, err := os.Open(dataPath)
			if err != nil {
				handle.Close()
				return nil, fmt.Errorf("packfile: opening data file: %w", err)
			}
			handle.DataFile = f
		}

		pkgLogger.Info("packfile: reader open cache_hit",
			"packfile_id", packfileID)
		return handle, nil
	}

	// L1 miss - try L2 (local disk)
	sstPath := r.sstPath(storageNamespace, packfileID)
	dataPath := r.dataPath(storageNamespace, packfileID)

	// Open SSTable and data file from L2
	sstFile, err := os.Open(sstPath)
	if err == nil {
		// L2 hit - open from local disk
		reader, err := OpenIndex(ctx, sstPath)
		if err != nil {
			sstFile.Close()
			return nil, fmt.Errorf("packfile: opening SSTable reader: %w", err)
		}

		// Open data file
		dataFile, err := os.Open(dataPath)
		if err != nil {
			sstFile.Close()
			return nil, fmt.Errorf("packfile: opening data file: %w", err)
		}

		// Add to L1 cache
		entry, ok := r.cache.entries.Load(packfileID)
		if !ok {
			// Create new entry if not already added by concurrent operation
			newEntry := &PackfileCacheEntry{
				PackfileID: packfileID,
				Reader:     reader.reader,
				Refcount:   1,
				FilePath:   sstPath,
			}
			r.cache.entries.Store(packfileID, newEntry)
			entry = newEntry
		} else {
			entry.(*PackfileCacheEntry).Refcount++
		}

		pkgLogger.Info("packfile: reader open L2_hit",
			"packfile_id", packfileID)

		return &PackfileHandle{
			PackfileID:   packfileID,
			IndexReader: reader,
			DataFilePath: dataPath,
			DataFile:     dataFile,
			handle: &cacheHandle{
				cache: r.cache,
				entry: entry.(*PackfileCacheEntry),
			},
		}, nil
	}

	// L2 miss - try L3 (object storage)
	if r.adapter != nil {
		handle, err := r.openFromL3(ctx, storageNamespace, packfileID)
		if err == nil {
			pkgLogger.Info("packfile: reader open L3_fetch",
				"packfile_id", packfileID)
			return handle, nil
		}
	}

	pkgLogger.Info("packfile: reader open unavailable",
		"packfile_id", packfileID)
	return nil, ErrPackfileUnavailable
}

// openFromL3 fetches a packfile from object storage (L3) and caches it to L2.
func (r *PackfileReader) openFromL3(ctx context.Context, storageNamespace string, packfileID PackfileID) (*PackfileHandle, error) {
	if r.adapter == nil {
		return nil, ErrPackfileUnavailable
	}

	// Construct L2 path
	l2Dir := filepath.Join(storageNamespace, "_packfiles", "packfiles", string(packfileID))
	sstPath := filepath.Join(l2Dir, "data.sst")
	dataPath := filepath.Join(l2Dir, "data.pack")

	// Ensure L2 directory exists
	if err := os.MkdirAll(l2Dir, 0755); err != nil {
		return nil, fmt.Errorf("packfile: creating L2 directory: %w", err)
	}

	// Fetch packfile from L3
	packfilePointer := block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       fmt.Sprintf("_packfiles/packfiles/%s/data.pack", packfileID),
	}

	reader, err := r.adapter.Get(ctx, packfilePointer)
	if err != nil {
		return nil, fmt.Errorf("packfile: fetching packfile from L3: %w", err)
	}

	// Write to L2
	dataFile, err := os.Create(dataPath)
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("packfile: creating data file in L2: %w", err)
	}

	_, err = io.Copy(dataFile, reader)
	reader.Close()
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("packfile: writing packfile to L2: %w", err)
	}
	dataFile.Close()

	// Fetch index from L3
	indexPointer := block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       fmt.Sprintf("_packfiles/packfiles/%s/data.sst", packfileID),
	}

	indexReader, err := r.adapter.Get(ctx, indexPointer)
	if err != nil {
		return nil, fmt.Errorf("packfile: fetching index from L3: %w", err)
	}

	// Write index to L2
	indexFile, err := os.Create(sstPath)
	if err != nil {
		indexReader.Close()
		return nil, fmt.Errorf("packfile: creating index file in L2: %w", err)
	}

	_, err = io.Copy(indexFile, indexReader)
	indexReader.Close()
	if err != nil {
		indexFile.Close()
		return nil, fmt.Errorf("packfile: writing index to L2: %w", err)
	}
	indexFile.Close()

	// Now open from L2
	return r.Open(ctx, storageNamespace, packfileID)
}

// sstPath returns the path to the SSTable index file for a packfile on L2.
func (r *PackfileReader) sstPath(storageNamespace string, packfileID PackfileID) string {
	return filepath.Join(storageNamespace, "_packfiles", "packfiles", string(packfileID), "data.sst")
}

// dataPath returns the path to the data file for a packfile on L2.
func (r *PackfileReader) dataPath(storageNamespace string, packfileID PackfileID) string {
	return filepath.Join(storageNamespace, "_packfiles", "packfiles", string(packfileID), "data.pack")
}
