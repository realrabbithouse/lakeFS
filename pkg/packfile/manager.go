package packfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrPackfileNotFound      = errors.New("packfile not found")
	ErrUploadSessionNotFound = errors.New("upload session not found")
	ErrSnapshotNotFound      = errors.New("snapshot not found")
	ErrManifestChecksumMismatch = errors.New("manifest checksum mismatch")
	ErrManifestObjectCountMismatch = errors.New("manifest object count mismatch")
)

// ManifestEntry represents one line in the NDJSON manifest.
type ManifestEntry struct {
	Path               string
	Checksum           string
	ContentType        string
	Metadata           map[string]string
	RelativeOffset     int64
	SizeUncompressed   int64
	SizeCompressed     int64
}

// Manifest represents the parsed manifest from the upload request.
type Manifest struct {
	PackfileChecksum string
	ObjectCount      int64
	Entries          []ManifestEntry
}

// Config holds packfile configuration.
type Config struct {
	StorageNamespace string
	LocalPath        string
	MaxPackSize      int64
	AsyncReplicate   bool
}

// Manager handles all packfile state transitions.
type Manager struct {
	store      kv.Store
	block      block.Adapter
	config     Config
	indexCache *IndexCache
}

// ManagerOption configures Manager behavior.
type ManagerOption func(*Manager)

// WithIndexCache sets the index cache for SSTable lookups.
func WithIndexCache(cache *IndexCache) ManagerOption {
	return func(m *Manager) {
		m.indexCache = cache
	}
}

// NewManager creates a new packfile Manager.
func NewManager(store kv.Store, blockAdapter block.Adapter, cfg Config, opts ...ManagerOption) *Manager {
	m := &Manager{store: store, block: blockAdapter, config: cfg}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// CreateUploadSession creates a kv UploadSession (status=in-progress).
func (m *Manager) CreateUploadSession(ctx context.Context, repoID, packfileID string, expectedSize int64, expectedChecksum, compression string, expectedObjectCount int64) (*UploadSession, error) {
	session := &UploadSession{
		UploadId:            uuid.NewString(),
		PackfileId:          packfileID,
		RepositoryId:        repoID,
		CreatedAt:           timestamppb.Now(),
		ExpectedSize:        expectedSize,
		ExpectedChecksum:    expectedChecksum,
		Compression:         compression,
		ExpectedObjectCount: expectedObjectCount,
	}
	if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(UploadPath(session.UploadId)), session); err != nil {
		return nil, err
	}
	return session, nil
}

// GetPackfile reads PackfileMetadata from kv.
func (m *Manager) GetPackfile(ctx context.Context, packfileID string) (*PackfileMetadata, error) {
	var meta PackfileMetadata
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(packfileID)), &meta)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, ErrPackfileNotFound
	}
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// ListPackfiles lists all packfiles for a repository.
// Uses snapshot's packfile IDs for efficient lookup when available.
func (m *Manager) ListPackfiles(ctx context.Context, repoID string) ([]*PackfileMetadata, error) {
	// Try to use snapshot's packfile IDs for efficient lookup
	snap, err := m.GetSnapshot(ctx, repoID)
	if err == nil {
		// Use packfile IDs from snapshot
		var packfileIDs []string
		packfileIDs = append(packfileIDs, snap.ActivePackfileIds...)
		packfileIDs = append(packfileIDs, snap.StagedPackfileIds...)

		var result []*PackfileMetadata
		for _, id := range packfileIDs {
			meta, err := m.GetPackfile(ctx, id)
			if err == nil {
				result = append(result, meta)
			}
		}
		return result, nil
	}

	// Fallback: scan all packfiles and filter by repoID
	prefix := "packfile:"
	iter, err := m.store.Scan(ctx, []byte(PackfilesPartition), kv.ScanOptions{KeyStart: []byte(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var result []*PackfileMetadata
	for iter.Next() {
		entry := iter.Entry()
		if entry == nil {
			continue
		}
		var meta PackfileMetadata
		if err := proto.Unmarshal(entry.Value, &meta); err != nil {
			continue
		}
		if meta.RepositoryId != repoID {
			continue
		}
		result = append(result, &meta)
	}
	return result, iter.Err()
}

// GetUploadSession reads an UploadSession from kv.
func (m *Manager) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	var session UploadSession
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(UploadPath(uploadID)), &session)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, ErrUploadSessionNotFound
	}
	if err != nil {
		return nil, err
	}
	return &session, nil
}

// ValidateManifest validates a parsed manifest against the upload session's expected values.
// It checks:
// - Object count matches the expected count from init request
// - Packfile checksum matches the expected checksum from init request
func (m *Manager) ValidateManifest(ctx context.Context, uploadID string, manifest *Manifest) error {
	session, err := m.GetUploadSession(ctx, uploadID)
	if err != nil {
		return err
	}

	// Validate object count
	if int64(len(manifest.Entries)) != session.ExpectedObjectCount {
		return fmt.Errorf("%w: expected %d, got %d",
			ErrManifestObjectCountMismatch, session.ExpectedObjectCount, len(manifest.Entries))
	}

	// Validate packfile checksum (format: "sha256:...")
	expectedChecksum := session.ExpectedChecksum
	if manifest.PackfileChecksum != expectedChecksum {
		return fmt.Errorf("%w: expected %s, got %s",
			ErrManifestChecksumMismatch, expectedChecksum, manifest.PackfileChecksum)
	}

	return nil
}

// CompleteUploadSession marks the upload done, creates PackfileMetadata (STAGED, SECOND_CLASS).
func (m *Manager) CompleteUploadSession(ctx context.Context, uploadID string) error {
	var session UploadSession
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(UploadPath(uploadID)), &session)
	if errors.Is(err, kv.ErrNotFound) {
		return ErrUploadSessionNotFound
	}
	if err != nil {
		return err
	}

	meta := &PackfileMetadata{
		PackfileId:    session.PackfileId,
		Status:        PackfileStatus_STAGED,
		Classification: PackfileClassification_SECOND_CLASS,
		RepositoryId:   session.RepositoryId,
		CreatedAt:     timestamppb.Now(),
	}
	if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(meta.PackfileId)), meta); err != nil {
		return err
	}

	// Start async replication to object storage
	m.StartReplication(ctx, session.PackfileId)

	return m.store.Delete(ctx, []byte(PackfilesPartition), []byte(UploadPath(uploadID)))
}

// AbortUploadSession deletes tmp files and removes kv entry.
func (m *Manager) AbortUploadSession(ctx context.Context, uploadID string) error {
	// Get session to find tmp path
	session, err := m.GetUploadSession(ctx, uploadID)
	if err != nil {
		return err
	}

	// Delete tmp file if exists
	if session.TmpPath != "" {
		_ = os.RemoveAll(session.TmpPath)
	}

	// Delete KV entry
	return m.store.Delete(ctx, []byte(PackfilesPartition), []byte(UploadPath(uploadID)))
}

// CleanupExpiredUploads scans for stale sessions (TTL=24h), deletes tmp files and kv entries.
func (m *Manager) CleanupExpiredUploads(ctx context.Context) error {
	prefix := "upload:"
	iter, err := m.store.Scan(ctx, []byte(PackfilesPartition), kv.ScanOptions{KeyStart: []byte(prefix)})
	if err != nil {
		return err
	}
	defer iter.Close()

	threshold := time.Now().Add(-24 * time.Hour)
	type sessionToDelete struct {
		key     []byte
		tmpPath string
	}
	var toDelete []sessionToDelete
	for iter.Next() {
		entry := iter.Entry()
		if entry == nil {
			continue
		}
		var session UploadSession
		if err := proto.Unmarshal(entry.Value, &session); err != nil {
			continue
		}
		createdAt := session.CreatedAt.AsTime()
		if createdAt.Before(threshold) {
			toDelete = append(toDelete, sessionToDelete{key: entry.Key, tmpPath: session.TmpPath})
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}

	for _, s := range toDelete {
		// Delete tmp file if exists
		if s.tmpPath != "" {
			_ = os.RemoveAll(s.tmpPath)
		}
		// Delete KV entry
		if err := m.store.Delete(ctx, []byte(PackfilesPartition), s.key); err != nil {
			return err
		}
	}
	return nil
}

// MergeStaged merges all STAGED packfiles for a repo into one STAGED packfile.
// Returns the new merged packfile's metadata.
func (m *Manager) MergeStaged(ctx context.Context, repoID string) (*PackfileMetadata, error) {
	// Get all staged packfiles for this repo
	packfiles, err := m.ListPackfiles(ctx, repoID)
	if err != nil {
		return nil, err
	}

	var staged []*PackfileMetadata
	for _, pf := range packfiles {
		if pf.Status == PackfileStatus_STAGED {
			staged = append(staged, pf)
		}
	}

	if len(staged) == 0 {
		return nil, nil // Nothing to merge
	}

	// Single staged packfile - nothing to merge
	if len(staged) == 1 {
		return staged[0], nil
	}

	// Merge multiple staged packfiles into one
	merged, err := m.mergePackfiles(ctx, staged, PackfileStatus_STAGED, ClassificationSecondClass)
	if err != nil {
		return nil, err
	}

	// Update snapshot to point to merged packfile
	err = m.UpdateSnapshot(ctx, repoID, func(snap *PackfileSnapshot) error {
		snap.StagedPackfileIds = []string{merged.PackfileId}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Mark source packfiles as superseded and delete their data
	for _, pf := range staged {
		pf.Status = PackfileStatus_SUPERSEDED
		if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(pf.PackfileId)), pf); err != nil {
			return nil, err
		}
		// Note: actual data cleanup happens in CleanupSuperseded
	}

	return merged, nil
}

// Commit transitions a STAGED packfile to COMMITTED (FIRST_CLASS) for a repository.
// It finds the staged packfile in the snapshot and transitions it.
func (m *Manager) Commit(ctx context.Context, repoID string) error {
	snap, err := m.GetSnapshot(ctx, repoID)
	if err != nil {
		return err
	}

	if len(snap.StagedPackfileIds) == 0 {
		return nil // Nothing to commit
	}

	// Get the staged packfile (first one)
	stagedID := snap.StagedPackfileIds[0]
	meta, err := m.GetPackfile(ctx, stagedID)
	if err != nil {
		return err
	}

	// Transition to COMMITTED FIRST_CLASS
	meta.Status = PackfileStatus_COMMITTED
	meta.Classification = PackfileClassification_FIRST_CLASS

	if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(stagedID)), meta); err != nil {
		return err
	}

	// Update snapshot: move from staged to active
	return m.UpdateSnapshot(ctx, repoID, func(snap *PackfileSnapshot) error {
		// Move all staged to active
		snap.ActivePackfileIds = append(snap.ActivePackfileIds, snap.StagedPackfileIds...)
		snap.StagedPackfileIds = nil
		return nil
	})
}

// GetObject returns a reader for the object with the given content hash.
// It searches through the packfiles for the repository.
// Uses SSTable index for O(1) lookup when available, falls back to full scan.
func (m *Manager) GetObject(ctx context.Context, repoID string, contentHash ContentHash) (io.Reader, error) {
	// Get snapshot to find packfiles
	snap, err := m.GetSnapshot(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("getting snapshot: %w", err)
	}

	// Collect all packfile IDs
	packfileIDs := make([]string, 0, len(snap.ActivePackfileIds)+len(snap.StagedPackfileIds))
	packfileIDs = append(packfileIDs, snap.ActivePackfileIds...)
	packfileIDs = append(packfileIDs, snap.StagedPackfileIds...)

	// Search each packfile for the content hash
	for _, pfID := range packfileIDs {
		meta, err := m.GetPackfile(ctx, pfID)
		if err != nil {
			continue
		}

		// Try index-based lookup first (O(1))
		found, err := m.getObjectFromIndex(ctx, meta, contentHash)
		if err != nil {
			// Log error but continue to fallback
			continue
		}
		if found != nil {
			return found, nil
		}

		// Fall back to full scan
		found, err = m.searchPackfileForHash(ctx, meta, contentHash)
		if err != nil {
			continue
		}
		if found != nil {
			return found, nil
		}
	}

	return nil, fmt.Errorf("content hash not found in any packfile")
}

// getObjectFromIndex tries to find the object using the SSTable index.
// Returns the reader if found via index, nil if not found in index.
func (m *Manager) getObjectFromIndex(ctx context.Context, meta *PackfileMetadata, contentHash ContentHash) (io.Reader, error) {
	// If no index is available, return nil to trigger full scan
	if meta.IndexPath == "" {
		return nil, nil
	}

	// Open the index file
	indexReader, err := NewIndexReader(meta.IndexPath)
	if err != nil {
		return nil, nil // Fall back to full scan
	}
	defer indexReader.Close()

	// Look up the hash in the index
	entry, err := indexReader.Lookup(contentHash)
	if err != nil || entry == nil {
		return nil, nil // Not found in index, fall back to full scan
	}

	// Found in index - get the object at the offset
	obj := block.ObjectPointer{
		StorageNamespace: meta.StorageNamespace,
		Identifier:       meta.PackfileId,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	reader, err := m.block.Get(ctx, obj)
	if err != nil {
		return nil, err
	}

	unpacker := NewUnpacker(reader)
	_, objReader, _, _, err := unpacker.GetObject(ctx, entry.Offset)
	if err != nil {
		reader.Close()
		return nil, err
	}

	return objReader, nil
}

// searchPackfileForHash scans a packfile for a content hash.
// Returns the reader if found, nil otherwise.
func (m *Manager) searchPackfileForHash(ctx context.Context, meta *PackfileMetadata, contentHash ContentHash) (io.Reader, error) {
	obj := block.ObjectPointer{
		StorageNamespace: meta.StorageNamespace,
		Identifier:       meta.PackfileId,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	reader, err := m.block.Get(ctx, obj)
	if err != nil {
		return nil, err
	}

	unpacker := NewUnpacker(reader)
	iter := unpacker.Scan(ctx, false)

	for iter.Next() {
		hash, _, _, _, objReader, err := iter.Object()
		if err != nil {
			iter.Close()
			reader.Close()
			return nil, err
		}

		if hash == contentHash {
			iter.Close()
			return objReader, nil
		}
	}
	iter.Close()
	reader.Close()

	if err := iter.Err(); err != nil {
		return nil, err
	}
	return nil, nil // Not found
}

// Merge merges source packfiles into a new COMMITTED SECOND_CLASS packfile.
// Marks sources as SUPERSEDED after the merged packfile is persisted.
// Returns the new merged packfile's metadata.
func (m *Manager) Merge(ctx context.Context, repoID string, sourceIDs []string) (*PackfileMetadata, error) {
	if len(sourceIDs) == 0 {
		return nil, errors.New("no source packfiles provided")
	}

	// Fetch source packfile metadata and validate they are COMMITTED
	var sources []*PackfileMetadata
	for _, id := range sourceIDs {
		meta, err := m.GetPackfile(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("getting source packfile %s: %w", id, err)
		}
		if meta.Status != PackfileStatus_COMMITTED {
			return nil, fmt.Errorf("source packfile %s is not COMMITTED (status=%v)", id, meta.Status)
		}
		sources = append(sources, meta)
	}

	// Merge into a new COMMITTED SECOND_CLASS packfile
	// This also marks source packfiles as SUPERSEDED after successful persistence
	merged, err := m.mergePackfiles(ctx, sources, PackfileStatus_COMMITTED, ClassificationSecondClass)
	if err != nil {
		return nil, err
	}

	// Update snapshot to add merged packfile to active
	err = m.UpdateSnapshot(ctx, repoID, func(snap *PackfileSnapshot) error {
		snap.ActivePackfileIds = append(snap.ActivePackfileIds, merged.PackfileId)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return merged, nil
}

// mergePackfiles creates a new packfile by merging source packfiles.
// Returns metadata for the newly created (merged) packfile.
func (m *Manager) mergePackfiles(ctx context.Context, sources []*PackfileMetadata, status PackfileStatus, classification Classification) (*PackfileMetadata, error) {
	if len(sources) == 0 {
		return nil, errors.New("no source packfiles provided")
	}

	// Generate new packfile ID
	newPackfileID := uuid.NewString()

	// Create temp file for merged packfile
	tmpFile, err := os.CreateTemp(m.config.LocalPath, "merge-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close() // Close so Packer can open it

	// Re-open for writing
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0)
	if err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("opening temp file: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(tmpPath)
	}()

	// Create packer for merged output
	packer := NewPacker(f, CompressionZstd, classification)

	// Track object count and sizes
	var totalObjectCount int64
	var totalSizeUncompressed int64

	// Track seen content hashes for deduplication
	seenHashes := make(map[ContentHash]bool)

	// Create index writer for O(1) content hash lookups
	// Estimate entry count based on source packfiles
	estimatedEntries := int64(0)
	for _, src := range sources {
		estimatedEntries += src.ObjectCount
	}
	indexPath := filepath.Join(m.config.LocalPath, newPackfileID+".index")
	indexWriter, err := NewIndexWriter(indexPath, CompressionZstd, WithEntryCount(int(estimatedEntries)))
	if err != nil {
		packer.Close()
		return nil, fmt.Errorf("creating index writer: %w", err)
	}

	// Merge each source packfile
	for _, src := range sources {
		srcMeta, err := m.GetPackfile(ctx, src.PackfileId)
		if err != nil {
			packer.Close()
			indexWriter.Close()
			os.Remove(indexPath)
			return nil, fmt.Errorf("getting source packfile %s: %w", src.PackfileId, err)
		}

		// Get source packfile data from block storage
		obj := block.ObjectPointer{
			StorageNamespace: srcMeta.StorageNamespace,
			Identifier:       srcMeta.PackfileId,
			IdentifierType:   block.IdentifierTypeRelative,
		}
		reader, err := m.block.Get(ctx, obj)
		if err != nil {
			packer.Close()
			indexWriter.Close()
			os.Remove(indexPath)
			return nil, fmt.Errorf("reading source packfile %s: %w", src.PackfileId, err)
		}

		// Create unpacker to scan objects
		unpacker := NewUnpacker(reader)
		iter := unpacker.Scan(ctx, false)

		for iter.Next() {
			// Get content hash, sizes, and reader from iterator
			contentHash, _, uncompressedSize, compressedSize, objReader, err := iter.Object()
			if err != nil {
				iter.Close()
				reader.Close()
				packer.Close()
				indexWriter.Close()
				os.Remove(indexPath)
				return nil, fmt.Errorf("reading object from %s: %w", src.PackfileId, err)
			}

			// Deduplicate by content hash - skip if already seen
			if seenHashes[contentHash] {
				continue
			}
			seenHashes[contentHash] = true

			// Record packfile-relative offset before appending
			packOffset := HeaderSize + int64(totalSizeUncompressed)

			_, _, err = packer.AppendObject(ctx, objReader, uncompressedSize)
			if err != nil {
				iter.Close()
				reader.Close()
				packer.Close()
				indexWriter.Close()
				os.Remove(indexPath)
				return nil, fmt.Errorf("appending object: %w", err)
			}

			// Add index entry with the packfile-relative offset
			indexWriter.AddEntry(IndexEntry{
				Hash:             contentHash,
				Offset:           packOffset,
				SizeUncompressed: uncompressedSize,
				SizeCompressed:   compressedSize,
			})

			totalObjectCount++
			totalSizeUncompressed += uncompressedSize
		}
		if err := iter.Err(); err != nil {
			iter.Close()
			reader.Close()
			packer.Close()
			indexWriter.Close()
			os.Remove(indexPath)
			return nil, fmt.Errorf("scanning source packfile %s: %w", src.PackfileId, err)
		}
		iter.Close()
		reader.Close()
	}

	// Close index writer first (writes SSTable)
	if err := indexWriter.Close(); err != nil {
		packer.Close()
		os.Remove(indexPath)
		return nil, fmt.Errorf("closing index writer: %w", err)
	}

	// Close packer to finalize footer
	if err := packer.Close(); err != nil {
		return nil, fmt.Errorf("closing packer: %w", err)
	}

	// Seek to beginning to stream merged data directly to block storage
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking temp file: %w", err)
	}

	// Get file size
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat temp file: %w", err)
	}
	actualSize := info.Size()

	// Upload merged packfile to block storage (streaming, no in-memory buffer)
	storageID := m.config.StorageNamespace
	obj := block.ObjectPointer{
		StorageNamespace: storageID,
		Identifier:       newPackfileID,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	_, err = m.block.Put(ctx, obj, actualSize, f, block.PutOpts{})
	if err != nil {
		return nil, fmt.Errorf("uploading merged packfile: %w", err)
	}

	// Mark source packfiles as SUPERSEDED only after merged packfile is persisted
	for _, src := range sources {
		src.Status = PackfileStatus_SUPERSEDED
		if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(src.PackfileId)), src); err != nil {
			return nil, fmt.Errorf("marking source packfile %s as superseded: %w", src.PackfileId, err)
		}
	}

	// Create source packfile IDs list
	sourceIDs := make([]string, len(sources))
	for i, src := range sources {
		sourceIDs[i] = src.PackfileId
	}

	// Determine protobuf classification
	protoClass := PackfileClassification_SECOND_CLASS
	if classification == ClassificationFirstClass {
		protoClass = PackfileClassification_FIRST_CLASS
	}

	// Create metadata for merged packfile
	meta := &PackfileMetadata{
		PackfileId:            newPackfileID,
		Status:                status,
		Classification:       protoClass,
		StorageNamespace:      storageID,
		ObjectCount:          totalObjectCount,
		SizeBytes:            totalSizeUncompressed,
		SizeBytesCompressed:  actualSize,
		CompressionAlgorithm: "zstd",
		ChecksumAlgorithm:     "sha256",
		CreatedAt:             timestamppb.Now(),
		SourcePackfileIds:     sourceIDs,
		IndexPath:             indexPath,
	}

	// Store metadata in kv
	if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(newPackfileID)), meta); err != nil {
		return nil, fmt.Errorf("storing merged packfile metadata: %w", err)
	}

	return meta, nil
}

// CleanupSuperseded deletes data for SUPERSEDED packfiles, transitions to DELETED.
func (m *Manager) CleanupSuperseded(ctx context.Context) error {
	prefix := "packfile:"
	iter, err := m.store.Scan(ctx, []byte(PackfilesPartition), kv.ScanOptions{KeyStart: []byte(prefix)})
	if err != nil {
		return err
	}
	defer iter.Close()

	var toDelete [][]byte
	for iter.Next() {
		entry := iter.Entry()
		if entry == nil {
			continue
		}
		var meta PackfileMetadata
		if err := proto.Unmarshal(entry.Value, &meta); err != nil {
			continue
		}
		if meta.Status == PackfileStatus_SUPERSEDED {
			toDelete = append(toDelete, entry.Key)
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}

	for _, key := range toDelete {
		if err := m.store.Delete(ctx, []byte(PackfilesPartition), key); err != nil {
			return err
		}
	}
	return nil
}

// GetSnapshot reads the PackfileSnapshot for a repository.
func (m *Manager) GetSnapshot(ctx context.Context, repoID string) (*PackfileSnapshot, error) {
	var snap PackfileSnapshot
	_, err := kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(SnapshotPath(repoID)), &snap)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, ErrSnapshotNotFound
	}
	if err != nil {
		return nil, err
	}
	return &snap, nil
}

// UpdateSnapshot atomically updates the PackfileSnapshot for a repository using SetIf.
func (m *Manager) UpdateSnapshot(ctx context.Context, repoID string, updateFn func(*PackfileSnapshot) error) error {
	const maxRetries = 5
	for retries := 0; retries < maxRetries; retries++ {
		var snap *PackfileSnapshot
		var predicate kv.Predicate

		existing, err := m.GetSnapshot(ctx, repoID)
		if err != nil && !errors.Is(err, ErrSnapshotNotFound) {
			return err
		}

		if existing != nil {
			snap = existing
		} else {
			// Create new snapshot if not found
			snap = &PackfileSnapshot{
				RepositoryId: repoID,
			}
		}

		// Apply update function
		if err := updateFn(snap); err != nil {
			return err
		}

		// Get current predicate for SetIf (only if snapshot already existed)
		if existing != nil {
			predicate, err = kv.GetMsg(ctx, m.store, PackfilesPartition, []byte(SnapshotPath(repoID)), &PackfileSnapshot{})
			if err != nil && !errors.Is(err, kv.ErrNotFound) {
				return err
			}
		}

		err = kv.SetMsgIf(ctx, m.store, PackfilesPartition, []byte(SnapshotPath(repoID)), snap, predicate)
		if err == nil {
			return nil
		}
		if errors.Is(err, kv.ErrPredicateFailed) && retries < maxRetries-1 {
			// Retry with fresh snapshot
			continue
		}
		return err
	}
	return fmt.Errorf("update snapshot exceeded max retries (%d)", maxRetries)
}

// StartReplication starts a background goroutine to replicate a packfile to object storage.
// It reads the packfile from local disk and uploads it via the block adapter.
func (m *Manager) StartReplication(ctx context.Context, packfileID string) {
	if !m.config.AsyncReplicate {
		return
	}
	go func() {
		if err := m.replicatePackfile(context.Background(), packfileID); err != nil {
			// Log error but don't fail - replication is best effort
			// In production, this would use a proper logger
		}
	}()
}

// replicatePackfile reads a packfile from local disk and uploads it via the block adapter.
func (m *Manager) replicatePackfile(ctx context.Context, packfileID string) error {
	// Open the local packfile
	localPath := m.config.LocalPath + "/_packfiles/" + packfileID + "/data.pack"
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening local packfile: %w", err)
	}
	defer f.Close()

	// Get file size
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat local packfile: %w", err)
	}
	size := info.Size()

	// Upload to object storage via block adapter
	obj := block.ObjectPointer{
		StorageNamespace: m.config.StorageNamespace,
		Identifier:       "_packfiles/" + packfileID + "/data.pack",
		IdentifierType:   block.IdentifierTypeRelative,
	}
	_, err = m.block.Put(ctx, obj, size, f, block.PutOpts{})
	if err != nil {
		return fmt.Errorf("uploading packfile to object storage: %w", err)
	}
	return nil
}
