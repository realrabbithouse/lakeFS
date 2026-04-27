package packfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
)

// Config holds packfile configuration.
type Config struct {
	StorageNamespace string
	LocalPath        string
	MaxPackSize      int64
}

// Manager handles all packfile state transitions.
type Manager struct {
	store  kv.Store
	block  block.Adapter
	config Config
}

// NewManager creates a new packfile Manager.
func NewManager(store kv.Store, blockAdapter block.Adapter, cfg Config) *Manager {
	return &Manager{store: store, block: blockAdapter, config: cfg}
}

// CreateUploadSession creates a kv UploadSession (status=in-progress).
func (m *Manager) CreateUploadSession(ctx context.Context, repoID, packfileID string) (*UploadSession, error) {
	session := &UploadSession{
		UploadId:     uuid.NewString(),
		PackfileId:   packfileID,
		RepositoryId: repoID,
		CreatedAt:    timestamppb.Now(),
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

// Merge merges source packfiles into a new COMMITTED SECOND_CLASS packfile.
// Marks sources as SUPERSEDED. Returns the new merged packfile's metadata.
func (m *Manager) Merge(ctx context.Context, repoID string, sourceIDs []string) (*PackfileMetadata, error) {
	if len(sourceIDs) == 0 {
		return nil, errors.New("no source packfiles provided")
	}

	// Fetch source packfile metadata
	var sources []*PackfileMetadata
	for _, id := range sourceIDs {
		meta, err := m.GetPackfile(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("getting source packfile %s: %w", id, err)
		}
		sources = append(sources, meta)
	}

	// Merge into a new COMMITTED SECOND_CLASS packfile
	merged, err := m.mergePackfiles(ctx, sources, PackfileStatus_COMMITTED, ClassificationSecondClass)
	if err != nil {
		return nil, err
	}

	// Mark source packfiles as SUPERSEDED
	for _, src := range sources {
		src.Status = PackfileStatus_SUPERSEDED
		if err := kv.SetMsg(ctx, m.store, PackfilesPartition, []byte(PackfilePath(src.PackfileId)), src); err != nil {
			return nil, err
		}
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

	// Merge each source packfile
	for _, src := range sources {
		srcMeta, err := m.GetPackfile(ctx, src.PackfileId)
		if err != nil {
			packer.Close()
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
			return nil, fmt.Errorf("reading source packfile %s: %w", src.PackfileId, err)
		}

		// Create unpacker to scan objects
		unpacker := NewUnpacker(reader.(io.ReadSeeker))
		iter := unpacker.Scan(ctx, false)

		for iter.Next() {
			_, offset, uncompressedSize, _, _, err := iter.Object()
			if err != nil {
				iter.Close()
				reader.Close()
				packer.Close()
				return nil, fmt.Errorf("reading object from %s at offset %d: %w", src.PackfileId, offset, err)
			}

			// Re-read the specific object using GetObject for the actual data
			_, reReader, _, _, err := unpacker.GetObject(ctx, offset)
			if err != nil {
				iter.Close()
				reader.Close()
				packer.Close()
				return nil, fmt.Errorf("re-reading object at offset %d: %w", offset, err)
			}

			_, _, err = packer.AppendObject(ctx, reReader, uncompressedSize)
			if err != nil {
				iter.Close()
				reader.Close()
				packer.Close()
				return nil, fmt.Errorf("appending object: %w", err)
			}

			totalObjectCount++
			totalSizeUncompressed += uncompressedSize
		}
		if err := iter.Err(); err != nil {
			iter.Close()
			reader.Close()
			packer.Close()
			return nil, fmt.Errorf("scanning source packfile %s: %w", src.PackfileId, err)
		}
		iter.Close()
		reader.Close()
	}

	// Close packer to finalize footer
	if err := packer.Close(); err != nil {
		return nil, fmt.Errorf("closing packer: %w", err)
	}

	// Seek to beginning to read merged data
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking temp file: %w", err)
	}

	mergedData, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("reading merged data: %w", err)
	}

	// Upload merged packfile to block storage
	storageID := m.config.StorageNamespace
	obj := block.ObjectPointer{
		StorageNamespace: storageID,
		Identifier:       newPackfileID,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	_, err = m.block.Put(ctx, obj, int64(len(mergedData)), bytes.NewReader(mergedData), block.PutOpts{})
	if err != nil {
		return nil, fmt.Errorf("uploading merged packfile: %w", err)
	}

	sizeCompressed := int64(len(mergedData))

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
		SizeBytesCompressed:  sizeCompressed,
		CompressionAlgorithm: "zstd",
		ChecksumAlgorithm:     "sha256",
		CreatedAt:             timestamppb.Now(),
		SourcePackfileIds:     sourceIDs,
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
	for {
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
		if errors.Is(err, kv.ErrPredicateFailed) {
			continue // Retry with fresh snapshot
		}
		return err
	}
}
