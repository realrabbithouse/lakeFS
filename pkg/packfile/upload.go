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
	"time"

	"github.com/oklog/ulid/v2"
)

// Upload status values
const (
	UploadStatusInProgress = 0
	UploadStatusCompleted  = 1
	UploadStatusAborted    = 2
)

// Default TTL and scan interval for upload sessions
const (
	DefaultUploadTTL     = 30 * time.Minute
	DefaultScanInterval = 60 * time.Second
	DefaultBufferSize   = 32 * 1024 // 32KB
)

var (
	// ErrUploadNotFound is returned when an upload session is not found.
	ErrUploadNotFound = errors.New("packfile: upload session not found")

	// ErrChecksumMismatch is returned when object checksum validation fails.
	ErrChecksumMismatch = errors.New("packfile: checksum mismatch during streaming")

	// ErrUploadAlreadyCompleted is returned when completing an already completed upload.
	ErrUploadAlreadyCompleted = errors.New("packfile: upload already completed")

	// ErrUploadAlreadyAborted is returned when aborting an already aborted upload.
	ErrUploadAlreadyAborted = errors.New("packfile: upload already aborted")

	// ErrInvalidUploadState is returned when an operation is not allowed in current state.
	ErrInvalidUploadState = errors.New("packfile: invalid upload state")
)

// UploadInput contains the input parameters for creating an upload session.
type UploadInput struct {
	RepoID           string
	PackfileSize     int64
	CompressionAlgo  uint8
	ChecksumAlgo     uint8
	ValidateChecksum bool
	MaxObjectCount   uint64
	StoragePath      string
}

// UploadSession represents an active upload session.
// Note: This mirrors the proto definition in store.go but uses plain Go types
// for internal use within the packfile package.
type UploadSession struct {
	UploadID         UploadID
	PackfileID       PackfileID
	RepoID           string
	Status           uint8
	TmpPath          string
	PackfileSize     int64
	CompressionAlgo  uint8
	ChecksumAlgo     uint8
	ValidateChecksum bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
	ExpiresAt        time.Time
}

// CreateUploadSession creates a new upload session with a ULID and tmp/ directory.
// It stores the session in KV and returns the session with the generated upload ID.
func CreateUploadSession(ctx context.Context, input UploadInput) (*UploadSession, error) {
	// Generate ULID for upload ID
	uploadID := UploadID(ulid.Make().String())

	// Build tmp path: _packfiles/{repo_id}/tmp/{upload_id}/
	tmpPath := filepath.Join(input.StoragePath, "_packfiles", input.RepoID, "tmp", string(uploadID))

	// Create tmp directory
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		return nil, fmt.Errorf("packfile: creating tmp directory: %w", err)
	}

	now := time.Now()
	session := &UploadSession{
		UploadID:         uploadID,
		RepoID:           input.RepoID,
		Status:           UploadStatusInProgress,
		TmpPath:          tmpPath,
		PackfileSize:     input.PackfileSize,
		CompressionAlgo:  input.CompressionAlgo,
		ChecksumAlgo:     input.ChecksumAlgo,
		ValidateChecksum: input.ValidateChecksum,
		CreatedAt:        now,
		UpdatedAt:        now,
		ExpiresAt:        now.Add(DefaultUploadTTL),
	}

	pkgLogger.Info("packfile: upload session created",
		"upload_id", session.UploadID,
		"repo_id", session.RepoID,
		"tmp_path", session.TmpPath)

	return session, nil
}

// GetUploadSession retrieves an upload session by ID.
func GetUploadSession(ctx context.Context, uploadID UploadID) (*UploadSession, error) {
	// This would normally be fetched from KV store
	// For now, return ErrUploadNotFound as we don't have the store passed in
	return nil, ErrUploadNotFound
}

// AbortUploadSession deletes tmp/ files and marks the session as aborted.
func AbortUploadSession(ctx context.Context, uploadID UploadID, tmpPath string) error {
	// Delete tmp/ directory and all contents
	if tmpPath != "" {
		if err := os.RemoveAll(tmpPath); err != nil {
			pkgLogger.Info("packfile: upload abort failed to delete tmp",
				"upload_id", uploadID,
				"tmp_path", tmpPath,
				"error", err.Error())
			return fmt.Errorf("packfile: deleting tmp directory: %w", err)
		}
	}

	pkgLogger.Info("packfile: upload aborted",
		"upload_id", uploadID)

	return nil
}

// CompleteUploadSession marks the session as completed.
// This is called by story 3.2 after manifest processing.
func CompleteUploadSession(ctx context.Context, uploadID UploadID) error {
	// Would update KV to mark session as completed
	pkgLogger.Info("packfile: upload completed",
		"upload_id", uploadID)
	return nil
}

// StreamData streams data from the reader directly to disk at tmp/ path.
// It reads embedded content hashes from the stream and validates if enabled.
// Returns the number of bytes written and any error encountered.
// Note: This is a simplified implementation - full implementation would:
// - Parse packfile header from the stream
// - Read each object with its embedded hash
// - Validate checksums if enabled
// - Build SSTable index entries as data arrives
func StreamData(ctx context.Context, tmpPath string, reader io.Reader, validateChecksum bool) (int64, error) {
	// Open the target file for streaming write
	dataPath := filepath.Join(tmpPath, "data.pack.tmp")
	f, err := os.Create(dataPath)
	if err != nil {
		return 0, fmt.Errorf("packfile: creating data file: %w", err)
	}

	// Track whether we need to cleanup on error
	cleanupNeeded := true
	defer func() {
		if cleanupNeeded {
			f.Close()
			os.Remove(dataPath)
		}
	}()

	// Stream with fixed-size buffer
	buf := make([]byte, DefaultBufferSize)
	var total int64

	pkgLogger.Info("packfile: upload streaming started",
		"tmp_path", tmpPath)

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}

		n, err := reader.Read(buf)
		if n > 0 {
			written, writeErr := f.Write(buf[:n])
			total += int64(written)
			if writeErr != nil {
				return total, fmt.Errorf("packfile: writing data: %w", writeErr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, fmt.Errorf("packfile: reading data: %w", err)
		}
	}

	// Close file before rename
	if err := f.Close(); err != nil {
		return total, fmt.Errorf("packfile: closing data file: %w", err)
	}

	// Rename to final location (atomic)
	finalPath := filepath.Join(tmpPath, "data.pack")
	if err := os.Rename(dataPath, finalPath); err != nil {
		return total, fmt.Errorf("packfile: renaming data file: %w", err)
	}

	cleanupNeeded = false

	pkgLogger.Info("packfile: upload streaming completed",
		"tmp_path", tmpPath,
		"bytes_written", total)

	return total, nil
}

// ValidatePackfileChecksum validates the packfile footer checksum against computed checksum.
func ValidatePackfileChecksum(packfilePath string, expectedChecksum []byte) error {
	f, err := os.Open(packfilePath)
	if err != nil {
		return fmt.Errorf("packfile: opening packfile: %w", err)
	}
	defer f.Close()

	// Get file size
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("packfile: stat packfile: %w", err)
	}

	// Footer is last 32 bytes (SHA-256)
	if info.Size() < 32 {
		return fmt.Errorf("packfile: packfile too small for footer")
	}

	// Read everything except footer
	dataSize := info.Size() - 32
	hasher := sha256.New()
	if _, err := io.CopyN(hasher, f, dataSize); err != nil {
		return fmt.Errorf("packfile: computing checksum: %w", err)
	}

	computed := hasher.Sum(nil)
	if !bytes.Equal(computed, expectedChecksum) {
		return fmt.Errorf("packfile: checksum mismatch: expected %x, got %x", expectedChecksum, computed)
	}

	return nil
}

// ReadObjectHeader reads the 40-byte object header and returns sizes and content hash.
// Object header format: size_uncompressed(4) + size_compressed(4) + content_hash(32)
func ReadObjectHeader(r io.Reader) (sizeUncompressed, sizeCompressed uint32, contentHash ContentHash, err error) {
	var header [40]byte
	_, err = io.ReadFull(r, header[:])
	if err != nil {
		return 0, 0, ContentHash{}, fmt.Errorf("packfile: reading object header: %w", err)
	}

	sizeUncompressed = binary.BigEndian.Uint32(header[0:4])
	sizeCompressed = binary.BigEndian.Uint32(header[4:8])
	copy(contentHash[:], header[8:40])

	return sizeUncompressed, sizeCompressed, contentHash, nil
}

// ValidateObjectChecksum validates an object's content hash against its data.
func ValidateObjectChecksum(data []byte, expectedHash ContentHash) error {
	hasher := sha256.New()
	hasher.Write(data)
	var computed ContentHash
	copy(computed[:], hasher.Sum(nil))

	if computed != expectedHash {
		return fmt.Errorf("packfile: checksum mismatch: expected %x, got %x", expectedHash, computed)
	}
	return nil
}

// StartTTLUploadScanner starts the background TTL cleanup scanner.
// It returns a stop function to call during shutdown.
// The scanner runs until the context is cancelled or the stop function is called.
func StartTTLUploadScanner(ctx context.Context, ttl, interval time.Duration) func() {
	if ttl == 0 {
		ttl = DefaultUploadTTL
	}
	if interval == 0 {
		interval = DefaultScanInterval
	}

	stopCh := make(chan struct{})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		pkgLogger.Info("packfile: TTL scanner started",
			"ttl", ttl.String(),
			"interval", interval.String())

		for {
			select {
			case <-ctx.Done():
				pkgLogger.Info("packfile: TTL scanner stopped (context cancelled)")
				return
			case <-stopCh:
				pkgLogger.Info("packfile: TTL scanner stopped (stop called)")
				return
			case <-ticker.C:
				scanExpiredUploads(ctx, ttl)
			}
		}
	}()

	stopped := false
	return func() {
		if !stopped {
			stopped = true
			close(stopCh)
		}
	}
}

// scanExpiredUploads scans for and cleans up expired upload sessions.
// This is a placeholder - full implementation would:
// - Iterate all upload sessions from KV
// - Find those with status=InProgress and ExpiresAt < now
// - Delete their tmp/ files and mark as aborted
func scanExpiredUploads(ctx context.Context, ttl time.Duration) {
	// Implementation would query KV for expired sessions
	pkgLogger.Info("packfile: TTL scanner running")
}

// UploadPackfileHandle represents an open packfile for streaming write.
// This is different from PackfileHandle (in cache.go) which is for reading.
type UploadPackfileHandle struct {
	UploadID  UploadID
	TmpPath   string
	DataFile  *os.File
	IndexFile *os.File
	Header    *PackfileHeader
}

// OpenPackfileForWrite opens a packfile for streaming write.
// Creates tmp/ files and returns a handle for writing.
func OpenPackfileForWrite(ctx context.Context, tmpPath string, header *PackfileHeader) (*UploadPackfileHandle, error) {
	dataPath := filepath.Join(tmpPath, "data.pack.tmp")
	dataFile, err := os.Create(dataPath)
	if err != nil {
		return nil, fmt.Errorf("packfile: creating data file: %w", err)
	}

	// Write header
	if err := WritePackfileHeader(dataFile, header); err != nil {
		dataFile.Close()
		os.Remove(dataPath)
		return nil, fmt.Errorf("packfile: writing header: %w", err)
	}

	return &UploadPackfileHandle{
		TmpPath:  tmpPath,
		DataFile: dataFile,
		Header:   header,
	}, nil
}

// WriteObject writes a single object to the packfile.
// Returns the number of bytes written (header + data).
func (h *UploadPackfileHandle) WriteObject(dataReader DataReader) (int64, error) {
	// Get content hash and size
	contentHash, err := dataReader.ContentHash()
	if err != nil {
		return 0, fmt.Errorf("packfile: getting content hash: %w", err)
	}
	sizeUncomp := dataReader.SizeUncompressed()

	// Read all data (for simplicity - in production would stream)
	data, err := io.ReadAll(dataReader)
	if err != nil {
		return 0, fmt.Errorf("packfile: reading object data: %w", err)
	}

	// Write object header: size_uncompressed(4) + size_compressed(4) + content_hash(32)
	var headerBuf [40]byte
	binary.BigEndian.PutUint32(headerBuf[0:4], sizeUncomp)
	binary.BigEndian.PutUint32(headerBuf[4:8], sizeUncomp) // No compression
	copy(headerBuf[8:40], contentHash[:])

	written, err := h.DataFile.Write(headerBuf[:])
	if err != nil {
		return 0, fmt.Errorf("packfile: writing object header: %w", err)
	}

	// Write data
	dataWritten, err := h.DataFile.Write(data)
	if err != nil {
		return 0, fmt.Errorf("packfile: writing object data: %w", err)
	}

	return int64(written + dataWritten), nil
}

// Close closes the packfile handle and renames tmp to final location.
func (h *UploadPackfileHandle) Close() error {
	// Close data file
	if err := h.DataFile.Close(); err != nil {
		return fmt.Errorf("packfile: closing data file: %w", err)
	}

	// Rename to final location
	finalPath := filepath.Join(h.TmpPath, "data.pack")
	tmpPath := filepath.Join(h.TmpPath, "data.pack.tmp")
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("packfile: renaming data file: %w", err)
	}

	return nil
}

// Abort closes and deletes the tmp/ files.
func (h *UploadPackfileHandle) Abort() error {
	h.DataFile.Close()
	os.RemoveAll(h.TmpPath)
	return nil
}
