package packfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
)

// mockAdapter implements block.Adapter for testing L3 operations
type mockAdapter struct {
	data     map[string]*bytes.Reader
	getError error
}

func (m *mockAdapter) Get(ctx context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	key := obj.StorageNamespace + "/" + obj.Identifier
	reader, ok := m.data[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	// Reset position for each read
	reader.Seek(0, io.SeekStart)
	return io.NopCloser(reader), nil
}

func (m *mockAdapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) (*block.PutResponse, error) {
	return nil, nil
}

func (m *mockAdapter) GetWalker(storageID string, opts block.WalkerOptions) (block.Walker, error) {
	return nil, nil
}

func (m *mockAdapter) GetPreSignedURL(ctx context.Context, obj block.ObjectPointer, mode block.PreSignMode, filename string) (string, time.Time, error) {
	return "", time.Time{}, nil
}

func (m *mockAdapter) GetPresignUploadPartURL(ctx context.Context, obj block.ObjectPointer, uploadID string, partNumber int) (string, error) {
	return "", nil
}

func (m *mockAdapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	key := obj.StorageNamespace + "/" + obj.Identifier
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockAdapter) GetRange(ctx context.Context, obj block.ObjectPointer, start, end int64) (io.ReadCloser, error) {
	return nil, nil
}

func (m *mockAdapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {
	return block.Properties{}, nil
}

func (m *mockAdapter) Copy(ctx context.Context, src, dst block.ObjectPointer) error {
	return nil
}

func (m *mockAdapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, r *http.Request, opts block.CreateMultiPartUploadOpts) (*block.CreateMultiPartUploadResponse, error) {
	return nil, nil
}

func (m *mockAdapter) UploadPart(ctx context.Context, obj block.ObjectPointer, size int64, reader io.Reader, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	return nil, nil
}

func (m *mockAdapter) UploadCopyPart(ctx context.Context, src, dst block.ObjectPointer, uploadID string, partNumber int) (*block.UploadPartResponse, error) {
	return nil, nil
}

func (m *mockAdapter) UploadCopyPartRange(ctx context.Context, src, dst block.ObjectPointer, uploadID string, partNumber int, start, end int64) (*block.UploadPartResponse, error) {
	return nil, nil
}

func (m *mockAdapter) ListParts(ctx context.Context, obj block.ObjectPointer, uploadID string, opts block.ListPartsOpts) (*block.ListPartsResponse, error) {
	return nil, nil
}

func (m *mockAdapter) ListMultipartUploads(ctx context.Context, obj block.ObjectPointer, opts block.ListMultipartUploadsOpts) (*block.ListMultipartUploadsResponse, error) {
	return nil, nil
}

func (m *mockAdapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {
	return nil
}

func (m *mockAdapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipart *block.MultipartUploadCompletion) (*block.CompleteMultiPartUploadResponse, error) {
	return nil, nil
}

func (m *mockAdapter) BlockstoreType() string {
	return "mock"
}

func (m *mockAdapter) BlockstoreMetadata(ctx context.Context) (*block.BlockstoreMetadata, error) {
	return nil, nil
}

func (m *mockAdapter) GetStorageNamespaceInfo(storageID string) *block.StorageNamespaceInfo {
	return nil
}

func (m *mockAdapter) ResolveNamespace(storageID, storageNamespace, key string, identifierType block.IdentifierType) (block.QualifiedKey, error) {
	return nil, nil
}

func (m *mockAdapter) GetRegion(ctx context.Context, storageID, storageNamespace string) (string, error) {
	return "", nil
}

func (m *mockAdapter) RuntimeStats() map[string]string {
	return nil
}

// sha256Hash computes SHA256 hash of data and returns ContentHash
func sha256Hash(data []byte) ContentHash {
	h := sha256.New()
	h.Write(data)
	var hash ContentHash
	copy(hash[:], h.Sum(nil))
	return hash
}

// setupPackfileL2 creates the L2 packfile directory structure at the path reader.go expects.
// The reader.go path pattern is: {storageNamespace}/_packfiles/packfiles/{packfileID}/data.{sst,pack}
// It writes data in proper packfile binary format (used by ReadObject).
func setupPackfileL2(t *testing.T, tmpDir string, packfileID PackfileID, data []byte) string {
	// Create at path: {tmpDir}/_packfiles/packfiles/{packfileID}/
	packfileDir := filepath.Join(tmpDir, "_packfiles", "packfiles", string(packfileID))
	err := os.MkdirAll(packfileDir, 0755)
	require.NoError(t, err)

	// Create SSTable index with the content hash we'll write
	contentHash := sha256Hash(data)
	entries := []IndexEntry{
		{
			ContentHash:      contentHash,
			Offset:           0,
			SizeUncompressed: uint32(len(data)),
			SizeCompressed:   uint32(len(data)),
		},
	}
	sstPath := filepath.Join(packfileDir, "data.sst")
	err = BuildIndexFromEntries(context.Background(), entries, sstPath, "none")
	require.NoError(t, err)

	// Create data file in proper packfile format: size_uncompressed(4) + size_compressed(4) + hash(32) + data
	dataPath := filepath.Join(packfileDir, "data.pack")
	f, err := os.Create(dataPath)
	require.NoError(t, err)
	defer f.Close()

	// Write size_uncompressed (big-endian)
	var sizeUncompressed [4]byte
	binary.BigEndian.PutUint32(sizeUncompressed[:], uint32(len(data)))
	_, err = f.Write(sizeUncompressed[:])
	require.NoError(t, err)

	// Write size_compressed (same as uncompressed for no compression)
	var sizeCompressed [4]byte
	binary.BigEndian.PutUint32(sizeCompressed[:], uint32(len(data)))
	_, err = f.Write(sizeCompressed[:])
	require.NoError(t, err)

	// Write content hash
	_, err = f.Write(contentHash[:])
	require.NoError(t, err)

	// Write data
	_, err = f.Write(data)
	require.NoError(t, err)

	return packfileDir
}

func TestPackfileReader_Open_L2Hit(t *testing.T) {
	tmpDir := t.TempDir()
	packfileID := PackfileID("pf1")

	setupPackfileL2(t, tmpDir, packfileID, []byte("test data content"))

	cache := NewReaderCache(10)
	reader := NewPackfileReader(cache, nil)

	// Open should hit L2 (local disk)
	handle, err := reader.Open(context.Background(), tmpDir, packfileID)
	require.NoError(t, err)
	require.NotNil(t, handle)
	assert.Equal(t, packfileID, handle.PackfileID)
	assert.NotNil(t, handle.IndexReader)

	// Should be cached in L1 now
	entry, ok := cache.entries.Load(packfileID)
	assert.True(t, ok)
	assert.Equal(t, int32(1), entry.(*PackfileCacheEntry).Refcount)

	handle.Close()
}

func TestPackfileReader_Open_L3Unavailable(t *testing.T) {
	tmpDir := t.TempDir()
	packfileID := PackfileID("nonexistent")

	// Create dir but no files
	packfileDir := filepath.Join(tmpDir, "_packfiles", "packfiles", string(packfileID))
	err := os.MkdirAll(packfileDir, 0755)
	require.NoError(t, err)

	// Create mock adapter that returns error
	mock := &mockAdapter{
		getError: os.ErrNotExist,
	}

	cache := NewReaderCache(10)
	reader := NewPackfileReader(cache, mock)

	// Open should fail with ErrPackfileUnavailable
	handle, err := reader.Open(context.Background(), tmpDir, packfileID)
	assert.ErrorIs(t, err, ErrPackfileUnavailable)
	assert.Nil(t, handle)
}

func TestPackfileHandle_GetObject_Found(t *testing.T) {
	tmpDir := t.TempDir()
	packfileID := PackfileID("test-pf")

	testData := []byte("test object data content")
	setupPackfileL2(t, tmpDir, packfileID, testData)

	cache := NewReaderCache(10)
	reader := NewPackfileReader(cache, nil)

	// Open the packfile
	handle, err := reader.Open(context.Background(), tmpDir, packfileID)
	require.NoError(t, err)

	// Get the object
	objReader, err := handle.GetObject(context.Background(), sha256Hash(testData))
	require.NoError(t, err)
	require.NotNil(t, objReader)

	// Read the data
	data, err := io.ReadAll(objReader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)

	// Object reader is a no-op closer - handle.Close handles actual close
	_ = objReader.Close()

	handle.Close()
}

func TestPackfileHandle_GetObject_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	packfileID := PackfileID("test-pf")

	setupPackfileL2(t, tmpDir, packfileID, []byte("test data"))

	cache := NewReaderCache(10)
	reader := NewPackfileReader(cache, nil)

	handle, err := reader.Open(context.Background(), tmpDir, packfileID)
	require.NoError(t, err)

	// Try to get a non-existent object
	nonExistentHash := sha256Hash([]byte("nonexistent data"))
	objReader, err := handle.GetObject(context.Background(), nonExistentHash)
	assert.ErrorIs(t, err, ErrObjectNotFound)
	assert.Nil(t, objReader)

	handle.Close()
}
