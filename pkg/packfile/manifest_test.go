package packfile

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 64-char hex string for testing
const validChecksum1 = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6abcd"
const validChecksum2 = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

func TestProcessManifest_Valid(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":2}
{"path":"images/logo.png","checksum":"` + validChecksum1 + `","content_type":"image/png","relative_offset":0,"size_uncompressed":12345,"size_compressed":6789}
{"path":"images/bg.png","checksum":"` + validChecksum2 + `","content_type":"image/png","relative_offset":8192,"size_uncompressed":23456,"size_compressed":11234}`

	reader := bytes.NewReader([]byte(manifestData))
	manifest, err := ProcessManifest(context.Background(), reader)

	require.NoError(t, err)
	require.NotNil(t, manifest)
	assert.Equal(t, uint64(2), manifest.Metadata.ObjectCount)
	assert.Len(t, manifest.Entries, 2)
	assert.Equal(t, "images/logo.png", manifest.Entries[0].Path)
	assert.Equal(t, "image/png", manifest.Entries[0].ContentType)
}

func TestProcessManifest_ObjectCountMismatch(t *testing.T) {
	// Object count says 3 but only 2 entries provided
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":3}
{"path":"images/logo.png","checksum":"` + validChecksum1 + `","content_type":"image/png","relative_offset":0,"size_uncompressed":12345,"size_compressed":6789}
{"path":"images/bg.png","checksum":"` + validChecksum2 + `","content_type":"image/png","relative_offset":8192,"size_uncompressed":23456,"size_compressed":11234}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object_count mismatch")
}

func TestProcessManifest_MissingPath(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"checksum":"` + validChecksum1 + `","content_type":"image/png","relative_offset":0,"size_uncompressed":12345,"size_compressed":6789}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required field 'path'")
}

func TestProcessManifest_MissingChecksum(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"path":"images/logo.png","content_type":"image/png","relative_offset":0,"size_uncompressed":12345,"size_compressed":6789}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required field 'checksum'")
}

func TestProcessManifest_InvalidChecksumHex(t *testing.T) {
	// Checksum is only 63 chars (should be 64)
	shortChecksum := "abc123def456abc123def456abc123def456abc123def456abc123def456abc12" // 64 chars
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"path":"images/logo.png","checksum":"` + shortChecksum + `","content_type":"image/png","relative_offset":0,"size_uncompressed":12345,"size_compressed":6789}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checksum must be 64 hex characters")
}

func TestProcessManifest_InvalidHexCharacter(t *testing.T) {
	// Checksum contains 'g' which is not valid hex
	invalidChecksum := "gg1234def456abc123def456abc123def456abc123def456abc123def456abc1" // 64 chars with 'g'
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"path":"images/logo.png","checksum":"` + invalidChecksum + `","content_type":"image/png","relative_offset":0,"size_uncompressed":12345,"size_compressed":6789}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid hex character")
}

func TestProcessManifest_EmptyManifest(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":0}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object_count must be greater than 0")
}

func TestProcessManifest_MetadataLineOnly(t *testing.T) {
	// Only metadata line, no entries
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":2}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object_count mismatch")
}

func TestProcessManifest_InvalidJSON(t *testing.T) {
	manifestData := `{invalid json here}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest metadata")
}

func TestProcessManifest_EntryInvalidJSON(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{invalid entry json}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manifest entry 2")
}

func TestProcessManifest_EmptyLines(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":2}

{"path":"images/logo.png","checksum":"` + validChecksum1 + `","content_type":"image/png","relative_offset":0,"size_uncompressed":12345,"size_compressed":6789}

{"path":"images/bg.png","checksum":"` + validChecksum2 + `","content_type":"image/png","relative_offset":8192,"size_uncompressed":23456,"size_compressed":11234}`

	reader := bytes.NewReader([]byte(manifestData))
	manifest, err := ProcessManifest(context.Background(), reader)

	require.NoError(t, err)
	require.NotNil(t, manifest)
	assert.Equal(t, uint64(2), manifest.Metadata.ObjectCount)
	assert.Len(t, manifest.Entries, 2)
}

func TestValidateManifest(t *testing.T) {
	tests := []struct {
		name      string
		manifest  *Manifest
		wantError bool
	}{
		{
			name: "valid manifest",
			manifest: &Manifest{
				Metadata: ManifestMetadata{ObjectCount: 2},
				Entries: []ManifestEntry{
					{Path: "a", Checksum: validChecksum1, SizeUncompressed: 100, SizeCompressed: 50},
					{Path: "b", Checksum: validChecksum2, SizeUncompressed: 200, SizeCompressed: 100},
				},
			},
			wantError: false,
		},
		{
			name:      "nil manifest",
			manifest:  nil,
			wantError: true,
		},
		{
			name: "empty entries",
			manifest: &Manifest{
				Metadata: ManifestMetadata{ObjectCount: 0},
				Entries: []ManifestEntry{},
			},
			wantError: true,
		},
		{
			name: "object count mismatch",
			manifest: &Manifest{
				Metadata: ManifestMetadata{ObjectCount: 5},
				Entries: []ManifestEntry{
					{Path: "a", Checksum: validChecksum1, SizeUncompressed: 100, SizeCompressed: 50},
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateManifest(tt.manifest)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCompleteUpload(t *testing.T) {
	ctx := context.Background()
	err := CompleteUpload(ctx, UploadID("test-upload"))
	require.NoError(t, err)
}

func TestAbortUpload(t *testing.T) {
	ctx := context.Background()
	err := AbortUpload(ctx, UploadID("test-upload"))
	require.NoError(t, err)
}

func TestProcessManifest_SizeValidation(t *testing.T) {
	entry := ManifestEntry{
		Path:             "test.png",
		Checksum:         validChecksum1,
		SizeUncompressed: 100,
		SizeCompressed:   50,
	}

	// Valid entry
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"path":"test.png","checksum":"` + validChecksum1 + `","content_type":"image/png","relative_offset":0,"size_uncompressed":100,"size_compressed":50}`

	reader := bytes.NewReader([]byte(manifestData))
	manifest, err := ProcessManifest(context.Background(), reader)
	require.NoError(t, err)
	require.NotNil(t, manifest)
	assert.Equal(t, entry.Path, manifest.Entries[0].Path)
}

func TestProcessManifest_ZeroUncompressedSize(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"path":"test.png","checksum":"` + validChecksum1 + `","content_type":"image/png","relative_offset":0,"size_uncompressed":0,"size_compressed":50}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "size_uncompressed must be greater than 0")
}

func TestProcessManifest_ZeroCompressedSize(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"path":"test.png","checksum":"` + validChecksum1 + `","content_type":"image/png","relative_offset":0,"size_uncompressed":100,"size_compressed":0}`

	reader := bytes.NewReader([]byte(manifestData))
	_, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "size_compressed must be greater than 0")
}

func TestProcessManifest_EmptyReader(t *testing.T) {
	reader := bytes.NewReader([]byte{})
	manifest, err := ProcessManifest(context.Background(), reader)

	assert.Error(t, err)
	assert.Nil(t, manifest)
}

func TestProcessManifest_WithMetadata(t *testing.T) {
	manifestData := `{"packfile_checksum":"` + validChecksum1 + `","object_count":1}
{"path":"test.json","checksum":"` + validChecksum1 + `","content_type":"application/json","metadata":{"key1":"value1","key2":"value2"},"relative_offset":0,"size_uncompressed":100,"size_compressed":50}`

	reader := bytes.NewReader([]byte(manifestData))
	manifest, err := ProcessManifest(context.Background(), reader)

	require.NoError(t, err)
	require.NotNil(t, manifest)
	assert.Equal(t, uint64(1), manifest.Metadata.ObjectCount)
	assert.Len(t, manifest.Entries, 1)
	assert.Equal(t, "application/json", manifest.Entries[0].ContentType)
	assert.Equal(t, "value1", manifest.Entries[0].Metadata["key1"])
}
