package packfile

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePackfileHeader(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    *PackfileHeader
		wantErr string
	}{
		{
			name: "valid header",
			data: func() []byte {
				h := &PackfileHeader{
					Magic:            [4]byte{'L', 'K', 'P', '1'},
					Version:          1,
					ChecksumAlgo:     ChecksumSHA256,
					CompressionAlgo:  CompressionNone,
					Classification:   ClassificationFirstClass,
					ObjectCount:      100,
					TotalSize:        1000,
					TotalSizeCompressed: 800,
				}
				return headerToBytes(h)
			}(),
			want: &PackfileHeader{
				Magic:            [4]byte{'L', 'K', 'P', '1'},
				Version:          1,
				ChecksumAlgo:     ChecksumSHA256,
				CompressionAlgo:  CompressionNone,
				Classification:   ClassificationFirstClass,
				ObjectCount:      100,
				TotalSize:        1000,
				TotalSizeCompressed: 800,
			},
		},
		{
			name:    "too short",
			data:    []byte{1, 2, 3},
			wantErr: "header too short",
		},
		{
			name: "invalid magic",
			data: func() []byte {
				h := &PackfileHeader{
					Magic:            [4]byte{'B', 'A', 'D', '1'},
					Version:          1,
					ChecksumAlgo:     ChecksumSHA256,
					CompressionAlgo:  CompressionNone,
					Classification:   ClassificationFirstClass,
					ObjectCount:      1,
				}
				return headerToBytes(h)
			}(),
			wantErr: "invalid magic",
		},
		{
			name: "zero object count",
			data: func() []byte {
				h := &PackfileHeader{
					Magic:            [4]byte{'L', 'K', 'P', '1'},
					Version:          1,
					ChecksumAlgo:     ChecksumSHA256,
					CompressionAlgo:  CompressionNone,
					Classification:   ClassificationFirstClass,
					ObjectCount:      0,
				}
				return headerToBytes(h)
			}(),
			wantErr: "object_count is zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePackfileHeader(bytes.NewReader(tt.data))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want.Magic, got.Magic)
			assert.Equal(t, tt.want.Version, got.Version)
			assert.Equal(t, tt.want.ChecksumAlgo, got.ChecksumAlgo)
			assert.Equal(t, tt.want.CompressionAlgo, got.CompressionAlgo)
			assert.Equal(t, tt.want.Classification, got.Classification)
			assert.Equal(t, tt.want.ObjectCount, got.ObjectCount)
			assert.Equal(t, tt.want.TotalSize, got.TotalSize)
			assert.Equal(t, tt.want.TotalSizeCompressed, got.TotalSizeCompressed)
		})
	}
}

func TestWritePackfileHeader(t *testing.T) {
	tests := []struct {
		name string
		h    *PackfileHeader
	}{
		{
			name: "round-trip",
			h: &PackfileHeader{
				Magic:            [4]byte{'L', 'K', 'P', '1'},
				Version:          1,
				ChecksumAlgo:     ChecksumSHA256,
				CompressionAlgo:  CompressionZstd,
				Classification:   ClassificationSecondClass,
				ObjectCount:      500,
				TotalSize:        100000,
				TotalSizeCompressed: 50000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WritePackfileHeader(&buf, tt.h)
			require.NoError(t, err)
			assert.Equal(t, 40, buf.Len())

			// Read back and verify
			got, err := ParsePackfileHeader(&buf)
			require.NoError(t, err)
			assert.Equal(t, tt.h.Magic, got.Magic)
			assert.Equal(t, tt.h.Version, got.Version)
			assert.Equal(t, tt.h.ChecksumAlgo, got.ChecksumAlgo)
			assert.Equal(t, tt.h.CompressionAlgo, got.CompressionAlgo)
			assert.Equal(t, tt.h.Classification, got.Classification)
			assert.Equal(t, tt.h.ObjectCount, got.ObjectCount)
			assert.Equal(t, tt.h.TotalSize, got.TotalSize)
			assert.Equal(t, tt.h.TotalSizeCompressed, got.TotalSizeCompressed)
		})
	}
}

func TestReadObject(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    *PackfileObject
		wantErr string
	}{
		{
			name: "valid object no compression",
			data: func() []byte {
				var buf bytes.Buffer
				// size_uncompressed = 10
				binary.Write(&buf, binary.BigEndian, uint32(10))
				// size_compressed = 10
				binary.Write(&buf, binary.BigEndian, uint32(10))
				// content hash = 32 zero bytes
				buf.Write(make([]byte, 32))
				// data = "1234567890"
				buf.WriteString("1234567890")
				return buf.Bytes()
			}(),
			want: &PackfileObject{
				SizeUncompressed: 10,
				SizeCompressed:   10,
				ContentHash:     ContentHash{},
			},
		},
		{
			name: "compressed exceeds uncompressed",
			data: func() []byte {
				var buf bytes.Buffer
				// size_uncompressed = 5
				binary.Write(&buf, binary.BigEndian, uint32(5))
				// size_compressed = 10 (GREATER than uncompressed!)
				binary.Write(&buf, binary.BigEndian, uint32(10))
				// content hash
				buf.Write(make([]byte, 32))
				// data (won't be read due to validation failure)
				buf.WriteString("12345678901234567890")
				return buf.Bytes()
			}(),
			wantErr: "corrupt object",
		},
		{
			name:    "truncated - missing size",
			data:    []byte{1, 2},
			wantErr: "unexpected end of file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadObject(bytes.NewReader(tt.data))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want.SizeUncompressed, got.SizeUncompressed)
			assert.Equal(t, tt.want.SizeCompressed, got.SizeCompressed)
			assert.Equal(t, tt.want.ContentHash, got.ContentHash)
			// Read the data to verify it's correct
			data, err := io.ReadAll(got.Data)
			require.NoError(t, err)
			assert.Equal(t, "1234567890", string(data))
		})
	}
}