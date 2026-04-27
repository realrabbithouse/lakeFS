package packfile

import (
	"errors"
	"io"

	"github.com/klauspost/compress/zstd"
)

// ErrUnsupportedCompression is returned when a compression algorithm is not supported.
var ErrUnsupportedCompression = errors.New("unsupported compression algorithm")

// CompressTo reads from r and writes compressed data to w using algorithm c.
// Uses streaming IO — no in-memory []byte for bulk data.
func CompressTo(r io.Reader, c Compression, w io.Writer) error {
	switch c {
	case CompressionNone:
		_, err := io.Copy(w, r)
		return err
	case CompressionZstd:
		enc, err := zstd.NewWriter(w)
		if err != nil {
			return err
		}
		_, err = io.Copy(enc, r)
		if closeErr := enc.Close(); err == nil {
			err = closeErr
		}
		return err
	default:
		return ErrUnsupportedCompression
	}
}

// UncompressTo reads compressed data from r and writes uncompressed data to w.
// Uses streaming IO — no in-memory []byte for bulk data.
func UncompressTo(r io.Reader, c Compression, w io.Writer) error {
	switch c {
	case CompressionNone:
		_, err := io.Copy(w, r)
		return err
	case CompressionZstd:
		dec, err := zstd.NewReader(r)
		if err != nil {
			return err
		}
		defer dec.Close()
		_, err = io.Copy(w, dec)
		return err
	default:
		return ErrUnsupportedCompression
	}
}

// NewCompressionReader returns a streaming io.Reader that compresses data from r
// using algorithm c. The caller must close the reader to clean up resources.
func NewCompressionReader(r io.Reader, c Compression) (io.Reader, error) {
	switch c {
	case CompressionNone:
		return r, nil
	case CompressionZstd:
		pr, pw := io.Pipe()
		go func() {
			pw.CloseWithError(CompressTo(r, c, pw))
		}()
		return pr, nil
	default:
		return nil, ErrUnsupportedCompression
	}
}

// NewDecompressionReader returns a streaming io.Reader that decompresses data from r
// using algorithm c. The caller must close the reader to clean up resources.
func NewDecompressionReader(r io.Reader, c Compression) (io.Reader, error) {
	switch c {
	case CompressionNone:
		return r, nil
	case CompressionZstd:
		dec, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &zstdReader{dec: dec, r: r}, nil
	default:
		return nil, ErrUnsupportedCompression
	}
}

// zstdReader wraps a zstd decoder to implement io.ReadCloser.
type zstdReader struct {
	dec *zstd.Decoder
	r   io.Reader
}

func (z *zstdReader) Read(p []byte) (int, error) {
	return z.dec.Read(p)
}

func (z *zstdReader) Close() error {
	z.dec.Close()
	return nil
}
