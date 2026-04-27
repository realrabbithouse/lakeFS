package packfile

import (
	"bytes"
	"io"
)

// testWriteSeeker implements io.WriteSeeker backed by bytes.Buffer for testing.
type testWriteSeeker struct {
	buf bytes.Buffer
}

func (ws *testWriteSeeker) Write(p []byte) (int, error) {
	return ws.buf.Write(p)
}

func (ws *testWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return 0, nil
	case io.SeekCurrent:
		return int64(ws.buf.Len()), nil
	case io.SeekEnd:
		return int64(ws.buf.Len()), nil
	}
	return 0, nil
}

// bytes returns the underlying buffer contents.
func (ws *testWriteSeeker) bytes() []byte {
	return ws.buf.Bytes()
}

// newTestWriteSeeker creates a new testWriteSeeker for testing.
func newTestWriteSeeker() *testWriteSeeker {
	return &testWriteSeeker{}
}
