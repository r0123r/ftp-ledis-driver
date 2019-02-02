package ledisdriver

import (
	"bytes"
	"io"
)

func NewSkipReadCloser(rd []byte) io.ReadCloser {
	return &SkipReadCloser{Reader: bytes.NewReader(rd)}
}

type SkipReadCloser struct {
	io.Closer
	io.Reader
}

func (s *SkipReadCloser) Read(data []byte) (int, error) {
	return s.Reader.Read(data)
}
func (s *SkipReadCloser) Close() error {
	return nil
}
