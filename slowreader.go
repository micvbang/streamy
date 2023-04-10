package streamy

import (
	"io"
	"time"
)

type SlowReader struct {
	rdr       io.Reader
	ReadDelay time.Duration
}

func NewSlowReader(rdr io.Reader, readDelay time.Duration) SlowReader {
	return SlowReader{
		rdr:       rdr,
		ReadDelay: readDelay,
	}
}

func (sr SlowReader) Read(buf []byte) (int, error) {
	time.Sleep(sr.ReadDelay)
	return sr.rdr.Read(buf)
}
