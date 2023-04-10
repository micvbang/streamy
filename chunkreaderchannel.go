package streamy

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type ChunkReader struct {
	chunkPaths <-chan string

	curChunkFile       *os.File
	curChunkFileOffset int
}

func NewChunkReader(chunkPaths <-chan string) *ChunkReader {
	return &ChunkReader{
		chunkPaths: chunkPaths,
	}
}

func (cr *ChunkReader) Read(buf []byte) (int, error) {
	if cr.curChunkFile == nil {
		err := cr.openNextChunk()
		if err != nil {
			return 0, err
		}
	}

	var totalRead int
	for totalRead < len(buf) {
		n, err := cr.curChunkFile.Read(buf[totalRead:])
		totalRead += n
		if err == nil { // no error, continue reading
			continue
		}

		if !errors.Is(err, io.EOF) { // unexpected error, return
			return totalRead, err
		} else { // EOF, try next file
			nextErr := cr.openNextChunk()
			if nextErr != nil {
				if errors.Is(nextErr, io.EOF) { // no more files, return EOF
					return totalRead, io.EOF
				}

				return totalRead, nextErr // unexpected error
			}
		}
	}

	return totalRead, nil
}

func (cr *ChunkReader) openNextChunk() error {
	if cr.curChunkFile != nil {
		err := cr.curChunkFile.Close()
		if err != nil {
			return fmt.Errorf("failed to close curChunkFile: %w", err)
		}
	}

	chunkPath, ok := <-cr.chunkPaths
	if !ok {
		// channel closed, read all chunks
		return io.EOF
	}
	f, err := os.Open(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to open chunk '%s': %w", chunkPath, err)
	}

	cr.curChunkFile = f
	cr.curChunkFileOffset = 0

	return nil
}
