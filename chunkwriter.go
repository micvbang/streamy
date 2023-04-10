package streamy

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type ChunkWriter struct {
	chunkPathPrefix string
	maxChunkSize    int

	mu            *sync.Mutex
	chunkPaths    []string
	chunksWritten chan<- string

	curChunkPath string
	curChunkSize int
	curChunkFile *os.File
}

func NewChunkWriter(filePathPrefix string, maxFileSize int, chunksWritten chan<- string) (*ChunkWriter, error) {
	sw := &ChunkWriter{
		chunkPathPrefix: filePathPrefix,
		maxChunkSize:    maxFileSize,
		mu:              &sync.Mutex{},
		chunksWritten:   chunksWritten,
	}
	err := sw.openNextChunk()
	if err != nil {
		return nil, err
	}

	return sw, nil
}

func (sw *ChunkWriter) Write(buf []byte) (int, error) {
	var n1 int
	var err error
	if sw.curChunkSize+len(buf) > sw.maxChunkSize {
		// write remainder, up to maxChunkSize for curChunk
		remainingBytes := sw.maxChunkSize - sw.curChunkSize
		n1, err = sw.curChunkFile.Write(buf[:remainingBytes])
		if err != nil {
			return 0, fmt.Errorf("failed to write remaining bytes: %w", err)
		}
		buf = buf[remainingBytes:]

		err = sw.openNextChunk()
		if err != nil {
			return 0, err
		}
	}

	n2, err := sw.curChunkFile.Write(buf)
	writeTotal := n1 + n2
	sw.curChunkSize += writeTotal

	return writeTotal, err
}

func (sw *ChunkWriter) Close() error {
	defer close(sw.chunksWritten)

	err := sw.curChunkFile.Close()
	if err != nil {
		return err
	}

	sw.chunkDone(sw.curChunkPath)
	return nil
}

func (sw *ChunkWriter) chunkDone(chunkPath string) {
	sw.mu.Lock()
	if sw.chunksWritten != nil {
		sw.chunksWritten <- chunkPath
	}
	sw.chunkPaths = append(sw.chunkPaths, chunkPath)
	sw.mu.Unlock()

}

func (sw *ChunkWriter) openNextChunk() error {
	if sw.curChunkFile != nil {
		err := sw.curChunkFile.Close()
		if err != nil {
			return fmt.Errorf("failed to close curFile: %w", err)
		}

		sw.chunkDone(sw.curChunkPath)
	}

	sw.curChunkPath = fmt.Sprintf("%s_%03d.bin", sw.chunkPathPrefix, len(sw.chunkPaths))
	log.Printf("opening file %s for writing", sw.curChunkPath)

	f, err := os.OpenFile(sw.curChunkPath, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	sw.curChunkFile = f
	sw.curChunkSize = 0
	return nil
}

func (sw *ChunkWriter) FilePaths() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.chunkPaths
}
