package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/micvbang/streamy"

	"github.com/google/uuid"
)

type request struct {
	chunkPathConsumers chan (chan<- string)

	chunkPaths []string
}

func (req request) Done() bool {
	return len(req.chunkPaths) != 0
}

func main() {
	listenAddr := ":8080"
	log.Printf("Listening on %s", listenAddr)

	mu := &sync.Mutex{}
	requests := make(map[uuid.UUID]*request, 64)
	router := makeRouter(mu, requests)

	err := http.ListenAndServe(listenAddr, router)
	log.Fatalf("http server returned: %s", err)
}

func makeRouter(mu *sync.Mutex, requests map[uuid.UUID]*request) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("request URL: %s\n", r.URL)

		createUploadHandler := newCreateUploadHandler(mu, requests)
		uploadHandler := newUploadHandler(mu, requests)
		downloadHandler := newDownloadHandler(mu, requests)

		switch r.URL.Path {
		case "/requests/new":
			createUploadHandler(w, r)
			return

		case "/requests/download":
			downloadHandler(w, r)
			return

		case "/requests/upload":
			uploadHandler(w, r)
			return

		default:
			log.Printf("unhandled path")
		}
	}
}

func newCreateUploadHandler(mu *sync.Mutex, requests map[uuid.UUID]*request) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestUUID := uuid.New()
		requests[requestUUID] = &request{
			chunkPathConsumers: make(chan (chan<- string), 8),
		}
		mu.Unlock()

		w.WriteHeader(201)
		w.Write([]byte(fmt.Sprintf(`{"request_id": "%s"}`, requestUUID)))
	}
}

func newDownloadHandler(mu *sync.Mutex, requests map[uuid.UUID]*request) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseForm()
		if err != nil {
			httpWriteError(w, http.StatusBadRequest, fmt.Sprintf("failed to parse multipart form: %s", err))
		}
		requestUUID, err := uuid.Parse(r.Form.Get("uuid"))
		if err != nil {
			httpWriteError(w, http.StatusBadRequest, fmt.Sprintf("failed to parse multipart form: %s", err))
			return
		}
		log.Printf("download request for %s", requestUUID)

		mu.Lock()
		req, ok := requests[requestUUID]
		if !ok {
			log.Printf("download requests %p %+v", requests, requests)
			httpWriteError(w, http.StatusBadRequest, "request id does not exist")
			mu.Unlock()
			return
		}
		chunkPaths := make(chan string, 32)
		if !req.Done() {
			req.chunkPathConsumers <- chunkPaths
		} else {
			chunkPaths = make(chan string, len(req.chunkPaths))
			for _, chunkPath := range req.chunkPaths {
				chunkPaths <- chunkPath
			}
			close(chunkPaths)
		}
		mu.Unlock()

		chunkReader := streamy.NewChunkReader(chunkPaths)

		n, err := io.Copy(w, chunkReader)
		if err != nil {
			log.Printf("failed to copy to http response: %s", err)
			return
		}

		log.Printf("total written %d", n)
	}
}

func newUploadHandler(mu *sync.Mutex, requests map[uuid.UUID]*request) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseMultipartForm(64 * 1024)
		if err != nil {
			httpWriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed to parse multipart form: %s", err))
			return
		}

		requestUUID, err := uuid.Parse(r.Form.Get("uuid"))
		if err != nil {
			httpWriteError(w, http.StatusBadRequest, fmt.Sprintf("failed to parse multipart form: %s", err))
			return
		}

		mu.Lock()
		req, ok := requests[requestUUID]
		if !ok {
			mu.Unlock()
			w.Write([]byte("invalid request id"))
			return
		}
		mu.Unlock()

		chunksComplete := make(chan string, 8)

		chunkWriter, err := streamy.NewChunkWriter(fmt.Sprintf("/tmp/streamy_%s", requestUUID), 4*1024*1024, chunksComplete)
		if err != nil {
			httpWriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed to init split writer: %s", err))
			mu.Unlock()
			return
		}

		// handle new downloaders arriving and new chunks being complete
		go handleNewChunkConsumers(req.chunkPathConsumers, chunksComplete)

		userFile, userFileHeader, err := r.FormFile("file")
		if err != nil {
			httpWriteError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read form file: %s", err))
			return
		}
		defer userFile.Close()
		log.Printf("user file size: %d", userFileHeader.Size)

		slowReader := streamy.NewSlowReader(userFile, 5*time.Millisecond)
		readTotal, err := io.CopyN(chunkWriter, slowReader, userFileHeader.Size)
		if err != nil {
			log.Printf("failed to copy %d bytes to chunk writer: %s", userFileHeader.Size, err)
		}

		err = chunkWriter.Close()
		if err != nil {
			log.Printf("failed to close splitWriter: %s", err)
			return
		}
		log.Printf("read %d/%d bytes", userFileHeader.Size, readTotal)

		// Upload done and chunks written. Allow following uploads to be served
		// directly from chunks.
		mu.Lock()
		req = requests[requestUUID]
		req.chunkPaths = chunkWriter.FilePaths()
		defer mu.Unlock()
	}
}

func httpWriteError(w http.ResponseWriter, statusCode int, msg string) {
	log.Print(msg)
	w.WriteHeader(statusCode)
	w.Write([]byte(msg))
}

func handleNewChunkConsumers(chunkPathConsumers <-chan (chan<- string), chunksComplete <-chan string) {
	go func() {
		chunkPaths := make([]string, 0, 64)
		chunkConsumers := make([]chan<- string, 0, 4)
		for {
			select {
			case consumer := <-chunkPathConsumers:
				chunkConsumers = append(chunkConsumers, consumer)
				for _, filePath := range chunkPaths {
					consumer <- filePath
				}
				continue

			case filePath, ok := <-chunksComplete:
				if !ok {
					for _, consumer := range chunkConsumers {
						close(consumer)
					}
					return
				}

				chunkPaths = append(chunkPaths, filePath)

				// NOTE: blocks if consumer doesn't drain its channel
				for _, consumer := range chunkConsumers {
					consumer <- filePath
				}
			}
		}
	}()

}
