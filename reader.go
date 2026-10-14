package s3io

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type chunkOut struct {
	n 	int
	p 	*[]byte
	err 	error	
}

type Reader struct {
	io.ReadCloser
	key          string
	length 	     uint64 // object length in total
	n 	     int
	total 	     uint64 // total bytes read
	partSize     int
	partNum      int
	numParts     int
	store        *FileStore
	maxWorkers   int
	isClosed     bool
	buf 	      []byte
	r, w         int       // buf read and write positions
	lastByte     int // last byte read for UnreadByte; -1 means invalid
	lastRuneSize int // size of last rune read for UnreadRune; -1 means invalid
	err 	     error
	chunkIn      chan *chunk
	chunkOut     chan *chunkOut
	objectDone   chan error
}

func (r *Reader) producer() {
}

func (r *Reader) close() error {
	if r.isClosed {
		return nil
	}
	return r.Close()
}

func (b *Reader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

// Available returns how many bytes are unused in the buffer.
func (r *Reader) Available() int { return len(r.buf) - r.n }

// Buffered returns the number of bytes that have been read into the current buffer.
func (r *Reader) Buffered() int { return r.n }

// Total returns the number of total bytes that have been read.
func (r *Reader) Total() uint64 { return r.total }

// Size returns the size of the underlying buffer in bytes.
func (r *Reader) Size() int { return len(r.buf) }

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying [Reader],
// hence n may be less than len(p).
// To read exactly len(p) bytes, use io.ReadFull(b, p).
// If the underlying [Reader] can return a non-zero count with io.EOF,
// then this Read method can do so as well; see the [io.Reader] docs.
func (b *Reader) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		if b.Buffered() > 0 {
			return 0, nil
		}
		return 0, b.readErr()
	}
	if b.r == b.w && b.total < b.length {
		if b.err != nil {
			return 0, b.readErr()
		}
		if len(p) >= b.Size() {
			// Large read, empty buffer.
			// Read directly into p to avoid copy.
			buf := b.store.allocpool.Alloc(int(CHUNK_SIZE))
			defer b.store.allocpool.Free(buf)
			r, _, err := b.downloadChunk(int64(b.total), int64(b.total) + int64(b.partSize), &buf)
			if err != nil {
				b.err = err
				return 0, err
			}
			defer r.Close()

			n := 0
			for {
				nn, err := r.Read(p)
				n += nn
				if err != nil {
					if err != io.EOF {
						b.err = err
					}
					break
				}
			}

			b.total += uint64(n)

			if n > 0 {
				b.lastByte = int(p[n-1])
				b.lastRuneSize = -1
			}
			return int(n), b.readErr()
		}
		// One read.
		b.r = 0
		b.w = 0

		buf := b.store.allocpool.Alloc(int(CHUNK_SIZE))
		defer b.store.allocpool.Free(buf)
		r, length, err := b.downloadChunk(int64(b.total), int64(b.total) + int64(b.partSize), &buf)
		if err != nil {
			b.err = err
			return 0, err
		}

		b.length = uint64(length)

		n := 0
		for {
			nn, err := r.Read(b.buf)
			n += nn
			if err != nil {
				if err != io.EOF {
					b.err = err
				}
				break
			}
		}

		r.Close()
		if n == 0 {
			return 0, b.readErr()
		}
		b.w += int(n)
	}
	
	if b.total == b.length {
		return 0, io.EOF
	}

	// copy as much as we can
	// Note: if the slice panics here, it is probably because
	// the underlying reader returned a bad count. See issue 49795.
	n = copy(p, b.buf[b.r:b.w])
	b.r += n
	b.total += uint64(n)
	b.lastByte = int(b.buf[b.r-1])
	b.lastRuneSize = -1
	return n, nil
}

// headObject fetches the metadata of a file
func (r *Reader) headObject() (*s3.HeadObjectOutput, error) {
	res, err := r.store.Service.HeadObject(
		&s3.HeadObjectInput{
			Bucket: aws.String(r.store.Config.Bucket),
			Key:    aws.String(r.key),
		})

	return res, err
}

// downloadChunk downloads the chunk from s3 it returns reader or error if any
func (r *Reader) downloadChunk(start int64, end int64, buf *[]byte) (io.ReadCloser, int64, error) {
	var err error

	res, err := r.store.Service.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(r.store.Config.Bucket),
			Key:    aws.String(r.key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end-1)),
		})

	var totalBytes int64 = 0
	if err == nil {
		if res.ContentRange == nil {
			// ContentLength is not nil then the full file contents is provided and is not chunked
			if res.ContentLength != nil {
				totalBytes = *res.ContentLength
			}
		} else {
			parts := strings.Split(*res.ContentRange, "/")
			total := int64(-1)
			var err error
			// Checking for whether or not a numbered total exists
			// If one does not exist, we will assume the total to be -1, undefined,
			// and sequentially download each chunk until hitting a 416 error
			totalStr := parts[len(parts)-1]
			if totalStr != "*" {
				total, err = strconv.ParseInt(totalStr, 10, 64)
				if err != nil {
					return nil, 0, err
				}
			}

			totalBytes = total
		}
	}

	if res.Body == nil {
		return nil, 0, errors.New("invalid response body")
	}

	// If the file cannot be found, we ignore this error and continue since the
	// upload may not have been finished yet. In this case we do not want to
	// return a ErrNotFound but a more meaning-full message.
	if err != nil {
		return nil, totalBytes, err
	}

	return res.Body, totalBytes, nil
}

func NewReader(store *FileStore, key string) (*Reader, error) {
	var (
		partSize    int = CHUNK_SIZE
		length      uint64 = 0
		numParts    int = 0
		concurrency int
	)
	
	r := &Reader{
		key: 		key,
		store: 		store,
	}

	// find out total size of the requested file
	if meta, err := r.headObject(); err != nil {
		return nil, err
	} else {
		length = uint64(*meta.ContentLength)
	}

	numParts = int(math.Round(float64(length / uint64(partSize))))
	if length%uint64(partSize) != 0 {
		numParts++
	}

	if int(numParts) < concurrency {
		concurrency = int(numParts)
	}

	r.buf = make([]byte, MIN_MULTIPART_SIZE)
	r.numParts = numParts
	r.partSize = partSize
	r.maxWorkers = concurrency
	r.length = length
	r.partNum  = 0
	r.total = 0
	r.n = 0
	r.r = 0
	return r, nil
}
