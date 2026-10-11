package s3io

import (
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
	/*
	var data *chunkOut

	if b.total == 0 {
		go b.producer()
	}
	*/

	// we have room in the buffer for atleast a part
	// or if the readable buffer is bigger what we have in store
	for (len(p) >= b.Buffered() && b.Available() > b.partSize && b.err == nil && b.total < b.length) {
		buf := b.store.allocpool.Alloc(int(MIN_MULTIPART_SIZE))
		n, err = b.downloadChunk(int64(b.total), int64(b.total) + int64(b.partSize), &buf)
		if err != nil && err != io.EOF {
			b.err = err
			break
		}

		nn := copy(b.buf[b.n:], buf[0:n])
		b.store.allocpool.Free(buf)
		b.n += nn
		b.total += uint64(n)
	}


	if b.Buffered() > len(p) {
		nn := copy(p, b.buf[b.r:])
		b.n -= nn
		b.r += nn
		return nn, nil
	}

	if b.Buffered() == 0 {
		return 0, io.EOF	
	}

	nn := copy(p, b.buf[b.r:b.r+b.n])
	b.r = 0
	b.n = 0

	return nn, nil
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
func (r *Reader) downloadChunk(start int64, end int64, buf *[]byte) (int, error) {
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
					return 0, err
				}
			}

			totalBytes = total
		}
	}
	
	if res.Body != nil {
		defer res.Body.Close()
	}

	// If the file cannot be found, we ignore this error and continue since the
	// upload may not have been finished yet. In this case we do not want to
	// return a ErrNotFound but a more meaning-full message.
	if err != nil {
		return 0, err
	}

	n := 0
	pos := 0
	b := *buf
	for {
		n, err = res.Body.Read(b[pos:])
		if err != nil && err != io.EOF {
			return pos, err
		}

		pos += n

		if err == io.EOF {
			break
		}

		if int64(pos) >= totalBytes {
			return pos, io.EOF
		}
	}

	return pos, err
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
