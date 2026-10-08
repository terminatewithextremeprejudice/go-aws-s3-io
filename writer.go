package s3io

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Writer struct {
	io.WriteCloser
	key         string
	multipartid string
	length 	    *uint64
	n 	    int
	total 	    uint64 // total bytes written
	numParts    int
	partSize    uint64
	partNum     int
	metadata    MetaData
	store       *FileStore
	maxWorkers  int
	isClosed    bool
	isMultipart bool
	buf 	    []byte // buffer used on non multipart uploads
	err 	    error
	semaphore   chan int
	mpartIn     chan *worker
}

func (u *Writer) producer() {
	var (
		err error
		workers chan *worker = make(chan *worker, u.maxWorkers)
		hasError chan error = make(chan error, 0)
	)
	
	go func() {
		for chunk := range workers {
			if err != nil {
				u.semaphore <- 1
				break
			}

			go func(chunk *worker) {
				defer u.store.allocpool.Free(*(chunk.ptr))
				buf := (*(chunk.ptr))[chunk.off:chunk.end]
				
				if _, werr := u.writeChunk(u.key, u.multipartid, chunk.num, &buf); werr != nil {
					err = werr
					hasError <- err
				}
				
				u.semaphore <- 1
			}(chunk)
		}

		hasError <- nil
	}()

	for part := range u.mpartIn {
		workers <- part	
	}

	close(workers)

	if err = <-hasError; err != nil {
		u.err = err
	}

	u.close()
}

func (u *Writer) writeChunk(key string, multipartId string, partNum int, b *[]byte) (*s3.UploadPartOutput, error) {
	r := bytes.NewReader(*b)

	return u.store.Service.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(u.store.Config.Bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(multipartId),
			PartNumber: aws.Int64(int64(partNum)),
			Body:       r,
		})
}

func (u *Writer) writeFile(key string, size int64, b []byte) (*s3.PutObjectOutput, error) {
	r := bytes.NewReader(b)

	return u.store.Service.PutObject(&s3.PutObjectInput{
			Bucket:        aws.String(u.store.Config.Bucket),
			Key:           aws.String(key),
			Body:          r,
			ContentLength: aws.Int64(size),
		})
}

// NewUpload is called when upload to S3 is started.
// It will create uuid of a file and .info file containing name and size of the
func (u *Writer) createMultipart(key *string, meta MetaData) (id string, err error) {
	// Convert meta data into a map of pointers for AWS Go SDK, sigh.
	metadata := make(map[string]*string, len(meta))
	for k, value := range meta {
		// Copying the value is required in order to prevent it from being
		// overwritten by the next iteration.
		v := nonASCIIRegexp.ReplaceAllString(value, "?")
		metadata[k] = &v
	}

	// Create the actual multipart upload
	res, err := u.store.Service.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:   aws.String(u.store.Config.Bucket),
		Key:      key,
		Metadata: metadata,
	})

	if err != nil {
		return "", fmt.Errorf("s3store: unable to create multipart upload:\n%s", err)
	}

	metadata = nil

	return *res.UploadId, nil
}

func (u *Writer) finishMultipart(key string, multipartId string) error {
	// Get uploaded parts
	parts, err := u.listAllParts()
	if err != nil {
		return err
	}

	// Transform the []*s3.Part slice to a []*s3.CompletedPart slice for the next
	// request.
	completedParts := make([]*s3.CompletedPart, len(parts))

	for index, part := range parts {
		completedParts[index] = &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		}
	}

	_, err = u.store.Service.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.store.Config.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(multipartId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	// If error happens completing request, return error and stop processng so that
	// the .info file won't get deleted and upload might be completed on next patch.
	if err != nil {
		return err
	}

	return nil
}

func (u *Writer) listAllParts() (parts []*s3.Part, err error) {
	partMarker := int64(0)
	for {
		// Get uploaded parts
		listPtr, err := u.store.Service.ListParts(&s3.ListPartsInput{
			Bucket:           aws.String(u.store.Config.Bucket),
			Key:              aws.String(u.key),
			UploadId:         aws.String(u.multipartid),
			PartNumberMarker: aws.Int64(partMarker),
		})
		if err != nil {
			return nil, err
		}

		parts = append(parts, (*listPtr).Parts...)

		if listPtr.IsTruncated != nil && *listPtr.IsTruncated {
			partMarker = *listPtr.NextPartNumberMarker
		} else {
			break
		}
	}
	return parts, nil
}

func (u *Writer) delete() error {
	// Delete the content
	_, err := u.store.Service.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(u.store.Config.Prefix),
		Delete: &s3.Delete{
			Objects: []*s3.ObjectIdentifier{
				{
					Key: &u.key,
				},
			},
			Quiet: aws.Bool(false),
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) close() error {
	if writer.isMultipart {
		close(writer.semaphore)
		close(writer.mpartIn)
	}

	if writer.isClosed {
		return nil
	}
	return writer.Close()
}

// Available returns how many bytes are unused in the buffer.
func (writer *Writer) Available() int { return len(writer.buf) - writer.n }

// Buffered returns the number of bytes that have been written into the current buffer.
func (writer *Writer) Buffered() int { return writer.n }

// Written returns the number of total bytes that have been written.
func (writer *Writer) Total() uint64 { return writer.total }

// Write writes the contents of b into the buffer.
func (writer *Writer) Write(b []byte) (nn int, err error) {
	if writer.isClosed {
		return 0, errors.New("cannot write to  closed writer")
	}

	for len(b) > writer.Available() && writer.err == nil {
		var n int
		n = copy(writer.buf[writer.n:], b)
		writer.n += n
		writer.Flush()
		nn += n
		b = b[n:]
	}
	if writer.err != nil {
		return nn, writer.err
	}
	n := copy(writer.buf[writer.n:], b)
	writer.n += n
	nn += n
	return nn, nil
}

// flush writes any buffered data to the s3
func (writer *Writer) Flush() error {
	var n = 0
	if writer.isClosed {
		return errors.New("writer has been closed")
	}
	if writer.err != nil {
		return writer.err
	}
	if writer.n == 0 {
		return nil
	}
	if writer.buf == nil {
		return errors.New("buffer not initialized")
	}
	if writer.isMultipart {
		// first flush with data in it
		if writer.total  == 0 {
			// start background process to handle individual chunks in parallel
			go writer.producer()
		}

		// wait for available worker
		writer.semaphore <- 1
		buf := writer.store.allocpool.Alloc(int(MIN_MULTIPART_SIZE))
		n = copy(buf, writer.buf[:writer.n])
		writer.mpartIn <- &worker{ptr: &buf, off: 0, end: int64(n)}
	}
	if !writer.isMultipart && uint64(writer.n) >= *writer.length {
		writer.Close()
	}

	writer.total += uint64(n)
	writer.n = 0
	return nil
}

// Reset discards any unflushed buffered data, clears any error, and
func (writer *Writer) Reset(w io.Writer) {
	if writer.buf == nil {
		writer.buf = make([]byte, writer.partSize)
	}

	writer.n = 0
	writer.total = 0
	writer.partSize = 0
	writer.partNum = 0
	writer.numParts = 0
	writer.multipartid = ""
	writer.isClosed = false
	writer.isMultipart = false
	writer.err = nil
}

func (writer *Writer) Close() error {
	// flush any underlying data if there's some left to write
	if writer.Buffered() > 0 {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	if writer.isMultipart {
		// Mark the multipart upload complete
		if err := writer.finishMultipart(writer.key, writer.multipartid); err != nil {
			// cleanup from s3
			writer.delete()
			writer.n = 0
			writer.err = err
			return err
		}
		
	} else {
		if _, err := writer.writeFile(writer.key, int64(writer.n), writer.buf[0:writer.n]); err != nil {
			writer.n = 0
			writer.err = err
			return err
		}
	}

	writer.isClosed = true

	return nil
} 

func NewWriter(store *FileStore, key string, length *uint64, meta MetaData, concurrency *int) (*Writer, error) {

	var (
		partSize    uint64 = MIN_MULTIPART_SIZE
		numParts    int = 0
		err 	    error
	)
	

	if store == nil {
		return nil, errors.New("store needs to be set")
	}

	if concurrency == nil {
		concurrency = new(int)
		*concurrency = store.MaxConcurrency
	}

	writer := &Writer{key: key,
		length: length, 
		maxWorkers: *concurrency,
		metadata: meta,
		store: store}

	if partSize, err = calcOptimalPartSize(*length); err == nil {
		numParts = int(*length / partSize)
		if numParts <= 0 {
			numParts = 1
		}

		if numParts < *concurrency {
			*concurrency = int(numParts)
		}
	}

	writer.maxWorkers = *concurrency

	// If the content length is not known before hand or the size exceeds MIN_MULTIPART_SIZE,
	// create multipart upload instead
	if numParts > 1 {
		id, err := writer.createMultipart(&key, meta)
		if err != nil {
			return nil, err
		}

		writer.multipartid = id
		writer.isMultipart = true
		writer.mpartIn = make(chan *worker)
		writer.semaphore = make(chan int, writer.maxWorkers)
	}

	writer.buf = make([]byte, MIN_MULTIPART_SIZE)
	writer.numParts = numParts
	writer.partSize = partSize
	writer.partNum  = 0
	writer.total = 0
	writer.n = 0
	return writer, nil
}
