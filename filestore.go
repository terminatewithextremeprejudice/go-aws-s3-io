package s3io

import (
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type Bucket struct {
	Region string
	Prefix string
	Bucket string
}

type chunk struct {
	ptr *[]byte
	num int
	off int64
	end int64
	err error
}

type MetaData map[string]string

const (
	MAX_OBJECT_SIZE     uint64 = 5 * 1024 * 1024 * 1024 * 1024
	MIN_MULTIPART_SIZE  uint64 = 1024 * 1024 * 5
	MAX_MULTIPART_SIZE  uint64 = 1024 * 1024 * 10
	MAX_MULTIPART_PARTS uint64 = 10000
	CHUNK_SIZE          uint64 = 1 * 1024 * 1024
	MAX_CONCURRENCY	    int    = 10
	MAX_RETRIES         int    = 9
)

// This regular expression matches every character which is not defined in the
// ASCII tables which range from 00 to 7F, inclusive.
var nonASCIIRegexp = regexp.MustCompile(`([^\x00-\x7F])`)

// Implements DataStore interface
type FileStore struct {
	Config Bucket

	// Service specifies an interface used to communicate with the S3 backend.
	// Usually, this is an instance of github.com/aws/aws-sdk-go/service/s3.S3
	// (http://docs.aws.amazon.com/sdk-for-go/api/service/s3/S3.html).
	Service s3iface.S3API

	// MaxPartSize specifies the maximum size of a single part uploaded to S3
	// in bytes. This value must be bigger than MinPartSize! In order to
	// choose the correct number, two things have to be kept in mind:
	//
	// If this value is too big and uploading the part to S3 is interrupted
	// expectedly, the entire part is discarded and the end user is required
	// to resume the upload and re-upload the entire big part. In addition, the
	// entire part must be written to disk before submitting to S3.
	//
	// If this value is too low, a lot of requests to S3 may be made, depending
	// on how fast data is coming in. This may result in an eventual overhead.
	MaxPartSize uint64
	// MinPartSize specifies the minimum size of a single part uploaded to S3
	// in bytes. This number needs to match with the underlying S3 backend or else
	// uploaded parts will be reject. AWS S3, for example, uses 5MB for this value.
	MinPartSize uint64
	// MaxMultipartParts is the maximum number of parts an S3 multipart upload is
	// allowed to have according to AWS S3 API specifications.
	// See: http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
	MaxMultipartParts uint64
	// MaxObjectSize is the maximum size an S3 Object can have according to S3
	// API specifications. See link above.
	MaxObjectSize uint64
	// MaxDownloadChunkSize specifies the size range of each individual
	MaxChunkSize uint64
	// MaxConcurrency specifies the maximum number of goroutines per each multipart upload process.
	MaxConcurrency int
	// MaxMultipartWorkers specifies the maximum number of goroutines per each multipart upload process.
	MaxMultipartWorkers int
	// Memory pool allocate reader chunks from
	allocpool *Allocpool
}

func NewFileStore(bucket Bucket) *FileStore {
	return &FileStore{
		Config:            bucket,
		MinPartSize:       MIN_MULTIPART_SIZE,
		MaxPartSize:       MAX_MULTIPART_SIZE,
		MaxMultipartParts: MAX_MULTIPART_PARTS,
		MaxObjectSize:     MAX_OBJECT_SIZE,
		MaxChunkSize:      CHUNK_SIZE,
		MaxConcurrency:    10,
		allocpool:         &Allocpool{make([]pagelist, lg2(maxalloc-1)+1)},
	}
}

// Initialize S3 session
func (store *FileStore) Init() error {
	// Create AWS Session
	cfg := aws.NewConfig().WithRegion(store.Config.Region)
	// cfg.HTTPClient = httpClient
	sess := session.Must(session.NewSession(cfg))
	store.Service = s3.New(sess, cfg)
	return nil
}

