package s3io

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func splitIds(id string) (uploadId, multipartId, projectId string) {
	index := strings.Index(id, "+")
	if index == -1 {
		return
	}

	s := strings.Split(id, "+")
	projectId = s[0]
	uploadId = s[1]
	multipartId = strings.Join(s[2:], "+")
	return uploadId, multipartId, projectId
}

// isAwsError tests whether an error object is an instance of the AWS error
// specified by its code.
func isAwsError(err error, code string) bool {
	if err, ok := err.(awserr.Error); ok && err.Code() == code {
		return true
	}
	return false
}

func calcOptimalPartSize(size uint64) (optimalPartSize uint64, err error) {
	switch {
	// When upload is smaller or equal MinPartSize, we upload in just one part.
	case size <= MIN_MULTIPART_SIZE:
		optimalPartSize = MIN_MULTIPART_SIZE
	// Does the upload fit in MaxMultipartParts parts or less with MinPartSize.
	case size <= MIN_MULTIPART_SIZE*MAX_MULTIPART_PARTS:
		optimalPartSize = MIN_MULTIPART_SIZE
	// Prerequisite: Be aware, that the result of an integer division (x/y) is
	// ALWAYS rounded DOWN, as there are no digits behind the comma.
	// In order to find out, whether we have an exact result or a rounded down
	// one, we can check, whether the remainder of that division is 0 (x%y == 0).
	//
	// So if the result of (size/MaxMultipartParts) is not a rounded down value,
	// then we can use it as our optimalPartSize. But if this division produces a
	// remainder, we have to round up the result by adding +1. Otherwise our
	// upload would not fit into MaxMultipartParts number of parts with that
	// size. We would need an additional part in order to upload everything.
	// While in almost all cases, we could skip the check for the remainder and
	// just add +1 to every result, but there is one case, where doing that would
	// doom our upload. When (MaxObjectSize == MaxPartSize * MaxMultipartParts),
	// by adding +1, we would end up with an optimalPartSize > MaxPartSize.
	// With the current S3 API specifications, we will not run into this problem,
	// but these specs are subject to change, and there are other stores as well,
	// which are implementing the S3 API (e.g. RIAK, Ceph RadosGW), but might
	// have different settings.
	case size%MAX_MULTIPART_PARTS == 0:
		optimalPartSize = size / MAX_MULTIPART_PARTS
	// Having a remainder larger than 0 means, the float result would have
	// digits after the comma (e.g. be something like 10.9). As a result, we can
	// only squeeze our upload into MaxMultipartParts parts, if we rounded UP
	// this division's result. That is what is happending here. We round up by
	// adding +1, if the prior test for (remainder == 0) did not succeed.
	default:
		optimalPartSize = size/MAX_MULTIPART_PARTS + 1
	}

	// optimalPartSize must never exceed MaxPartSize
	if optimalPartSize > MAX_MULTIPART_SIZE {
		return optimalPartSize, fmt.Errorf("calcOptimalPartSize: to upload %v bytes optimalPartSize %v must exceed MaxPartSize %v", size, optimalPartSize, MAX_MULTIPART_SIZE)
	}
	return optimalPartSize, nil
}

func NewMultiError(errs []error) error {
	message := "Multiple errors occurred:\n"
	for _, err := range errs {
		message += "\t" + err.Error() + "\n"
	}
	return errors.New(message)
}

