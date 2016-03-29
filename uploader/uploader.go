package uploader

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/twitchscience/aws_utils/common"
)

type FileTypeHeader string

const (
	Gzip FileTypeHeader = "application/x-gzip"
	Text FileTypeHeader = "text/plain"
)

type Uploader interface {
	Upload(*UploadRequest) (*UploadReceipt, error)
}

type S3UploaderBuilder struct {
	Bucket           string
	KeyNameGenerator S3KeyNameGenerator
	S3Manager        *s3manager.Uploader
}

type S3Uploader struct {
	Bucket           string
	KeyNameGenerator S3KeyNameGenerator
	S3Manager        *s3manager.Uploader
}

func (builder *S3UploaderBuilder) BuildUploader() Uploader {
	return &S3Uploader{
		Bucket:           builder.Bucket,
		KeyNameGenerator: builder.KeyNameGenerator,
		S3Manager:        builder.S3Manager,
	}
}

var retrier = &common.Retrier{
	Times:         3,
	BackoffFactor: 2,
}

func (worker *S3Uploader) Upload(req *UploadRequest) (*UploadReceipt, error) {
	file, err := os.Open(req.Filename)
	if err != nil {
		return nil, err
	}
	// This means that if we fail to talk to s3 we still remove the file.
	// I think that this is the correct behavior as we dont want to cause
	// a HD overflow in case of a http timeout.
	defer os.Remove(req.Filename)
	keyName := worker.KeyNameGenerator.GetKeyName(req.Filename)

	err = retrier.Retry(func() error {
		// We need to seek to ensure that the retries read from the start of the file
		file.Seek(0, 0)

		_, e := worker.S3Manager.Upload(&s3manager.UploadInput{
			Bucket:      aws.String(worker.Bucket),
			Key:         aws.String(keyName),
			ACL:         aws.String("bucket-owner-full-control"),
			ContentType: aws.String(string(req.FileType)),
			Body:        file,
		})

		return e
	})
	if err != nil {
		return nil, err
	}
	return &UploadReceipt{
		Path:    req.Filename,
		KeyName: worker.Bucket + "/" + keyName,
	}, nil
}
