package uploader

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/twinj/uuid"
)

const (
	testFiles = 10

	// We want to try different sizes of files, so we increment the file size
	// by this contant for each file we test. That means the final file will because
	// testFiles * fileSizeIncrement + 1 in size. We write a single byte at the end
	// of every file, that is why the +1 is there
	fileSizeIncrement = 123456
)

var (
	testBucket = flag.String("uploadTestBucket", "", "Bucket to use for uploader test. (Test skipped if not set)")
)

type simpleNameGenerator struct {
	prefix string
}

func (s *simpleNameGenerator) GetKeyName(in string) string { return s.prefix + "/" + in }

// Generate some files of increasing size
func createTempFiles(tempfolder string) error {
	for i := 0; i < testFiles; i++ {
		f, err := os.Create(fmt.Sprintf("%s/%d.log", tempfolder, i))
		if err != nil {
			return err
		}
		//defer f.Close()

		_, err = f.Seek(int64(i*fileSizeIncrement), os.SEEK_SET)
		f.WriteString(" ")
		if err != nil {
			return err
		}
		f.Close()
	}

	return nil
}

func TestUploader(t *testing.T) {
	if len(*testBucket) == 0 {
		t.Skip("Skipping test because no testBucket was specified")
	}

	awsSession := session.New()

	tempfolder := os.TempDir() + uuid.NewV4().String()
	err := os.Mkdir(tempfolder, 0777)
	if err != nil {
		t.Errorf("Error creating temp folder %s: %v", tempfolder, err)
	}
	defer func() {
		err = os.RemoveAll(tempfolder)
		if err != nil {
			t.Errorf("Error cleaning out temp folder %s: %v", tempfolder, err)
		}
	}()

	err = createTempFiles(tempfolder)
	if err != nil {
		t.Errorf("Error creating temporary files: %v", err)
	}

	kng := &simpleNameGenerator{prefix: "aws_utils_test"}
	factory := NewFactory(*testBucket, kng, s3manager.NewUploader(awsSession))

	s3client := s3.New(awsSession)

	var wg sync.WaitGroup
	for i := 0; i < testFiles; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			fn := fmt.Sprintf("%s/%d.log", tempfolder, index)

			k := kng.GetKeyName(fn)
			defer func() {
				_, e := s3client.DeleteObject(&s3.DeleteObjectInput{
					Bucket: testBucket,
					Key:    aws.String(k),
				})
				if e != nil {
					t.Errorf("Failed to delete %s from S3: %v", k, e)
				}
			}()

			u := factory.NewUploader()
			_, err = u.Upload(&UploadRequest{
				Filename: fn,
				FileType: Text,
				retry:    2,
			})
			if err != nil {
				t.Errorf("Failed to upload %s: %v", fn, err)
			}

			s := int64(index*fileSizeIncrement) + 1
			o, e := s3client.GetObject(&s3.GetObjectInput{
				Bucket: testBucket,
				Key:    aws.String(k),
			})
			if e != nil {
				t.Errorf("Failed to get information about %s: %v", k, e)
			} else {
				if aws.Int64Value(o.ContentLength) != s {
					t.Errorf("Expected length for %s was %d, got %d", k, aws.Int64Value(o.ContentLength), s)
				}
				s3client.DeleteObjects(&s3.DeleteObjectsInput{})
			}
		}(i)
	}
	wg.Wait()
}
