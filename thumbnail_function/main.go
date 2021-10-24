package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
)



var (
	sess *session.Session
)

func init() {
	sess = session.New()
}

func HandleRequest(ctx context.Context, event events.S3Event) (int, error) {
	log.Println(event.Records[0].S3.Bucket.Name)
	log.Println(event.Records[0].S3.Object.Key)

	//s3Bucket := "download-demo"
	//s3Key := "test-s3-mp4/test-path/test.mp4"

	s3Bucket := event.Records[0].S3.Bucket.Name
	s3Key, err := url.QueryUnescape(event.Records[0].S3.Object.Key)
	if err != nil {
		log.Printf("url.QueryUnescape err is : %s \n", err.Error())
		return 1, err
	}

	fileOut := "/tmp/" + s3Key

	err = downloadS3ToFile(sess, s3Bucket, s3Key, fileOut)
	if err != nil {
		log.Printf("downloadS3ToFile err is : %s \n", err.Error())
		return 1, err
	}

	picOutTo := fileOut + "-out"

	err = convertMp4ToJpg(fileOut, picOutTo)
	if err != nil {
		log.Printf("convertMp4ToJpg err is : %s \n", err.Error())
		return 1, err
	}

	s3OutPath := s3Key + "-out/"

	err = uploadPicsToS3(sess, picOutTo, s3Bucket, s3OutPath)
	if err != nil {
		log.Printf("uploadPicsToS3 err is : %s \n", err.Error())
		return 1, err
	}

	return 0, nil
}

func main() {
	lambda.Start(HandleRequest)
}

func downloadS3ToFile(sess *session.Session, s3Bucket, s3Key, filePath string) error {

	fileDir := filepath.Dir(filePath)
	err := os.MkdirAll(fileDir, os.ModePerm)
	if err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(sess)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(s3Bucket),
			Key:    aws.String(s3Key),
		})

	if err != nil {
		return err
	}

	log.Println("Downloaded", file.Name(), numBytes, "bytes")

	return nil
}

func convertMp4ToJpg(fileFrom, fileTo string) error {
	err := os.MkdirAll(fileTo, os.ModePerm)
	if err != nil {
		return err
	}

	cmdArguments := []string{"-i", fileFrom, "-f", "image2", "-r", "1/2", fileTo + "/output_%04d.jpg"}
	cmd := exec.Command("ffmpeg", cmdArguments...)

	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		return err
	}

	log.Printf("command output: \n%v", out.String())

	return nil
}

func uploadPicsToS3(sess *session.Session, fileFrom, s3Bucket, s3To string) error {
	dir := NewDirectoryIterator(s3Bucket, fileFrom, s3To)

	uploader := s3manager.NewUploader(sess)

	if err := uploader.UploadWithIterator(aws.BackgroundContext(), dir); err != nil {
		exitErrorf("failed to upload %q, %v", err)
	}
	log.Printf("successfully uploaded %q to %q", fileFrom, s3Bucket + "/" + s3To)

	return nil
}

// DirectoryIterator represents an iterator of a specified directory
type DirectoryIterator struct {
	filePaths []string
	bucket    string
	next      struct {
		path string
		f    *os.File
	}
	s3Path string
	err error
}

// NewDirectoryIterator builds a new DirectoryIterator
func NewDirectoryIterator(bucket, dir, s3Path string) s3manager.BatchUploadIterator {
	var paths []string
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})

	return &DirectoryIterator{
		filePaths: paths,
		bucket:    bucket,
		s3Path: s3Path,
	}
}

// Next returns whether next file exists or not
func (di *DirectoryIterator) Next() bool {
	if len(di.filePaths) == 0 {
		di.next.f = nil
		return false
	}

	f, err := os.Open(di.filePaths[0])
	di.err = err
	di.next.f = f

	_, file := filepath.Split(di.filePaths[0])

	di.next.path = di.s3Path + file
	di.filePaths = di.filePaths[1:]

	return true && di.Err() == nil
}

// Err returns error of DirectoryIterator
func (di *DirectoryIterator) Err() error {
	return di.err
}

// UploadObject uploads a file
func (di *DirectoryIterator) UploadObject() s3manager.BatchUploadObject {
	f := di.next.f
	return s3manager.BatchUploadObject{
		Object: &s3manager.UploadInput{
			Bucket: &di.bucket,
			Key:    &di.next.path,
			Body:   f,
		},
		After: func() error {
			return f.Close()
		},
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(exitError)
}

const exitError = 1