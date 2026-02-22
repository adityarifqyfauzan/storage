package storage

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"os"
	pathutil "path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type AmazonS3Backend struct {
	Bucket     string
	Client     *s3.S3
	Downloader *s3manager.Downloader
	Prefix     string
	Uploader   *s3manager.Uploader
	SSE        string
}

type AmazonS3Options struct {
	S3ForcePathStyle *bool
}

func newS3Service(region, endpoint string, creds *credentials.Credentials, forcePathStyle bool) *s3.S3 {
	httpClient := http.DefaultClient

	if os.Getenv("AWS_INSECURE_SKIP_VERIFY") == "true" {
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
	}

	sess := session.Must(session.NewSession())

	cfg := &aws.Config{
		HTTPClient:       httpClient,
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		DisableSSL:       aws.Bool(strings.HasPrefix(endpoint, "http://")),
		S3ForcePathStyle: aws.Bool(forcePathStyle),
	}

	if creds != nil {
		cfg.Credentials = creds
	}

	return s3.New(sess, cfg)
}

func NewAmazonS3Backend(bucket, prefix, region, endpoint, sse string) *AmazonS3Backend {
	forcePathStyle := endpoint != ""
	service := newS3Service(region, endpoint, nil, forcePathStyle)

	return &AmazonS3Backend{
		Bucket:     bucket,
		Client:     service,
		Downloader: s3manager.NewDownloaderWithClient(service),
		Prefix:     cleanPrefix(prefix),
		Uploader:   s3manager.NewUploaderWithClient(service),
		SSE:        sse,
	}
}

func NewAmazonS3BackendWithOptions(bucket, prefix, region, endpoint, sse string, options *AmazonS3Options) *AmazonS3Backend {
	forcePathStyle := endpoint != ""
	if options != nil && options.S3ForcePathStyle != nil {
		forcePathStyle = *options.S3ForcePathStyle
	}

	service := newS3Service(region, endpoint, nil, forcePathStyle)

	return &AmazonS3Backend{
		Bucket:     bucket,
		Client:     service,
		Downloader: s3manager.NewDownloaderWithClient(service),
		Prefix:     cleanPrefix(prefix),
		Uploader:   s3manager.NewUploaderWithClient(service),
		SSE:        sse,
	}
}

func NewAmazonS3BackendWithCredentials(bucket, prefix, region, endpoint, sse string, creds *credentials.Credentials) *AmazonS3Backend {
	forcePathStyle := endpoint != ""
	service := newS3Service(region, endpoint, creds, forcePathStyle)

	return &AmazonS3Backend{
		Bucket:     bucket,
		Client:     service,
		Downloader: s3manager.NewDownloaderWithClient(service),
		Prefix:     cleanPrefix(prefix),
		Uploader:   s3manager.NewUploaderWithClient(service),
		SSE:        sse,
	}
}

func (b AmazonS3Backend) ListObjects(prefix string) ([]Object, error) {
	var objects []Object
	fullPrefix := pathutil.Join(b.Prefix, prefix)

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.Bucket),
		Prefix: aws.String(fullPrefix),
	}

	for {
		result, err := b.Client.ListObjectsV2(input)
		if err != nil {
			return objects, err
		}

		for _, obj := range result.Contents {
			path := removePrefixFromObjectPath(fullPrefix, *obj.Key)
			if objectPathIsInvalid(path) {
				continue
			}

			objects = append(objects, Object{
				Path:         path,
				Content:      []byte{},
				LastModified: *obj.LastModified,
			})
		}

		if !aws.BoolValue(result.IsTruncated) {
			break
		}

		input.ContinuationToken = result.NextContinuationToken
	}

	return objects, nil
}

func (b AmazonS3Backend) GetObject(path string) (Object, error) {
	var object Object
	object.Path = path

	input := &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
	}

	result, err := b.Client.GetObject(input)
	if err != nil {
		return object, err
	}
	defer result.Body.Close()

	content, err := io.ReadAll(result.Body)
	if err != nil {
		return object, err
	}

	object.Content = content
	object.LastModified = *result.LastModified

	return object, nil
}

func (b AmazonS3Backend) PutObject(path string, content []byte) error {
	input := &s3manager.UploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
		Body:   bytes.NewReader(content),
	}

	if b.SSE != "" {
		input.ServerSideEncryption = aws.String(b.SSE)
	}

	_, err := b.Uploader.Upload(input)
	return err
}

func (b AmazonS3Backend) PutObjectStream(ctx context.Context, path string, content io.Reader) error {
	input := &s3manager.UploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
		Body:   content,
	}

	if b.SSE != "" {
		input.ServerSideEncryption = aws.String(b.SSE)
	}

	_, err := b.Uploader.UploadWithContext(ctx, input)
	return err
}

func (b AmazonS3Backend) DeleteObject(path string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
	}

	_, err := b.Client.DeleteObject(input)
	return err
}
