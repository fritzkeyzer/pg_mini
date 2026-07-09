// Package s3_store is an S3-backed implementation of the pg_mini Store
// interface. It uses minio-go, which works against AWS S3, MinIO, Cloudflare
// R2, Backblaze B2 and any other S3-compatible endpoint.
package s3_store

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store is a pg_mini Store backed by an S3 bucket. Object keys are formed by
// joining Prefix with the requested name.
type Store struct {
	client *minio.Client
	bucket string
	prefix string
	ctx    context.Context
}

// Config configures a Store.
type Config struct {
	// Endpoint is the S3 host, e.g. "s3.amazonaws.com" or "localhost:9000".
	Endpoint string
	// Bucket is the target bucket. It must already exist.
	Bucket string
	// Prefix is an optional key prefix (a virtual "directory") for all names.
	Prefix string
	// AccessKeyID and SecretAccessKey authenticate requests. If both are
	// empty, the IAM credential chain is used instead.
	AccessKeyID     string
	SecretAccessKey string
	// Region is optional; required by some endpoints (e.g. AWS).
	Region string
	// UseSSL toggles HTTPS. Set true for AWS/R2 etc.
	UseSSL bool
}

// New builds a Store from cfg. ctx bounds every subsequent read and write.
func New(ctx context.Context, cfg Config) (*Store, error) {
	var creds *credentials.Credentials
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" {
		creds = credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, "")
	} else {
		creds = credentials.NewIAM("")
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	return &Store{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		ctx:    ctx,
	}, nil
}

func (s *Store) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return s.prefix + "/" + name
}

// Open returns a reader for the named object.
func (s *Store) Open(name string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(s.ctx, s.bucket, s.key(name), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	// GetObject is lazy; probe so a missing key fails here, not on first Read.
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	return obj, nil
}

// Create returns a writer that streams to the named object and finalises the
// upload on Close. The write is only durable once Close returns without error.
func (s *Store) Create(name string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	w := &objectWriter{pw: pw, done: make(chan error, 1)}

	go func() {
		// size -1 streams the body via multipart upload without a known length.
		_, err := s.client.PutObject(s.ctx, s.bucket, s.key(name), pr, -1, minio.PutObjectOptions{})
		pr.CloseWithError(err)
		w.done <- err
	}()

	return w, nil
}

// objectWriter feeds an io.Pipe whose read end is consumed by PutObject.
type objectWriter struct {
	pw   *io.PipeWriter
	done chan error
}

func (w *objectWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *objectWriter) Close() error {
	if err := w.pw.Close(); err != nil {
		return err
	}
	return <-w.done
}
