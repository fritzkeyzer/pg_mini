package pg_mini

import (
	"context"
	"testing"

	"github.com/fritzkeyzer/pg_mini/s3_store"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

// startMinio boots a MinIO container, creates the bucket, and returns a Config
// pointing at it.
func startMinio(t *testing.T, bucket string) s3_store.Config {
	t.Helper()
	ctx := context.Background()

	container, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminate minio container: %v", err)
		}
	})

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get minio connection string: %v", err)
	}

	cfg := s3_store.Config{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
		UseSSL:          false,
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		t.Fatalf("create minio client: %v", err)
	}
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket %s: %v", bucket, err)
	}

	return cfg
}

func TestE2E_S3RoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	ctx := context.Background()
	connStr := startPostgres(t)

	setupConn := connect(t, connStr)
	execSQLFile(t, setupConn, "testdata/e2e/company/setup.sql")
	original := snapshotDB(t, setupConn)

	// Export into S3 under a key prefix.
	cfg := startMinio(t, "pg-mini-test")
	cfg.Prefix = "backups/company"

	exportStore, err := s3_store.New(ctx, cfg)
	if err != nil {
		t.Fatalf("new s3 export store: %v", err)
	}
	exp := &Export{
		DB:           connect(t, connStr),
		RootTable:    "company",
		Store:        exportStore,
		NoAnimations: true,
	}
	if err := exp.Run(ctx); err != nil {
		t.Fatalf("export: %v", err)
	}

	// Wipe the database.
	truncateAll(t, connect(t, connStr))
	emptySnap := snapshotDB(t, connect(t, connStr))
	for table, rows := range emptySnap {
		if len(rows) != 0 {
			t.Fatalf("table %s should be empty after truncate, has %d rows", table, len(rows))
		}
	}

	// Import from a fresh Store, proving the objects were durably written.
	importStore, err := s3_store.New(ctx, cfg)
	if err != nil {
		t.Fatalf("new s3 import store: %v", err)
	}
	imp := &Import{
		DB:           connect(t, connStr),
		RootTable:    "company",
		Truncate:     true,
		Store:        importStore,
		NoAnimations: true,
	}
	if err := imp.Run(ctx); err != nil {
		t.Fatalf("import: %v", err)
	}

	restored := snapshotDB(t, connect(t, connStr))
	compareSnapshots(t, original, restored)
}
