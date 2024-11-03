package pg_mini

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
)

func copyToCSVQuery(tbl string) string {
	return fmt.Sprintf("COPY %s TO STDOUT WITH CSV HEADER DELIMITER ',';", tmpTblName(tbl))
}

type copyOutRes struct {
	FileName string
	Rows     int64
	Duration time.Duration
	FileSize int64
}

func copyToCSV(ctx context.Context, conn *pgx.Conn, tbl, query, dir string) (*copyOutRes, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create directory %s: %v", dir, err)
	}

	fileName := filepath.Join(dir, tbl+".csv")
	file, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	defer file.Close()

	bufWriter := bufio.NewWriterSize(file, 1024*1024)
	defer bufWriter.Flush()

	queryStart := time.Now()
	copyCount, err := conn.PgConn().CopyTo(ctx, bufWriter, query)
	if err != nil {
		return nil, fmt.Errorf("copying data: %w", err)
	}
	duration := time.Since(queryStart)

	err = bufWriter.Flush()
	if err != nil {
		return nil, fmt.Errorf("flushing data: %w", err)
	}

	err = file.Close()
	if err != nil {
		return nil, fmt.Errorf("closing file: %w", err)
	}

	fileStats, err := os.Stat(fileName)
	if err != nil {
		return nil, fmt.Errorf("statting file: %w", err)
	}

	return &copyOutRes{
		FileName: fileName,
		Rows:     copyCount.RowsAffected(),
		Duration: duration,
		FileSize: fileStats.Size(),
	}, nil
}
