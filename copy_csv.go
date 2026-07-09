package pg_mini

import (
	"bufio"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type copyOutRes struct {
	FileName string
	Rows     int64
	Duration time.Duration
	FileSize int64
}

func copyToCSV(ctx context.Context, conn *pgx.Conn, store Storage, tbl, query string) (*copyOutRes, error) {
	name := tbl + ".csv"
	w, err := store.Create(name)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			w.Close()
		}
	}()

	cw := &countingWriter{w: w}
	bufWriter := bufio.NewWriterSize(cw, 1024*1024)

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

	closed = true
	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("closing file: %w", err)
	}

	return &copyOutRes{
		FileName: name,
		Rows:     copyCount.RowsAffected(),
		Duration: duration,
		FileSize: cw.n,
	}, nil
}

type copyInRes struct {
	FileName string
	Rows     int64
	Duration time.Duration
	FileSize int64
}

func copyFromCSV(ctx context.Context, conn *pgx.Conn, store Storage, tbl, query string) (*copyInRes, error) {
	name := tbl + ".csv"
	r, err := store.Open(name)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			r.Close()
		}
	}()

	cr := &countingReader{r: r}

	queryStart := time.Now()
	copyCount, err := conn.PgConn().CopyFrom(ctx, cr, query)
	if err != nil {
		return nil, fmt.Errorf("copying data: %w", err)
	}
	duration := time.Since(queryStart)

	closed = true
	err = r.Close()
	if err != nil {
		return nil, fmt.Errorf("closing file: %w", err)
	}

	return &copyInRes{
		FileName: name,
		Rows:     copyCount.RowsAffected(),
		Duration: duration,
		FileSize: cr.n,
	}, nil
}
