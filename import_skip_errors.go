package pg_mini

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type rowImportRes struct {
	FileName     string
	Processed    int64
	Inserted     int64
	Skipped      int64
	Failed       int64
	Duration     time.Duration
	FileSize     int64
	Mode         string
	UsedFallback bool
}

func insertRowsFromCSV(
	ctx context.Context,
	conn *pgx.Conn,
	tbl string,
	cols []string,
	nullableCols map[string]bool,
	query,
	dir string,
	maxErrors int,
	softInsert bool,
) (*rowImportRes, error) {
	fileName := filepath.Join(dir, tbl+".csv")
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("opening csv file: %w", err)
	}
	defer file.Close()

	fileStats, err := os.Stat(fileName)
	if err != nil {
		return nil, fmt.Errorf("statting file: %w", err)
	}

	r := csv.NewReader(bufio.NewReaderSize(file, 1024*1024))
	header, err := r.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return &rowImportRes{FileName: fileName, FileSize: fileStats.Size()}, nil
		}
		return nil, fmt.Errorf("reading csv header: %w", err)
	}

	headerPos := make(map[string]int, len(header))
	for idx, col := range header {
		headerPos[col] = idx
	}

	colIndexes := make([]int, len(cols))
	for idx, col := range cols {
		pos, ok := headerPos[col]
		if !ok {
			return nil, fmt.Errorf("csv missing column %q", col)
		}
		colIndexes[idx] = pos
	}

	res := &rowImportRes{
		FileName: fileName,
		FileSize: fileStats.Size(),
	}

	start := time.Now()
	line := 1

	for {
		record, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			res.Failed++
			line++
			if !isDuplicateKeyError(err) {
				slog.Error("Row import failed",
					"table", tbl,
					"line", line,
					"error", err,
				)
			}
			if maxErrors >= 0 && res.Failed > int64(maxErrors) {
				res.Duration = time.Since(start)
				return res, fmt.Errorf("max errors exceeded for table %s: failed=%d max=%d", tbl, res.Failed, maxErrors)
			}
			continue
		}

		line++
		res.Processed++

		args := make([]any, len(cols))
		rowHasError := false
		for idx, colIdx := range colIndexes {
			if colIdx >= len(record) {
				rowHasError = true
				slog.Error("Row import failed",
					"table", tbl,
					"line", line,
					"error", fmt.Sprintf("column index %d out of range for record with %d values", colIdx, len(record)),
				)
				break
			}
			colName := cols[idx]
			if nullableCols[colName] && record[colIdx] == "" {
				args[idx] = nil
			} else {
				args[idx] = record[colIdx]
			}
		}

		if rowHasError {
			res.Failed++
			if maxErrors >= 0 && res.Failed > int64(maxErrors) {
				res.Duration = time.Since(start)
				return res, fmt.Errorf("max errors exceeded for table %s: failed=%d max=%d", tbl, res.Failed, maxErrors)
			}
			continue
		}

		tag, err := conn.Exec(ctx, query, args...)
		if err != nil {
			if softInsert && isDuplicateKeyError(err) {
				res.Skipped++
				continue
			}

			res.Failed++
			slog.Error("Row import failed",
				"table", tbl,
				"line", line,
				"error", err,
			)
			if maxErrors >= 0 && res.Failed > int64(maxErrors) {
				res.Duration = time.Since(start)
				return res, fmt.Errorf("max errors exceeded for table %s: failed=%d max=%d", tbl, res.Failed, maxErrors)
			}
			continue
		}

		if tag.RowsAffected() == 0 {
			res.Skipped++
		} else {
			res.Inserted += tag.RowsAffected()
		}
	}

	res.Duration = time.Since(start)
	return res, nil
}

func isDuplicateKeyError(err error) bool {
	return strings.Contains(err.Error(), "(SQLSTATE 23505)")
	//var pgErr *pgconn.PgError
	//if errors.As(err, &pgErr) {
	//	return pgErr.Code == "23505"
	//}
	//return false
}

func getNullableColumns(ctx context.Context, conn *pgx.Conn, table string) (map[string]bool, error) {
	query := `
		SELECT column_name, is_nullable
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
	`

	rows, err := conn.Query(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("querying nullable columns: %w", err)
	}
	defer rows.Close()

	result := map[string]bool{}
	for rows.Next() {
		var columnName string
		var isNullable string
		if err := rows.Scan(&columnName, &isNullable); err != nil {
			return nil, fmt.Errorf("scanning nullable column row: %w", err)
		}
		result[columnName] = isNullable == "YES"
	}

	return result, nil
}
