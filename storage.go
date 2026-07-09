package pg_mini

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Storage is a minimal, FS-shaped abstraction over where pg_mini reads and
// writes its export artifacts: schema.json, graph.json, the *_queries.json
// files, and one <table>.csv per table. Names are simple relative keys such as
// "schema.json" or "users.csv".
//
// Implement it to back exports/imports with something other than the local
// filesystem (S3, GCS, in-memory, etc.). Export.Storage / Import.Storage are
// required; use DirStorage(dir) for the original file-based behaviour.
type Storage interface {
	// Create opens name for writing, truncating any existing entry.
	Create(name string) (io.WriteCloser, error)
	// Open opens name for reading.
	Open(name string) (io.ReadCloser, error)
}

// DirStorage returns a Storage backed by a local directory. Nested names are
// created as needed.
func DirStorage(dir string) Storage {
	return dirStorage{dir: dir}
}

type dirStorage struct {
	dir string
}

func (d dirStorage) Create(name string) (io.WriteCloser, error) {
	p := filepath.Join(d.dir, name)
	if err := os.MkdirAll(filepath.Dir(p), os.ModePerm); err != nil {
		return nil, fmt.Errorf("create directory %s: %w", filepath.Dir(p), err)
	}
	return os.Create(p)
}

func (d dirStorage) Open(name string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(d.dir, name))
}

// saveJSON encodes v as indented JSON into the named storage entry. It closes
// the writer exactly once and surfaces any close error, which matters for
// backends that finalize the write on Close (e.g. an S3 upload).
func saveJSON(s Storage, name string, v any) (err error) {
	f, err := s.Create(name)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close %s: %w", name, cerr)
		}
	}()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err = enc.Encode(v); err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	return nil
}

// loadJSON decodes the named storage entry into ptr.
func loadJSON(s Storage, name string, ptr any) error {
	f, err := s.Open(name)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(ptr); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}

// countingWriter tracks bytes written so callers can report sizes without
// re-statting the underlying storage.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// countingReader tracks bytes read.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
