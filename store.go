package pg_mini

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Store abstracts where pg_mini reads and writes its export artifacts
// (schema.json, graph.json, *_queries.json, one <table>.csv per table). Names
// are relative keys such as "schema.json" or "users.csv". Implement it to back
// exports/imports with S3, GCS, in-memory, etc.; use DirStore for the local
// filesystem.
type Store interface {
	// Create opens name for writing, truncating any existing entry.
	Create(name string) (io.WriteCloser, error)
	// Open opens name for reading.
	Open(name string) (io.ReadCloser, error)
}

// DirStore returns a Store backed by a local directory.
func DirStore(dir string) Store {
	return dirStore{dir: dir}
}

type dirStore struct {
	dir string
}

func (d dirStore) Create(name string) (io.WriteCloser, error) {
	p := filepath.Join(d.dir, name)
	if err := os.MkdirAll(filepath.Dir(p), os.ModePerm); err != nil {
		return nil, fmt.Errorf("create directory %s: %w", filepath.Dir(p), err)
	}
	return os.Create(p)
}

func (d dirStore) Open(name string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(d.dir, name))
}

// saveJSON encodes v as indented JSON into name, surfacing any Close error
// (backends like S3 finalize the write on Close).
func saveJSON(s Store, name string, v any) (err error) {
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

// loadJSON decodes name into ptr.
func loadJSON(s Store, name string, ptr any) error {
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

// countingWriter tracks bytes written.
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
