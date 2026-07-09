package pg_mini

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
)

// memStorage is an in-memory Storage, demonstrating that pg_mini can back
// exports/imports with something other than the local filesystem. It is also
// used by TestE2E_CustomStorage to prove the seam end-to-end.
type memStorage struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemStorage() *memStorage {
	return &memStorage{files: map[string][]byte{}}
}

func (m *memStorage) Create(name string) (io.WriteCloser, error) {
	return &memWriter{store: m, name: name}, nil
}

func (m *memStorage) Open(name string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[name]
	if !ok {
		return nil, &memNotExist{name: name}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type memWriter struct {
	store *memStorage
	name  string
	buf   bytes.Buffer
}

func (w *memWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *memWriter) Close() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.files[w.name] = append([]byte(nil), w.buf.Bytes()...)
	return nil
}

type memNotExist struct{ name string }

func (e *memNotExist) Error() string { return "memStorage: not found: " + e.name }

func TestDirStorage_RoundTrip(t *testing.T) {
	s := DirStorage(t.TempDir())

	// Nested name exercises the MkdirAll path.
	w, err := s.Create("nested/dir/data.csv")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := io.WriteString(w, "hello,world\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := s.Open("nested/dir/data.csv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello,world\n" {
		t.Fatalf("got %q, want %q", got, "hello,world\n")
	}
}

func TestStorage_JSONRoundTrip(t *testing.T) {
	type payload struct {
		Name  string
		Count int
	}
	want := payload{Name: "orders", Count: 42}

	s := newMemStorage()
	if err := saveJSON(s, "meta.json", want); err != nil {
		t.Fatalf("saveJSON: %v", err)
	}

	var got payload
	if err := loadJSON(s, "meta.json", &got); err != nil {
		t.Fatalf("loadJSON: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// TestE2E_CustomStorage runs a full export → import round-trip through an
// in-memory Storage backend, with no artifacts ever touching the filesystem.
func TestE2E_CustomStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	connStr := startPostgres(t)
	ctx := context.Background()

	setupConn := connect(t, connStr)
	execSQLFile(t, setupConn, "testdata/e2e/company/setup.sql")
	original := snapshotDB(t, setupConn)

	store := newMemStorage()

	exp := &Export{
		DB:           connect(t, connStr),
		RootTable:    "company",
		Storage:      store,
		NoAnimations: true,
	}
	if err := exp.Run(ctx); err != nil {
		t.Fatalf("export: %v", err)
	}

	// Artifacts live only in memory.
	if _, err := store.Open("schema.json"); err != nil {
		t.Fatalf("expected schema.json in custom storage: %v", err)
	}

	truncateAll(t, connect(t, connStr))

	imp := &Import{
		DB:           connect(t, connStr),
		RootTable:    "company",
		Truncate:     true,
		Storage:      store,
		NoAnimations: true,
	}
	if err := imp.Run(ctx); err != nil {
		t.Fatalf("import: %v", err)
	}

	restored := snapshotDB(t, connect(t, connStr))
	compareSnapshots(t, original, restored)
}
