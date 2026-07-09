package pg_mini

import (
	_ "embed"
	"encoding/json"
)

//go:embed version.json
var versionJSON []byte

// Version is the current pg_mini version, read from the embedded version.json.
// This is the single source of truth for the version number.
var Version = func() string {
	var v struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(versionJSON, &v); err != nil {
		panic("pg_mini: invalid version.json: " + err.Error())
	}
	return v.Version
}()
