package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate-go-client/v6"
)

// efMode says how the efArray values reach the server during runQueries.
type efMode string

const (
	// efModeRequest sends the value as vector_index_params with every query
	// (ef for hnsw/dynamic, searchProbe for hfresh). Weaviate >= 1.42.
	efModeRequest efMode = "request"
	// efModeSchema rewrites the collection's vectorIndexConfig before each
	// batch of queries (the pre-1.42 behaviour, and the only option for flat).
	efModeSchema efMode = "schema"
)

// fetchServerVersion returns the server version reported by GET /v1/meta.
func fetchServerVersion(ctx context.Context, client *weaviate.Client) (string, error) {
	meta, err := client.Metadata(ctx)
	if err != nil {
		return "", err
	}
	if meta == nil || meta.Version == "" {
		return "", errors.New("meta response carries no version")
	}
	return meta.Version, nil
}

// serverSupportsVectorIndexParams reports whether a server version string
// (e.g. "1.42.0-rc.1", "v1.40.6", "1.42.0-rc.1-352208b") is at least 1.42 by
// major.minor. Anything that does not start with two numeric components is an
// error so callers can fall back deliberately.
func serverSupportsVectorIndexParams(version string) (bool, error) {
	core := strings.TrimPrefix(strings.TrimSpace(version), "v")
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) < 2 {
		return false, errors.Errorf("cannot parse server version %q", version)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false, errors.Errorf("cannot parse server version %q", version)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return false, errors.Errorf("cannot parse server version %q", version)
	}
	if major != 1 {
		return major > 1, nil
	}
	return minor >= 42, nil
}

// resolveEfMode decides how efArray values are applied for this run.
// serverVersion may be empty when /v1/meta could not be read; the returned
// reason is meant for the log line.
func resolveEfMode(cfg *Config, serverVersion string) (efMode, string) {
	if cfg.IndexType == "flat" {
		return efModeSchema, "flat has no per-request knob: rescoreLimit is updated in the schema"
	}
	supported, parseErr := serverSupportsVectorIndexParams(serverVersion)
	switch cfg.EfMode {
	case "schema":
		return efModeSchema, "--efMode=schema"
	case "request":
		if parseErr != nil || !supported {
			return efModeRequest, fmt.Sprintf("--efMode=request forced although server version %q is below 1.42 or unknown", serverVersion)
		}
		return efModeRequest, "--efMode=request"
	}
	if cfg.IndexType == "dynamic" {
		return efModeSchema, "dynamic index: the flat stage's rescoreLimit is schema-only, auto keeps schema updates"
	}
	if parseErr != nil {
		return efModeSchema, fmt.Sprintf("server version %q could not be parsed, falling back to schema updates", serverVersion)
	}
	if !supported {
		return efModeSchema, fmt.Sprintf("server %s is below 1.42, using schema updates", serverVersion)
	}
	return efModeRequest, fmt.Sprintf("server %s supports vector_index_params", serverVersion)
}
