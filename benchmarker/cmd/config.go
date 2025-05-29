package cmd

import (
	"os"
	"strings"

	"github.com/pkg/errors"
)

type Config struct {
	Mode                     string
	Origin                   string
	Queries                  int
	QueriesFile              string
	Parallel                 int
	Limit                    int
	ClassName                string
	NamedVector              string
	IndexType                string
	ReplicationFactor        int
	API                      string
	HttpAuth                 string
	Dimensions               int
	MultiVectorDimensions    int
	MuveraEnabled            bool
	MuveraKSim               int
	MuveraDProjections       int
	MuveraRepetition         int
	DB                       string
	WhereFilter              string
	OutputFormat             string
	OutputFile               string
	BenchmarkFile            string
	BatchSize                int
	Shards                   int
	DistanceMetric           string
	MaxConnections           int
	Labels                   string
	LabelMap                 map[string]string
	EfConstruction           int
	EfArray                  string
	QueryOnly                bool
	QueryDuration            int
	BQ                       bool
	Cache                    bool
	RescoreLimit             int
	PQ                       string
	SQ                       string
	LASQ                     string
	SkipQuery                bool
	SkipAsyncReady           bool
	SkipTombstonesEmpty      bool
	SkipMemoryStats          bool
	PQRatio                  uint
	PQSegments               uint
	TrainingLimit            int
	Tenant                   string
	StartTenantNum           int
	NumTenants               int
	ExistingSchema           bool
	HttpOrigin               string
	HttpScheme               string
	UpdatePercentage         float64
	UpdateRandomized         bool
	UpdateIterations         int
	Offset                   int
	CleanupIntervalSeconds   int
	QueryDelaySeconds        int
	DynamicThreshold         int
	Filter                   bool
	FlatSearchCutoff         int
	FilterStrategy           string
	AsyncReplicationEnabled  bool
	MemoryMonitoringEnabled  bool
	MemoryMonitoringInterval int
	MemoryMonitoringFile     string
}

func (c *Config) Validate() error {
	if err := c.validateCommon(); err != nil {
		return err
	}

	// validate specific
	switch c.Mode {
	case "random-vectors":
		return c.validateRandomVectors()
	case "random-text":
		return c.validateRandomText()
	case "dataset":
		return c.validateDataset()
	case "ann-benchmark":
		return c.validateANN()
	default:
		return errors.Errorf("unrecognized mode %q", c.Mode)
	}
}

func (c *Config) performUpdates() bool {
	return c.UpdatePercentage > 0 && c.UpdatePercentage < 1 && c.UpdateIterations > 0
}

func (c *Config) validateCommon() error {
	if c.Origin == "" {
		return errors.Errorf("origin must be set")
	}

	switch c.API {
	case "graphql", "grpc":
	default:
		return errors.Errorf("unsupported API %q", c.API)
	}

	switch c.OutputFormat {
	case "text", "":
		c.OutputFormat = "text"
	case "json":
	default:
		return errors.Errorf("unsupported output format %q, must be one of [text, json]",
			c.OutputFormat)

	}

	httpAuth, httpAuthPresent := os.LookupEnv("HTTP_AUTH")
	if httpAuthPresent {
		c.HttpAuth = httpAuth
	}

	if c.API == "grpc" && c.WhereFilter != "" {
		return errors.Errorf("where parameter is not yet supported on grpc")
	}

	return nil
}

func (c Config) validateRandomText() error {
	return nil
}

func (c Config) validateRandomVectors() error {
	return nil
}

func (c Config) validateDataset() error {
	if c.QueriesFile == "" {
		return errors.Errorf("a queries input file must be provided")
	}

	return nil
}

func (c *Config) parseLabels() {
	result := make(map[string]string)
	pairs := strings.Split(c.Labels, ",")

	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2) // SplitN to make sure we only split on the first "="
		if len(kv) == 2 {
			result[kv[0]] = kv[1]
		}
	}

	c.LabelMap = result
}

func (c Config) validateANN() error {
	if c.BenchmarkFile == "" {
		return errors.Errorf("a vector benchmark file must be provided")
	}

	if c.API != "grpc" {
		return errors.Errorf("only grpc is supported for ann-benchmark")
	}

	if c.DistanceMetric == "" {
		return errors.Errorf("distance metric must be set")
	}

	return nil
}
