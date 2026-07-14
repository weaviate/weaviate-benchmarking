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
	RQ                       string
	RQBits                   uint
	SkipQuery                bool
	SkipAsyncReady           bool
	SkipTombstonesEmpty      bool
	SkipMemoryStats          bool
	WaitForBackground        bool
	PQRatio                  uint
	PQSegments               uint
	TrainingLimit            int
	Tenant                   string
	StartTenantNum           int
	NumTenants               int
	ExistingSchema           bool
	MetricsEndpoint          string
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
	DatasetRepo              string
	Dataset                  string
	MaxPostingSizeKB         int
	Replicas                 int
	RngFactor                float64

	// BM25 benchmark
	CorpusFile      string
	SearchType      string
	QueryProperties string
	BM25Operator    string
	MinOrTokens     int
	BM25K1          float64
	BM25B           float64
	Tokenization    string
	FilterCount     int

	// BM25 tombstone generation (slow-path reproduction)
	TombstonePercentage float64
	TombstoneMode       string
	TombstoneIterations int
	TombstoneConcurrent bool
	MeasureBaseline     bool

	// BM25 quality measurement (qrels-based regression gate)
	MeasureQuality bool
	QrelsFile      string
	NDCGCutoff     int
	RecallCutoff   int
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
	case "bm25-benchmark":
		return c.validateBM25()
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
	if c.BenchmarkFile == "" && c.DatasetRepo == "" {
		return errors.Errorf("a vector benchmark file or a dataset repository and dataset must be provided")
	}

	if c.BenchmarkFile == "" && !(c.DatasetRepo != "" && c.Dataset != "") {
		return errors.Errorf("if a vector benchmark file is not provided both a dataset repo and a dataset must be provided")
	}

	if c.API != "grpc" {
		return errors.Errorf("only grpc is supported for ann-benchmark")
	}

	if c.DistanceMetric == "" {
		return errors.Errorf("distance metric must be set")
	}

	return nil
}

func (c Config) validateBM25() error {
	if c.API != "grpc" {
		return errors.Errorf("only grpc is supported for bm25-benchmark")
	}

	if !c.QueryOnly && c.CorpusFile == "" {
		return errors.Errorf("a corpus file (--corpus, BEIR corpus.jsonl) must be provided unless --query is set")
	}

	if c.QueriesFile == "" {
		return errors.Errorf("a queries file (--queriesFile, BEIR queries.jsonl) must be provided")
	}

	if c.SearchType != "bm25" {
		return errors.Errorf("unsupported searchType %q: only \"bm25\" is implemented (hybrid is reserved for a future version)", c.SearchType)
	}

	switch c.BM25Operator {
	case "", "or", "and":
	default:
		return errors.Errorf("unsupported bm25Operator %q, must be one of [or, and] (empty = server default)", c.BM25Operator)
	}

	switch c.TombstoneMode {
	case "", "update", "delete":
	default:
		return errors.Errorf("unsupported tombstoneMode %q, must be one of [update, delete]", c.TombstoneMode)
	}

	if c.TombstonePercentage < 0 || c.TombstonePercentage > 1 {
		return errors.Errorf("tombstonePercentage must be between 0 and 1")
	}

	if c.TombstonePercentage > 0 && c.CorpusFile == "" {
		return errors.Errorf("a corpus file (--corpus) is required to generate tombstones")
	}

	if c.TombstoneConcurrent && c.QueryDuration <= 0 {
		return errors.Errorf("--tombstoneConcurrent requires --queryDuration > 0 so queries overlap the background churn")
	}

	if c.TombstoneConcurrent && c.TombstoneMode == "delete" {
		return errors.Errorf("--tombstoneConcurrent requires --tombstoneMode update (delete does not sustain churn)")
	}

	if c.Parallel < 1 {
		return errors.Errorf("parallel must be at least 1")
	}

	if c.MeasureQuality {
		if c.CorpusFile == "" {
			return errors.Errorf("--measureQuality requires --corpus (needed to map qrels doc-ids to document indices, even with --query)")
		}
		if c.Filter {
			return errors.Errorf("--measureQuality is incompatible with --filter (a category filter misaligns results with corpus-wide qrels)")
		}
		if c.NumTenants > 0 {
			return errors.Errorf("--measureQuality requires single-tenant (--numTenants 0)")
		}
		if c.NDCGCutoff < 1 || c.RecallCutoff < 1 {
			return errors.Errorf("--ndcgCutoff and --recallCutoff must be at least 1")
		}
	}

	return nil
}
