module github.com/semi-technologies/weaviate-benchmarking/benchmarker

go 1.26.10

require (
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.1
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.65.0
	github.com/rs/zerolog v1.35.1
	github.com/spf13/cobra v1.8.1
	github.com/stretchr/testify v1.12.1
	github.com/weaviate/hdf5 v0.0.0-20230911114900-3cd888ffadcd
	github.com/weaviate/weaviate v1.40.2
	github.com/weaviate/weaviate-go-client/v6 v6.0.0-rc.0.0.20261021133446-32344349954f
	github.com/weaviate/weaviate/grpc/generated/protocol v0.0.0
	golang.org/x/exp v0.0.0-20250210185358-939b2ce775ac
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/dprotaso/go-yit v0.0.0-20220510233725-9ba8df137936 // indirect
	github.com/getkin/kin-openapi v0.139.0 // indirect
	github.com/go-openapi/swag/jsonname v0.26.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.7.0 // indirect
	github.com/oapi-codegen/runtime v1.4.1 // indirect
	github.com/oasdiff/yaml v0.1.0 // indirect
	github.com/oasdiff/yaml3 v0.0.13 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/speakeasy-api/jsonpath v0.6.3 // indirect
	github.com/speakeasy-api/openapi v1.23.0 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	github.com/woodsbury/decimal128 v1.4.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	google.golang.org/api v0.295.0 // indirect
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-openapi/jsonpointer v0.23.1 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/gomlx/go-huggingface v0.3.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/compress v1.19.1 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/parquet-go/parquet-go v0.29.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260908043556-f8649ddbbfe6 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The gRPC protocol lives in a nested module of the weaviate repo, which the
// server only references through a local replace; pin it to the revision the
// go client was built against.
replace github.com/weaviate/weaviate/grpc/generated/protocol => github.com/weaviate/weaviate/grpc/generated/protocol v0.0.0-20260910114322-ea6fe86ddafd
