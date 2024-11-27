package cmd

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"github.com/hashicorp/go-retryablehttp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/auth"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

type CompressionType byte

const (
	CompressionTypePQ   CompressionType = 0
	CompressionTypeSQ   CompressionType = 1
	CompressionTypeLASQ CompressionType = 2
)

// Batch of vectors and offset for writing to Weaviate
type Batch struct {
	Vectors [][]float32
	Offset  int
	Filters []int
}

// Weaviate https://github.com/weaviate/weaviate-chaos-engineering/tree/main/apps/ann-benchmarks style format
// mixed camel / snake case for compatibility
type ResultsJSONBenchmark struct {
	Api              string  `json:"api"`
	Ef               int     `json:"ef"`
	EfConstruction   int     `json:"efConstruction"`
	MaxConnections   int     `json:"maxConnections"`
	Mean             float64 `json:"meanLatency"`
	P99Latency       float64 `json:"p99Latency"`
	QueriesPerSecond float64 `json:"qps"`
	Shards           int     `json:"shards"`
	Parallelization  int     `json:"parallelization"`
	Limit            int     `json:"limit"`
	ImportTime       float64 `json:"importTime"`
	RunID            string  `json:"run_id"`
	Dataset          string  `json:"dataset_file"`
	Recall           float64 `json:"recall"`
	HeapAllocBytes   float64 `json:"heap_alloc_bytes"`
	HeapInuseBytes   float64 `json:"heap_inuse_bytes"`
	HeapSysBytes     float64 `json:"heap_sys_bytes"`
}

// Convert an int to a uuid formatted string
func uuidFromInt(val int) string {
	bytes := make([]byte, 16)
	binary.BigEndian.PutUint64(bytes[8:], uint64(val))
	id, err := uuid.FromBytes(bytes)
	if err != nil {
		panic(err)
	}

	return id.String()
}

// Convert a uuid formatted string to an int
func intFromUUID(uuidStr string) int {
	id, err := uuid.Parse(uuidStr)
	if err != nil {
		panic(err)
	}
	val := binary.BigEndian.Uint64(id[8:])
	return int(val)
}

// Writes a single batch of vectors to Weaviate using gRPC
func writeChunk(chunk *Batch, client *weaviategrpc.WeaviateClient, cfg *Config, namedVector string) {

	objects := make([]*weaviategrpc.BatchObject, len(chunk.Vectors))

	for i, vector := range chunk.Vectors {

		if namedVector != "" {
			vectors := make([]*weaviategrpc.Vectors, 1)
			vectors[0] = &weaviategrpc.Vectors{
				VectorBytes: encodeVector(vector),
				Name:        namedVector,
			}

			objects[i] = &weaviategrpc.BatchObject{
				Uuid:       uuidFromInt(i + chunk.Offset + cfg.Offset),
				Vectors:    vectors,
				Collection: cfg.ClassName,
			}
		} else {
			objects[i] = &weaviategrpc.BatchObject{
				Uuid:        uuidFromInt(i + chunk.Offset + cfg.Offset),
				VectorBytes: encodeVector(vector),
				Collection:  cfg.ClassName,
			}
		}

		if cfg.Tenant != "" {
			objects[i].Tenant = cfg.Tenant
		}
		if cfg.Filter {
			nonRefProperties, err := structpb.NewStruct(map[string]interface{}{
				"category": strconv.Itoa(chunk.Filters[i]),
			})
			if err != nil {
				log.Fatalf("Error creating filtered struct: %v", err)
			}
			objects[i].Properties = &weaviategrpc.BatchObject_Properties{
				NonRefProperties: nonRefProperties,
			}
		}
	}

	batchRequest := &weaviategrpc.BatchObjectsRequest{
		Objects: objects,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()

	if cfg.HttpAuth != "" {
		md := metadata.Pairs(
			"Authorization", fmt.Sprintf("Bearer %s", cfg.HttpAuth),
		)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	response, err := (*client).BatchObjects(ctx, batchRequest)
	if err != nil {
		log.Fatalf("could not send batch: %v", err)
	}

	for _, result := range response.GetErrors() {
		if result.Error != "" {
			log.Printf("Error for index %d: %s", result.Index, result.Error)
		} else {
			log.Printf("Successfully processed object at index %d", result.Index)
		}
	}

}

func createClient(cfg *Config) *weaviate.Client {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 10

	wcfg := weaviate.Config{
		Host:             cfg.HttpOrigin,
		Scheme:           cfg.HttpScheme,
		ConnectionClient: retryClient.HTTPClient,
		StartupTimeout:   60 * time.Second,
	}
	if cfg.HttpAuth != "" {
		wcfg.AuthConfig = auth.ApiKey{Value: cfg.HttpAuth}
		wcfg.ConnectionClient = nil
	}
	client, err := weaviate.NewClient(wcfg)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}
	return client
}

// Re/create Weaviate schema
func createSchema(cfg *Config, client *weaviate.Client) {

	err := client.Schema().ClassDeleter().WithClassName(cfg.ClassName).Do(context.Background())
	if err != nil {
		log.Fatalf("Error deleting class: %v", err)
	}

	multiTenancyEnabled := false
	if cfg.NumTenants > 0 {
		multiTenancyEnabled = true
	}

	var classObj = &models.Class{
		Class:       cfg.ClassName,
		Description: fmt.Sprintf("Created by the Weaviate Benchmarker at %s", time.Now().String()),
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: multiTenancyEnabled,
		},
	}

	if cfg.Shards > 1 {
		classObj.ShardingConfig = map[string]interface{}{
			"desiredCount": cfg.Shards,
		}
	}

	if cfg.ReplicationFactor > 1 {
		classObj.ReplicationConfig = &models.ReplicationConfig{
			Factor: int64(cfg.ReplicationFactor),
		}
	}

	var vectorIndexConfig map[string]interface{}

	if cfg.IndexType == "hnsw" {
		vectorIndexConfig = map[string]interface{}{
			"distance":               cfg.DistanceMetric,
			"efConstruction":         float64(cfg.EfConstruction),
			"maxConnections":         float64(cfg.MaxConnections),
			"cleanupIntervalSeconds": cfg.CleanupIntervalSeconds,
			"flatSearchCutoff":       cfg.FlatSearchCutoff,
		}
		if cfg.PQ == "auto" {
			vectorIndexConfig["pq"] = map[string]interface{}{
				"enabled":       true,
				"rescoreLimit":  cfg.RescoreLimit,
				"segments":      cfg.PQSegments,
				"trainingLimit": cfg.TrainingLimit,
			}
		} else if cfg.BQ {
			vectorIndexConfig["bq"] = map[string]interface{}{
				"enabled":      true,
				"rescoreLimit": cfg.RescoreLimit,
				"cache":        true,
			}
		} else if cfg.SQ == "auto" {
			vectorIndexConfig = map[string]interface{}{
				"distance":               cfg.DistanceMetric,
				"efConstruction":         float64(cfg.EfConstruction),
				"maxConnections":         float64(cfg.MaxConnections),
				"cleanupIntervalSeconds": cfg.CleanupIntervalSeconds,
				"sq": map[string]interface{}{
					"enabled":       true,
					"trainingLimit": cfg.TrainingLimit,
				},
			}
		} else if cfg.LASQ == "auto" {
			vectorIndexConfig = map[string]interface{}{
				"distance":               cfg.DistanceMetric,
				"efConstruction":         float64(cfg.EfConstruction),
				"maxConnections":         float64(cfg.MaxConnections),
				"cleanupIntervalSeconds": cfg.CleanupIntervalSeconds,
				"lasq": map[string]interface{}{
					"enabled":       true,
					"trainingLimit": cfg.TrainingLimit,
				},
			}
		}
	} else if cfg.IndexType == "flat" {
		vectorIndexConfig = map[string]interface{}{
			"distance": cfg.DistanceMetric,
		}
		if cfg.BQ {
			vectorIndexConfig["bq"] = map[string]interface{}{
				"enabled":      true,
				"rescoreLimit": cfg.RescoreLimit,
				"cache":        cfg.Cache,
			}
		}
	} else if cfg.IndexType == "dynamic" {
		log.WithFields(log.Fields{"threshold": cfg.DynamicThreshold}).Info("Building dynamic vector index")
		vectorIndexConfig = map[string]interface{}{
			"distance":  cfg.DistanceMetric,
			"threshold": cfg.DynamicThreshold,
			"hnsw": map[string]interface{}{
				"efConstruction":         float64(cfg.EfConstruction),
				"maxConnections":         float64(cfg.MaxConnections),
				"cleanupIntervalSeconds": cfg.CleanupIntervalSeconds,
				"flatSearchCutoff":       cfg.FlatSearchCutoff,
			},
		}
		if cfg.PQ == "auto" {
			vectorIndexConfig["hnsw"].(map[string]interface{})["pq"] = map[string]interface{}{
				"enabled":       true,
				"rescoreLimit":  cfg.RescoreLimit,
				"segments":      cfg.PQSegments,
				"trainingLimit": cfg.TrainingLimit,
			}
		} else if cfg.BQ {
			vectorIndexConfig["hnsw"].(map[string]interface{})["bq"] = map[string]interface{}{
				"enabled":      true,
				"rescoreLimit": cfg.RescoreLimit,
				"cache":        true,
			}
		}
	} else {
		log.Fatalf("Unknown index type %s", cfg.IndexType)
	}

	vectorIndexConfig["filterStrategy"] = cfg.FilterStrategy

	// Multi target vector is configured by setting the VectorConfig property and it can't be used with VectorIndexConfig at class level
	if cfg.MultiTargetVector > 0 {

		vectorConfig := make(map[string]models.VectorConfig)
		for i := 0; i < cfg.MultiTargetVector; i++ {
			vectorConfig[fmt.Sprintf("named_vector_%d", i)] = models.VectorConfig{
				Vectorizer:        map[string]interface{}{"none": nil},
				VectorIndexType:   cfg.IndexType,
				VectorIndexConfig: vectorIndexConfig,
			}
		}
		classObj.VectorConfig = vectorConfig
	} else {
		classObj.VectorIndexConfig = vectorIndexConfig
	}

	err = client.Schema().ClassCreator().WithClass(classObj).Do(context.Background())
	if err != nil {
		panic(err)
	}
	log.Printf("Created class %s", cfg.ClassName)

}

func deleteChunk(chunk *Batch, client *weaviate.Client, cfg *Config) {
	log.Debugf("Deleting chunk of %d vectors index %d", len(chunk.Vectors), chunk.Offset)
	for i := range chunk.Vectors {
		uuid := uuidFromInt(i + chunk.Offset + cfg.Offset)
		err := client.Data().Deleter().WithClassName(cfg.ClassName).WithID(uuid).Do(context.Background())
		if err != nil {
			log.Fatalf("Error deleting object: %v", err)
		}
	}
}

func deleteUuidSlice(cfg *Config, client *weaviate.Client, slice []int) {
	log.WithFields(log.Fields{"length": len(slice), "class": cfg.ClassName}).Printf("Deleting objects to trigger tombstone operations")
	for _, i := range slice {
		err := client.Data().Deleter().WithClassName(cfg.ClassName).WithID(uuidFromInt(i)).Do(context.Background())
		if err != nil {
			log.Fatalf("Error deleting object: %v", err)
		}
	}
	log.WithFields(log.Fields{"length": len(slice), "class": cfg.ClassName}).Printf("Completed deletes")
}

func deleteUuidRange(cfg *Config, client *weaviate.Client, start int, end int) {
	var slice []int
	for i := start; i < end; i++ {
		slice = append(slice, i)
	}
	deleteUuidSlice(cfg, client, slice)
}

func addTenantIfNeeded(cfg *Config, client *weaviate.Client) {
	if cfg.Tenant == "" {
		return
	}
	err := client.Schema().TenantsCreator().
		WithClassName(cfg.ClassName).
		WithTenants(models.Tenant{Name: cfg.Tenant}).
		Do(context.Background())
	if err != nil {
		log.Printf("Error adding tenant retrying in 1 second %v", err)
		time.Sleep(1 * time.Second)
		addTenantIfNeeded(cfg, client)
	}
}

// Update ef parameter on the Weaviate schema
func updateEf(ef int, cfg *Config, client *weaviate.Client) {

	classConfig, err := client.Schema().ClassGetter().WithClassName(cfg.ClassName).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if cfg.MultiTargetVector > 0 {
		for i := 0; i < cfg.MultiTargetVector; i++ {
			key := fmt.Sprintf("named_vector_%d", i)
			vectorConfig := classConfig.VectorConfig[key]
			vectorIndexConfig := vectorConfig.VectorIndexConfig.(map[string]interface{})
			switch cfg.IndexType {
			case "hnsw":
				vectorIndexConfig["ef"] = ef
			case "flat":
				bq := (vectorIndexConfig["bq"].(map[string]interface{}))
				bq["rescoreLimit"] = ef
			case "dynamic":
				hnswConfig := vectorIndexConfig["hnsw"].(map[string]interface{})
				hnswConfig["ef"] = ef
			}
			vectorConfig.VectorIndexConfig = vectorIndexConfig
			classConfig.VectorConfig[key] = vectorConfig
		}
	} else {
		vectorIndexConfig := classConfig.VectorIndexConfig.(map[string]interface{})
		switch cfg.IndexType {
		case "hnsw":
			vectorIndexConfig["ef"] = ef
		case "flat":
			bq := (vectorIndexConfig["bq"].(map[string]interface{}))
			bq["rescoreLimit"] = ef
		case "dynamic":
			hnswConfig := vectorIndexConfig["hnsw"].(map[string]interface{})
			hnswConfig["ef"] = ef
		}

		classConfig.VectorIndexConfig = vectorIndexConfig
	}

	err = client.Schema().ClassUpdater().WithClass(classConfig).Do(context.Background())

	if err != nil {
		panic(err)
	}

}

func waitReady(cfg *Config, client *weaviate.Client, indexStart time.Time, maxDuration time.Duration, minQueueSize int64) time.Time {

	start := time.Now()
	current := time.Now()

	log.Infof("Waiting for queue to be empty\n")
	for current.Sub(start) < maxDuration {
		nodesStatus, err := client.Cluster().NodesStatusGetter().WithOutput("verbose").Do(context.Background())
		if err != nil {
			panic(err)
		}
		totalShardQueue := int64(0)
		for _, n := range nodesStatus.Nodes {
			for _, s := range n.Shards {
				if s.Class == cfg.ClassName && s.VectorQueueLength > 0 {
					totalShardQueue += s.VectorQueueLength
				}
			}
		}
		if totalShardQueue < minQueueSize {
			log.WithFields(log.Fields{"duration": current.Sub(start)}).Printf("Queue ready\n")
			log.WithFields(log.Fields{"duration": current.Sub(indexStart)}).Printf("Total load and queue ready\n")
			return current
		}
		time.Sleep(2 * time.Second)
		current = time.Now()
	}
	log.Fatalf("Queue wasn't ready in %s\n", maxDuration)
	return current
}

// Update ef parameter on the Weaviate schema
func enableCompression(cfg *Config, client *weaviate.Client, dimensions uint, compressionType CompressionType) {
	classConfig, err := client.Schema().ClassGetter().WithClassName(cfg.ClassName).Do(context.Background())
	if err != nil {
		panic(err)
	}

	var segments uint
	vectorIndexConfig := classConfig.VectorIndexConfig.(map[string]interface{})

	switch compressionType {
	case CompressionTypePQ:
		if dimensions%cfg.PQRatio != 0 {
			log.Fatalf("PQ ratio of %d and dimensions of %d incompatible", cfg.PQRatio, dimensions)
		}
		segments = dimensions / cfg.PQRatio
		vectorIndexConfig["pq"] = map[string]interface{}{
			"enabled":       true,
			"segments":      segments,
			"trainingLimit": cfg.TrainingLimit,
			"rescoreLimit":  cfg.RescoreLimit,
		}
	case CompressionTypeSQ:
		vectorIndexConfig["sq"] = map[string]interface{}{
			"enabled":       true,
			"trainingLimit": cfg.TrainingLimit,
			"rescoreLimit":  cfg.RescoreLimit,
		}
	case CompressionTypeLASQ:
		vectorIndexConfig["lasq"] = map[string]interface{}{
			"enabled":       true,
			"trainingLimit": cfg.TrainingLimit,
		}
	}

	classConfig.VectorIndexConfig = vectorIndexConfig

	err = client.Schema().ClassUpdater().WithClass(classConfig).Do(context.Background())

	if err != nil {
		panic(err)
	}
	switch compressionType {
	case CompressionTypePQ:
		log.WithFields(log.Fields{"segments": segments, "dimensions": dimensions}).Printf("Enabled PQ. Waiting for shard ready.\n")
	case CompressionTypeSQ:
		log.Printf("Enabled SQ. Waiting for shard ready.\n")
	case CompressionTypeLASQ:
		log.Printf("Enabled LASQ. Waiting for shard ready.\n")
	}

	start := time.Now()

	for {
		time.Sleep(3 * time.Second)
		diff := time.Since(start)
		if diff.Minutes() > 50 {
			log.Fatalf("Shard still not ready after 50 minutes, exiting..\n")
		}
		shards, err := client.Schema().ShardsGetter().WithClassName(cfg.ClassName).Do(context.Background())
		if err != nil || len(shards) == 0 {
			if weaviateErr, ok := err.(*fault.WeaviateClientError); ok {
				log.Warnf("Error getting schema: %v", weaviateErr.DerivedFromError)
			} else {
				log.Warnf("Error getting schema: %v", err)
			}
			continue
		}
		ready := true
		for _, shard := range shards {
			if shard.Status != "READY" {
				ready = false
			}
		}
		if ready {
			break
		}
	}

	endTime := time.Now()
	switch compressionType {
	case CompressionTypePQ:
		log.WithFields(log.Fields{"segments": segments, "dimensions": dimensions}).Printf("PQ Completed in %v\n", endTime.Sub(start))
	case CompressionTypeSQ:
		log.Printf("SQ Completed in %v\n", endTime.Sub(start))
	case CompressionTypeLASQ:
		log.Printf("LASQ Completed in %v\n", endTime.Sub(start))
	}

}

func convert1DChunk[D float32 | float64](input []D, dimensions int, batchRows int) [][]float32 {
	chunkData := make([][]float32, batchRows)
	for i := range chunkData {
		chunkData[i] = make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = float32(input[i*dimensions+j])
		}
	}
	return chunkData
}

func getHDF5ByteSize(dataset *hdf5.Dataset) uint {

	datatype, err := dataset.Datatype()
	if err != nil {
		log.Fatalf("Unabled to read datatype\n")
	}

	// log.WithFields(log.Fields{"size": datatype.Size()}).Printf("Parsing HDF5 byte format\n")
	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		log.Fatalf("Unable to load dataset with byte size %d\n", byteSize)
	}
	return byteSize
}

// Load a large dataset from an hdf5 file and stream it to Weaviate
// startOffset and maxRecords are ignored if equal to 0
func loadHdf5Streaming(dataset *hdf5.Dataset, chunks chan<- Batch, cfg *Config, startOffset uint, maxRecords uint, filters []int) {
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	byteSize := getHDF5ByteSize(dataset)

	rows := dims[0]
	dimensions := dims[1]

	// Handle offsetting the data for product quantization
	i := uint(0)
	if maxRecords != 0 && maxRecords < rows {
		rows = maxRecords
	}

	if startOffset != 0 && i < rows {
		i = startOffset
	}

	batchSize := uint(cfg.BatchSize)

	log.WithFields(log.Fields{"rows": rows, "dimensions": dimensions}).Printf(
		"Reading HDF5 dataset")

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	for ; i < rows; i += batchSize {

		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}
		}

		offset := []uint{i, 0}
		count := []uint{batchRows, dimensions}

		if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		var chunkData [][]float32

		if byteSize == 4 {
			chunkData1D := make([]float32, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(batchRows))

		}

		if (i+batchRows)%10000 == 0 {
			log.Printf("Imported %d/%d rows", i+batchRows, rows)
		}

		filter := []int{}
		if len(filters) > 0 {
			filter = filters[i : i+batchRows]
		}

		chunks <- Batch{Vectors: chunkData, Offset: int(i), Filters: filter}
	}
}

// Read an entire dataset from an hdf5 file at once
func loadHdf5Float32(file *hdf5.File, name string) [][]float32 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening loadHdf5Float32 dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	byteSize := getHDF5ByteSize(dataset)

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	var chunkData [][]float32

	if byteSize == 4 {
		chunkData1D := make([]float32, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}

	return chunkData
}

func loadHdf5Categories(file *hdf5.File, name string) []int {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening neighbors dataset: %v", err)
	}
	defer dataset.Close()

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()
	if len(dims) != 1 {
		log.Fatal("expected 1 dimension")
	}

	elements := dims[0]
	byteSize := getHDF5ByteSize(dataset)

	chunkData := make([]int, elements)

	if byteSize == 4 {
		chunkData32 := make([]int32, elements)
		dataset.Read(&chunkData32)
		for i := range chunkData {
			chunkData[i] = int(chunkData32[i])
		}
	} else if byteSize == 8 {
		dataset.Read(&chunkData)
	}

	return chunkData
}

// Read an entire dataset from an hdf5 file at once (neighbours)
func loadHdf5Neighbors(file *hdf5.File, name string) [][]int {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening neighbors dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	byteSize := getHDF5ByteSize(dataset)

	chunkData := make([][]int, rows)

	if byteSize == 4 {
		chunkData1D := make([]int32, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = make([]int, dimensions)
			for j := uint(0); j < dimensions; j++ {
				chunkData[i][j] = int(chunkData1D[uint(i)*dimensions+j])
			}
		}
	} else if byteSize == 8 {
		chunkData1D := make([]int, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
		}
	}

	return chunkData
}

func calculateHdf5TrainExtent(file *hdf5.File, cfg *Config) (uint, uint) {
	dataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	extent, _, _ := dataspace.SimpleExtentDims()
	dimensions := extent[1]
	rows := extent[0]
	return rows, dimensions
}

func loadHdf5Train(file *hdf5.File, cfg *Config, offset uint, maxRows uint, updatePercent float32) uint {
	dataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	extent, _, _ := dataspace.SimpleExtentDims()
	dimensions := extent[1]

	filters := []int{}
	if cfg.Filter {
		filters = loadHdf5Categories(file, "train_categories")
	}

	chunks := make(chan Batch, 10)

	go func() {
		loadHdf5Streaming(dataset, chunks, cfg, offset, maxRows, filters)
		close(chunks)
	}()

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Import workers will primary use the direct gRPC client
			// If triggering deletes before import, we need to use the normal go client
			grpcCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			httpOption := grpc.WithInsecure()
			if cfg.HttpScheme == "https" {
				creds := credentials.NewTLS(&tls.Config{
					InsecureSkipVerify: true,
				})
				httpOption = grpc.WithTransportCredentials(creds)
			}
			defer cancel()
			opts := []retry.CallOption{
				retry.WithBackoff(retry.BackoffExponential(100 * time.Millisecond)),
			}
			grpcConn, err := grpc.DialContext(grpcCtx, cfg.Origin, httpOption, grpc.WithUnaryInterceptor(retry.UnaryClientInterceptor(opts...)))
			if err != nil {
				log.Fatalf("Did not connect: %v", err)
			}
			defer grpcConn.Close()
			grpcClient := weaviategrpc.NewWeaviateClient(grpcConn)
			weaviateClient := createClient(cfg)

			if cfg.MultiTargetVector > 0 {
				for chunk := range chunks {
					for i := 0; i < cfg.MultiTargetVector; i++ {
						processChunk(chunk, &grpcClient, weaviateClient, cfg, fmt.Sprintf("named_vector_%d", i), updatePercent)
					}
				}
			} else {
				for chunk := range chunks {
					processChunk(chunk, &grpcClient, weaviateClient, cfg, "", updatePercent)
				}
			}
		}()
	}

	wg.Wait()
	return dimensions
}

// Load an hdf5 file in the format of ann-benchmarks.com
// returns total time duration for load
func loadANNBenchmarksFile(file *hdf5.File, cfg *Config, client *weaviate.Client, maxRows uint) time.Duration {

	addTenantIfNeeded(cfg, client)
	startTime := time.Now()

	if cfg.PQ == "enabled" {
		dimensions := loadHdf5Train(file, cfg, 0, uint(cfg.TrainingLimit), 0)
		log.Printf("Pausing to enable PQ.")
		enableCompression(cfg, client, dimensions, CompressionTypePQ)
		loadHdf5Train(file, cfg, uint(cfg.TrainingLimit), 0, 0)

	} else if cfg.SQ == "enabled" {
		dimensions := loadHdf5Train(file, cfg, 0, uint(cfg.TrainingLimit), 0)
		log.Printf("Pausing to enable SQ.")
		enableCompression(cfg, client, dimensions, CompressionTypeSQ)
		loadHdf5Train(file, cfg, uint(cfg.TrainingLimit), 0, 0)

	} else if cfg.LASQ == "enabled" {
		dimensions := loadHdf5Train(file, cfg, 0, uint(cfg.TrainingLimit), 0)
		log.Printf("Pausing to enable LASQ.")
		enableCompression(cfg, client, dimensions, CompressionTypeLASQ)
		loadHdf5Train(file, cfg, uint(cfg.TrainingLimit), 0, 0)

	} else {
		loadHdf5Train(file, cfg, 0, maxRows, 0)
	}
	endTime := time.Now()
	log.WithFields(log.Fields{"duration": endTime.Sub(startTime)}).Printf("Total load time\n")
	if !cfg.SkipAsyncReady {
		endTime = waitReady(cfg, client, startTime, 4*time.Hour, 1000)
	}
	return endTime.Sub(startTime)
}

// Load a dataset multiple time with different tenants
func loadHdf5MultiTenant(file *hdf5.File, cfg *Config, client *weaviate.Client) time.Duration {

	startTime := time.Now()

	for i := 0; i < cfg.NumTenants; i++ {
		cfg.Tenant = fmt.Sprintf("%d", i)
		loadANNBenchmarksFile(file, cfg, client, 0)
	}

	endTime := time.Now()
	log.WithFields(log.Fields{"duration": endTime.Sub(startTime)}).Printf("Multi-tenant load time\n")
	return endTime.Sub(startTime)
}

func parseEfValues(s string) ([]int, error) {
	strs := strings.Split(s, ",")
	nums := make([]int, len(strs))
	for i, str := range strs {
		num, err := strconv.Atoi(str)
		if err != nil {
			return nil, fmt.Errorf("error converting efArray '%s' to integer: %v", str, err)
		}
		nums[i] = num
	}
	return nums, nil
}

func runQueries(cfg *Config, importTime time.Duration, testData [][]float32, neighbors [][]int, filters []int) {

	runID := strconv.FormatInt(time.Now().Unix(), 10)

	efCandidates, err := parseEfValues(cfg.EfArray)
	if err != nil {
		log.Fatalf("Error parsing efArray, expected commas separated format \"16,32,64\" but:%v\n", err)
	}

	// Read once at this point (after import and compaction delay) to get accurate memory stats
	memstats := &Memstats{}
	if !cfg.SkipMemoryStats {
		memstats, err = readMemoryMetrics(cfg)
		if err != nil {
			log.Warnf("Error reading memory stats: %v", err)
			memstats = &Memstats{}
		}
	}

	client := createClient(cfg)

	var benchmarkResultsMap []map[string]interface{}
	for _, ef := range efCandidates {
		updateEf(ef, cfg, client)

		var result Results

		if cfg.QueryDuration > 0 {
			result = benchmarkANNDuration(*cfg, testData, neighbors, filters)
		} else {
			result = benchmarkANN(*cfg, testData, neighbors, filters)
		}

		log.WithFields(log.Fields{"mean": result.Mean, "qps": result.QueriesPerSecond, "recall": result.Recall,
			"parallel": cfg.Parallel, "limit": cfg.Limit,
			"api": cfg.API, "ef": ef, "count": result.Total, "failed": result.Failed}).Info("Benchmark result")

		dataset := filepath.Base(cfg.BenchmarkFile)

		var resultMap map[string]interface{}

		benchResult := ResultsJSONBenchmark{
			Api:              cfg.API,
			Ef:               ef,
			EfConstruction:   cfg.EfConstruction,
			MaxConnections:   cfg.MaxConnections,
			Mean:             result.Mean.Seconds(),
			P99Latency:       result.Percentiles[len(result.Percentiles)-1].Seconds(),
			QueriesPerSecond: result.QueriesPerSecond,
			Shards:           cfg.Shards,
			Parallelization:  cfg.Parallel,
			Limit:            cfg.Limit,
			ImportTime:       importTime.Seconds(),
			RunID:            runID,
			Dataset:          dataset,
			Recall:           result.Recall,
			HeapAllocBytes:   memstats.HeapAllocBytes,
			HeapInuseBytes:   memstats.HeapInuseBytes,
			HeapSysBytes:     memstats.HeapSysBytes,
		}

		jsonData, err := json.Marshal(benchResult)
		if err != nil {
			log.Fatalf("Error converting result to json")
		}

		if err := json.Unmarshal(jsonData, &resultMap); err != nil {
			log.Fatalf("Error converting json to map")
		}

		if cfg.LabelMap != nil {
			for key, value := range cfg.LabelMap {
				resultMap[key] = value
			}
		}

		benchmarkResultsMap = append(benchmarkResultsMap, resultMap)

	}

	data, err := json.MarshalIndent(benchmarkResultsMap, "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling benchmark results: %v", err)
	}

	os.Mkdir("./results", 0755)

	err = os.WriteFile(fmt.Sprintf("./results/%s.json", runID), data, 0644)
	if err != nil {
		log.Fatalf("Error writing benchmark results to file: %v", err)
	}
}

var annBenchmarkCommand = &cobra.Command{
	Use:   "ann-benchmark",
	Short: "Benchmark ANN Benchmark style hdf5 files",
	Long:  `Run a gRPC benchmark on an hdf5 file in the format of ann-benchmarks.com`,
	Run: func(cmd *cobra.Command, args []string) {

		cfg := globalConfig
		cfg.Mode = "ann-benchmark"

		if err := cfg.Validate(); err != nil {
			fatal(err)
		}

		cfg.parseLabels()

		file, err := hdf5.OpenFile(cfg.BenchmarkFile, hdf5.F_ACC_RDONLY)
		if err != nil {
			log.Fatalf("Error opening file: %v\n", err)
		}
		defer file.Close()

		client := createClient(&cfg)

		importTime := 0 * time.Second

		if !cfg.QueryOnly {

			if !cfg.ExistingSchema {
				createSchema(&cfg, client)
			}

			log.WithFields(log.Fields{"index": cfg.IndexType, "efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
				"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile}).Info("Starting import")

			if cfg.NumTenants > 0 {
				importTime = loadHdf5MultiTenant(file, &cfg, client)
			} else {
				importTime = loadANNBenchmarksFile(file, &cfg, client, 0)
			}

			sleepDuration := time.Duration(cfg.QueryDelaySeconds) * time.Second
			log.Printf("Waiting for %s to allow for compaction etc\n", sleepDuration)
			time.Sleep(sleepDuration)
		}

		log.WithFields(log.Fields{"index": cfg.IndexType, "efC": cfg.EfConstruction, "m": cfg.MaxConnections, "shards": cfg.Shards,
			"distance": cfg.DistanceMetric, "dataset": cfg.BenchmarkFile}).Info("Benchmark configuration")

		neighbors := loadHdf5Neighbors(file, "neighbors")
		testData := loadHdf5Float32(file, "test")
		testFilters := make([]int, 0)
		if cfg.Filter {
			testFilters = loadHdf5Categories(file, "test_categories")
		}

		runQueries(&cfg, importTime, testData, neighbors, testFilters)

		if cfg.performUpdates() {

			totalRowCount, _ := calculateHdf5TrainExtent(file, &cfg)
			updateRowCount := uint(math.Floor(float64(totalRowCount) * cfg.UpdatePercentage))

			log.Printf("Performing %d update iterations\n", cfg.UpdateIterations)

			for i := 0; i < cfg.UpdateIterations; i++ {

				startTime := time.Now()

				if cfg.UpdateRandomized {
					loadHdf5Train(file, &cfg, 0, 0, float32(cfg.UpdatePercentage))
				} else {
					deleteUuidRange(&cfg, client, 0, int(updateRowCount))
					loadHdf5Train(file, &cfg, 0, updateRowCount, 0)
				}

				log.WithFields(log.Fields{"duration": time.Since(startTime)}).Printf("Total delete and update time\n")

				if !cfg.SkipTombstonesEmpty {
					err := waitTombstonesEmpty(&cfg)
					if err != nil {
						log.Fatalf("Error waiting for tombstones to be empty: %v", err)
					}
				}

				runQueries(&cfg, importTime, testData, neighbors, testFilters)

			}

		}

	},
}

func initAnnBenchmark() {
	rootCmd.AddCommand(annBenchmarkCommand)

	numCPU := runtime.NumCPU()

	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.Labels,
		"labels", "", "Labels of format key1=value1,key2=value2,...")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.BenchmarkFile,
		"vectors", "v", "", "Path to the hdf5 file containing the vectors")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "Vector", "Class name for testing")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.DistanceMetric,
		"distance", "d", "", "Set distance metric (mandatory)")
	annBenchmarkCommand.PersistentFlags().BoolVarP(&globalConfig.QueryOnly,
		"query", "q", false, "Do not import data and only run query tests")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.QueryDuration,
		"queryDuration", 0, "Instead of querying the test dataset once, query for the specified duration in seconds (default 0)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.BQ,
		"bq", false, "Set BQ")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.Cache,
		"cache", false, "Set cache")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.RescoreLimit,
		"rescoreLimit", 256, "Rescore limit (default 256) for BQ")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.PQ,
		"pq", "disabled", "Set PQ (disabled, auto, or enabled) (default disabled)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.SQ,
		"sq", "disabled", "Set SQ (disabled, auto, or enabled) (default disabled)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.LASQ,
		"lasq", "disabled", "Set LASQ (disabled, auto, or enabled) (default disabled)")
	annBenchmarkCommand.PersistentFlags().UintVar(&globalConfig.PQRatio,
		"pqRatio", 4, "Set PQ segments = dimensions / ratio (must divide evenly default 4)")
	annBenchmarkCommand.PersistentFlags().UintVar(&globalConfig.PQSegments,
		"pqSegments", 256, "Set PQ segments")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.SkipAsyncReady,
		"skipAsyncReady", false, "Skip async ready (default false)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.SkipMemoryStats,
		"skipMemoryStats", false, "Skip memory stats (default false)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.SkipTombstonesEmpty,
		"skipTombstonesEmpty", false, "Skip waiting for tombstone to be empty after update (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.TrainingLimit,
		"trainingLimit", 100000, "Set PQ trainingLimit (default 100000)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.EfConstruction,
		"efConstruction", 256, "Set Weaviate efConstruction parameter (default 256)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.EfArray,
		"efArray", "16,24,32,48,64,96,128,256,512", "Array of ef parameters as comma separated list")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.IndexType,
		"indexType", "hnsw", "Index type (hnsw or flat)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MultiTargetVector,
		"MultiTargetVector", 0, "Number of multiple target vectors (default 0)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.MaxConnections,
		"maxConnections", 16, "Set Weaviate efConstruction parameter (default 16)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.Shards,
		"shards", 1, "Set number of Weaviate shards")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.BatchSize,
		"batchSize", "b", 1000, "Batch size for insert operations")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.Parallel,
		"parallel", "p", numCPU, "Set the number of parallel threads which send queries")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.ExistingSchema,
		"existingSchema", false, "Leave the schema as-is (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.NumTenants,
		"numTenants", 0, "Number of tenants to use (default 0)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.StartTenantNum,
		"startTenant", 0, "Tenant # to start at if using multiple tenants (default 0)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.API,
		"api", "a", "grpc", "The API to use on benchmarks")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.Origin,
		"grpcOrigin", "u", "localhost:50051", "The gRPC origin that Weaviate is running at")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.HttpOrigin,
		"httpOrigin", "localhost:8080", "The http origin for Weaviate (only used if grpc enabled)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.HttpScheme,
		"httpScheme", "http", "The http scheme (http or https)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFormat,
		"format", "f", "text", "Output format, one of [text, json]")
	annBenchmarkCommand.PersistentFlags().IntVarP(&globalConfig.Limit,
		"limit", "l", 10, "Set the query limit / k (default 10)")
	annBenchmarkCommand.PersistentFlags().Float64Var(&globalConfig.UpdatePercentage,
		"updatePercentage", 0.0, "After loading the dataset, update the specified percentage of vectors")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.UpdateRandomized,
		"updateRandomized", false, "Whether to randomize which vectors are updated (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.UpdateIterations,
		"updateIterations", 1, "Number of iterations to update the dataset if updatePercentage is set")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.CleanupIntervalSeconds,
		"cleanupIntervalSeconds", 300, "HNSW cleanup interval seconds (default 300)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.QueryDelaySeconds,
		"queryDelaySeconds", 30, "How long to wait before querying (default 30)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.Offset,
		"offset", 0, "Offset for uuids (useful to load the same dataset multiple times)")
	annBenchmarkCommand.PersistentFlags().StringVarP(&globalConfig.OutputFile,
		"output", "o", "", "Filename for an output file. If none provided, output to stdout only")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.DynamicThreshold,
		"dynamicThreshold", 10_000, "Threshold to trigger the update in the dynamic index (default 10 000)")
	annBenchmarkCommand.PersistentFlags().BoolVar(&globalConfig.Filter,
		"filter", false, "Whether to use filtering for the dataset (default false)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.FlatSearchCutoff,
		"flatSearchCutoff", 40000, "Flat search cut off (default 40 000)")
	annBenchmarkCommand.PersistentFlags().StringVar(&globalConfig.FilterStrategy,
		"filterStrategy", "sweeping", "Use a different filter strategy (options are sweeping or acorn)")
	annBenchmarkCommand.PersistentFlags().IntVar(&globalConfig.ReplicationFactor,
		"replicationFactor", 1, "Replication factor")
}

func benchmarkANN(cfg Config, queries Queries, neighbors Neighbors, filters []int) Results {
	cfg.Queries = len(queries)

	i := 0
	return benchmark(cfg, func(className string) QueryWithNeighbors {
		defer func() { i++ }()

		tenant := ""
		if cfg.NumTenants > 0 {
			tenant = fmt.Sprint(rand.Intn(cfg.NumTenants))
		}
		filter := -1
		if cfg.Filter {
			filter = filters[i]
		}

		return QueryWithNeighbors{
			Query:     nearVectorQueryGrpc(cfg, queries[i], cfg.Limit, tenant, filter),
			Neighbors: neighbors[i],
		}

	})
}

type Number interface {
	constraints.Float | constraints.Integer
}

func median[T Number](data []T) float64 {
	dataCopy := make([]T, len(data))
	copy(dataCopy, data)

	slices.Sort(dataCopy)

	var median float64
	l := len(dataCopy)
	if l == 0 {
		return 0
	} else if l%2 == 0 {
		median = float64((dataCopy[l/2-1] + dataCopy[l/2]) / 2.0)
	} else {
		median = float64(dataCopy[l/2])
	}

	return median
}

type sampledResults struct {
	Min              []time.Duration
	Max              []time.Duration
	Mean             []time.Duration
	Took             []time.Duration
	QueriesPerSecond []float64
	Recall           []float64
	Results          []Results
}

func benchmarkANNDuration(cfg Config, queries Queries, neighbors Neighbors, filters []int) Results {
	cfg.Queries = len(queries)

	var samples sampledResults

	startTime := time.Now()

	var results Results

	for time.Since(startTime) < time.Duration(cfg.QueryDuration)*time.Second {
		results = benchmarkANN(cfg, queries, neighbors, filters)
		samples.Min = append(samples.Min, results.Min)
		samples.Max = append(samples.Max, results.Max)
		samples.Mean = append(samples.Mean, results.Mean)
		samples.Took = append(samples.Took, results.Took)
		samples.QueriesPerSecond = append(samples.QueriesPerSecond, results.QueriesPerSecond)
		samples.Recall = append(samples.Recall, results.Recall)
		samples.Results = append(samples.Results, results)
	}

	var medianResult Results

	medianResult.Min = time.Duration(median(samples.Min))
	medianResult.Max = time.Duration(median(samples.Max))
	medianResult.Mean = time.Duration(median(samples.Mean))
	medianResult.Took = time.Duration(median(samples.Took))
	medianResult.QueriesPerSecond = median(samples.QueriesPerSecond)
	medianResult.Percentiles = results.Percentiles
	medianResult.PercentilesLabels = results.PercentilesLabels
	medianResult.Total = results.Total
	medianResult.Successful = results.Successful
	medianResult.Failed = results.Failed
	medianResult.Parallelization = cfg.Parallel
	medianResult.Recall = median(samples.Recall)

	return medianResult
}

func processChunk(chunk Batch, grpcClient *weaviategrpc.WeaviateClient, weaviateClient *weaviate.Client, cfg *Config, namedVector string, updatePercent float32) {
	if updatePercent > 0 && rand.Float32() < updatePercent {
		deleteChunk(&chunk, weaviateClient, cfg)
	}
	writeChunk(&chunk, grpcClient, cfg, namedVector)
}
