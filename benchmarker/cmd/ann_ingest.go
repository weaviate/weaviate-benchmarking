package cmd

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	log "github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/auth"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/byteops"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

type CompressionType byte

const (
	CompressionTypePQ           CompressionType = 0
	CompressionTypeSQ           CompressionType = 1
	CompressionTypeRQ           CompressionType = 2
	CompressionTypeUncompressed CompressionType = 255
)

// Batch of vectors and offset for writing to Weaviate
type Batch struct {
	Vectors [][]float32
	Offset  int
	Filters []int
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

// writeChunk writes a single batch of vectors to Weaviate using gRPC.
func writeChunk(chunk *Batch, client *weaviategrpc.WeaviateClient, cfg *Config) {
	objects := make([]*weaviategrpc.BatchObject, len(chunk.Vectors))

	for i, vector := range chunk.Vectors {
		objects[i] = &weaviategrpc.BatchObject{
			Uuid:       uuidFromInt(i + chunk.Offset + cfg.Offset),
			Collection: cfg.ClassName,
		}
		if cfg.Tenant != "" {
			objects[i].Tenant = cfg.Tenant
		}
		if cfg.MultiVectorDimensions > 0 {
			if len(vector)%cfg.MultiVectorDimensions != 0 {
				log.Fatalf("Vector length %d is not a multiple of dimensions %d",
					len(vector), cfg.MultiVectorDimensions)
			}
			rows := len(vector) / cfg.MultiVectorDimensions

			multiVec := make([][]float32, rows)
			for i := 0; i < rows; i++ {
				start := i * cfg.MultiVectorDimensions
				end := start + cfg.MultiVectorDimensions
				multiVec[i] = vector[start:end]
			}
			objects[i].Vectors = []*weaviategrpc.Vectors{{
				Name:        "multivector",
				VectorBytes: byteops.Fp32SliceOfSlicesToBytes(multiVec),
				Type:        weaviategrpc.Vectors_VECTOR_TYPE_MULTI_FP32,
			}}
		} else {
			objects[i].VectorBytes = encodeVector(vector)
		}
		if cfg.NamedVector != "" {
			vectors := make([]*weaviategrpc.Vectors, 1)
			vectors[0] = &weaviategrpc.Vectors{
				VectorBytes: encodeVector(vector),
				Name:        cfg.NamedVector,
			}
			objects[i].Vectors = vectors
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

// enableCompression enables the specified compression type on an already-loaded collection.
func enableCompression(cfg *Config, client *weaviate.Client, dimensions uint, compressionType CompressionType) {
	classConfig, err := client.Schema().ClassGetter().WithClassName(cfg.ClassName).Do(context.Background())
	if err != nil {
		panic(err)
	}

	var segments uint
	var vectorIndexConfig map[string]interface{}

	if cfg.MultiVectorDimensions > 0 {
		vectorIndexConfig = classConfig.VectorConfig["multivector"].VectorIndexConfig.(map[string]interface{})
	} else {
		if cfg.NamedVector == "" {
			vectorIndexConfig = classConfig.VectorIndexConfig.(map[string]interface{})
		} else {
			vectorIndexConfig = classConfig.VectorConfig[cfg.NamedVector].VectorIndexConfig.(map[string]interface{})
			classConfig.Vectorizer = ""
		}
	}

	switch compressionType {
	case CompressionTypePQ:
		if dimensions%cfg.PQRatio != 0 {
			log.Fatalf("PQ ratio of %d and dimensions of %d incompatible", cfg.PQRatio, dimensions)
		}
		if !cfg.MuveraEnabled {
			segments = dimensions / cfg.PQRatio
		} else {
			segments = uint(math.Pow(2, float64(cfg.MuveraKSim))*float64(cfg.MuveraDProjections)*float64(cfg.MuveraRepetition)) / cfg.PQRatio
		}

		pqConfig := map[string]interface{}{
			"enabled":       true,
			"segments":      segments,
			"trainingLimit": cfg.TrainingLimit,
		}
		if cfg.RescoreLimit > -1 {
			pqConfig["rescoreLimit"] = cfg.RescoreLimit
		}
		vectorIndexConfig["pq"] = pqConfig
	case CompressionTypeSQ:
		sqConfig := map[string]interface{}{
			"enabled":       true,
			"trainingLimit": cfg.TrainingLimit,
		}
		if cfg.RescoreLimit > -1 {
			sqConfig["rescoreLimit"] = cfg.RescoreLimit
		}
		vectorIndexConfig["sq"] = sqConfig
	case CompressionTypeRQ:
		rqConfig := map[string]interface{}{
			"enabled": true,
			"bits":    cfg.RQBits,
		}
		if cfg.RescoreLimit > -1 {
			rqConfig["rescoreLimit"] = cfg.RescoreLimit
		}
		vectorIndexConfig["rq"] = rqConfig
	}

	if cfg.MultiVectorDimensions > 0 {
		vectorConfig := classConfig.VectorConfig["multivector"]
		vectorConfig.VectorIndexConfig = vectorIndexConfig
		classConfig.VectorConfig["multivector"] = vectorConfig
	} else {
		if cfg.NamedVector == "" {
			classConfig.VectorIndexConfig = vectorIndexConfig
		} else {
			vectorConfig := classConfig.VectorConfig[cfg.NamedVector]
			vectorConfig.VectorIndexConfig = vectorIndexConfig
			classConfig.VectorConfig[cfg.NamedVector] = vectorConfig
		}
	}

	err = client.Schema().ClassUpdater().WithClass(classConfig).Do(context.Background())
	if err != nil {
		panic(err)
	}
	switch compressionType {
	case CompressionTypePQ:
		log.WithFields(log.Fields{"segments": segments, "dimensions": dimensions}).Printf("Enabled PQ. Waiting for shard ready.\n")
	case CompressionTypeSQ:
		log.Printf("Enabled SQ. Waiting for shard ready.\n")
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
	case CompressionTypeRQ:
		log.Printf("RQ Completed in %v\n", endTime.Sub(start))
	}
}
