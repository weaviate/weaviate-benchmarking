package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	log "github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func loadTrainData(ds Dataset, cfg *Config, offset uint, maxRows uint, updatePercent float32) {
	chunks := make(chan Batch, 10)
	go func() {
		ds.StreamTrainData(chunks, cfg.BatchSize, int(offset), int(maxRows))
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

			for chunk := range chunks {
				if updatePercent > 0 {
					if rand.Float32() < updatePercent {
						deleteChunk(&chunk, weaviateClient, cfg)
						writeChunk(&chunk, &grpcClient, cfg)
					}
				} else {
					writeChunk(&chunk, &grpcClient, cfg)
				}
			}
		}()
	}

	wg.Wait()
}

func getCompressionType(cfg *Config) CompressionType {
	if cfg.PQ == "enabled" {
		return CompressionTypePQ
	}
	if cfg.SQ == "enabled" {
		return CompressionTypeSQ
	}
	if cfg.RQ == "enabled" {
		return CompressionTypeRQ
	}
	return CompressionTypeUncompressed
}

// Load into Weaviate a dataset in the format of ann-benchmarks.com
// returns total time duration for load
func loadANNBenchmarksData(ds Dataset, cfg *Config, client *weaviate.Client, maxRows uint) time.Duration {
	addTenantIfNeeded(cfg, client)
	startTime := time.Now()
	compressionType := getCompressionType(cfg)
	switch compressionType {
	case CompressionTypePQ, CompressionTypeSQ, CompressionTypeRQ:
		// Load the first TrainingLimit rows of the dataset before enabling
		// compression and loading the remaining rows.
		loadTrainData(ds, cfg, 0, uint(cfg.TrainingLimit), 0)
		log.Printf("Pausing to enable compression.")
		enableCompression(cfg, client, uint(ds.Dimension()), compressionType)
		loadTrainData(ds, cfg, uint(cfg.TrainingLimit), 0, 0)
	case CompressionTypeUncompressed:
		loadTrainData(ds, cfg, 0, maxRows, 0)
	}
	endTime := time.Now()
	log.WithFields(log.Fields{"duration": endTime.Sub(startTime)}).Printf("Total load time\n")
	if !cfg.SkipAsyncReady {
		endTime = waitReady(cfg, client, startTime, 4*time.Hour, 1000)
	}
	return endTime.Sub(startTime)
}

// Load a dataset multiple time with different tenants
func loadANNBenchmarksDataMultiTenant(ds Dataset, cfg *Config, client *weaviate.Client) time.Duration {
	startTime := time.Now()

	for i := 0; i < cfg.NumTenants; i++ {
		cfg.Tenant = fmt.Sprintf("%d", i)
		loadANNBenchmarksData(ds, cfg, client, 0)
	}

	endTime := time.Now()
	log.WithFields(log.Fields{"duration": endTime.Sub(startTime)}).Printf("Multi-tenant load time\n")
	return endTime.Sub(startTime)
}
