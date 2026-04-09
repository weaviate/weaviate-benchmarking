package cmd

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
)

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

	classObj := &models.Class{
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
			pqConfig := map[string]interface{}{
				"enabled":       true,
				"segments":      cfg.PQSegments,
				"trainingLimit": cfg.TrainingLimit,
			}
			if cfg.RescoreLimit > -1 {
				pqConfig["rescoreLimit"] = cfg.RescoreLimit
			}
			vectorIndexConfig["pq"] = pqConfig
		} else if cfg.BQ {
			bqConfig := map[string]interface{}{
				"enabled": true,
			}
			if cfg.RescoreLimit > -1 {
				bqConfig["rescoreLimit"] = cfg.RescoreLimit
			}
			vectorIndexConfig["bq"] = bqConfig
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
		} else if cfg.RQ == "auto" {
			rqConfig := map[string]interface{}{
				"enabled": true,
				"bits":    cfg.RQBits,
			}
			if cfg.RescoreLimit > -1 {
				rqConfig["rescoreLimit"] = cfg.RescoreLimit
			}
			vectorIndexConfig = map[string]interface{}{
				"distance":               cfg.DistanceMetric,
				"efConstruction":         float64(cfg.EfConstruction),
				"maxConnections":         float64(cfg.MaxConnections),
				"cleanupIntervalSeconds": cfg.CleanupIntervalSeconds,
				"rq":                     rqConfig,
			}
		}
	} else if cfg.IndexType == "flat" {
		// Validate that BQ and RQ are not both enabled
		if cfg.BQ && cfg.RQ == "auto" {
			log.Fatalf("Cannot enable both BQ and RQ on flat index type")
		}

		vectorIndexConfig = map[string]interface{}{
			"distance": cfg.DistanceMetric,
		}
		if cfg.BQ {
			bqConfig := map[string]interface{}{
				"enabled": true,
				"cache":   cfg.Cache,
			}
			if cfg.RescoreLimit > -1 {
				bqConfig["rescoreLimit"] = cfg.RescoreLimit
			}
			vectorIndexConfig["bq"] = bqConfig
		} else if cfg.RQ == "auto" {
			rqConfig := map[string]interface{}{
				"enabled": true,
				"bits":    cfg.RQBits,
				"cache":   cfg.Cache,
			}
			if cfg.RescoreLimit > -1 {
				rqConfig["rescoreLimit"] = cfg.RescoreLimit
			}
			vectorIndexConfig["rq"] = rqConfig
			log.WithFields(log.Fields{"bits": cfg.RQBits, "indexType": "flat"}).Printf("Enabled RQ on flat index type")
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
			"flat": map[string]interface{}{},
		}
		if cfg.PQ == "auto" {
			pqConfig := map[string]interface{}{
				"enabled":       true,
				"segments":      cfg.PQSegments,
				"trainingLimit": cfg.TrainingLimit,
			}
			if cfg.RescoreLimit > -1 {
				pqConfig["rescoreLimit"] = cfg.RescoreLimit
			}
			vectorIndexConfig["hnsw"].(map[string]interface{})["pq"] = pqConfig
		} else if cfg.BQ {
			bqConfig := map[string]interface{}{
				"enabled": true,
				"cache":   true,
			}
			if cfg.RescoreLimit > -1 {
				bqConfig["rescoreLimit"] = cfg.RescoreLimit
			}
			vectorIndexConfig["hnsw"].(map[string]interface{})["bq"] = bqConfig
		} else if cfg.RQ == "auto" {
			vectorIndexConfig["flat"].(map[string]interface{})["rq"] = map[string]interface{}{
				"enabled": true,
				"bits":    cfg.RQBits,
			}
			vectorIndexConfig["hnsw"].(map[string]interface{})["rq"] = map[string]interface{}{
				"enabled": true,
				"bits":    cfg.RQBits,
			}
		}
	} else if cfg.IndexType == "hfresh" {
		vectorIndexConfig = map[string]interface{}{
			"distance":         cfg.DistanceMetric,
			"maxPostingSizeKB": cfg.MaxPostingSizeKB,
			"replicas":         cfg.Replicas,
			"rngFactor":        cfg.RngFactor,
			"rq": map[string]interface{}{
				"rescoreLimit": cfg.RescoreLimit,
			},
		}
	} else {
		log.Fatalf("Unknown index type %s", cfg.IndexType)
	}

	vectorIndexConfig["filterStrategy"] = cfg.FilterStrategy

	if cfg.NamedVector != "" {
		vectorConfig := make(map[string]models.VectorConfig)
		vectorConfig[cfg.NamedVector] = models.VectorConfig{
			Vectorizer:        map[string]interface{}{"none": nil},
			VectorIndexType:   cfg.IndexType,
			VectorIndexConfig: vectorIndexConfig,
		}
		classObj.VectorConfig = vectorConfig
	} else {
		if cfg.MultiVectorDimensions > 0 {
			vectorIndexConfig = map[string]interface{}{}
			if cfg.PQ == "auto" {
				pqConfig := map[string]interface{}{
					"enabled":       true,
					"segments":      cfg.PQSegments,
					"trainingLimit": cfg.TrainingLimit,
				}
				if cfg.RescoreLimit > -1 {
					pqConfig["rescoreLimit"] = cfg.RescoreLimit
				}
				vectorIndexConfig["pq"] = pqConfig
			} else if cfg.BQ {
				bqConfig := map[string]interface{}{
					"enabled": true,
					"cache":   true,
				}
				if cfg.RescoreLimit > -1 {
					bqConfig["rescoreLimit"] = cfg.RescoreLimit
				}
				vectorIndexConfig["bq"] = bqConfig
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
			} else if cfg.RQ == "auto" {
				rqConfig := map[string]interface{}{
					"enabled": true,
					"bits":    cfg.RQBits,
				}
				if cfg.RescoreLimit > -1 {
					rqConfig["rescoreLimit"] = cfg.RescoreLimit
				}

				vectorIndexConfig = map[string]interface{}{
					"distance":               cfg.DistanceMetric,
					"efConstruction":         float64(cfg.EfConstruction),
					"maxConnections":         float64(cfg.MaxConnections),
					"cleanupIntervalSeconds": cfg.CleanupIntervalSeconds,
					"rq":                     rqConfig,
				}
			}
			vectorIndexConfig["multivector"] = map[string]interface{}{
				"enabled": true,
				"muvera": map[string]interface{}{
					"enabled":      cfg.MuveraEnabled,
					"ksim":         cfg.MuveraKSim,
					"dprojections": cfg.MuveraDProjections,
					"repetition":   cfg.MuveraRepetition,
				},
			}

			classObj.VectorConfig = map[string]models.VectorConfig{
				"multivector": {
					Vectorizer: map[string]interface{}{
						"none": map[string]interface{}{},
					},
					VectorIndexConfig: vectorIndexConfig,
					VectorIndexType:   cfg.IndexType,
				},
			}
		} else {
			classObj.VectorIndexType = cfg.IndexType
			classObj.VectorIndexConfig = vectorIndexConfig
		}
	}

	if cfg.ReplicationFactor > 1 || cfg.AsyncReplicationEnabled {
		classObj.ReplicationConfig = &models.ReplicationConfig{
			Factor:       int64(cfg.ReplicationFactor),
			AsyncEnabled: cfg.AsyncReplicationEnabled,
		}
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

// updateEf updates the ef (or rescore limit / search probe) parameter on the Weaviate schema.
func updateEf(ef int, cfg *Config, client *weaviate.Client) {
	classConfig, err := client.Schema().ClassGetter().WithClassName(cfg.ClassName).Do(context.Background())
	if err != nil {
		panic(err)
	}

	var vectorIndexConfig map[string]interface{}

	if cfg.NamedVector != "" {
		vectorIndexConfig = classConfig.VectorConfig[cfg.NamedVector].VectorIndexConfig.(map[string]interface{})
	} else if cfg.MultiVectorDimensions > 0 {
		vectorIndexConfig = classConfig.VectorConfig["multivector"].VectorIndexConfig.(map[string]interface{})
	} else {
		vectorIndexConfig = classConfig.VectorIndexConfig.(map[string]interface{})
	}

	switch cfg.IndexType {
	case "hnsw":
		vectorIndexConfig["ef"] = ef
	case "flat":
		if bq, exists := vectorIndexConfig["bq"]; exists && cfg.BQ {
			bqConfig := bq.(map[string]interface{})
			bqConfig["rescoreLimit"] = ef
		} else if rq, exists := vectorIndexConfig["rq"]; exists {
			rqConfig := rq.(map[string]interface{})
			rqConfig["rescoreLimit"] = ef
		}
	case "dynamic":
		hnswConfig := vectorIndexConfig["hnsw"].(map[string]interface{})
		hnswConfig["ef"] = ef
		flatConfig := vectorIndexConfig["flat"].(map[string]interface{})
		if bq, exists := flatConfig["bq"]; exists && cfg.BQ {
			bqConfig := bq.(map[string]interface{})
			bqConfig["rescoreLimit"] = ef
		} else if rq, exists := flatConfig["rq"]; exists {
			rqConfig := rq.(map[string]interface{})
			rqConfig["rescoreLimit"] = ef
		}
	case "hfresh":
		vectorIndexConfig["searchProbe"] = ef
	}

	if cfg.NamedVector != "" {
		vectorConfig := classConfig.VectorConfig[cfg.NamedVector]
		vectorConfig.VectorIndexConfig = vectorIndexConfig
		classConfig.VectorConfig[cfg.NamedVector] = vectorConfig
	} else if cfg.MultiVectorDimensions > 0 {
		vectorConfig := classConfig.VectorConfig["multivector"]
		vectorConfig.VectorIndexConfig = vectorIndexConfig
		classConfig.VectorConfig["multivector"] = vectorConfig
	} else {
		classConfig.VectorIndexConfig = vectorIndexConfig
	}

	err = client.Schema().ClassUpdater().WithClass(classConfig).Do(context.Background())
	if err != nil {
		panic(err)
	}
}
