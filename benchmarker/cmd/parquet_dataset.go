package cmd

import (
	"errors"
	"io"

	log "github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
)

type ParquetDataset struct {
	hubDataset  *datasets.HubDataset
	neighbors   [][]int
	testVectors [][]float32
	dimension   int
	trainRows   int
}

func NewParquetDataset(datasetID string, subset string, multiVectorDimension int, useFilters bool) *ParquetDataset {
	if useFilters {
		log.Fatalf("parquet datasets do not support filters (yet)")
	}

	if multiVectorDimension > 0 {
		log.Fatalf("parquet datasets do not support multi-vectors (yet)")
	}

	logger := log.New()
	hubDataset := datasets.NewHubDataset(datasetID, subset, logger)
	trainReader, err := hubDataset.NewDataReader(datasets.TrainSplit, 0, -1, 100)
	if err != nil {
		log.Fatalf("failed to open training data set")
	}
	defer trainReader.Close()
	trainRows := trainReader.NumRowsInFile()
	chunk, err := trainReader.ReadNextChunk()
	if err != nil && !errors.Is(err, io.EOF) {
		log.Fatalf("failed to read first chunk of training data to identify data dimension")
	}
	dimension := len(chunk.Vectors[0])
	return &ParquetDataset{
		hubDataset: hubDataset,
		dimension:  dimension,
		trainRows:  trainRows,
	}
}

func (ds *ParquetDataset) Close() {}

func (ds *ParquetDataset) TestFilters() []int {
	return make([]int, 0)
}

func (ds *ParquetDataset) TrainFilters() []int {
	return make([]int, 0)
}

func (ds *ParquetDataset) loadTestData() {
	neighbors, vectors, err := ds.hubDataset.LoadTestData()
	if err != nil {
		log.Fatalf("Error loading test data: %v", err)
	}
	ds.testVectors = vectors
	// Cast the neighbors from uint64 to int.
	ds.neighbors = make([][]int, len(neighbors))
	for i, knn := range neighbors {
		ds.neighbors[i] = make([]int, len(knn))
		for j := range knn {
			ds.neighbors[i][j] = int(knn[j])
		}
	}
}

func (ds *ParquetDataset) Neighbors() [][]int {
	if ds.neighbors == nil {
		ds.loadTestData()
	}
	return ds.neighbors
}

func (ds *ParquetDataset) TestVectors() [][]float32 {
	if ds.testVectors == nil {
		ds.loadTestData()
	}
	return ds.testVectors
}

func (ds *ParquetDataset) StreamTrainData(chunks chan<- Batch, batchSize int, startOffset int, maxRows int) {
	startRow := startOffset
	endRow := startRow + maxRows
	if maxRows == 0 {
		endRow = -1
	}
	trainReader, err := ds.hubDataset.NewDataReader(datasets.TrainSplit, startRow, endRow, batchSize)
	if err != nil {
		log.Fatalf("failed to open training data set")
	}
	defer trainReader.Close()

	for {
		chunk, err := trainReader.ReadNextChunk()
		if err != nil && !errors.Is(err, io.EOF) {
			log.Fatalf("failed while reading chunk of training data: %v", err)
		}
		batch := Batch{
			Vectors: chunk.Vectors,
			Offset:  chunk.RowOffset,
			Filters: make([]int, 0),
		}
		// Logging here for compatibility with the HDF5 dataset.
		if (batch.Offset+batchSize)%10000 == 0 {
			log.Printf("Imported %d/%d rows", batch.Offset+batchSize, ds.NumTrainVectors())
		}
		chunks <- batch
		if errors.Is(err, io.EOF) {
			break
		}
	}
}

func (ds *ParquetDataset) Dimension() int {
	return ds.dimension
}

func (ds *ParquetDataset) NumTrainVectors() int {
	return ds.trainRows
}
