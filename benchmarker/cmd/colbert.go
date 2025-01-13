package cmd

import "C"

import (
	"unsafe"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/weaviate/hdf5"
)

func loadHdf5StreamingColbert(dataset *hdf5.Dataset, chunks chan<- Batch, cfg *Config, startOffset uint, maxRecords uint, filters []int) {
	dataspace := dataset.Space()
	dims, _, err := dataspace.SimpleExtentDims()
	if err != nil {
		log.Fatalf("Error getting dimensions: %v", err)
	}

	rows := dims[0]

	i := uint(0)
	if maxRecords != 0 && maxRecords < rows {
		rows = maxRecords
	}

	if startOffset != 0 && i < rows {
		i = startOffset
	}

	batchSize := uint(cfg.BatchSize)

	log.WithFields(log.Fields{"rows": rows}).Printf("Reading HDF5 Colbert dataset")

	// Create memory space for batch reading
	memDims := []uint{1}
	memspace, err := hdf5.CreateSimpleDataspace(memDims, nil)
	if err != nil {
		log.Fatalf("Error creating memory space: %v", err)
	}
	defer memspace.Close()

	// For variable-length arrays
	type hvl_t struct {
		len C.size_t
		p   unsafe.Pointer
	}

	for ; i < rows; i += batchSize {
		batchRows := batchSize
		if i+batchSize > rows {
			batchRows = rows - i
		}

		chunkData := make([][]float32, batchRows)

		// Process each row in the batch
		for j := uint(0); j < batchRows; j++ {
			// Allocate memory for one hvl_t struct
			vlen := make([]hvl_t, 1)

			// Select row in file
			offset := []uint{i + j}
			count := []uint{1}
			err = dataspace.SelectHyperslab(offset, nil, count, nil)
			if err != nil {
				log.Fatalf("Error selecting hyperslab: %v", err)
			}

			// Read the variable length data
			err = dataset.ReadSubset(&vlen[0], memspace, dataspace)
			if err != nil {
				log.Fatalf("Error reading dataset: %v", err)
			}

			// Convert the data to []float32
			length := int(vlen[0].len)
			data := make([]float32, length)
			src := unsafe.Slice((*float32)(vlen[0].p), length)
			copy(data, src)

			// Add check length is a multiple of dimensions
			if length%cfg.MultiVectorDimensions != 0 {
				log.Fatalf("Length %d is not a multiple of dimensions %d",
					length, cfg.MultiVectorDimensions)
			}

			chunkData[j] = data
		}

		if (i+batchRows)%10000 == 0 {
			log.Printf("Imported %d/%d rows", i+batchRows, rows)
		}

		filter := []int{}
		if len(filters) > 0 {
			filter = filters[i : i+batchRows]
		}

		chunks <- Batch{
			Vectors: chunkData,
			Offset:  int(i),
			Filters: filter,
		}
	}
}

func loadHdf5Colbert(file *hdf5.File, name string, dimensions int) [][]float32 {

	var result [][]float32

	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()

	dataspace := dataset.Space()
	fileDims, _, err := dataspace.SimpleExtentDims()
	if err != nil {
		log.Fatalf("Error getting dimensions: %v", err)
	}
	log.Infof("Number of vectors: %v", fileDims[0])

	result = make([][]float32, fileDims[0])

	// For variable-length arrays, we need to allocate a slice of hvl_t structs
	type hvl_t struct {
		len C.size_t
		p   unsafe.Pointer
	}

	// Create memory space for single row
	memDims := []uint{1}
	memspace, err := hdf5.CreateSimpleDataspace(memDims, nil)
	if err != nil {
		log.Fatalf("Error creating memory space: %v", err)
	}
	defer memspace.Close()

	// Iterate through all vectors
	for i := uint(0); i < fileDims[0]; i++ {
		// Allocate memory for one hvl_t struct
		vlen := make([]hvl_t, 1)

		// Select row i in file
		offset := []uint{i}
		count := []uint{1}
		err = dataspace.SelectHyperslab(offset, nil, count, nil)
		if err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		// Read the variable length data
		err = dataset.ReadSubset(&vlen[0], memspace, dataspace)
		if err != nil {
			log.Fatalf("Error reading dataset: %v", err)
		}

		// Convert the data to []float32
		length := int(vlen[0].len)
		data := make([]float32, length)
		src := unsafe.Slice((*float32)(vlen[0].p), length)
		copy(data, src)

		log.Infof("Vector %d:", i)
		log.Infof("  Length: %d", length)
		log.Infof("  First three values: %v, %v, %v", data[0], data[1], data[2])

		// Add check length is a multiple of dimensions
		if length%dimensions != 0 {
			log.Fatalf("Length %d is not a multiple of dimensions %d", length, dimensions)
		}
		result[i] = data
	}
	return result
}

var colbertCmd = &cobra.Command{
	Use:        "colbert",
	Short:      "Load multidimensional data",
	Deprecated: "This command is deprecated and will be removed in the future",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := globalConfig

		file, err := hdf5.OpenFile(cfg.BenchmarkFile, hdf5.F_ACC_RDONLY)
		if err != nil {
			log.Fatalf("Error opening file: %v\n", err)
		}
		defer file.Close()

		res := loadHdf5Colbert(file, "train", cfg.MultiVectorDimensions)

		log.Infof("First vector:")
		log.Infof("  Length: %d", len(res[0]))
		log.Infof("  First three values: %v, %v, %v", res[0][0], res[0][1], res[0][2])

	},
}

func initColbert() {
	rootCmd.AddCommand(colbertCmd)
	colbertCmd.PersistentFlags().StringVarP(&globalConfig.BenchmarkFile,
		"vectors", "v", "", "Path to the hdf5 file containing the vectors")
	colbertCmd.PersistentFlags().IntVarP(&globalConfig.MultiVectorDimensions,
		"multiVector", "m", 0, "Enable multi-dimensional vectors with the specified number of dimensions")
	colbertCmd.PersistentFlags().StringVarP(&globalConfig.ClassName,
		"className", "c", "Vector", "The Weaviate class to run the benchmark against")
	colbertCmd.PersistentFlags().StringVarP(&globalConfig.Origin,
		"grpcOrigin", "u", "localhost:50051", "The gRPC origin that Weaviate is running at")
	colbertCmd.PersistentFlags().StringVar(&globalConfig.HttpOrigin,
		"httpOrigin", "localhost:8080", "The http origin for Weaviate (without http scheme)")
	colbertCmd.PersistentFlags().StringVar(&globalConfig.HttpScheme,
		"httpScheme", "http", "The http scheme (http or https)")
}
