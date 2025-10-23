package cmd

import (
	log "github.com/sirupsen/logrus"
	"github.com/weaviate/hdf5"
)

type Hdf5Dataset struct {
	file                 *hdf5.File
	trainDimension       int
	trainRows            int
	multiVectorDimension int
	useFilters           bool
}

func NewHdf5Dataset(filePath string, multiVectorDimension int, filters bool) *Hdf5Dataset {
	file, err := hdf5.OpenFile(filePath, hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatalf("Error opening file: %v\n", err)
	}

	dataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	extent, _, _ := dataspace.SimpleExtentDims()
	rows := int(extent[0])
	var dimension int
	if multiVectorDimension > 0 {
		dimension = multiVectorDimension
	} else {
		dimension = int(extent[1])
	}

	return &Hdf5Dataset{
		file:                 file,
		trainDimension:       dimension,
		trainRows:            rows,
		multiVectorDimension: multiVectorDimension,
		useFilters:           filters,
	}
}

func (ds *Hdf5Dataset) Close() {
	ds.file.Close()
}

func (ds *Hdf5Dataset) TestFilters() []int {
	if !ds.useFilters {
		return make([]int, 0)
	}
	return loadHdf5Categories(ds.file, "test_categories")
}

func (ds *Hdf5Dataset) TrainFilters() []int {
	if !ds.useFilters {
		return make([]int, 0)
	}
	return loadHdf5Categories(ds.file, "train_categories")
}

func (ds *Hdf5Dataset) Neighbors() [][]int {
	return loadHdf5Neighbors(ds.file, "neighbors")
}

func (ds *Hdf5Dataset) TestVectors() [][]float32 {
	if ds.multiVectorDimension > 0 {
		return loadHdf5Colbert(ds.file, "test", ds.multiVectorDimension)
	} else {
		return loadHdf5Float32(ds.file, "test")
	}
}

func (ds *Hdf5Dataset) Dimension() int {
	return ds.trainDimension
}

func (ds *Hdf5Dataset) NumTrainVectors() int {
	return ds.trainRows
}

// Simply reads the data in chunks and passes it into the chunks channel.
// Used to read data from one thread while other threads load data into Weaviate.
func (ds *Hdf5Dataset) StreamTrainData(chunks chan<- Batch, batchSize int, startOffset int, maxRows int) {
	trainFilters := ds.TrainFilters()
	dataset, err := ds.file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening dataset: %v", err)
	}
	defer dataset.Close()

	if ds.multiVectorDimension > 0 {
		loadHdf5StreamingColbert(dataset, chunks, uint(batchSize), uint(startOffset), uint(maxRows), ds.multiVectorDimension, trainFilters)
	} else {
		loadHdf5Streaming(dataset, chunks, uint(batchSize), uint(startOffset), uint(maxRows), trainFilters)
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
	if byteSize != 4 && byteSize != 8 && byteSize != 16 {
		log.Fatalf("Unable to load dataset with byte size %d\n", byteSize)
	}
	return byteSize
}

// Load a large dataset from an hdf5 file and stream it to Weaviate
// startOffset and maxRecords are ignored if equal to 0
func loadHdf5Streaming(dataset *hdf5.Dataset, chunks chan<- Batch, batchSize uint, startOffset uint, maxRecords uint, filters []int) {
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
