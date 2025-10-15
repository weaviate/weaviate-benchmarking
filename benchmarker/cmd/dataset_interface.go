package cmd

type Dataset interface {
	StreamTrainData(chunks chan<- Batch, batchSize int, startOffset int, maxRecords int)
	TestVectors() [][]float32
	Neighbors() [][]int
	TrainFilters() []int
	TestFilters() []int
	Dimension() int
	NumTrainVectors() int
	Close()
}
