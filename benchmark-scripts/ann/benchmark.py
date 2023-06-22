from functions import *


if __name__ == '__main__':

    # variables
    weaviate_url = 'http://weaviate:8084'
    CPUs = 32
    efConstruction_array = [64, 128]
    maxConnections_array = [16, 32]
    ef_array = [64, 128, 256, 512]

    benchmark_file_array = [
        ['deep-1M.hdf5','cosine']
#        ['deep-image-96-angular.hdf5', 'cosine'],
#        ['mnist-784-euclidean.hdf5', 'l2-squared'],
#        ['gist-960-euclidean.hdf5', 'l2-squared'],
#        ['glove-25-angular.hdf5', 'cosine']
    ]   
 
    # Starts the actual benchmark, prints "completed" when done
    #print("bm file", benchmark_file_array)
    run_the_benchmarks(weaviate_url, CPUs, efConstruction_array, maxConnections_array, ef_array, benchmark_file_array)
