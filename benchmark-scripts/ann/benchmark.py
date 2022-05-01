from functions import *


if __name__ == '__main__':

    # variables
    weaviate_url = 'http://weaviate:8080'
    CPUs = 32
    efConstruction_array = [64, 128, 256]
    maxConnections_array = [16, 32, 64]
    ef_array = [64, 128, 256, 512]

    benchmark_file_array = [
        ['mnist-784-euclidean.hdf5', 'l2-squared'],
        ['glove-25-angular.hdf5', 'cosine'],
        ['sift-128-euclidean.hdf5', 'l2-squared'],
        ['deep-image-96-angular.hdf5', 'cosine'],
        ['glove-200-angular.hdf5', 'cosine'],
        ['glove-50-angular.hdf5', 'cosine'],
        ['nytimes-256-angular.hdf5', 'cosine'],
        ['glove-100-angular.hdf5', 'cosine'],
        ['lastfm-64-dot.hdf5', 'cosine']
    ]   
 
    # Starts the actual benchmark, prints "completed" when done
    run_the_benchmarks(weaviate_url, CPUs, efConstruction_array, maxConnections_array, ef_array, benchmark_file_array)
