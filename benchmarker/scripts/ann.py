#!/usr/bin/env python3
import subprocess, os

efConstruction = [64, 128]
maxConnections = [16, 32]
ef = [64, 128, 256, 512]
limits = [10, 100]
parallel = 32

datasets = [
    ['dbpedia-100k-openai-ada002.hdf5', 'l2-squared'],
    ['deep-image-96-angular.hdf5', 'cosine'],
    ['mnist-784-euclidean.hdf5', 'l2-squared'],
    ['gist-960-euclidean.hdf5', 'l2-squared'],
    ['glove-25-angular.hdf5', 'cosine']
]

if __name__ == '__main__':

    try:
        weaviate_url = os.environ["WEAVIATE_ORIGIN"]
    except KeyError:
        raise RuntimeError("Environment variable WEAVIATE_ORIGIN is not set.")
    
    try:
        weaviate_http_url = os.environ["WEAVIATE_HTTP_ORIGIN"]
    except KeyError:
        raise RuntimeError("Environment variable WEAVIATE_HTTP_ORIGIN is not set.")
    
    try:
        dataset_directory = os.environ["DATASET_DIRECTORY"]
    except KeyError:
        raise RuntimeError("Environment variable DATASET_DIRECTORY is not set.")

    for efc in efConstruction:
        for maxC in maxConnections:
            for benchmark_file, metric in datasets:
                for limit in limits:
                    cmd = [
                        './benchmarker', 'ann-benchmark',
                        '--efConstruction', str(efc),
                        '--maxConnections', str(maxC),
                        '--efArray', ','.join(map(str, ef)),
                        '--limit', str(limit),
                        '--parallel', str(parallel),
                        '--vectors', os.path.join(dataset_directory, benchmark_file),
                        '--distance', metric,
                        '--origin', weaviate_url,
                        '--httpOrigin', weaviate_http_url,
                    ]
                    
                    print(f"{' '.join(cmd)}")                    
                    try:
                        subprocess.check_call(cmd)
                    except subprocess.CalledProcessError:
                        print(f"Error occurred while running benchmark")
                        exit(1)

    print("All benchmarks completed successfully!")