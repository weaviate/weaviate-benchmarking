import os
import gc
from typing import Sequence, Optional
import uuid
import json
import time
import subprocess
from math import ceil
from concurrent.futures import ProcessPoolExecutor, as_completed
import h5py
from weaviate import Client
from loguru import logger


class BenchmarkImportError(Exception):

    def __init__(self, counter: int):
        self.counter = counter


def add_batch(
        client: Client,
        counter: int,
        nr_vectors: int,
        process_num: int,
        start_index: int,
    ) -> None:
    """
    Submit Batch to Weaviate server.

    Parameters
    ----------
    client : Client
        Weaviate client object instance.
    counter : int
        The count of object already imported (at process level, not global).
    nr_vectors : int
        Number of vectors to needs to be imported (at process level, not global).
    process_num : int
        Process number, used for logging only. NOT the PID, just a counter to keep track of the
        process progress.
    start_index : int
        The global start index of the dataset for the current Process. The process is going to
        import the data in this interval [start_index: start_index + nr_vectors].
    """

    start_time = time.monotonic()
    results = client.batch.create_objects()
    stop_time = time.monotonic()
    handle_results(results)
    run_time = round(stop_time - start_time)

    logger.info(
        f'Import status (process {process_num}) => start_index {start_index}: '
        f'added {counter} of {nr_vectors} objects in {run_time} seconds'
    )


def handle_results(results: Optional[dict]) -> None:
    """
    Handle error message from batch requests logs the message as an info message.

    Parameters
    ----------
    results : Optional[dict]
        The returned results for Batch creation.
    """

    if results is not None:
        for result in results:
            if (
                'result' in result
                and 'errors' in result['result']
                and 'error' in result['result']['errors']
            ):
                for message in result['result']['errors']['error']:
                    logger.error(message['message'])


def match_results(test_set: Sequence, weaviate_result_set: dict, k: int):
    """
    Match the results from Weaviate to the benchmark data. If a result is in the returned set,
    score goes +1. Because there is checked for 100 neighbors a score of 100 == perfect.
    """

    # set score
    score = 0

    # return if no result
    if weaviate_result_set['data']['Get']['Benchmark'] == None:
        return score

    # create array from Weaviate result
    weaviate_result_array = []
    for weaviate_result in weaviate_result_set['data']['Get']['Benchmark'][:k]:
        weaviate_result_array.append(weaviate_result['counter'])

    # match scores
    for nn in test_set[:k]:
        if nn in weaviate_result_array:
            score += 1

    return score


def run_speed_test(l: int, CPUs:int, weaviate_url: str):
    """
    Runs the actual speed test in Go.
    """

    process = subprocess.Popen(
        ['./benchmarker','dataset', '-u', weaviate_url, '-c', 'Benchmark', '-q', 'queries.json', '-p', str(CPUs), '-f', 'json', '-l', str(l)],
        stdout=subprocess.PIPE,
    )
    result_raw = process.communicate()[0].decode('utf-8')
    return json.loads(result_raw)


def conduct_benchmark(
        weaviate_url: str,
        CPUs: int,
        ef: int,
        client: Client,
        benchmark_file: tuple,
        efConstruction: int,
        maxConnections: int,
    ):
    """
    Conducts the benchmark, note that the NN results and speed test run separately from each other.
    """

    # result obj
    results = {
        'benchmarkFile': benchmark_file[0],
        'distanceMetric': benchmark_file[1],
        'totalTested': 0,
        'ef': ef,
        'efConstruction': efConstruction,
        'maxConnections': maxConnections,
        'recall': {
            '100': {
                'highest': 0,
                'lowest': 100,
                'average': 0
            },
            '10': {
                'highest': 0,
                'lowest': 100,
                'average': 0
            },
            '1': {
                'highest': 0,
                'lowest': 100,
                'average': 0
            },
        },
        'requestTimes': {}
    }

    # update schema for ef setting
    logger.info('Update "ef" to ' + str(ef) + ' in schema')
    client.schema.update_config('Benchmark', { 'vectorIndexConfig': { 'ef': ef } })

    ##
    # Run the score test
    ##
    c = 0
    all_scores = {
        '100':[],
        '10':[],
        '1': [],
    }

    logger.info('Find neighbors with ef = ' + str(ef))
    with h5py.File('/var/hdf5/' + benchmark_file[0], 'r') as f:
        test_vectors = f['test']
        test_vectors_len = len(f['test'])
        for test_vector in test_vectors:

            # set certainty for  l2-squared
            nearVector = { "vector": test_vector.tolist() }
            
            # Start request
            query_result = client.query.get("Benchmark", ["counter"]).with_near_vector(nearVector).with_limit(100).do()    

            for k in [1, 10,100]:
                k_label=f'{k}'
                score = match_results(f['neighbors'][c], query_result, k)
                if score == 0:
                    logger.info('There is a 0 score, this most likely means there is an issue with the dataset OR you have very low index settings. Found for vector: ' + str(test_vector[0]))
                all_scores[k_label].append(score)
                
                # set if high and low score
                if score > results['recall'][k_label]['highest']:
                    results['recall'][k_label]['highest'] = score
                if score < results['recall'][k_label]['lowest']:
                    results['recall'][k_label]['lowest'] = score

            # log ouput
            if (c % 1000) == 0:
                logger.info('Validated ' + str(c) + ' of ' + str(test_vectors_len))

            c+=1

    ##
    # Run the speed test
    ##
    logger.info('Run the speed test')
    train_vectors_len = 0
    with h5py.File('/var/hdf5/' + benchmark_file[0], 'r') as f:
        train_vectors_len = len(f['train'])
        test_vectors_len = len(f['test'])
        vector_write_array = []
        for vector in f['test']:
            vector_write_array.append(vector.tolist())
        with open('queries.json', 'w', encoding='utf-8') as jf:
            json.dump(vector_write_array, jf, indent=2)
        results['requestTimes']['limit_1'] = run_speed_test(1, CPUs, weaviate_url)
        results['requestTimes']['limit_10'] = run_speed_test(10, CPUs, weaviate_url)
        results['requestTimes']['limit_100'] = run_speed_test(100, CPUs, weaviate_url)

    # add final results
    results['totalTested'] = c
    results['totalDatasetSize'] = train_vectors_len
    for k in ['1', '10', '100']:
        results['recall'][k]['average'] = sum(all_scores[k]) / len(all_scores[k])

    return results


def create_schema(
        client: Client,
        efConstruction: int,
        maxConnections: int,
        distance: str,
    ) -> None:
    """
    Create schema, if one exists it is going to be deleted (along with all the objects) and
    recreated.

    Parameters
    ----------
    client : Client
        Weaviate client instance.
    efConstruction : int
        Vector index configuration efConstruction value.
    maxConnections : int
        Vector index configuration maxConnections value.
    distance : str
        Vector index configuration distance to be used.
    """

    # Delete schema if available
    if client.schema.contains():
        try:
            client.schema.delete_all()
            # Sleeping to avoid load timeouts
        except:
            logger.exception(
                'Something is wrong with removing the class, sleep 4 minutes and try again'
            )
            time.sleep(240)
            client.schema.delete_all()

    # Create schema
    schema = {
        "classes": [{
            "class": 'Benchmark',
            "description": "A class for benchmarking purposes",
            "properties": [
                {
                    "dataType": [
                        "int"
                    ],
                    "description": "The number of the counter in the dataset",
                    "name": "counter"
                }
            ],
            "vectorIndexConfig": {
                "ef": -1,
                "efConstruction": efConstruction,
                "maxConnections": maxConnections,
                "vectorCacheMaxObjects": 1_000_000_000,
                "distance": distance,
            }
        }]
    }

    client.schema.create(schema)


def import_data_slice_to_weaviate(
        weaviate_url: str,
        batch_size: int,
        vectors: Sequence,
        process_num: int,
        start_index: int,
    ) -> int:
    """
    Import a slice of the dataset in a different Process. On exceptions during import saves the
    global counter (stop index) to a process specific file (to avoid writing to the same file at
    once).

    Parameters
    ----------
    weaviate_url : str
        Weaviate URL used to create new Client instance (this means that each process is going to
        have a separate requests.Session).
    batch_size : int
        Batch size.
    vectors : Sequence
        The vectors of the data to be imported.
    process_num : int
        Process number used for log messages and file name that contains the stop index in case of
        exceptions being raised.
    start_index : int
        The global start index of the dataset for the current Process. The process is going to
        import the data in this interval [start_index: start_index + len(vectors)].

    Raises
    ------
    Exception
        Re-raise the Exception. Contains the number of objects imported by this process and can be
        accessed like this: 'error.counter'.

    Returns
    -------
    int
        Number of objects imported.
    """
    
    counter = 0
    batch_counter = 0
    stop_index = start_index
    nr_vectors = len(vectors)
    logger.info(
        f'Start import sub-process {process_num}, vectors start index {start_index}'
    )
    try:
        client = Client(
            url=weaviate_url,
            timeout_config=(5, 60),
        )
        client.batch.configure(
            timeout_retries=10,
        )
        for vector in vectors:
            client.batch.add_data_object(
                data_object={'counter': counter + start_index},
                class_name='Benchmark',
                uuid=str(uuid.uuid3(uuid.NAMESPACE_DNS, str(counter + start_index))),
                vector=vector,
            )
            if batch_counter == batch_size:
                add_batch(
                    client=client, 
                    counter=counter,
                    nr_vectors=nr_vectors,
                    process_num=process_num,
                    start_index=start_index,
                )
                stop_index += batch_counter
                batch_counter = 0
            counter += 1
            batch_counter += 1
        add_batch(
            client=client, 
            counter=counter,
            nr_vectors=nr_vectors,
            process_num=process_num,
            start_index=start_index,
        )
    except Exception:
        logger.exception(
            f"sub-process {process_num}: Import failed at relative counter: {counter}, "
            f"global counter: {counter + start_index}"
        )
        with open(f'/var/logs/stop_index_process_{process_num}.txt', 'w') as file:
            file.write(str(counter + start_index))
        raise BenchmarkImportError(counter) from None

    del client
    del vectors
    gc.collect()
    return counter


def import_data_into_weaviate(
        batch_size: int,
        data_file: str,
        weaviate_url: str,
        nr_processes: int,
        nr_cores: int,
    ) -> int:
    """
    Import data into Weaviate. This is done using parallel Processes.

    Parameters
    ----------
    batch_size : int
        Batch size.
    data_file : str
        Data file name that contains the vectors.
    weaviate_url : str
        Weaviate URL.
    nr_processes : int
        Number of processes to create. Processes are not going to be created all at once,
        they are going to be created in a batch of `nr_cores`. The data is split into
        `nr_processes`, this allows to have less data in Memory. If the scrip fails due
        to OOM increase this value.
    nr_cores : int
        Number of cores the machine that runs this scrip has. This is used to create batches
        of processes to run in parallel.

    Returns
    -------
    int
        Duration of the import in seconds.
    """
    
    import_failed = False
    total_objects_imported = 0

    # Import
    start_time = time.monotonic()
    for proc_batch in range(ceil(nr_processes/nr_cores)):
        with h5py.File(f'/var/hdf5/{data_file}', 'r') as f:
            nr_vectors = f['train'].shape[0]
            nr_vectors_per_core = int(nr_vectors/nr_processes)

            start_indexes = [nr_vectors_per_core * i for i in range(nr_processes)]
            end_indexes = start_indexes[1:].copy()
            end_indexes.append(-1)

            # if scrip fails and you want to resume, changes the start_indexes
            # after this comment to the desired values
            # start_indexes = []

            with ProcessPoolExecutor() as executor:
                results = []
                for i in range(nr_cores):
                    current_index = i + proc_batch * nr_cores
                    if current_index == nr_vectors:
                        break
                    results.append(
                        executor.submit(
                            import_data_slice_to_weaviate,
                            weaviate_url=weaviate_url,
                            batch_size=batch_size,
                            vectors=f['train'][start_indexes[current_index]:end_indexes[current_index]],
                            subprocess_number=current_index,
                            data_start_index=start_indexes[current_index]
                        )
                    )
                for f in as_completed(results):
                    try:
                        total_objects_imported += f.result()
                    except BenchmarkImportError as error:
                        total_objects_imported += error.counter
                        import_failed = True
        gc.collect()
    end_time = time.monotonic()

    if import_failed:
        logger.error(
            f"Import failed. Total objects imported: {total_objects_imported} in {import_time} seconds"
        )
        exit(1)
    
    import_time = round(end_time - start_time)
    logger.info(
        f'done importing {total_objects_imported} objects in {import_time} seconds'
    )
    return import_time


def run_the_benchmarks(
        weaviate_url: str,
        CPUs: int,
        efConstruction_array: list,
        maxConnections_array: list,
        ef_array: list,
        benchmark_file_array: list,
    ):
    """Runs the actual benchmark.
       Results are stored in a JSON file"""

    # Connect to Weaviate Weaviate
    try:
        # if weaviate is running in the same docker-compose.yml then this function is going to
        # create a Client faster than Weaviate is ready, so we sleep 10 seconds
        time.sleep(10)
        client = Client(weaviate_url, timeout_config=(5, 60))
    except Exception:
        print('Error, can\'t connect to Weaviate, is it running?')
        print('Retrying to connect in 30 seconds.')
        time.sleep(30)
        try:
            client = Client(weaviate_url, timeout_config=(5, 60))
        except Exception:
            print('Error, can\'t connect to Weaviate, is it running? Exiting ...')
            exit(1)

    # iterate over settings
    for benchmark_file in benchmark_file_array:
        for efConstruction in efConstruction_array:
            for maxConnections in maxConnections_array:

                create_schema(
                    client=client,
                    efConstruction=efConstruction,
                    maxConnections=maxConnections,
                    distance=benchmark_file[1],
                )
                logger.info(
                    f"Start import process for {benchmark_file[0]}, ef: {efConstruction}, "
                    f"maxConnections: {maxConnections}, CPUs: {CPUs}"
                )
                # import data
                import_time = import_data_into_weaviate(
                    batch_size=10_000,
                    data_file=benchmark_file[0],
                    weaviate_url=weaviate_url,
                    nr_processes=1_000,
                    nr_cores=CPUs,
                )

                # Find neighbors based on UUID and ef settings
                results = []
                for ef in ef_array:
                    result = conduct_benchmark(weaviate_url, CPUs, ef, client, benchmark_file, efConstruction, maxConnections)
                    result['importTime'] = import_time
                    results.append(result)
                
                # write json file
                if not os.path.exists('results'):
                    os.makedirs('results')
                output_json = f'results/weaviate_benchmark__{benchmark_file[0]}__{efConstruction}__{maxConnections}.json'
                logger.info('Writing JSON file with results to: ' + output_json)
                with open(output_json, 'w') as outfile:
                    json.dump(results, outfile)

    logger.info('completed')
