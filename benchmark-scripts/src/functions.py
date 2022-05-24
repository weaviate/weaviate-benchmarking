import os
from typing import Sequence
import uuid
import json
import time
import datetime
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import h5py
from weaviate import Client
import loguru


def add_batch(client: Client, count: int, vector_len: int, subprocess_num: int, start_index: int):
    '''Adds batch to Weaviate and returns
       the time it took to complete in seconds.'''

    start_time = datetime.datetime.now()
    results = client.batch.create_objects()
    stop_time = datetime.datetime.now()
    handle_results(results)
    run_time = stop_time - start_time

    loguru.logger.info(
        f'Import status (sub-process {subprocess_num}) => start_index {start_index}: added {count} of {vector_len} objects in {run_time.seconds} seconds'
    )


def handle_results(results):
    '''Handle error message from batch requests
       logs the message as an info message.'''
    if results is not None:
        for result in results:
            if 'result' in result and 'errors' in result['result'] and  'error' in result['result']['errors']:
                for message in result['result']['errors']['error']:
                    loguru.logger.error(message['message'])


def match_results(test_set: Sequence, weaviate_result_set: dict, k: int):
    '''Match the results from Weaviate to the benchmark data.
       If a result is in the returned set, score goes +1.
       Because there is checked for 100 neighbors a score
       of 100 == perfect'''

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
    '''Runs the actual speed test in Go'''
    process = subprocess.Popen(['./benchmarker','dataset', '-u', weaviate_url, '-c', 'Benchmark', '-q', 'queries.json', '-p', str(CPUs), '-f', 'json', '-l', str(l)], stdout=subprocess.PIPE)
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
    '''Conducts the benchmark, note that the NN results
       and speed test run seperatly from each other'''

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
    loguru.logger.info('Update "ef" to ' + str(ef) + ' in schema')
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

    loguru.logger.info('Find neighbors with ef = ' + str(ef))
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
                    loguru.logger.info('There is a 0 score, this most likely means there is an issue with the dataset OR you have very low index settings. Found for vector: ' + str(test_vector[0]))
                all_scores[k_label].append(score)
                
                # set if high and low score
                if score > results['recall'][k_label]['highest']:
                    results['recall'][k_label]['highest'] = score
                if score < results['recall'][k_label]['lowest']:
                    results['recall'][k_label]['lowest'] = score

            # log ouput
            if (c % 1000) == 0:
                loguru.logger.info('Validated ' + str(c) + ' of ' + str(test_vectors_len))

            c+=1

    ##
    # Run the speed test
    ##
    loguru.logger.info('Run the speed test')
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


def remove_weaviate_class(client: Client):
    '''Removes the main class and tries again on error'''
    try:
        client.schema.delete_all()
        # Sleeping to avoid load timeouts
    except:
        loguru.logger.exception('Something is wrong with removing the class, sleep and try again')
        time.sleep(240)
        remove_weaviate_class(client)


def import_data_slice_to_weaviate(
        weaviate_url: str,
        batch_size: int,
        vectors: Sequence,
        subprocess_number: int,
        data_start_index: int,
    ):
    """
    Import a slice of the dataset in a different process (core).
    On exceptions during import saves the global counter to a sub-process specific
    file (to avoid writing to the same file at once).
    """
    
    try:
        client = Client(weaviate_url, timeout_config=(5, 60))
        client.batch.configure(
            timeout_retries=10,
        )
    except Exception as error:
        loguru.logger.exception(
            f"sub-process {subprocess_number}: Can't connect to Weaviate, is it running?"
        )
        raise error
    
    counter = 0
    batch_c = 0
    vector_len = len(vectors)
    loguru.logger.info(
        f'Start import sub-process {subprocess_number}, vectors start index {data_start_index}'
    )
    try:
        for vector in vectors:
            client.batch.add_data_object(
                data_object={'counter': counter + data_start_index},
                class_name='Benchmark',
                uuid=str(uuid.uuid3(uuid.NAMESPACE_DNS, str(counter + data_start_index))),
                vector = vector
            )
            if batch_c == batch_size:
                add_batch(client, counter, vector_len, subprocess_number, data_start_index)
                batch_c = 0
            counter += 1
            batch_c += 1
        add_batch(client, counter, vector_len, subprocess_number, data_start_index)
    except Exception as error:
        loguru.logger.exception(
            f"sub-process {subprocess_number}: Import failed at relative counter: {counter}, global counter: {counter + data_start_index}"
        )
        with open(f'/var/logs/stop_counter_subprocess_{subprocess_number}.txt', 'w') as file:
            file.write(str(counter + data_start_index))
        raise error


def import_into_weaviate(
        client: Client,
        efConstruction: int,
        maxConnections: int,
        benchmark_file: tuple,
        weaviate_url: str,
        nr_cores: int,
    ):
    '''Imports the data into Weaviate'''
    
    # variables
    benchmark_import_batch_size = 10_000
    benchmark_class = 'Benchmark'
    import_time = 0

    # Delete schema if available
    current_schema = client.schema.get()
    if len(current_schema['classes']) > 0:
        remove_weaviate_class(client)

    # Create schema
    schema = {
        "classes": [{
            "class": benchmark_class,
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
                "vectorCacheMaxObjects": 1000000000,
                "distance": benchmark_file[1]
            }
        }]
    }

    client.schema.create(schema)

    import_failed = False

    # Import
    loguru.logger.info(
        f'Start import process for {benchmark_file[0]}, ef {efConstruction}, maxConnections {maxConnections}'
    )
    start_time = time.monotonic()
    with h5py.File('/var/hdf5/' + benchmark_file[0], 'r') as f:
        data_to_import = f['train']
        nr_vectors_per_core = int(len(data_to_import)/nr_cores)

        start_indexes = [nr_vectors_per_core * i for i in range(nr_cores)]
        end_indexes = start_indexes[1:].copy()
        end_indexes.append(-1)

        # if scrip fails and you want to resume, changes the start_indexes after this comment to the desired values
        # start_indexes = []

        with ProcessPoolExecutor() as executor:
            results = []
            for i in range(nr_cores):
                results.append(
                    executor.submit(
                        import_data_slice_to_weaviate,
                        weaviate_url=weaviate_url,
                        batch_size=benchmark_import_batch_size,
                        vectors=data_to_import[start_indexes[i]:end_indexes[i]],
                        subprocess_number=i,
                        data_start_index=start_indexes[i]
                    )
                )
            for f in as_completed(results):
                try:
                    f.result()
                except Exception:
                    loguru.logger.exception(
                        'Something went wrong!'
                    )
                    import_failed = True
    
    end_time = time.monotonic()
    if import_failed:
        loguru.logger.error('Some import sub-processes failed! Check logs!')
        exit(1)
    
    loguru.logger.info('done importing ' + str(c) + ' objects in ' + str(end_time - start_time) + ' seconds')

    return import_time


def run_the_benchmarks(
        weaviate_url: str,
        CPUs: int,
        efConstruction_array: list,
        maxConnections_array: list,
        ef_array: list,
        benchmark_file_array: list,
    ):
    '''Runs the actual benchmark.
       Results are stored in a JSON file'''

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

    client.batch.configure(
        timeout_retries=10,
    )

    # iterate over settings
    for benchmark_file in benchmark_file_array:
        for efConstruction in efConstruction_array:
            for maxConnections in maxConnections_array:
               
                # import data
                import_time = import_into_weaviate(
                    client=client,
                    efConstruction=efConstruction,
                    maxConnections=maxConnections,
                    benchmark_file=benchmark_file,
                    weaviate_url=weaviate_url,
                    nr_cores=int(CPUs/2),
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
                output_json = 'results/weaviate_benchmark' + '__' + benchmark_file[0] + '__' + str(efConstruction) + '__' + str(maxConnections) + '.json'
                loguru.logger.info('Writing JSON file with results to: ' + output_json)
                with open(output_json, 'w') as outfile:
                    json.dump(results, outfile)

    loguru.logger.info('completed')
