import h5py, weaviate, uuid, datetime, json, loguru, subprocess, os, re


def available_cpu_count():
    """ Number of available virtual or physical CPUs on this system, i.e.
    user/real as output by time(1) when called with an optimally scaling
    userspace-only program"""

    # cpuset
    # cpuset may restrict the number of *available* processors
    try:
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$',
                      open('/proc/self/status').read())
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                return res
    except IOError:
        pass

    # Python 2.6+
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        pass

    # https://github.com/giampaolo/psutil
    try:
        import psutil
        return psutil.cpu_count()   # psutil.NUM_CPUS on old versions
    except (ImportError, AttributeError):
        pass

    # POSIX
    try:
        res = int(os.sysconf('SC_NPROCESSORS_ONLN'))

        if res > 0:
            return res
    except (AttributeError, ValueError):
        pass

    # Windows
    try:
        res = int(os.environ['NUMBER_OF_PROCESSORS'])

        if res > 0:
            return res
    except (KeyError, ValueError):
        pass

    # jython
    try:
        from java.lang import Runtime
        runtime = Runtime.getRuntime()
        res = runtime.availableProcessors()
        if res > 0:
            return res
    except ImportError:
        pass

    # BSD
    try:
        sysctl = subprocess.Popen(['sysctl', '-n', 'hw.ncpu'],
                                  stdout=subprocess.PIPE)
        scStdout = sysctl.communicate()[0]
        res = int(scStdout)

        if res > 0:
            return res
    except (OSError, ValueError):
        pass

    # Linux
    try:
        res = open('/proc/cpuinfo').read().count('processor\t:')

        if res > 0:
            return res
    except IOError:
        pass

    # Solaris
    try:
        pseudoDevices = os.listdir('/devices/pseudo/')
        res = 0
        for pd in pseudoDevices:
            if re.match(r'^cpuid@[0-9]+$', pd):
                res += 1

        if res > 0:
            return res
    except OSError:
        pass

    # Other UNIXes (heuristic)
    try:
        try:
            dmesg = open('/var/run/dmesg.boot').read()
        except IOError:
            dmesgProcess = subprocess.Popen(['dmesg'], stdout=subprocess.PIPE)
            dmesg = dmesgProcess.communicate()[0]

        res = 0
        while '\ncpu' + str(res) + ':' in dmesg:
            res += 1

        if res > 0:
            return res
    except OSError:
        pass

    raise Exception('Can not determine number of CPUs on this system')


def add_batch(client, c, vector_len):
    start_time = datetime.datetime.now()
    results = client.batch.create_objects()
    stop_time = datetime.datetime.now()
    handle_results(results)
    run_time = stop_time - start_time
    if (c % 10000) == 0:
        loguru.logger.info('Import status => added ' + str(c) + ' of ' + str(vector_len) + ' objects')
    return run_time.seconds


def handle_results(results):
    if results is not None:
        for result in results:
            if 'result' in result and 'errors' in result['result'] and  'error' in result['result']['errors']:
                for message in result['result']['errors']['error']:
                    loguru.logger.info(message['message'])


def match_results(test_set, weaviate_result_set):

    # set score
    score = 0

    # return if no result
    if weaviate_result_set['data']['Get']['Benchmark'] == None:
        return score

    # create array from Weaviate result
    weaviate_result_array = []
    for weaviate_result in weaviate_result_set['data']['Get']['Benchmark']:
        weaviate_result_array.append(weaviate_result['counter'])

    # match scores
    for nn in test_set:
        if nn in weaviate_result_array:
            score += 1
    
    return score


def conduct_benchmark(weaviate_url, ef, client, benchmark_file, efConstruction, maxConnections):

    # result obj
    results = {
        'benchmarkFile': benchmark_file[0],
        'distanceMetric': benchmark_file[1],
        'totalTested': 0,
        'ef': ef,
        'efConstruction': efConstruction,
        'maxConnections': maxConnections,
        'score': {
            'highest': 0,
            'lowest': 100,
            'average': 0
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
    all_scores = []
    loguru.logger.info('Find neighbors with ef = ' + str(ef))
    with h5py.File('/var/hdf5/' + benchmark_file[0], 'r') as f:
        test_vectors = f['test']
        test_vectors_len = len(f['test'])
        for test_vector in test_vectors:

            # set certainty for  l2-squared
            nearVector = { "vector": test_vector.tolist() }
            
            # Start request
            query_result = client.query.get("Benchmark", ["counter"]).with_near_vector(nearVector).with_limit(100).do()    
            score = match_results(f['neighbors'][c], query_result)
            if score == 0:
                loguru.logger.info('There is a 0 score, this most likely means there is an issue with the dataset OR you have very low index settings. Found for vector: ' + str(test_vector[0]))
            all_scores.append(score)
            
            # set if high and low score
            if score > results['score']['highest']:
                results['score']['highest'] = score
            if score < results['score']['lowest']:
                results['score']['lowest'] = score

            # log ouput
            if (c % 1000) == 0:
                loguru.logger.info('Validated ' + str(c) + ' of ' + str(test_vectors_len))

            c+=1

    ##
    # Run the speed test
    ##
    loguru.logger.info('Run the speed test')
    with h5py.File('/var/hdf5/' + benchmark_file[0], 'r') as f:
        test_vectors_len = len(f['test'])
        vector_write_array = []
        for vector in f['test']:
            vector_write_array.append(vector.tolist())
        with open('queries.json', 'w', encoding='utf-8') as jf:
            json.dump(vector_write_array, jf, indent=2)
        process = subprocess.Popen(['./benchmarker','dataset', '-u', weaviate_url, '-c', 'Benchmark', '-q', 'queries.json', '-p', str(available_cpu_count()), '-f', 'json', '-l', '100'], stdout=subprocess.PIPE)
        result_raw = process.communicate()[0].decode('utf-8')
        results['requestTimes'] = json.loads(result_raw)

    # add final results
    results['totalTested'] = c
    results['totalDatasetSize'] = test_vectors_len
    results['score']['average'] = sum(all_scores) / len(all_scores)

    return results


def import_into_weaviate(client, efConstruction, maxConnections, benchmark_file):
    
    # variables
    benchmark_import_batch_size = 1000
    benchmark_class = 'Benchmark'
    import_time = 0

    # Delete schema if available
    current_schema = client.schema.get()
    if len(current_schema['classes']) > 0:
        client.schema.delete_all()

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
                    "description": "The number of the couter in the dataset",
                    "name": "counter"
                }
            ],
            "vectorIndexConfig": {
                "ef": -1,
                "efConstruction": efConstruction,
                "maxConnections": maxConnections,
                "vectorCacheMaxObjects": 10000000,
                "distance": benchmark_file[1]
            }
        }]
    }

    client.schema.create(schema)

    # Import
    loguru.logger.info('Start import process for ' + benchmark_file[0] + ', ef' + str(efConstruction) + ', maxConnections' + str(maxConnections))
    with h5py.File('/var/hdf5/' + benchmark_file[0], 'r') as f:
        vectors = f['train']
        c = 0
        batch_c = 0
        vector_len = len(vectors)
        for vector in vectors:
            client.batch.add_data_object({
                    'counter': c
                },
                'Benchmark',
                str(uuid.uuid3(uuid.NAMESPACE_DNS, str(c))),
                vector = vector
            )
            if batch_c == benchmark_import_batch_size:
                import_time += add_batch(client, c, vector_len)
                batch_c = 0
            c += 1
            batch_c += 1
        import_time += add_batch(client, c, vector_len)
    loguru.logger.info('done importing ' + str(c) + ' objects in ' + str(import_time) + ' seconds')

    return import_time


def run_the_benchmarks(weaviate_url, efConstruction_array, maxConnections_array, ef_array, benchmark_file_array):

    # Connect to Weaviate Weaviate
    try:
        client = weaviate.Client(weaviate_url)
    except:
        print('Error, can\'t connect to Weaviate, is it running?')
        exit(1)

    # itterate over settings
    for benchmark_file in benchmark_file_array:
        for efConstruction in efConstruction_array:
            for maxConnections in maxConnections_array:
               
                # import data
                import_time = import_into_weaviate(client, efConstruction, maxConnections, benchmark_file)

                # Find neighbors based on UUID and ef settings
                results = []
                for ef in ef_array:
                    result = conduct_benchmark(weaviate_url, ef, client, benchmark_file, efConstruction, maxConnections)
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