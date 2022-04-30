import weaviate, uuid, re, datetime, json, loguru


def create_schema(client):

    # Delete schema if available
    current_schema = client.schema.get()
    if len(current_schema['classes']) > 0:
        client.schema.delete_all()

    # Create schema
    schema = {
        "classes": [{
            "class": "MediaType",
            "description": "A media type for benchmarking purposes",
            "vectorizer": "none",
            "vectorIndexConfig": {
                "skip": True
            },
            "properties": [
                {
                    "dataType": [
                        "string"
                    ],
                    "description": "Media type of news source for benchmarking purposes",
                    "name": "name",
                    "indexInverted": True
                }, {
                    "dataType": [
                        "Article"
                    ],
                    "description": "Articles this news type has for benchmarking purposes",
                    "name": "hasArticles",
                    "indexInverted": True
                }
            ]
        }, {
            "class": "Article",
            "description": "A news article for benchmarking purposes",
            "vectorizer": "none",
            "vectorIndexConfig": {
                "skip": True
            },
            "properties": [
                {
                    "dataType": [
                        "string"
                    ],
                    "description": "Title of news article for benchmarking purposes",
                    "name": "title",
                    "indexInverted": True
                },
                {
                    "dataType": [
                        "text"
                    ],
                    "description": "Content of news article for benchmarking purposes",
                    "name": "content",
                    "indexInverted": True
                },
                {
                    "dataType": [
                        "int"
                    ],
                    "description": "Count of words in news article for benchmarking purposes",
                    "name": "wordCount",
                    "indexInverted": True
                },
                {
                    "dataType": [
                        "date"
                    ],
                    "description": "Date of news article for benchmarking purposes",
                    "name": "published",
                    "indexInverted": True
                },
                {
                    "dataType": [
                        "mediaType"
                    ],
                    "description": "media type of news article for benchmarking purposes",
                    "name": "ofMediaType",
                    "indexInverted": True
                },
                {
                    "dataType": [
                        "string"
                    ],
                    "description": "source of news article for benchmarking purposes",
                    "name": "source",
                    "indexInverted": True
                }
            ]
        }]
    }
    client.schema.create(schema)


def handle_results(results):
    if results is not None:
        for result in results:
            if 'result' in result and 'errors' in result['result'] and  'error' in result['result']['errors']:
                for message in result['result']['errors']['error']:
                    loguru.logger.info(message['message'])


def add_batch(client):
    start_time = datetime.datetime.now()
    results = client.batch.create_objects()
    stop_time = datetime.datetime.now()
    handle_results(results)
    run_time = stop_time - start_time
    return run_time.seconds


def create_news_type(client, news_type):

    news_type_uuid = str(uuid.uuid3(uuid.NAMESPACE_DNS, news_type))

    client.data_object.create({
        "name": news_type,
    }, "MediaType", news_type_uuid)

    return news_type_uuid


def cross_ref_data(client, benchmark_file):
    file = open('/var/ii/' + benchmark_file, 'r')
    benchmark_file_lines = file.readlines()
    for benchmark_file_line in benchmark_file_lines:
        news_obj = json.loads(benchmark_file_line)
        with client.batch as batch:
            client.batch.add_reference(str(uuid.uuid3(uuid.NAMESPACE_DNS, news_obj['media-type'])), "MediaType", "hasArticles", news_obj['id'])

def import_data(client, benchmark_file):

    news_type_array = []
    batch_c = 0
    import_time = 0
    c = 0

    file = open('/var/ii/' + benchmark_file, 'r')
    benchmark_file_lines = file.readlines()
    for benchmark_file_line in benchmark_file_lines:
        news_obj = json.loads(benchmark_file_line)

        # get news type
        if news_obj['media-type'] not in news_type_array:
            create_news_type(client, news_obj['media-type'])
            news_type_array.append(news_obj['media-type'])
        news_type = news_obj['media-type']

        # get news ID
        news_id = news_obj['id']

        # get news source
        news_source = news_obj['source']

        # get publish date
        news_published = news_obj['published']

        # get title
        news_title = news_obj['title']

        # get content
        news_content = news_obj['content'].replace(' \n', ' ').replace('\n', '')
        news_content = re.sub(' +', ' ', news_content)

        # add to batch
        client.batch.add_data_object({
                'title': news_title,
                'content': news_content,
                'published': news_published,
                'source': news_source,
                'wordCount': len(news_content.split()),
                'ofMediaType': [{
                    'beacon': 'weaviate://localhost/' + str(uuid.uuid3(uuid.NAMESPACE_DNS, news_type))
                }]
            },
            'Article',
            news_id
        )

        batch_c += 1
        c += 1

        if batch_c == 10000:
            import_time += add_batch(client)
            batch_c = 0
            loguru.logger.info('Added ' + str(c) + ' objects')

    return {
        'importTime': import_time,
        'dataObjects': c
    }


if __name__ == '__main__':

    # variables
    benchmark_file_array = ['signalmedia-1m.jsonl']
 
    # Connect to Weaviate Weaviate
    try:
        client = weaviate.Client("http://weaviate:8080")
    except:
        print('Error, can\'t connect to Weaviate, is it running?')
        exit(1)

    # Create the schema
    create_schema(client)

    # Import the data
    for benchmark_file in benchmark_file_array:
        results =  import_data(client, benchmark_file)
        cross_ref_data(client, benchmark_file)

    # write json file
    output_json = 'weaviate_benchmark' + '__' + benchmark_file + '__inverted_index.json'
    loguru.logger.info('Writing JSON file with results to: ' + output_json)
    with open(output_json, 'w') as outfile:
        json.dump(results, outfile)

    loguru.logger.info('completed')
