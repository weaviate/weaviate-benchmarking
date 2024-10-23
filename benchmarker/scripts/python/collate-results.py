#!/usr/bin/env python3

import argparse
import os
import glob
import json
from typing import List, Dict, Any


INSTANCE_TYPE = 'n4-highmem-16'
RUN = "hnsw"
IGNORE_FIRST_TEST = True
EF_VALS = [64]

def get_all_data_as_dict(results_directory: str) -> List[Dict[str,Any]]:
    data = []

    for file_path in glob.glob(results_directory + "/*.json"):
        with open(file_path, 'r') as file:
            file_data = json.load(file)[(IGNORE_FIRST_TEST == True):]
            data.extend(file_data)

    return data


def filter_data(data: List[Dict[str,Any]], dataset: str, limit: int, sorted_by: str="qps") -> List[Dict[str,Any]]:
    def _filter_data(item: Dict) -> bool:
        if (
            item['dataset_file'] == dataset 
            and item['limit'] == limit
            and item['instance_type'] == INSTANCE_TYPE
            and item['run'] == RUN
            and item['ef'] in EF_VALS
        ):
            return True
        return False

    return sorted(
        [entry for entry in data if _filter_data(entry)],
        key=lambda x: x[sorted_by],
        reverse=True,
    )


def collate_results(data: List[Dict[str,Any]], out=None):
    """Collate results from the results directory into a markdown table."""

    print("| efConstruction | maxConnections | ef | **Recall** | **QPS** | Mean Latency | p99 Latency | Import time |", file=out)
    print("| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |", file=out)
    for entry in data:
        latencyms = "{:.2f}ms".format(entry['meanLatency'] * 1000)
        p99ms = "{:.2f}ms".format(entry['p99Latency'] * 1000)
        recallfmt = "{:.2f}%".format(entry['recall'] * 100)
        importtimefmt = "{:.0f}s".format(entry['importTime'])
        qpsfmt = "{:.0f}".format(entry['qps'])
        print(f"| {entry['efConstruction']} | {entry['maxConnections']} | {entry['ef']} | **{recallfmt}** | **{qpsfmt}** | {latencyms} | {p99ms} | {importtimefmt} |", file=out)


def weaviate_io_results(results_directory: str):

    data = get_all_data_as_dict(results_directory)
    datasets = set()
    limits = set()

    for item in data:
        datasets.add(item["dataset_file"])
        limits.add(item["limit"])

    for dataset in datasets:
        with open(f"ann-{dataset.replace(".hdf5", ".mdx")}", mode="w") as file:
            print("import Tabs from '@theme/Tabs';", file=file)
            print("import TabItem from '@theme/TabItem';\n", file=file)
            print('<Tabs groupId="limits">', file=file)
            for limit in limits:
                print(f'<TabItem value="{limit}" label="Limit {limit}">\n', file=file)
                collate_results(
                    data=filter_data(data, dataset, limit),
                    out=file,
                )
                print('\n</TabItem>', file=file)
            print('</Tabs>', file=file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collate ann results into markdown tables.")
    parser.add_argument('-d', '--dataset', default="all", type=str, help="The dataset file to filter by. If the value is 'all' or not specified, it will be computed for all datasets.")
    parser.add_argument('-r', '--results', default="./results", help="The directory containing benchmark results")
    parser.add_argument('-l', '--limit', default=10, type=int, help="The number of results returned by the ANN to fiter by")
    args = parser.parse_args()

    if args.dataset != "all":
        filtered_data = filter_data(
            data=get_all_data_as_dict(args.results),
            dataset=args.dataset,
            limit=args.limit,
        )

        collate_results(filtered_data)
    else:
        weaviate_io_results(args.results)
