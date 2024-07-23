#!/usr/bin/env python3

import argparse
import os
import glob
import json

def collate_results(dataset, results_directory):
    """Collate results from the results directory into a markdown table."""
    json_files_pattern = os.path.join(results_directory, "*.json")

    data = []

    for file_path in glob.glob(json_files_pattern):
        with open(file_path, 'r') as file:
            file_data = json.load(file)
            data.extend(file_data)

    filtered_data = [entry for entry in data if entry['dataset_file'] == dataset]
    sorted_data = sorted(filtered_data, key=lambda x: x['qps'], reverse=True)

    print("""| efConstruction | maxConnections | ef | **Recall** | **QPS** | Mean Latency | p99 Latency | Import time |
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |""")
    for entry in sorted_data:
        latencyms = "{:.2f}ms".format(entry['meanLatency'] * 1000)
        p99ms = "{:.2f}ms".format(entry['p99Latency'] * 1000)
        recallfmt = "{:.2f}%".format(entry['recall'] * 100)
        importtimefmt = "{:.2f}s".format(entry['importTime'])
        qpsfmt = "{:.0f}".format(entry['qps'])
        print(f"| {entry['efConstruction']} | {entry['maxConnections']} | {entry['ef']} | **{recallfmt}** | **{qpsfmt}** | {latencyms} | {p99ms} | {importtimefmt} |")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collate ann results into markdown tables.")
    parser.add_argument('-d', '--dataset', required=True, help="The dataset file to filter by.")
    parser.add_argument('-r', '--results', default="./results", help="The directory containing benchmark results")
    args = parser.parse_args()

    collate_results(args.dataset, os.path.expanduser(args.results))
