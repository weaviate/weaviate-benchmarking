#!/bin/bash

set -e
set -x

SOURCEDIR="/mnt/nas1/weaviate_benchmark_results/h5py"
SOURCE="$USER@192.168.99.107:$SOURCEDIR"
mkdir -p ./results
rsync -azvdO --no-owner --no-group --no-perms "$SOURCE" ./results/

