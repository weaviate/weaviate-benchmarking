#!/bin/bash

set -e
set -x

if [ -d "../results" ]; then
    echo "Found results directory"
    TARGETDIR="/mnt/nas1/weaviate_benchmark_results/h5py/$(hostname)/$(echo $USER)/"
    mkdir -p $TARGETDIR
    TARGET="$USER@192.168.99.107:$TARGETDIR"
    rsync -azvdO --no-owner --no-group --no-perms ../results/ "$TARGET"
    exit 0
else
    echo "Error: Could not find results directory."
    exit 1
fi

