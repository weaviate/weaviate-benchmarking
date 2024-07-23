#!/bin/bash

set -eou pipefail

echo "Run benchmark script"
/app/benchmarker ann-benchmark \
  -v /app/datasets/${DATASET}.hdf5 \
  --distance $DISTANCE \
  --indexType $INDEX_TYPE \
  --updatePercentage $UPDATE_PERCENTAGE \
  --cleanupIntervalSeconds $CLEANUP_INTERVAL_SECONDS \
  --updateIterations $UPDATE_ITERATIONS \
  --grpcOrigin "${WEAVIATE_URL}:50051" \
  --httpOrigin "${WEAVIATE_URL}:8080" \
  --updateRandomized


echo "Run complete, now analyze the results"
python3 /app/scripts/python/update_stability.py

echo "Passed!"