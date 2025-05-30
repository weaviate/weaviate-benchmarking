# Memory Monitoring Feature

This document describes the new memory monitoring functionality added to the Weaviate benchmarker.

## Overview

The memory monitoring feature allows you to continuously record memory metrics during the execution of benchmarks. It runs in a background thread and records memory statistics at regular intervals, writing the results to a JSON file for later analysis.

## Features

- **Continuous Monitoring**: Records memory metrics every N seconds (configurable)
- **Background Execution**: Runs in a separate goroutine without affecting benchmark performance
- **Thread-Safe**: Uses proper synchronization to avoid race conditions
- **Automatic File Output**: Saves results to JSON file in the `results/` directory
- **Configurable Interval**: Customize how frequently metrics are recorded
- **Custom Filename**: Specify your own output filename or use auto-generated names

## Configuration Options

### Command Line Flags

- `--memoryMonitoringEnabled`: Enable continuous memory monitoring (default: false)
- `--memoryMonitoringInterval`: Memory monitoring interval in seconds (default: 5)
- `--memoryMonitoringFile`: Memory monitoring output file name (default: memory_metrics_<timestamp>.json)

### Examples

```bash
# Enable memory monitoring with default settings (5-second intervals)
./weaviate-benchmarker ann-benchmark \
  --memoryMonitoringEnabled \
  --vectors dataset.hdf5 \
  --distance cosine

# Enable memory monitoring with custom interval and filename
./weaviate-benchmarker ann-benchmark \
  --memoryMonitoringEnabled \
  --memoryMonitoringInterval 2 \
  --memoryMonitoringFile "my_memory_metrics.json" \
  --vectors dataset.hdf5 \
  --distance cosine

# Enable memory monitoring for a long-running benchmark
./weaviate-benchmarker ann-benchmark \
  --memoryMonitoringEnabled \
  --memoryMonitoringInterval 10 \
  --vectors large_dataset.hdf5 \
  --distance cosine \
  --queryDuration 300
```

## Output Format

The memory monitoring feature outputs a JSON file containing an array of memory metric entries. Each entry includes:

- `timestamp`: ISO 8601 formatted timestamp
- `heap_alloc_bytes`: Currently allocated heap memory in bytes
- `heap_inuse_bytes`: Heap memory currently in use in bytes  
- `heap_sys_bytes`: Total heap memory obtained from the OS in bytes

### Example Output

```json
[
    {
        "timestamp": "2024-01-15T10:30:00Z",
        "heap_alloc_bytes": 134217728,
        "heap_inuse_bytes": 167772160,
        "heap_sys_bytes": 201326592
    },
    {
        "timestamp": "2024-01-15T10:30:05Z",
        "heap_alloc_bytes": 142606336,
        "heap_inuse_bytes": 175161344,
        "heap_sys_bytes": 201326592
    }
]
```

## Implementation Details

### Architecture

The memory monitoring system consists of several components:

1. **MemoryMonitor**: Main struct that manages the monitoring process
2. **Background Goroutine**: Runs the monitoring loop in a separate thread
3. **Metrics Collection**: Uses the existing `readMemoryMetrics()` function
4. **File Output**: Writes results to JSON file in the `results/` directory

### Thread Safety

- Uses `sync.Mutex` to protect shared data structures
- Proper goroutine lifecycle management with `sync.WaitGroup`
- Context-based cancellation for clean shutdown

### Performance Impact

- Minimal performance impact on benchmarks
- Configurable interval allows balancing detail vs. overhead
- Background execution doesn't block main benchmark operations


## Use Cases

### Performance Analysis
Monitor memory usage patterns during different phases of benchmarking:
- Data loading phase
- Index building phase  
- Query execution phase
- Update operations

### Memory Leak Detection
Track memory usage over time to identify potential memory leaks:
```bash
./weaviate-benchmarker ann-benchmark \
  --memoryMonitoringEnabled \
  --memoryMonitoringInterval 1 \
  --queryDuration 600 \
  --vectors dataset.hdf5 \
  --distance cosine
```

### Resource Planning
Understand memory requirements for different dataset sizes and configurations:
```bash
./weaviate-benchmarker ann-benchmark \
  --memoryMonitoringEnabled \
  --memoryMonitoringInterval 5 \
  --vectors large_dataset.hdf5 \
  --distance cosine \
  --shards 4
```

## Command Line Analysis
```bash
# Extract peak memory usage
jq 'max_by(.heap_alloc_bytes) | .heap_alloc_bytes / 1024 / 1024' results/memory_metrics_*.json

# Calculate average memory usage
jq '[.[].heap_alloc_bytes] | add / length / 1024 / 1024' results/memory_metrics_*.json

# Find memory usage at specific time
jq '.[] | select(.timestamp | startswith("2024-01-15T10:30"))' results/memory_metrics_*.json
```