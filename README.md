# Worker

## Setup

### Prerequisites
```bash
pip install grpcio-tools grpcio
```

### Generate Protocol Buffer Files
```bash
./protos/generate_proto.sh
```

## Running the System

### 1. Generate Proto Files (First Time Only)
```bash
cd protos
pip install grpcio-tools
./generate_proto.sh
cd ..
```

### 2. Start Master Node
```bash
python master_node.py # in another repo
```

### 3. Start Worker Nodes
```bash
# Terminal 1 - First worker
python -m src.worker_app.main --port 50051
```

## Command Line Arguments

The worker node supports the following command-line arguments:

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--worker-id` | string | auto-generated | Unique identifier for this worker |
| `--port` | integer | 50051 | Port to listen on |
| `--master-address` | string | localhost:50050 | Address of the master node |
| `--max-jobs` | integer | 1 | Maximum number of concurrent jobs |
| `--log-level` | string | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `--cpu-cores` | integer | auto-detect | Number of CPU cores to use |
| `--memory-mb` | int | auto-detect | Amount of memory in MB to use |
| `--cache-size-mb` | int | 2048 | Data cache size limit in MB |

Example usage with custom arguments:
```bash
python -m src.worker_app.main \
    --worker-id worker_001 \
    --port 50051 \
    --master-address localhost:50050 \
    --max-jobs 2 \
    --log-level DEBUG \
    --cpu-cores 4 \
    --memory-mb 4096 \
    --cache-size-mb 1024
```


