# MongoDB to Google Cloud Storage Pipeline

A high-performance pipeline for transferring massive datasets (41+ million records) from MongoDB to Google Cloud Storage with parallel processing, progress tracking, and error handling.

## Features

- **Efficient Data Transfer**: Optimized for massive datasets with chunked processing
- **Parallel Processing**: Multi-worker architecture for maximum throughput
- **Progress Tracking**: Redis-based progress monitoring across distributed processes
- **Error Handling**: Robust error handling with retry mechanisms
- **Memory Management**: Built-in memory usage monitoring and limits
- **Flexible Output**: Support for Parquet and JSONL formats
- **Resumable**: Can resume from failed chunks using Redis tracking

## Architecture

```
main.py                 # Main orchestrator
src/
├── __init__.py        # Package initialization
├── progress.py        # Configuration and progress tracking
├── mongodb_exporter.py # MongoDB data extraction
└── gcs_uploader.py    # Google Cloud Storage upload
```

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Set up Google Cloud authentication**:
   ```bash
   gcloud auth application-default login
   # Or set GOOGLE_APPLICATION_CREDENTIALS
   ```

4. **Start Redis** (for progress tracking):
   ```bash
   redis-server
   ```

5. **Test MongoDB connection** (optional but recommended):
   ```bash
   python test_connection.py
   ```

6. **Run the pipeline**:
   ```bash
   python main.py
   ```

## Configuration

Key environment variables in `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `MONGO_USERNAME` | MongoDB username (alternative to URI auth) | `""` |
| `MONGO_PASSWORD` | MongoDB password (alternative to URI auth) | `""` |
| `MONGO_AUTH_SOURCE` | MongoDB auth database | `admin` |
| `MONGO_DB` | Database name | `your_database` |
| `MONGO_COLLECTION` | Collection name | `your_collection` |
| `GCP_PROJECT_ID` | Google Cloud Project ID | `your-project-id` |
| `GCS_BUCKET` | GCS bucket name | `your-gcs-bucket` |
| `GCS_PREFIX` | Object prefix in GCS | `mongodb_export/` |
| `OUTPUT_FORMAT` | Output format (parquet/jsonl) | `parquet` |
| `CHUNK_SIZE` | Records per chunk | `100000` |
| `MAX_WORKERS` | Parallel workers | `4` |
| `MAX_MEMORY_USAGE_GB` | Memory limit per worker | `8.0` |

### MongoDB Authentication

The pipeline supports two authentication methods:

**Method 1: URI-based authentication (recommended)**
```bash
MONGO_URI=mongodb://username:password@localhost:27017/your_database?authSource=admin
```

**Method 2: Separate credential fields**
```bash
MONGO_URI=mongodb://localhost:27017/
MONGO_USERNAME=your_username
MONGO_PASSWORD=your_password
MONGO_AUTH_SOURCE=admin
```

For admin database authentication, ensure `authSource=admin` is specified in the URI or set `MONGO_AUTH_SOURCE=admin`.

## Monitoring Progress

Use the included `monitor.py` script to track progress:

```bash
python monitor.py
```

This will show real-time statistics including:
- Total/completed/failed chunks
- Processing rate
- Data transfer rates
- ETA for completion

## Performance Tuning

For optimal performance with 41 trillion records:

1. **Chunk Size**: Start with 100K records per chunk, adjust based on memory usage
2. **Workers**: Set to number of CPU cores, but consider I/O limits
3. **Memory**: Monitor memory usage and adjust `MAX_MEMORY_USAGE_GB`
4. **Network**: Ensure sufficient bandwidth to GCS
5. **MongoDB**: Use read preferences and connection pooling
6. **Redis**: Use a dedicated Redis instance for large-scale jobs

## Error Handling

The pipeline includes robust error handling:

- **Chunk-level retries**: Failed chunks are retried automatically
- **Progress persistence**: Redis tracks progress across restarts
- **Memory monitoring**: Prevents OOM with configurable limits
- **Connection management**: Handles MongoDB connection issues gracefully

## Resuming Failed Jobs

If the pipeline fails or is interrupted:

1. Check Redis for job progress: `python monitor.py`
2. Restart the pipeline: `python main.py`
3. The pipeline will automatically skip completed chunks

## Requirements

- Python 3.8+
- MongoDB instance with read access
- Google Cloud Storage bucket with write access
- Redis instance for progress tracking
- Sufficient disk space for temporary files

## File Formats

### Parquet (Recommended)
- Better compression and query performance
- Schema evolution support
- Optimized for analytics workloads

### JSONL
- Human-readable format
- Maintains original MongoDB document structure
- Easier debugging and inspection
