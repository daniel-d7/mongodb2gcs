#!/usr/bin/env python3
"""
MongoDB to Google Cloud Storage Pipeline
========================================

Handles the transfer of massive datasets from MongoDB to Google Cloud Storage with:
- Efficient chunking and parallel processing
- Progress tracking and monitoring
- Error handling and recovery mechanisms
- Memory-efficient streaming
"""

import os
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from pathlib import Path

# Third-party imports
import pymongo
from pymongo import MongoClient
from pymongo.cursor import Cursor
from google.cloud import storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
import orjson  # Faster JSON serialization
from tqdm import tqdm
import psutil
import redis  # For progress tracking across distributed processes


@dataclass
class Config:
    """Configuration class for the data pipeline"""
    # MongoDB settings
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    mongo_db: str = os.getenv("MONGO_DB", "your_database")
    mongo_collection: str = os.getenv("MONGO_COLLECTION", "your_collection")
    
    # MongoDB authentication (alternative to URI-based auth)
    mongo_username: str = os.getenv("MONGO_USERNAME", "")
    mongo_password: str = os.getenv("MONGO_PASSWORD", "")
    mongo_auth_source: str = os.getenv("MONGO_AUTH_SOURCE", "admin")
    
    # GCS settings
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "your-project-id")
    gcs_bucket: str = os.getenv("GCS_BUCKET", "your-gcs-bucket")
    gcs_prefix: str = os.getenv("GCS_PREFIX", "mongodb_export/")
    
    # Processing settings
    chunk_size: int = int(os.getenv("CHUNK_SIZE", "100000"))
    max_workers: int = int(os.getenv("MAX_WORKERS", "4"))
    output_format: str = os.getenv("OUTPUT_FORMAT", "parquet")  # parquet or jsonl
    compression: str = os.getenv("COMPRESSION", "gzip")
    
    # Memory management
    max_memory_usage_gb: float = float(os.getenv("MAX_MEMORY_USAGE_GB", "8.0"))
    
    # Error handling
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    retry_delay: int = int(os.getenv("RETRY_DELAY", "5"))
    
    # Progress tracking
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_file: str = os.getenv("LOG_FILE", "mongo2gcs.log")


class ProgressTracker:
    """Redis-based progress tracking for distributed processing"""
    
    def __init__(self, config: Config):
        self.redis_client = redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            decode_responses=True
        )
        self.job_key = f"mongo2gcs:{int(time.time())}"
        self.logger = logging.getLogger(__name__)
    
    def initialize_job(self, total_chunks: int, total_records: int):
        """Initialize job tracking"""
        job_info = {
            "total_chunks": total_chunks,
            "total_records": total_records,
            "start_time": time.time(),
            "status": "running"
        }
        self.redis_client.hset(self.job_key, mapping=job_info)
        self.logger.info(f"Initialized job tracking: {self.job_key}")
    
    def update_chunk_status(self, chunk_id: int, status: str, **kwargs):
        """Update the status of a specific chunk"""
        chunk_key = f"{self.job_key}:chunk:{chunk_id}"
        chunk_info = {
            "status": status,
            "updated_at": time.time(),
            **kwargs
        }
        self.redis_client.hset(chunk_key, mapping=chunk_info)
    
    def get_progress(self) -> Dict[str, Any]:
        """Get current progress statistics"""
        job_info = self.redis_client.hgetall(self.job_key)
        if not job_info:
            return {}
        
        total_chunks = int(job_info.get("total_chunks", 0))
        completed_chunks = 0
        failed_chunks = 0
        
        # Count completed and failed chunks
        for chunk_id in range(total_chunks):
            chunk_key = f"{self.job_key}:chunk:{chunk_id}"
            chunk_info = self.redis_client.hgetall(chunk_key)
            if chunk_info:
                status = chunk_info.get("status", "pending")
                if status == "completed":
                    completed_chunks += 1
                elif status == "failed":
                    failed_chunks += 1
        
        progress_pct = (completed_chunks / total_chunks * 100) if total_chunks > 0 else 0
        
        return {
            "total_chunks": total_chunks,
            "completed_chunks": completed_chunks,
            "failed_chunks": failed_chunks,
            "pending_chunks": total_chunks - completed_chunks - failed_chunks,
            "progress_percentage": progress_pct,
            "start_time": float(job_info.get("start_time", 0)),
            "total_records": int(job_info.get("total_records", 0))
        }
    
    def set_total_records(self, count: int):
        """Set the total number of records to process"""
        self.redis_client.set(f"{self.job_key}:total", count)
    
    def increment_processed(self, count: int = 1):
        """Increment the processed records counter"""
        return self.redis_client.incrby(f"{self.job_key}:processed", count)
    
    def increment_failed(self, count: int = 1):
        """Increment the failed records counter"""
        return self.redis_client.incrby(f"{self.job_key}:failed", count)
    
    def reset(self):
        """Reset all progress counters"""
        keys = [
            f"{self.job_key}:total",
            f"{self.job_key}:processed",
            f"{self.job_key}:failed"
        ]
        self.redis_client.delete(*keys)
