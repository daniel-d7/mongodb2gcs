#!/usr/bin/env python3
"""
GCS Uploader - Handles file uploads to Google Cloud Storage
"""

import logging
import time
import json
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd
from google.cloud import storage

from .progress import Config


class GCSUploader:
    """Handles file uploads to Google Cloud Storage"""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = storage.Client(project=config.gcp_project_id)
        self.bucket = self.client.bucket(config.gcs_bucket)
        self.logger = logging.getLogger(__name__)
    
    def upload_chunk_to_gcs(self, chunk_data: Dict[str, Any]) -> Dict[str, Any]:
        """Upload a chunk of data to GCS"""
        upload_start_time = time.time()
        chunk_index = chunk_data.get("chunk_index", 0)
        
        try:
            records = chunk_data["records"]
            
            if not records:
                return {
                    "chunk_index": chunk_index,
                    "gcs_path": None,
                    "file_size_mb": 0,
                    "upload_duration": 0,
                    "success": False,
                    "error": "No records to upload"
                }
            
            # Create temporary files
            temp_dir = Path("temp")
            temp_dir.mkdir(exist_ok=True)
            
            if self.config.output_format == "parquet":
                file_path = temp_dir / f"chunk_{chunk_index:06d}.parquet"
                gcs_path = f"{self.config.gcs_prefix}chunk_{chunk_index:06d}.parquet"
                
                # Convert to DataFrame and save as Parquet
                df = pd.DataFrame(records)
                df.to_parquet(file_path, index=False, engine='pyarrow')
                
            else:  # JSONL format
                file_path = temp_dir / f"chunk_{chunk_index:06d}.jsonl"
                gcs_path = f"{self.config.gcs_prefix}chunk_{chunk_index:06d}.jsonl"
                
                # Save as JSONL
                with open(file_path, 'w') as f:
                    for record in records:
                        json.dump(record, f)
                        f.write('\n')
            
            # Upload to GCS
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(str(file_path))
            
            # Get file size
            file_size_mb = file_path.stat().st_size / 1024 / 1024
            
            # Clean up temp file
            file_path.unlink()
            
            upload_duration = time.time() - upload_start_time
            
            return {
                "chunk_index": chunk_index,
                "gcs_path": gcs_path,
                "file_size_mb": file_size_mb,
                "upload_duration": upload_duration,
                "success": True
            }
            
        except Exception as e:
            self.logger.error(f"Error uploading chunk {chunk_index} to GCS: {str(e)}")
            return {
                "chunk_index": chunk_index,
                "gcs_path": None,
                "file_size_mb": 0,
                "upload_duration": 0,
                "success": False,
                "error": str(e)
            }
    
    def verify_upload(self, gcs_path: str) -> bool:
        """Verify that a file exists in GCS"""
        try:
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
        except Exception as e:
            self.logger.error(f"Error verifying upload {gcs_path}: {str(e)}")
            return False
