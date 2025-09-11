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
    
    def _clean_records_for_parquet(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and normalize records for Parquet format compatibility"""
        if not records:
            return records
        
        # First pass: analyze column types to determine best conversion strategy
        column_types = self._analyze_column_types(records)
        
        cleaned_records = []
        
        for record in records:
            cleaned_record = {}
            
            for key, value in record.items():
                expected_type = column_types.get(key, 'mixed')
                cleaned_record[key] = self._normalize_value_with_type(value, expected_type)
                
            cleaned_records.append(cleaned_record)
        
        return cleaned_records
    
    def _analyze_column_types(self, records: List[Dict[str, Any]]) -> Dict[str, str]:
        """Analyze the types of values in each column to determine best conversion strategy"""
        column_types = {}
        column_samples = {}
        
        # Sample first 100 records to analyze types
        sample_size = min(100, len(records))
        sample_records = records[:sample_size]
        
        for record in sample_records:
            for key, value in record.items():
                if key not in column_samples:
                    column_samples[key] = []
                column_samples[key].append(type(value).__name__)
        
        # Determine predominant type for each column
        for column, type_samples in column_samples.items():
            type_counts = {}
            for type_name in type_samples:
                type_counts[type_name] = type_counts.get(type_name, 0) + 1
            
            # Find most common type
            most_common_type = max(type_counts, key=type_counts.get)
            
            # Check if we have mixed types
            unique_types = set(t for t in type_samples if t != 'NoneType')
            
            if len(unique_types) > 1:
                # Mixed types - convert all to string
                column_types[column] = 'mixed'
            elif most_common_type in ['list', 'dict']:
                # Complex types - convert to JSON string
                column_types[column] = 'json'
            else:
                column_types[column] = most_common_type
        
        return column_types
    
    def _normalize_value_with_type(self, value: Any, expected_type: str) -> Any:
        """Normalize a single value based on the expected column type"""
        if value is None:
            return None
        
        # Handle mixed types - convert everything to string
        if expected_type == 'mixed':
            if isinstance(value, (list, dict)):
                return json.dumps(value) if value else None
            return str(value)
        
        # Handle JSON types (lists and dicts)
        if expected_type == 'json':
            if isinstance(value, (list, dict)):
                return json.dumps(value) if value else None
            return str(value)  # Convert non-JSON to string for consistency
        
        # Handle datetime objects
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        
        # Handle list/array types - convert to JSON string for consistency
        if isinstance(value, (list, tuple)):
            return json.dumps(value) if value else None
        
        # Handle dict/object types - convert to JSON string
        if isinstance(value, dict):
            return json.dumps(value) if value else None
        
        # For basic types, return as-is but ensure consistency
        if isinstance(value, (str, int, float, bool)):
            return value
        
        # For other types, convert to string
        try:
            return str(value)
        except Exception:
            return None
    
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
                
                try:
                    # Clean and normalize data before DataFrame conversion
                    cleaned_records = self._clean_records_for_parquet(records)
                    
                    # Convert to DataFrame and save as Parquet
                    df = pd.DataFrame(cleaned_records)
                    df.to_parquet(file_path, index=False, engine='pyarrow')
                    
                except Exception as e:
                    self.logger.error(f"Error converting chunk {chunk_index} to Parquet: {str(e)}")
                    # Try to save sample of problematic data for debugging
                    sample_records = records[:5] if len(records) > 5 else records
                    self.logger.debug(f"Sample records from chunk {chunk_index}: {json.dumps(sample_records, indent=2, default=str)}")
                    raise
                
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
