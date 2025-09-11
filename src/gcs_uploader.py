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
        
        # Schema consistency tracking
        self.global_schema = {}
        self.schema_lock = None  # Will be set if using multiprocessing
        
    def update_global_schema(self, fields: set, field_strategies: Dict[str, str]):
        """Update the global schema with new fields and strategies"""
        for field in fields:
            if field not in self.global_schema:
                self.global_schema[field] = field_strategies.get(field, 'string')
            elif self.global_schema[field] != field_strategies.get(field, 'string'):
                # Handle schema conflicts - prefer more general types
                current = self.global_schema[field]
                new = field_strategies.get(field, 'string')
                self.global_schema[field] = self._resolve_schema_conflict(current, new)
    
    def _resolve_schema_conflict(self, current_strategy: str, new_strategy: str) -> str:
        """Resolve conflicts between different normalization strategies"""
        # Priority order: json_string > string > mostly_* > specific types
        strategy_priority = {
            'json_string': 1,
            'string': 2,
            'mostly_string': 3,
            'mostly_float': 4,
            'mostly_integer': 5,
            'mostly_boolean': 6,
            'iso_string': 7,
            'json_object': 8,
            'json_array': 9,
            'float': 10,
            'integer': 11,
            'boolean': 12
        }
        
        current_priority = strategy_priority.get(current_strategy, 999)
        new_priority = strategy_priority.get(new_strategy, 999)
        
        if current_priority <= new_priority:
            return current_strategy
        else:
            self.logger.debug(f"Schema conflict resolved: {current_strategy} -> {new_strategy}")
            return new_strategy
    
    def _clean_records_for_parquet(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and normalize ALL records for Parquet format compatibility"""
        if not records:
            return records
        
        # Comprehensive field normalization approach
        # 1. First pass: collect all unique field names across all records
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        # 2. Second pass: analyze field types and create normalization strategy
        field_strategies = self._create_normalization_strategies(records, all_fields)
        
        # 3. Update global schema for consistency across chunks
        self.update_global_schema(all_fields, field_strategies)
        
        # 4. Use global schema for normalization to ensure consistency
        effective_strategies = {}
        for field in all_fields:
            effective_strategies[field] = self.global_schema.get(field, 'string')
        
        # 5. Normalize all records with consistent field set and strategies
        cleaned_records = []
        
        for record in records:
            cleaned_record = {}
            
            # Process all known fields (from global schema + current chunk)
            all_known_fields = set(self.global_schema.keys()) | all_fields
            
            for field in all_known_fields:
                raw_value = record.get(field)
                strategy = effective_strategies.get(field, self.global_schema.get(field, 'string'))
                cleaned_record[field] = self._apply_normalization_strategy(raw_value, strategy, field)
                
            cleaned_records.append(cleaned_record)
        
        # Log schema information
        self.logger.debug(f"Applied normalization strategies: {len(effective_strategies)} fields")
        self.logger.debug(f"Global schema now contains: {len(self.global_schema)} fields")
        
        return cleaned_records
    
    def _create_normalization_strategies(self, records: List[Dict[str, Any]], all_fields: set) -> Dict[str, str]:
        """Create comprehensive normalization strategies for all fields"""
        field_strategies = {}
        
        # Sample records to analyze field patterns (use more samples for better analysis)
        sample_size = min(500, len(records))
        sample_records = records[:sample_size]
        
        for field in all_fields:
            field_analysis = {
                'types': {},
                'null_count': 0,
                'total_count': 0,
                'sample_values': []
            }
            
            # Analyze field across sample records
            for record in sample_records:
                value = record.get(field)
                field_analysis['total_count'] += 1
                
                if value is None:
                    field_analysis['null_count'] += 1
                    continue
                
                value_type = self._get_detailed_type(value)
                field_analysis['types'][value_type] = field_analysis['types'].get(value_type, 0) + 1
                
                # Collect sample values for debugging
                if len(field_analysis['sample_values']) < 5:
                    field_analysis['sample_values'].append(value)
            
            # Determine normalization strategy based on analysis
            strategy = self._determine_field_strategy(field, field_analysis)
            field_strategies[field] = strategy
            
            # Log strategy for important fields or complex cases
            if field_analysis['types'] and len(field_analysis['types']) > 1:
                self.logger.debug(f"Field '{field}' has mixed types {list(field_analysis['types'].keys())}, using strategy: {strategy}")
        
        return field_strategies
    
    def _get_detailed_type(self, value: Any) -> str:
        """Get detailed type information for a value"""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            return 'string'
        elif isinstance(value, list):
            if not value:
                return 'empty_list'
            # Check if it's a homogeneous list
            inner_types = set(self._get_detailed_type(item) for item in value[:10])  # Sample first 10 items
            if len(inner_types) == 1:
                return f'list_of_{inner_types.pop()}'
            else:
                return 'mixed_list'
        elif isinstance(value, dict):
            if not value:
                return 'empty_dict'
            return 'object'
        elif hasattr(value, 'isoformat'):  # datetime-like objects
            return 'datetime'
        else:
            return 'unknown'
    
    def _determine_field_strategy(self, field_name: str, analysis: Dict) -> str:
        """Determine the best normalization strategy for a field"""
        types = analysis['types']
        total_non_null = analysis['total_count'] - analysis['null_count']
        
        if total_non_null == 0:
            return 'null'  # Field is always null
        
        # Special handling for MongoDB _id field
        if field_name == '_id':
            return 'string'  # Always convert ObjectId to string
        
        # If only one type, use optimized strategy
        if len(types) == 1:
            single_type = list(types.keys())[0]
            if single_type in ['boolean', 'integer', 'float', 'string']:
                return single_type
            elif single_type.startswith('list_'):
                return 'json_array'
            elif single_type in ['object', 'empty_dict']:
                return 'json_object'
            elif single_type == 'datetime':
                return 'iso_string'
            else:
                return 'string'  # Fallback for unknown types
        
        # Mixed types - determine best strategy
        
        # If we have mostly primitives with some nulls, keep the primitive type
        primitive_types = {'boolean', 'integer', 'float', 'string'}
        type_counts = {t: count for t, count in types.items() if t in primitive_types}
        
        if type_counts:
            # Check if one primitive type dominates (>70%)
            dominant_type = max(type_counts, key=type_counts.get)
            if type_counts[dominant_type] / total_non_null > 0.7:
                return f'mostly_{dominant_type}'
        
        # If we have complex types mixed with primitives, convert all to JSON string
        complex_types = {'object', 'mixed_list', 'empty_dict', 'empty_list'}
        if any(t in types for t in complex_types) or any(t.startswith('list_') for t in types):
            return 'json_string'
        
        # If we have mixed primitives, convert all to string
        return 'string'
    
    def _apply_normalization_strategy(self, value: Any, strategy: str, field_name: str) -> Any:
        """Apply the determined normalization strategy to a field value"""
        if value is None:
            return None
        
        try:
            if strategy == 'null':
                return None
            
            elif strategy == 'string' or strategy.startswith('mostly_string'):
                return self._safe_string_conversion(value)
            
            elif strategy == 'boolean' or strategy.startswith('mostly_boolean'):
                return self._safe_boolean_conversion(value)
            
            elif strategy == 'integer' or strategy.startswith('mostly_integer'):
                return self._safe_integer_conversion(value)
            
            elif strategy == 'float' or strategy.startswith('mostly_float'):
                return self._safe_float_conversion(value)
            
            elif strategy == 'json_array':
                return self._safe_json_conversion(value) if isinstance(value, list) else str(value)
            
            elif strategy == 'json_object':
                return self._safe_json_conversion(value) if isinstance(value, dict) else str(value)
            
            elif strategy == 'json_string':
                return self._safe_json_conversion(value) if isinstance(value, (list, dict)) else str(value)
            
            elif strategy == 'iso_string':
                return value.isoformat() if hasattr(value, 'isoformat') else str(value)
            
            else:  # Fallback to string
                return self._safe_string_conversion(value)
                
        except Exception as e:
            self.logger.warning(f"Error normalizing field '{field_name}' with strategy '{strategy}': {e}")
            return str(value) if value is not None else None
    
    def _safe_string_conversion(self, value: Any) -> str:
        """Safely convert any value to string"""
        if isinstance(value, str):
            return value
        elif isinstance(value, (list, dict)):
            return json.dumps(value, default=str)
        elif hasattr(value, 'isoformat'):
            return value.isoformat()
        else:
            return str(value)
    
    def _safe_boolean_conversion(self, value: Any) -> bool:
        """Safely convert value to boolean"""
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        elif isinstance(value, (int, float)):
            return bool(value)
        else:
            return bool(value)
    
    def _safe_integer_conversion(self, value: Any) -> int:
        """Safely convert value to integer"""
        if isinstance(value, int):
            return value
        elif isinstance(value, float):
            return int(value) if value.is_integer() else int(value)
        elif isinstance(value, str):
            try:
                return int(float(value))  # Handle string floats
            except ValueError:
                return 0
        else:
            return 0
    
    def _safe_float_conversion(self, value: Any) -> float:
        """Safely convert value to float"""
        if isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return 0.0
        else:
            return 0.0
    
    def _safe_json_conversion(self, value: Any) -> str:
        """Safely convert complex types to JSON string"""
        try:
            return json.dumps(value, default=str, ensure_ascii=False)
        except Exception:
            return str(value)
    
    def _save_diagnostic_info(self, chunk_index: int, records: List[Dict[str, Any]], error: Exception):
        """Save diagnostic information for failed chunks"""
        try:
            diagnostic_dir = Path("diagnostic")
            diagnostic_dir.mkdir(exist_ok=True)
            
            # Save error info
            error_file = diagnostic_dir / f"chunk_{chunk_index:06d}_error.txt"
            with open(error_file, 'w') as f:
                f.write(f"Error: {str(error)}\n")
                f.write(f"Error type: {type(error).__name__}\n")
                f.write(f"Chunk index: {chunk_index}\n")
                f.write(f"Record count: {len(records)}\n")
            
            # Save sample records
            sample_size = min(10, len(records))
            sample_file = diagnostic_dir / f"chunk_{chunk_index:06d}_sample.json"
            with open(sample_file, 'w') as f:
                json.dump(records[:sample_size], f, indent=2, default=str)
            
            # Analyze field types in the failed chunk
            if records:
                field_analysis = {}
                for record in records[:100]:  # Analyze first 100 records
                    for field, value in record.items():
                        if field not in field_analysis:
                            field_analysis[field] = set()
                        field_analysis[field].add(self._get_detailed_type(value))
                
                analysis_file = diagnostic_dir / f"chunk_{chunk_index:06d}_analysis.json"
                # Convert sets to lists for JSON serialization
                serializable_analysis = {k: list(v) for k, v in field_analysis.items()}
                with open(analysis_file, 'w') as f:
                    json.dump(serializable_analysis, f, indent=2)
                
                self.logger.error(f"Diagnostic info saved for chunk {chunk_index} in diagnostic/ directory")
            
        except Exception as diag_error:
            self.logger.error(f"Failed to save diagnostic info for chunk {chunk_index}: {diag_error}")
    
    def export_schema_info(self) -> Dict[str, Any]:
        """Export the final schema information for documentation or BigQuery table creation"""
        schema_info = {
            "total_fields": len(self.global_schema),
            "field_strategies": self.global_schema,
            "bigquery_schema": self._generate_bigquery_schema(),
            "export_timestamp": time.time()
        }
        
        # Save schema to file
        try:
            schema_file = Path("schema_export.json")
            with open(schema_file, 'w') as f:
                json.dump(schema_info, f, indent=2)
            self.logger.info(f"Schema information exported to {schema_file}")
        except Exception as e:
            self.logger.error(f"Failed to export schema: {e}")
        
        return schema_info
    
    def _generate_bigquery_schema(self) -> List[Dict[str, str]]:
        """Generate BigQuery schema based on field strategies"""
        bq_schema = []
        
        strategy_to_bq_type = {
            'string': 'STRING',
            'mostly_string': 'STRING',
            'integer': 'INTEGER',
            'mostly_integer': 'INTEGER',
            'float': 'FLOAT',
            'mostly_float': 'FLOAT',
            'boolean': 'BOOLEAN',
            'mostly_boolean': 'BOOLEAN',
            'json_string': 'STRING',
            'json_object': 'STRING',
            'json_array': 'STRING',
            'iso_string': 'TIMESTAMP',
            'null': 'STRING'  # Default to STRING for null fields
        }
        
        for field_name, strategy in self.global_schema.items():
            bq_type = strategy_to_bq_type.get(strategy, 'STRING')
            mode = 'NULLABLE'  # All fields are nullable by default
            
            bq_schema.append({
                "name": field_name,
                "type": bq_type,
                "mode": mode,
                "description": f"Normalized from MongoDB field using strategy: {strategy}"
            })
        
        return bq_schema
    
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
                    self.logger.info(f"Normalizing {len(records)} records for chunk {chunk_index}")
                    cleaned_records = self._clean_records_for_parquet(records)
                    self.logger.info(f"Successfully normalized chunk {chunk_index}")
                    
                    # Convert to DataFrame and save as Parquet
                    df = pd.DataFrame(cleaned_records)
                    
                    # Log DataFrame info for debugging
                    self.logger.debug(f"Chunk {chunk_index} DataFrame shape: {df.shape}")
                    self.logger.debug(f"Chunk {chunk_index} DataFrame columns: {list(df.columns)}")
                    
                    df.to_parquet(file_path, index=False, engine='pyarrow')
                    self.logger.info(f"Successfully saved chunk {chunk_index} as Parquet")
                    
                except Exception as e:
                    self.logger.error(f"Error converting chunk {chunk_index} to Parquet: {str(e)}")
                    # Save diagnostic information
                    self._save_diagnostic_info(chunk_index, records, e)
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
