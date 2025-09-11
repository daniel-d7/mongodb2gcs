#!/usr/bin/env python3
"""
MongoDB Exporter - Handles MongoDB data extraction with efficient cursor management
"""

import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import pymongo
from pymongo import MongoClient
from pymongo.cursor import Cursor
from bson import ObjectId
import psutil

from .progress import Config


class MongoDBExporter:
    """Handles MongoDB data extraction with efficient cursor management"""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Create MongoDB client with proper authentication
        self.client = self._create_mongo_client(config)
        self.db = self.client[config.mongo_db]
        self.collection = self.db[config.mongo_collection]
    
    def _create_mongo_client(self, config: Config) -> MongoClient:
        """Create MongoDB client with authentication if credentials are provided"""
        # If username and password are provided separately, build URI with auth
        if config.mongo_username and config.mongo_password:
            # Use separate auth parameters
            self.logger.info(f"Connecting to MongoDB with username authentication (auth_source: {config.mongo_auth_source})")
            return MongoClient(
                config.mongo_uri,
                username=config.mongo_username,
                password=config.mongo_password,
                authSource=config.mongo_auth_source
            )
        else:
            # Use URI-based authentication (credentials should be in URI)
            self.logger.info("Connecting to MongoDB using URI-based authentication")
            return MongoClient(config.mongo_uri)
    
    def test_connection(self) -> bool:
        """Test MongoDB connection and authentication"""
        try:
            # Test connection by getting server info
            self.client.admin.command('ping')
            self.logger.info("MongoDB connection successful")
            
            # Test database access
            self.db.command('ping')
            self.logger.info(f"Database '{self.config.mongo_db}' access successful")
            
            # Test collection access
            count = self.collection.estimated_document_count()
            self.logger.info(f"Collection '{self.config.mongo_collection}' access successful, estimated count: {count:,}")
            
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection test failed: {str(e)}")
            return False
    
    def get_total_count(self) -> int:
        """Get total number of documents in the collection"""
        return self.collection.estimated_document_count()
    
    def create_cursor_for_range(self, start_id: ObjectId, end_id: ObjectId) -> Cursor:
        """Create a cursor for a specific ObjectId range"""
        query = {
            "_id": {
                "$gte": start_id,
                "$lt": end_id
            }
        }
        return self.collection.find(query).batch_size(self.config.chunk_size)
    
    def get_id_ranges(self, num_chunks: int) -> List[Tuple[ObjectId, ObjectId]]:
        """Divide the collection into roughly equal chunks by ObjectId ranges"""
        self.logger.info(f"Calculating {num_chunks} ObjectId ranges...")
        
        # Get min and max ObjectIds
        min_doc = self.collection.find().sort("_id", 1).limit(1).next()
        max_doc = self.collection.find().sort("_id", -1).limit(1).next()
        
        min_id = min_doc["_id"]
        max_id = max_doc["_id"]
        
        # Calculate time-based ranges (ObjectIds are time-based)
        min_timestamp = min_id.generation_time
        max_timestamp = max_id.generation_time
        time_diff = max_timestamp - min_timestamp
        chunk_duration = time_diff / num_chunks
        
        ranges = []
        current_time = min_timestamp
        
        for i in range(num_chunks):
            start_time = current_time
            end_time = current_time + chunk_duration if i < num_chunks - 1 else max_timestamp + timedelta(seconds=1)
            
            start_id = ObjectId.from_datetime(start_time)
            end_id = ObjectId.from_datetime(end_time)
            
            ranges.append((start_id, end_id))
            current_time = end_time
        
        return ranges
    
    def export_chunk(self, start_id: ObjectId, end_id: ObjectId, chunk_index: int) -> Dict[str, Any]:
        """Export a chunk of data and return metadata"""
        chunk_start_time = time.time()
        
        try:
            cursor = self.create_cursor_for_range(start_id, end_id)
            records = []
            record_count = 0
            
            for doc in cursor:
                # Convert ObjectId to string for JSON serialization
                doc["_id"] = str(doc["_id"])
                records.append(doc)
                record_count += 1
                
                # Check memory usage periodically
                if record_count % 1000 == 0:
                    memory_usage_gb = psutil.Process().memory_info().rss / 1024 / 1024 / 1024
                    if memory_usage_gb > self.config.max_memory_usage_gb:
                        self.logger.warning(f"Memory usage ({memory_usage_gb:.2f}GB) exceeds limit")
                        break
            
            chunk_duration = time.time() - chunk_start_time
            
            return {
                "chunk_index": chunk_index,
                "records": records,
                "record_count": record_count,
                "duration": chunk_duration,
                "start_id": str(start_id),
                "end_id": str(end_id),
                "success": True
            }
            
        except Exception as e:
            self.logger.error(f"Error exporting chunk {chunk_index}: {str(e)}")
            return {
                "chunk_index": chunk_index,
                "records": [],
                "record_count": 0,
                "duration": 0,
                "start_id": str(start_id),
                "end_id": str(end_id),
                "success": False,
                "error": str(e)
            }
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
