#!/usr/bin/env python3
"""
MongoDB to Google Cloud Storage Pipeline
========================================

Simplified pipeline to transfer data from MongoDB to Google Cloud Storage.
"""

import os
import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

from src.progress import Config, ProgressTracker
from src.mongodb_exporter import MongoDBExporter
from src.gcs_uploader import GCSUploader


def setup_logging(config: Config):
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(config.log_file),
            logging.StreamHandler()
        ]
    )


def process_chunk(start_id_str, end_id_str, chunk_index, config_dict):
    """Process a single chunk - extract from MongoDB and upload to GCS"""
    from bson import ObjectId
    
    # Recreate config and components in worker process
    config = Config(**config_dict)
    mongodb_exporter = MongoDBExporter(config)
    gcs_uploader = GCSUploader(config)
    progress_tracker = ProgressTracker(config)
    
    try:
        # Convert string IDs back to ObjectIds
        start_id = ObjectId(start_id_str)
        end_id = ObjectId(end_id_str)
        
        # Export chunk from MongoDB
        chunk_data = mongodb_exporter.export_chunk(start_id, end_id, chunk_index)
        
        if not chunk_data["success"]:
            progress_tracker.update_chunk_status(chunk_index, "failed", 
                                               error=chunk_data.get("error"))
            return chunk_data
        
        # Upload to GCS
        upload_result = gcs_uploader.upload_chunk_to_gcs(chunk_data)
        
        if upload_result["success"]:
            progress_tracker.update_chunk_status(chunk_index, "completed", 
                                               gcs_path=upload_result["gcs_path"],
                                               file_size_mb=upload_result["file_size_mb"])
        else:
            progress_tracker.update_chunk_status(chunk_index, "failed", 
                                               error=upload_result.get("error"))
        
        # Combine results
        result = {**chunk_data, **upload_result}
        
        return result
        
    except Exception as e:
        error_msg = f"Error processing chunk {chunk_index}: {str(e)}"
        logging.error(error_msg)
        progress_tracker.update_chunk_status(chunk_index, "failed", error=error_msg)
        return {
            "chunk_index": chunk_index,
            "success": False,
            "error": error_msg
        }
    finally:
        mongodb_exporter.close()


def main():
    """Main pipeline execution"""
    # Load configuration
    config = Config()
    
    # Setup logging
    setup_logging(config)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting MongoDB to GCS transfer pipeline")
    logger.info(f"Source: {config.mongo_db}.{config.mongo_collection}")
    logger.info(f"Destination: gs://{config.gcs_bucket}/{config.gcs_prefix}")
    logger.info(f"Format: {config.output_format}")
    logger.info(f"Workers: {config.max_workers}")
    
    # Initialize components
    mongodb_exporter = MongoDBExporter(config)
    progress_tracker = ProgressTracker(config)
    
    # Test MongoDB connection before starting
    logger.info("Testing MongoDB connection...")
    if not mongodb_exporter.test_connection():
        logger.error("MongoDB connection test failed. Please check your connection settings and authentication.")
        return
    
    try:
        # Get total count
        total_count = mongodb_exporter.get_total_count()
        logger.info(f"Total documents to process: {total_count:,}")
        
        # Calculate number of chunks
        records_per_chunk = config.chunk_size
        total_chunks = (total_count + records_per_chunk - 1) // records_per_chunk
        logger.info(f"Processing in {total_chunks} chunks of {records_per_chunk:,} records each")
        
        # Get ID ranges for parallel processing
        id_ranges = mongodb_exporter.get_id_ranges(total_chunks)
        
        # Initialize progress tracking
        progress_tracker.initialize_job(total_chunks, total_count)
        
        # Process chunks in parallel
        start_time = time.time()
        successful_chunks = 0
        failed_chunks = 0
        
        # Convert config to dict for multiprocessing
        config_dict = {
            'mongo_uri': config.mongo_uri,
            'mongo_db': config.mongo_db,
            'mongo_collection': config.mongo_collection,
            'mongo_username': config.mongo_username,
            'mongo_password': config.mongo_password,
            'mongo_auth_source': config.mongo_auth_source,
            'gcp_project_id': config.gcp_project_id,
            'gcs_bucket': config.gcs_bucket,
            'gcs_prefix': config.gcs_prefix,
            'output_format': config.output_format,
            'chunk_size': config.chunk_size,
            'max_workers': config.max_workers,
            'max_memory_usage_gb': config.max_memory_usage_gb,
            'redis_url': config.redis_url,
            'redis_host': config.redis_host,
            'redis_port': config.redis_port,
            'redis_db': config.redis_db,
            'max_retries': config.max_retries,
            'retry_delay': config.retry_delay,
            'log_level': config.log_level,
            'log_file': config.log_file
        }
        
        with ProcessPoolExecutor(max_workers=config.max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {
                executor.submit(
                    process_chunk, 
                    str(start_id), 
                    str(end_id), 
                    chunk_index, 
                    config_dict
                ): chunk_index
                for chunk_index, (start_id, end_id) in enumerate(id_ranges)
            }
            
            # Process results with progress bar
            with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
                for future in as_completed(future_to_chunk):
                    chunk_index = future_to_chunk[future]
                    try:
                        result = future.result()
                        if result["success"]:
                            successful_chunks += 1
                            pbar.set_postfix(
                                success=successful_chunks, 
                                failed=failed_chunks,
                                rate=f"{successful_chunks/(time.time()-start_time):.2f}/s"
                            )
                        else:
                            failed_chunks += 1
                            logger.error(f"Chunk {chunk_index} failed: {result.get('error', 'Unknown error')}")
                        
                        pbar.update(1)
                        
                    except Exception as e:
                        failed_chunks += 1
                        logger.error(f"Exception processing chunk {chunk_index}: {str(e)}")
                        pbar.update(1)
        
        # Final statistics
        total_time = time.time() - start_time
        logger.info(f"Pipeline completed in {total_time:.2f} seconds")
        logger.info(f"Successful chunks: {successful_chunks}")
        logger.info(f"Failed chunks: {failed_chunks}")
        logger.info(f"Processing rate: {successful_chunks/total_time:.2f} chunks/second")
        
        # Export schema information
        if successful_chunks > 0:
            try:
                # Create a GCS uploader to access schema info
                gcs_uploader = GCSUploader(config)
                schema_info = gcs_uploader.export_schema_info()
                logger.info(f"Exported schema with {schema_info['total_fields']} fields")
            except Exception as e:
                logger.error(f"Failed to export schema: {e}")
        
        # Get final progress stats
        stats = progress_tracker.get_progress()
        logger.info(f"Final progress: {stats}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        mongodb_exporter.close()


if __name__ == "__main__":
    main()
