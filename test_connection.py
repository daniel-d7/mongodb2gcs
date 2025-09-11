#!/usr/bin/env python3
"""
MongoDB Connection Test Script
=============================

Test MongoDB connection and authentication before running the main pipeline.
"""

import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.append('src')

from src.progress import Config
from src.mongodb_exporter import MongoDBExporter


def main():
    """Test MongoDB connection"""
    # Load environment variables
    load_dotenv()
    
    print("MongoDB Connection Test")
    print("=" * 30)
    
    # Create config
    config = Config()
    
    print(f"MongoDB URI: {config.mongo_uri}")
    print(f"Database: {config.mongo_db}")
    print(f"Collection: {config.mongo_collection}")
    
    if config.mongo_username:
        print(f"Username: {config.mongo_username}")
        print(f"Auth Source: {config.mongo_auth_source}")
    
    print("\nTesting connection...")
    
    exporter = None
    try:
        # Create MongoDB exporter and test connection
        exporter = MongoDBExporter(config)
        
        if exporter.test_connection():
            print("✅ Connection successful!")
            
            # Get collection count
            count = exporter.get_total_count()
            print(f"✅ Collection document count: {count:,}")
            
        else:
            print("❌ Connection failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)
    
    finally:
        if exporter:
            try:
                exporter.close()
            except:
                pass


if __name__ == "__main__":
    main()
