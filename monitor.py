#!/usr/bin/env python3
"""
Simple progress monitor for the MongoDB to GCS pipeline
Usage: python3 monitor.py
"""

import time
import redis
import os
from datetime import datetime


def monitor_progress():
    """Monitor and display progress in real-time"""
    
    # Redis configuration from environment
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        r.ping()  # Test connection
    except Exception as e:
        print(f"Cannot connect to Redis: {e}")
        print("Make sure Redis is running and accessible")
        return
    
    key_prefix = "mongo2gcs"
    start_time = time.time()
    
    print("MongoDB to GCS Progress Monitor")
    print("=" * 40)
    print("Press Ctrl+C to exit\n")
    
    try:
        while True:
            # Get progress data
            total = int(r.get(f"{key_prefix}:total") or 0)
            processed = int(r.get(f"{key_prefix}:processed") or 0)
            failed = int(r.get(f"{key_prefix}:failed") or 0)
            
            remaining = total - processed - failed
            progress_percent = (processed / total * 100) if total > 0 else 0
            
            elapsed = time.time() - start_time
            
            # Clear screen
            print("\033[2J\033[H", end="")
            
            print("MongoDB to GCS Progress Monitor")
            print("=" * 40)
            print(f"Total Records:     {total:,}")
            print(f"Processed:         {processed:,}")
            print(f"Failed:            {failed:,}")
            print(f"Remaining:         {remaining:,}")
            print(f"Progress:          {progress_percent:.2f}%")
            
            if processed > 0 and elapsed > 0:
                rate = processed / elapsed
                print(f"Processing Rate:   {rate:.1f} records/sec")
                
                if remaining > 0:
                    eta_seconds = remaining / rate
                    eta_hours = eta_seconds / 3600
                    if eta_hours < 24:
                        print(f"ETA:               {eta_hours:.1f} hours")
                    else:
                        eta_days = eta_hours / 24
                        print(f"ETA:               {eta_days:.1f} days")
            
            # Progress bar
            bar_width = 40
            filled = int(bar_width * progress_percent / 100)
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"\n[{bar}] {progress_percent:.1f}%")
            
            print(f"\nLast updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("Press Ctrl+C to exit")
            
            time.sleep(5)  # Update every 5 seconds
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")


if __name__ == "__main__":
    monitor_progress()
