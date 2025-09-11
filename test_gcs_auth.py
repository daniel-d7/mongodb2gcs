#!/usr/bin/env python3
"""
Test script to verify Google Cloud Storage authentication setup
"""

import os
import sys
from pathlib import Path

def test_token_file():
    """Test if token.json file exists and is valid"""
    print("🔍 Checking for token.json file...")
    
    token_paths = [
        Path("token.json"),
        Path("token") / "token.json"
    ]
    
    token_path = None
    for path in token_paths:
        if path.exists():
            token_path = path
            print(f"✅ Found token.json at: {path}")
            break
    
    if not token_path:
        print("❌ token.json not found in main directory or token/ folder")
        print("📝 Please follow the setup guide in GCS_AUTH_SETUP.md")
        return False
    
    # Test if it's valid JSON
    try:
        import json
        with open(token_path) as f:
            token_data = json.load(f)
        
        required_fields = ['type', 'project_id', 'private_key', 'client_email']
        missing_fields = [field for field in required_fields if field not in token_data]
        
        if missing_fields:
            print(f"❌ token.json is missing required fields: {missing_fields}")
            return False
        
        print("✅ token.json is valid JSON with required fields")
        print(f"📋 Project ID: {token_data.get('project_id')}")
        print(f"📧 Service Account: {token_data.get('client_email')}")
        return True
        
    except json.JSONDecodeError:
        print("❌ token.json is not valid JSON")
        return False
    except Exception as e:
        print(f"❌ Error reading token.json: {e}")
        return False

def test_dependencies():
    """Test if required packages are installed"""
    print("\n🔍 Checking required packages...")
    
    required_packages = [
        'google.cloud.storage',
        'google.oauth2.service_account'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n📦 Install missing packages with:")
        print("pip install google-cloud-storage google-auth")
        return False
    
    return True

def test_env_config():
    """Test environment configuration"""
    print("\n🔍 Checking environment configuration...")
    
    # Try to load from .env file
    env_file = Path(".env")
    env_vars = {}
    
    if env_file.exists():
        print("✅ Found .env file")
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    else:
        print("⚠️  No .env file found, checking environment variables")
    
    # Check required environment variables
    required_env_vars = ['GCP_PROJECT_ID', 'GCS_BUCKET']
    for var in required_env_vars:
        value = env_vars.get(var) or os.getenv(var)
        if value:
            print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: Not set")
    
    return all(env_vars.get(var) or os.getenv(var) for var in required_env_vars)

def test_gcs_authentication():
    """Test Google Cloud Storage authentication"""
    print("\n🔍 Testing GCS authentication...")
    
    try:
        from src.progress import Config
        from src.gcs_uploader import GCSUploader
        
        print("✅ Successfully imported modules")
        
        config = Config()
        print(f"✅ Configuration loaded")
        print(f"📋 Project: {config.gcp_project_id}")
        print(f"🪣 Bucket: {config.gcs_bucket}")
        
        uploader = GCSUploader(config)
        print("✅ GCS authentication successful!")
        return True
        
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return False
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Google Cloud Storage Authentication Test")
    print("=" * 50)
    
    tests = [
        ("Token File", test_token_file),
        ("Dependencies", test_dependencies),
        ("Environment Config", test_env_config),
        ("GCS Authentication", test_gcs_authentication)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        success = test_func()
        results.append((test_name, success))
    
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS:")
    
    all_passed = True
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if not success:
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All tests passed! You're ready to run the MongoDB to GCS pipeline.")
    else:
        print("⚠️  Some tests failed. Please check the setup guide in GCS_AUTH_SETUP.md")
        sys.exit(1)

if __name__ == "__main__":
    main()
