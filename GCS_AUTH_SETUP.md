# Google Cloud Storage Authentication Setup Guide

## Method 1: Service Account Key File (token.json)

### Step 1: Create Service Account in Google Cloud Console

1. **Go to Google Cloud Console:**
   - Navigate to [Google Cloud Console](https://console.cloud.google.com/)
   - Select your project

2. **Create Service Account:**
   - Go to **IAM & Admin** → **Service Accounts**
   - Click **"Create Service Account"**
   - Enter a name (e.g., `mongodb2gcs-service-account`)
   - Enter description: `Service account for MongoDB to GCS data transfer`
   - Click **"Create and Continue"**

3. **Grant Required Permissions:**
   Add these roles to your service account:
   - **Storage Object Admin** (for read/write access to GCS objects)
   - **Storage Admin** (if you need to create/manage buckets)
   
   Click **"Continue"** then **"Done"**

### Step 2: Download Service Account Key

1. **Generate Key File:**
   - In the Service Accounts list, click on your newly created service account
   - Go to **"Keys"** tab
   - Click **"Add Key"** → **"Create new key"**
   - Select **"JSON"** format
   - Click **"Create"**

2. **Download and Rename:**
   - The key file will be downloaded automatically
   - Rename it to `token.json`

### Step 3: Place token.json in Your Project

Place the `token.json` file in one of these locations:

**Option A: Main Directory (Recommended)**
```
mongodb2gcs/
├── token.json          ← Place here
├── main.py
├── requirements.txt
└── src/
```

**Option B: Token Subfolder**
```
mongodb2gcs/
├── token/
│   └── token.json      ← Or place here
├── main.py
├── requirements.txt
└── src/
```

### Step 4: Verify Authentication

Run this test command to verify your setup:

```powershell
# Test authentication
python -c "from src.gcs_uploader import GCSUploader; from src.progress import Config; uploader = GCSUploader(Config()); print('Authentication successful!')"
```

## Security Best Practices

### 1. Keep token.json Secure
- **Never commit** `token.json` to version control
- Add it to `.gitignore`:
  ```
  # Add to .gitignore
  token.json
  token/token.json
  *.json
  ```

### 2. File Permissions
Set appropriate file permissions (Windows):
```powershell
icacls token.json /inheritance:r /grant:r "%USERNAME%:F"
```

### 3. Environment Variables (Alternative)
You can also set the path via environment variable:
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\path\to\your\token.json"
```

## Troubleshooting

### Common Issues:

1. **"token.json not found"**
   - Verify file exists in main directory or token/ folder
   - Check file name is exactly `token.json`

2. **"403 Forbidden" or "Provided scope(s) are not authorized"**
   - Verify service account has correct IAM roles
   - Ensure Storage Object Admin permission is granted

3. **"Invalid credentials"**
   - Re-download the service account key
   - Verify the JSON file is not corrupted

4. **"Project not found"**
   - Check GCP_PROJECT_ID in your .env file matches the project in token.json

### Verification Steps:

1. **Check token.json format:**
   ```powershell
   python -c "import json; print('Valid JSON' if json.load(open('token.json')) else 'Invalid')"
   ```

2. **Test GCS access:**
   ```powershell
   python -c "
   from google.cloud import storage
   from google.oauth2 import service_account
   
   credentials = service_account.Credentials.from_service_account_file('token.json')
   client = storage.Client(credentials=credentials)
   buckets = list(client.list_buckets(max_results=1))
   print('GCS access successful!')
   "
   ```

## Required Dependencies

Ensure you have the required packages:
```bash
pip install google-cloud-storage google-auth
```

## Environment Configuration

Update your `.env` file with correct project settings:
```
GCP_PROJECT_ID=your-actual-project-id
GCS_BUCKET=your-bucket-name
```

The service account authentication is now implemented and will automatically:
- Look for `token.json` in main directory first
- Fall back to `token/token.json` if not found in main directory  
- Provide detailed error messages if authentication fails
- Test the connection during initialization
- Use appropriate scopes for GCS operations
