# S3 Data Upload Plan

## Overview
This document outlines the plan for uploading audio files from `data/s3_data/` to S3 and storing metadata in MongoDB.

## Data Structure Analysis

### Source Data Structure
Each folder in `data/s3_data/` contains:
- `audio.mp3` - Audio file to be uploaded to S3
- `{folder_id}.json` - JSON file with annotation data containing:
  - `id`: Numeric ID
  - `filename`: "audio.mp3"
  - `annotations`: Array of annotation objects with:
    - `start`: Start timestamp (e.g., "0:00:00.201000")
    - `end`: End timestamp (e.g., "0:00:00.811000")
    - `Transcription`: Array containing transcription text
- `ref_text.txt` - Reference text file containing the full reference transcription

### MongoDB Schema (from `utils/storage.py`)
The MongoDB collection `transcriptions` expects documents with:
```python
{
    'transcription_data': {
        # Flexible structure - can contain words, phrases, annotations, etc.
        # Should include metadata like:
        # - audio_path
        # - audio_duration (optional)
        # - language (optional)
        # - transcription_type (optional)
        # - Any other relevant fields
    },
    's3_metadata': {
        'bucket': str,
        'key': str,  # S3 object key/path
        'url': str,  # S3 URL
        'region': str,
        'size_bytes': int,
        'uploaded_at': str  # ISO format datetime
    },
    'user_id': str,  # Creator/owner (default: 'anonymous')
    'assigned_user_id': str | None,  # Assigned user (default: None)
    'created_at': datetime,
    'updated_at': datetime
}
```

## Upload Strategy

### 1. Data Processing
For each folder in `data/s3_data/`:
1. **Read JSON file**: Load `{folder_id}.json` to get annotations
2. **Read ref_text**: Load `ref_text.txt` to get reference text
3. **Get audio file info**:
   - File path: `{folder_id}/audio.mp3`
   - Calculate audio duration using audio utilities
   - Get file size
4. **Extract folder ID**: Use folder name as identifier

### 2. S3 Upload
- **S3 Key Format**: `audio/{folder_id}_audio.mp3`
- **Filename for S3**: `{folder_id}_audio.mp3` (without the audio/ prefix in the filename itself)
- **Bucket**: From environment variable `S3_BUCKET_NAME` (default: 'transcription-audio-files')
- **Region**: From environment variable `S3_REGION` (default: 'us-east-1')
- **Use**: `StorageManager.upload_audio_to_s3()` method

### 3. MongoDB Document Structure
Create `transcription_data` dictionary with:
```python
{
    # Original annotation data
    'annotations': [...],  # From JSON file
    'id': int,  # From JSON file
    'filename': str,  # From JSON file
    
    # Additional metadata
    'ref_text': str,  # From ref_text.txt
    'audio_path': str,  # S3 key or folder_id based path
    'audio_duration': float,  # Calculated from audio file
    'transcription_type': 'annotations',  # To distinguish from words/phrases
    'source_folder_id': str,  # The folder ID
    'language': str | None,  # Detect or infer from annotations if possible
    'total_annotations': int  # Count of annotations
}
```

### 4. MongoDB Insertion
- **Collection**: `transcriptions` (default)
- **Use**: `StorageManager.save_to_mongodb()` method
- **user_id**: Set to 'anonymous' or from configuration
- **assigned_user_id**: Set to None

## Implementation Plan

### Phase 1: Preparation
1. ✅ Examine MongoDB schema (completed)
2. ✅ Analyze data structure (completed)
3. Create upload script

### Phase 2: Script Development
1. Create `upload_s3_data.py` script with:
   - Function to scan `data/s3_data/` directory
   - Function to process each folder
   - Function to upload audio to S3
   - Function to create MongoDB document
   - Function to handle errors and retries
   - Progress tracking and logging

### Phase 3: Validation
1. Test with a single folder first
2. Verify:
   - Audio file uploaded to S3 correctly
   - S3 metadata generated correctly
   - MongoDB document created with correct structure
   - All data (JSON, ref_text) preserved
3. Check for edge cases:
   - Missing files (audio.mp3, JSON, ref_text.txt)
   - Invalid JSON
   - Corrupted audio files

### Phase 4: Batch Processing
1. Process all folders with:
   - Progress bar/status updates
   - Error handling and logging
   - Skip already processed folders (optional)
   - Resume capability (optional)

## Error Handling

### Missing Files
- If `audio.mp3` missing: Log error and skip folder
- If `{folder_id}.json` missing: Log error and skip folder
- If `ref_text.txt` missing: Log warning, continue with empty ref_text

### Upload Failures
- S3 upload failure: Retry with exponential backoff (max 3 retries)
- MongoDB insertion failure: Log error, keep S3 upload (or rollback S3)

### Duplicate Handling
- Check if document with same folder_id already exists
- Option to skip or update existing documents

## Environment Variables Required

```bash
# S3 Configuration
S3_BUCKET_NAME=transcription-audio-files  # or custom bucket name
S3_REGION=us-east-1  # or your region
AWS_ACCESS_KEY_ID=your_access_key  # or ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=your_secret_key  # or SECRET_ACCESS_KEY

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017/  # or your MongoDB URI
MONGODB_DATABASE=transcription_db  # or custom database name
MONGODB_COLLECTION=transcriptions  # or custom collection name
```

## Dependencies
- `boto3` - S3 upload
- `pymongo` - MongoDB operations
- `pydub` - Audio duration calculation (already in utils)
- Standard library: `os`, `json`, `pathlib`

## Output/Logging
- Progress: Console output with folder count and current folder
- Errors: Log to console with detailed error messages
- Summary: Report total processed, successful, failed at end

## Next Steps
1. Review and approve this plan
2. Create the upload script
3. Test with sample folder
4. Execute batch upload

