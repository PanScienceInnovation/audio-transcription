"""
Script to upload audio files from data/s3_data/ to S3 and store metadata in MongoDB.
"""
import os
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import time
from dotenv import load_dotenv

# Load environment variables from .env file
script_dir = Path(__file__).parent
env_path = script_dir / '.env'
load_dotenv(dotenv_path=env_path)

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.storage import StorageManager
from utils.audio_utils import get_audio_duration


def parse_timestamp(timestamp_str: str) -> float:
    """Convert timestamp string (HH:MM:SS.microseconds) to seconds."""
    try:
        parts = timestamp_str.split(':')
        hours = float(parts[0])
        minutes = float(parts[1])
        sec_parts = parts[2].split('.')
        seconds = float(sec_parts[0])
        microseconds = float(sec_parts[1]) if len(sec_parts) > 1 else 0.0
        total_seconds = hours * 3600 + minutes * 60 + seconds + microseconds / 1000000
        return total_seconds
    except (ValueError, IndexError) as e:
        print(f"⚠️  Warning: Could not parse timestamp '{timestamp_str}': {e}")
        return 0.0


def read_ref_text(ref_text_path: str) -> str:
    """Read reference text from file."""
    try:
        if not os.path.exists(ref_text_path):
            return ""
        with open(ref_text_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except Exception as e:
        print(f"⚠️  Warning: Could not read ref_text.txt: {e}")
        return ""


def read_json_data(json_path: str) -> Optional[Dict[str, Any]]:
    """Read JSON annotation data from file."""
    try:
        if not os.path.exists(json_path):
            return None
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in {json_path}: {e}")
        return None
    except Exception as e:
        print(f"❌ Error: Could not read JSON file {json_path}: {e}")
        return None


def process_folder(folder_path: str, storage_manager: StorageManager, 
                   user_id: Optional[str] = None) -> Tuple[bool, str]:
    """
    Process a single folder: upload audio to S3 and save metadata to MongoDB.
    
    Args:
        folder_path: Path to the folder containing audio.mp3, {folder_id}.json, ref_text.txt
        storage_manager: StorageManager instance for S3 and MongoDB operations
        user_id: User ID to associate with the transcription (default: 'anonymous')
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    folder_name = os.path.basename(folder_path.rstrip('/'))
    folder_id = folder_name
    
    print(f"\n📁 Processing folder: {folder_id}")
    
    # Define file paths
    audio_path = os.path.join(folder_path, 'audio.mp3')
    json_path = os.path.join(folder_path, f'{folder_id}.json')
    ref_text_path = os.path.join(folder_path, 'ref_text.txt')
    
    # Validate required files exist
    if not os.path.exists(audio_path):
        error_msg = f"❌ Audio file not found: {audio_path}"
        print(error_msg)
        return False, error_msg
    
    if not os.path.exists(json_path):
        error_msg = f"❌ JSON file not found: {json_path}"
        print(error_msg)
        return False, error_msg
    
    # Read JSON data
    json_data = read_json_data(json_path)
    if json_data is None:
        error_msg = f"❌ Failed to read JSON data from {json_path}"
        return False, error_msg
    
    # Read ref_text (optional - continue if missing)
    ref_text = read_ref_text(ref_text_path)
    if not ref_text:
        print(f"⚠️  Warning: ref_text.txt not found or empty for {folder_id}")
    
    # Get audio duration
    try:
        audio_duration = get_audio_duration(audio_path)
        print(f"   Audio duration: {audio_duration:.2f} seconds")
    except Exception as e:
        print(f"⚠️  Warning: Could not calculate audio duration: {e}")
        audio_duration = None
    
    # Get file size
    file_size = os.path.getsize(audio_path)
    
    # Prepare transcription_data for MongoDB
    transcription_data = {
        # Original annotation data from JSON
        'annotations': json_data.get('annotations', []),
        'id': json_data.get('id'),
        'filename': json_data.get('filename', 'audio.mp3'),
        
        # Additional metadata
        'ref_text': ref_text,
        'audio_path': f'{folder_id}_audio.mp3',  # Filename format as requested
        'source_folder_id': folder_id,
        'transcription_type': 'annotations',
        'total_annotations': len(json_data.get('annotations', []))
    }
    
    # Add audio_duration if available
    if audio_duration is not None:
        transcription_data['audio_duration'] = audio_duration
    
    # Try to infer language from annotations (if possible)
    # This is optional - can be added later if needed
    if json_data.get('language'):
        transcription_data['language'] = json_data['language']
    
    # Prepare S3 key
    s3_key = f"audio/{folder_id}_audio.mp3"
    
    # Upload to S3
    print(f"   Uploading to S3: {s3_key}")
    s3_result = storage_manager.upload_audio_to_s3(audio_path, s3_key)
    
    if not s3_result['success']:
        error_msg = f"❌ S3 upload failed: {s3_result.get('error', 'Unknown error')}"
        print(f"   {error_msg}")
        return False, error_msg
    
    s3_metadata = s3_result['metadata']
    print(f"   ✅ S3 upload successful: {s3_metadata.get('url', s3_key)}")
    
    # Save to MongoDB
    print(f"   Saving to MongoDB...")
    mongo_result = storage_manager.save_to_mongodb(
        transcription_data=transcription_data,
        s3_metadata=s3_metadata,
        user_id=user_id
    )
    
    if not mongo_result['success']:
        error_msg = f"❌ MongoDB save failed: {mongo_result.get('error', 'Unknown error')}"
        print(f"   {error_msg}")
        # Note: S3 upload succeeded, but MongoDB failed
        # We keep the S3 upload but log the error
        return False, error_msg
    
    mongo_id = mongo_result.get('document_id', 'N/A')
    print(f"   ✅ MongoDB save successful: {mongo_id}")
    
    return True, f"Successfully processed {folder_id} (MongoDB ID: {mongo_id})"


def check_duplicate(folder_id: str, storage_manager: StorageManager) -> bool:
    """
    Check if a transcription with this folder_id already exists in MongoDB.
    
    Args:
        folder_id: Folder ID to check
        storage_manager: StorageManager instance
        
    Returns:
        True if duplicate exists, False otherwise
    """
    try:
        if not storage_manager.collection:
            return False
        
        # Check if any document has this folder_id in transcription_data.source_folder_id
        existing = storage_manager.collection.find_one({
            'transcription_data.source_folder_id': folder_id
        })
        
        return existing is not None
    except Exception as e:
        print(f"⚠️  Warning: Error checking for duplicates: {e}")
        return False


def main():
    """Main function to process all folders in data/s3_data/."""
    # Configuration
    s3_data_dir = os.path.join(os.path.dirname(__file__), 'data', 's3_data')
    user_id = os.getenv('UPLOAD_USER_ID', 'anonymous')  # Optional: set user ID
    
    # Check if directory exists
    if not os.path.exists(s3_data_dir):
        print(f"❌ Error: Directory not found: {s3_data_dir}")
        sys.exit(1)
    
    print("🚀 Starting S3 Data Upload Process")
    print(f"   Source directory: {s3_data_dir}")
    print(f"   User ID: {user_id}")
    print()
    
    # Initialize storage manager
    print("📦 Initializing StorageManager...")
    storage_manager = StorageManager()
    
    if not storage_manager.s3_client:
        print("❌ Error: S3 client not initialized. Please check AWS credentials.")
        sys.exit(1)
    
    if not storage_manager.collection:
        print("❌ Error: MongoDB not initialized. Please check MongoDB connection.")
        sys.exit(1)
    
    print("✅ StorageManager initialized successfully")
    print()
    
    # Get all folders
    folders = []
    for item in os.listdir(s3_data_dir):
        item_path = os.path.join(s3_data_dir, item)
        if os.path.isdir(item_path) and not item.startswith('.'):
            folders.append(item_path)
    
    total_folders = len(folders)
    print(f"📊 Found {total_folders} folders to process")
    print()
    
    if total_folders == 0:
        print("⚠️  No folders found to process.")
        sys.exit(0)
    
    # Process folders
    successful = 0
    failed = 0
    skipped = 0
    errors = []
    
    # Option to check for duplicates and skip them
    check_duplicates = os.getenv('SKIP_DUPLICATES', 'false').lower() == 'true'
    
    for i, folder_path in enumerate(folders, 1):
        folder_id = os.path.basename(folder_path)
        
        print(f"[{i}/{total_folders}] Processing: {folder_id}")
        
        # Check for duplicates if enabled
        if check_duplicates:
            if check_duplicate(folder_id, storage_manager):
                print(f"   ⏭️  Skipping {folder_id} - already exists in MongoDB")
                skipped += 1
                continue
        
        # Process folder
        success, message = process_folder(folder_path, storage_manager, user_id)
        
        if success:
            successful += 1
        else:
            failed += 1
            errors.append(f"{folder_id}: {message}")
        
        # Small delay to avoid rate limiting
        if i < total_folders:
            time.sleep(0.1)
    
    # Print summary
    print()
    print("=" * 60)
    print("📊 UPLOAD SUMMARY")
    print("=" * 60)
    print(f"   Total folders: {total_folders}")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    if check_duplicates:
        print(f"   ⏭️  Skipped (duplicates): {skipped}")
    print()
    
    if errors:
        print("❌ Errors encountered:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"   - {error}")
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more errors")
        print()
    
    print("✨ Upload process completed!")
    
    if failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()

