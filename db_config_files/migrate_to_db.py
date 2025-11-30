#!/usr/bin/env python3
"""
Script to process flagged data from data/flagged_data/data_2:
- Upload audio.mp3 files to S3
- Transform and save transcription metadata to MongoDB
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


def detect_language(text: str) -> str:
    """Detect language from text (simple heuristic)."""
    # Common language detection based on character ranges
    if any('\u0980' <= char <= '\u09FF' for char in text):  # Bengali
        return "Bengali"
    elif any('\u0A80' <= char <= '\u0AFF' for char in text):  # Gujarati
        return "Gujarati"
    elif any('\u0900' <= char <= '\u097F' for char in text):  # Hindi/Devanagari
        return "Hindi"
    elif any('\u0C80' <= char <= '\u0CFF' for char in text):  # Kannada
        return "Kannada"
    elif any('\u0D00' <= char <= '\u0D7F' for char in text):  # Malayalam
        return "Malayalam"
    elif any('\u0B00' <= char <= '\u0B7F' for char in text):  # Odia
        return "Odia"
    elif any('\u0A00' <= char <= '\u0A7F' for char in text):  # Gurmukhi (Punjabi)
        return "Punjabi"
    elif any('\u0B80' <= char <= '\u0BFF' for char in text):  # Tamil
        return "Tamil"
    elif any('\u0C00' <= char <= '\u0C7F' for char in text):  # Telugu
        return "Telugu"
    else:
        return "English"  # Default


def clean_word(word: str) -> str:
    """Remove HTML-like tags from word (e.g., <AI>...</AI>)."""
    import re
    # Remove <AI>...</AI> tags
    word = re.sub(r'<AI>.*?</AI>', '', word)
    # Remove any remaining HTML-like tags
    word = re.sub(r'<[^>]+>', '', word)
    return word.strip()


def transform_annotations_to_words(annotations: list, language: Optional[str] = None) -> Tuple[list, str]:
    """
    Transform annotations format to words format for MongoDB.
    
    Args:
        annotations: List of annotation objects with 'start', 'end', 'Transcription'
        language: Optional language hint
        
    Returns:
        Tuple of (words list, detected language)
    """
    words = []
    detected_language = language or "Gujarati"  # Default based on sample data
    
    for annotation in annotations:
        start_str = annotation.get('start', '0:00:00.000000')
        end_str = annotation.get('end', '0:00:00.000000')
        transcription_list = annotation.get('Transcription', [])
        
        if not transcription_list:
            continue
        
        # Get the word (first element of Transcription array)
        word_text = transcription_list[0] if isinstance(transcription_list, list) else str(transcription_list)
        
        # Clean the word (remove HTML tags)
        word_text = clean_word(word_text)
        
        if not word_text:
            continue
        
        # Detect language from first word if not provided
        if not language:
            detected_language = detect_language(word_text)
        
        # Parse timestamps
        start_seconds = parse_timestamp(start_str)
        end_seconds = parse_timestamp(end_str)
        duration = end_seconds - start_seconds
        
        # Create word object
        word_obj = {
            'start': start_str,
            'end': end_str,
            'duration': round(duration, 2),
            'word': word_text,
            'language': detected_language
        }
        
        words.append(word_obj)
    
    return words, detected_language


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
        folder_path: Path to the folder containing audio.mp3, transcriptions/{folder_id}.json, ref_text.txt
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
    transcriptions_dir = os.path.join(folder_path, 'transcriptions')
    
    # Find JSON file in transcriptions folder
    json_files = []
    if os.path.exists(transcriptions_dir):
        for file in os.listdir(transcriptions_dir):
            if file.endswith('.json'):
                json_files.append(os.path.join(transcriptions_dir, file))
    
    if not json_files:
        error_msg = f"❌ No JSON file found in transcriptions folder: {transcriptions_dir}"
        print(error_msg)
        return False, error_msg
    
    # Use the first JSON file found
    json_path = json_files[0]
    print(f"   Using JSON file: {os.path.basename(json_path)}")
    
    # Validate required files exist
    if not os.path.exists(audio_path):
        error_msg = f"❌ Audio file not found: {audio_path}"
        print(error_msg)
        return False, error_msg
    
    # Read JSON data
    json_data = read_json_data(json_path)
    if json_data is None:
        error_msg = f"❌ Failed to read JSON data from {json_path}"
        return False, error_msg
    
    # Get annotations
    annotations = json_data.get('annotations', [])
    if not annotations:
        error_msg = f"❌ No annotations found in JSON file: {json_path}"
        print(error_msg)
        return False, error_msg
    
    # Transform annotations to words format
    print(f"   Transforming {len(annotations)} annotations to words format...")
    words, detected_language = transform_annotations_to_words(annotations)
    
    if not words:
        error_msg = f"❌ No valid words extracted from annotations"
        print(error_msg)
        return False, error_msg
    
    print(f"   Extracted {len(words)} words, detected language: {detected_language}")
    
    # Get audio duration
    try:
        audio_duration = get_audio_duration(audio_path)
        print(f"   Audio duration: {audio_duration:.2f} seconds")
    except Exception as e:
        print(f"⚠️  Warning: Could not calculate audio duration: {e}")
        audio_duration = 0.0
    
    # Prepare transcription_data for MongoDB (matching the schema)
    transcription_data = {
        'words': words,
        'audio_duration': round(audio_duration, 2),
        'total_words': len(words),
        'transcription_type': 'words',
        'language': detected_language,
        'metadata': {
            'audio_path': f"/api/audio/{folder_id}_audio.mp3",
            'filename': f"{folder_id}_audio.mp3"
        }
    }
    
    # Generate S3 key without timestamp prefix
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
        
        # Check if any document has this folder_id in the filename
        existing = storage_manager.collection.find_one({
            'transcription_data.metadata.filename': f"{folder_id}_audio.mp3"
        })
        
        return existing is not None
    except Exception as e:
        print(f"⚠️  Warning: Error checking for duplicates: {e}")
        return False


def main():
    """Main function to process all folders in data/flagged_data/data_2/."""
    # Configuration
    data_dir = os.path.join(os.path.dirname(__file__), 'data', 'flagged_data', 'data_3')
    user_id = os.getenv('UPLOAD_USER_ID', 'anonymous')  # Optional: set user ID
    
    # Check if directory exists
    if not os.path.exists(data_dir):
        print(f"❌ Error: Directory not found: {data_dir}")
        sys.exit(1)
    
    print("🚀 Starting Flagged Data Processing")
    print(f"   Source directory: {data_dir}")
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
    
    # Get all folders (subdirectories with numeric names)
    folders = []
    for item in os.listdir(data_dir):
        item_path = os.path.join(data_dir, item)
        if os.path.isdir(item_path) and not item.startswith('.'):
            # Check if it's a numeric folder name (or any folder)
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
    print("📊 PROCESSING SUMMARY")
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
    
    print("✨ Processing completed!")
    
    if failed > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()

