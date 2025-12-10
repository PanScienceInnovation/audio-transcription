#!/usr/bin/env python3
"""
Script to upload Telugu transcription data to MongoDB and S3.
- Uploads audio files to S3 bucket in telugu_audios folder
- Transforms JSON transcription data to match MongoDB schema
- Uploads to MongoDB collection telugu_transcriptions
"""
import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List
from pydub import AudioSegment

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Try backend/.env first
    backend_dir = Path(__file__).parent / 'backend'
    env_path = backend_dir / '.env'
    load_dotenv(dotenv_path=env_path)
    
    # Also load from root .env if it exists
    root_env = Path(__file__).parent / '.env'
    if root_env.exists():
        load_dotenv(dotenv_path=root_env)
except ImportError:
    # dotenv is optional, continue without it
    pass

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.storage import StorageManager


def timestamp_to_seconds(timestamp: str) -> float:
    """Convert timestamp string to seconds."""
    # Handle formats: "0:00:01.005000" (H:MM:SS.mmm) or "0:00.153" (MM:SS.mmm)
    parts = timestamp.split(':')
    if len(parts) == 3:  # H:MM:SS.mmm
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
        return hours * 3600 + minutes * 60 + seconds
    elif len(parts) == 2:  # MM:SS.mmm
        minutes = int(parts[0])
        seconds = float(parts[1])
        return minutes * 60 + seconds
    else:
        return float(timestamp)


def seconds_to_timestamp(seconds: float) -> str:
    """Convert seconds to HH:MM:SS.mmm format (e.g., "00:00:00.153", "00:00:02.027")."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    # Format: HH:MM:SS.mmm (2 digits for hours, 2 digits for minutes, 2 digits for seconds, 3 decimal places)
    # secs:05.3f ensures 2 digits before decimal (00.153) and 3 decimal places
    return f"{hours:02d}:{minutes:02d}:{secs:05.3f}"


def transform_json_to_mongodb_schema(json_data: Dict[str, Any], audio_duration: float) -> Dict[str, Any]:
    """
    Transform the JSON transcription format to MongoDB schema.
    
    Input format:
    {
        "id": 1,
        "filename": "file.wav",
        "annotations": [
            {"start": "0:00:01.005000", "end": "0:00:01.395000", "Transcription": ["word"]}
        ]
    }
    
    Output format (transcription_data):
    {
        "words": [
            {
                "start": "0:00.153",
                "end": "0:00.533",
                "word": "word",
                "language": "Telugu",
                "duration": 0.38,
                "is_edited": false,
                "edited_in_review_round": false
            }
        ],
        "language": "Telugu",
        "audio_duration": 17.832,
        "total_words": 20,
        "transcription_type": "words",
        "metadata": {
            "filename": "file.wav",
            "audio_path": ""
        },
        "edited_words_count": 0,
        "review_round_edited_words_count": 0
    }
    """
    annotations = json_data.get('annotations', [])
    filename = json_data.get('filename', '')
    
    words = []
    for annotation in annotations:
        start_str = annotation.get('start', '')
        end_str = annotation.get('end', '')
        transcription_list = annotation.get('Transcription', [])
        
        if not transcription_list:
            continue
        
        word_text = transcription_list[0] if transcription_list else ''
        
        # Convert timestamps to seconds
        start_seconds = timestamp_to_seconds(start_str)
        end_seconds = timestamp_to_seconds(end_str)
        duration = end_seconds - start_seconds
        
        # Convert back to MM:SS.mmm format
        start_formatted = seconds_to_timestamp(start_seconds)
        end_formatted = seconds_to_timestamp(end_seconds)
        
        word_entry = {
            'start': start_formatted,
            'end': end_formatted,
            'word': word_text,
            'language': 'Telugu',
            'duration': duration,
            'is_edited': False,
            'edited_in_review_round': False
        }
        words.append(word_entry)
    
    transcription_data = {
        'words': words,
        'language': 'Telugu',
        'audio_duration': audio_duration,
        'total_words': len(words),
        'transcription_type': 'words',
        'metadata': {
            'filename': filename,
            'audio_path': ''
        },
        'edited_words_count': 0,
        'review_round_edited_words_count': 0
    }
    
    return transcription_data


def get_audio_duration(audio_path: Path) -> float:
    """Get audio duration in seconds."""
    try:
        audio = AudioSegment.from_file(str(audio_path))
        return len(audio) / 1000.0
    except Exception as e:
        print(f"⚠️  Warning: Could not get audio duration for {audio_path}: {e}")
        return 0.0


def upload_telugu_transcriptions():
    """Upload all Telugu transcriptions to MongoDB and S3."""
    script_dir = Path(__file__).parent
    telugu_sample_dir = script_dir / "data" / "telugu_sample"
    
    if not telugu_sample_dir.exists():
        print(f"❌ ERROR: {telugu_sample_dir} directory not found")
        sys.exit(1)
    
    # Initialize storage manager with custom collection name
    storage = StorageManager()
    storage.mongodb_collection = 'telugu_transcriptions'
    
    # Reinitialize MongoDB collection with new name
    if storage.mongo_client:
        storage.db = storage.mongo_client[storage.mongodb_database]
        storage.collection = storage.db[storage.mongodb_collection]
        print(f"✅ Using MongoDB collection: {storage.mongodb_collection}")
    
    if not storage.s3_client:
        print("❌ ERROR: S3 client not initialized. Please check AWS credentials.")
        sys.exit(1)
    
    if not storage.collection:
        print("❌ ERROR: MongoDB collection not initialized. Please check MongoDB connection.")
        sys.exit(1)
    
    # Find all subdirectories with transcriptions
    subdirs = [d for d in telugu_sample_dir.iterdir() if d.is_dir() and (d / "transcriptions").exists()]
    subdirs.sort()
    
    total = len(subdirs)
    print(f"📁 Found {total} transcription directories to process\n")
    
    if total == 0:
        print(f"⚠️  WARNING: No transcription directories found in {telugu_sample_dir}")
        sys.exit(0)
    
    success_count = 0
    error_count = 0
    errors = []
    
    for idx, subdir in enumerate(subdirs, 1):
        print(f"\n{'='*80}")
        print(f"[{idx}/{total}] Processing: {subdir.name}")
        print(f"{'='*80}")
        
        # Find JSON file
        transcription_dir = subdir / "transcriptions"
        json_files = list(transcription_dir.glob("*.json"))
        
        if not json_files:
            print(f"⚠️  WARNING: No JSON file found in {transcription_dir}")
            error_count += 1
            errors.append((subdir.name, "No JSON file found"))
            continue
        
        json_file = json_files[0]  # Take first JSON file
        
        # Find corresponding audio file
        audio_file = telugu_sample_dir / f"{subdir.name}.wav"
        
        if not audio_file.exists():
            print(f"⚠️  WARNING: Audio file not found: {audio_file}")
            error_count += 1
            errors.append((subdir.name, f"Audio file not found: {audio_file.name}"))
            continue
        
        try:
            # Read JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            print(f"📄 JSON file: {json_file.name}")
            print(f"🎵 Audio file: {audio_file.name}")
            
            # Get audio duration
            audio_duration = get_audio_duration(audio_file)
            print(f"⏱️  Audio duration: {audio_duration:.3f} seconds")
            
            # Transform JSON to MongoDB schema
            transcription_data = transform_json_to_mongodb_schema(json_data, audio_duration)
            print(f"📝 Transformed {len(transcription_data['words'])} words")
            
            # Upload audio to S3
            # S3 key format: telugu_audios/{filename}
            s3_key = f"telugu_audios/{audio_file.name}"
            print(f"☁️  Uploading to S3: {s3_key}")
            
            s3_result = storage.upload_audio_to_s3(str(audio_file), s3_key)
            
            if not s3_result['success']:
                print(f"❌ S3 upload failed: {s3_result.get('error', 'Unknown error')}")
                error_count += 1
                errors.append((subdir.name, f"S3 upload failed: {s3_result.get('error')}"))
                continue
            
            s3_metadata = s3_result['metadata']
            print(f"✅ Uploaded to S3: {s3_metadata['url']}")
            
            # Save to MongoDB with all required fields
            print(f"💾 Saving to MongoDB collection: telugu_transcriptions")
            
            # Prepare document with all required fields matching the schema
            document = {
                'transcription_data': transcription_data,
                's3_metadata': s3_metadata,
                'user_id': 'system',
                'assigned_user_id': None,
                'review_round': 0,
                'review_history': [],
                'manual_status': 'pending',  # Set to pending initially
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc)
            }
            
            try:
                result = storage.collection.insert_one(document)
                document_id = str(result.inserted_id)
                print(f"✅ Successfully uploaded {subdir.name}")
                print(f"   MongoDB ID: {document_id}")
                success_count += 1
            except Exception as e:
                print(f"❌ MongoDB save failed: {e}")
                error_count += 1
                errors.append((subdir.name, f"MongoDB save failed: {str(e)}"))
                continue
            
        except Exception as e:
            print(f"❌ ERROR processing {subdir.name}: {e}")
            import traceback
            traceback.print_exc()
            error_count += 1
            errors.append((subdir.name, str(e)))
    
    # Print summary
    print(f"\n{'='*80}")
    print("📊 UPLOAD SUMMARY")
    print(f"{'='*80}")
    print(f"Total directories: {total}")
    print(f"✅ Successfully uploaded: {success_count}")
    print(f"❌ Errors: {error_count}")
    print(f"{'='*80}\n")
    
    if errors:
        print("❌ Files with errors:")
        for file_name, error_msg in errors:
            print(f"  - {file_name}: {error_msg}")
        print()
    
    return success_count, error_count


if __name__ == "__main__":
    try:
        success, errors = upload_telugu_transcriptions()
        sys.exit(0 if errors == 0 else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

