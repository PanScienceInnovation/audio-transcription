"""
Script to duplicate audio files in the database, S3, and MongoDB with new filenames.
Creates copies of specified files with names like Sample1.mp3, Sample2.mp3, etc.
"""
import os
import sys
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timezone
from bson import ObjectId
import boto3
from botocore.exceptions import ClientError

# Load environment variables
script_dir = Path(__file__).parent
# Try backend/.env first, then root .env
backend_env = script_dir / 'backend' / '.env'
root_env = script_dir / '.env'
if backend_env.exists():
    load_dotenv(dotenv_path=backend_env)
elif root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    # Try loading from any .env file
    load_dotenv()

# MongoDB Configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'transcription_db')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION', 'transcriptions')

# S3 Configuration
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'transcription-audio-files')
S3_REGION = os.getenv('S3_REGION', 'us-east-1')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('SECRET_ACCESS_KEY')

# List of files to duplicate
FILES_TO_DUPLICATE = [
    '7238202_audio.mp3',
    '5649469_audio.mp3',
    '5605746_audio.mp3',
    '5538842_audio.mp3',
]


def get_content_type(file_path: str) -> str:
    """Get content type based on file extension."""
    extension = os.path.splitext(file_path)[1].lower()
    content_types = {
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.m4a': 'audio/mp4',
        '.ogg': 'audio/ogg',
        '.flac': 'audio/flac',
        '.aac': 'audio/aac',
    }
    return content_types.get(extension, 'audio/mpeg')


def download_from_s3(s3_client, bucket: str, key: str, local_path: str) -> bool:
    """Download a file from S3 to local path."""
    try:
        s3_client.download_file(bucket, key, local_path)
        print(f"   ✅ Downloaded from S3: {key}")
        return True
    except ClientError as e:
        print(f"   ❌ Error downloading from S3: {e}")
        return False


def upload_to_s3(s3_client, bucket: str, local_path: str, s3_key: str) -> dict:
    """Upload a file to S3 and return metadata."""
    try:
        file_size = os.path.getsize(local_path)
        content_type = get_content_type(local_path)
        
        s3_client.upload_file(
            local_path,
            bucket,
            s3_key,
            ExtraArgs={'ContentType': content_type}
        )
        
        s3_url = f"https://{bucket}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
        
        s3_metadata = {
            'bucket': bucket,
            'key': s3_key,
            'url': s3_url,
            'region': S3_REGION,
            'size_bytes': file_size,
            'uploaded_at': datetime.now(timezone.utc).isoformat()
        }
        
        print(f"   ✅ Uploaded to S3: {s3_key}")
        return {'success': True, 'metadata': s3_metadata}
    except Exception as e:
        print(f"   ❌ Error uploading to S3: {e}")
        return {'success': False, 'error': str(e)}


def find_document_by_filename(collection, filename: str):
    """Find a MongoDB document by filename."""
    # Try to find by metadata.filename first
    query = {'transcription_data.metadata.filename': filename}
    doc = collection.find_one(query)
    
    if doc:
        return doc
    
    # Try to find by audio_path containing the filename
    query = {'transcription_data.metadata.audio_path': {'$regex': filename}}
    doc = collection.find_one(query)
    
    if doc:
        return doc
    
    # Try to find by transcription_data.audio_path
    query = {'transcription_data.audio_path': {'$regex': filename}}
    doc = collection.find_one(query)
    
    return doc


def duplicate_file(collection, s3_client, original_filename: str, new_filename: str, index: int):
    """Duplicate a file: download from S3, upload with new name, create new MongoDB document."""
    print(f"\n[{index}] Processing: {original_filename} -> {new_filename}")
    print("-" * 80)
    
    # Find the original document
    original_doc = find_document_by_filename(collection, original_filename)
    
    if not original_doc:
        print(f"   ❌ Document not found for filename: {original_filename}")
        return False
    
    print(f"   ✅ Found document: {original_doc['_id']}")
    
    # Get S3 key from original document
    s3_metadata = original_doc.get('s3_metadata', {})
    original_s3_key = s3_metadata.get('key', '')
    
    if not original_s3_key:
        print(f"   ❌ No S3 key found in document")
        return False
    
    print(f"   📦 Original S3 key: {original_s3_key}")
    
    # Get transcription data
    transcription_data = original_doc.get('transcription_data', {}).copy()
    
    # Create temporary file for download
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp3') as temp_file:
        temp_local_path = temp_file.name
    
    try:
        # Download from S3
        if not download_from_s3(s3_client, S3_BUCKET_NAME, original_s3_key, temp_local_path):
            return False
        
        # Generate new S3 key
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        new_s3_key = f"audio/{timestamp}_{new_filename}"
        
        # Upload to S3 with new filename
        upload_result = upload_to_s3(s3_client, S3_BUCKET_NAME, temp_local_path, new_s3_key)
        
        if not upload_result.get('success'):
            return False
        
        new_s3_metadata = upload_result['metadata']
        
        # Update transcription_data metadata with new filename
        if 'metadata' not in transcription_data:
            transcription_data['metadata'] = {}
        
        transcription_data['metadata']['filename'] = new_filename
        transcription_data['metadata']['audio_path'] = f"/api/audio/{new_filename}"
        
        # If audio_path exists at root level, update it too
        if 'audio_path' in transcription_data:
            transcription_data['audio_path'] = f"/api/audio/{new_filename}"
        
        # Create new MongoDB document
        new_document = {
            'transcription_data': transcription_data,
            's3_metadata': new_s3_metadata,
            'user_id': original_doc.get('user_id', 'anonymous'),
            'assigned_user_id': None,  # Reset assignment
            'created_at': datetime.now(timezone.utc),
            'updated_at': datetime.now(timezone.utc)
        }
        
        # Insert new document
        result = collection.insert_one(new_document)
        print(f"   ✅ Created new MongoDB document: {result.inserted_id}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error during duplication: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Clean up temporary file
        if os.path.exists(temp_local_path):
            os.unlink(temp_local_path)


def main():
    """Main function to duplicate files."""
    print("=" * 80)
    print("🔄 Duplicating Audio Files with New Filenames")
    print("=" * 80)
    print()
    
    # Initialize S3 client
    try:
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            s3_client = boto3.client(
                's3',
                region_name=S3_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        else:
            s3_client = boto3.client('s3', region_name=S3_REGION)
        print(f"✅ S3 client initialized")
        print(f"   Bucket: {S3_BUCKET_NAME}, Region: {S3_REGION}")
    except Exception as e:
        print(f"❌ Error initializing S3 client: {e}")
        sys.exit(1)
    
    # Connect to MongoDB
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        # Test connection
        client.admin.command('ping')
        print(f"✅ Connected to MongoDB: {MONGODB_DATABASE}")
        print(f"   Collection: {MONGODB_COLLECTION}")
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        sys.exit(1)
    
    print()
    print(f"📋 Files to duplicate: {len(FILES_TO_DUPLICATE)}")
    print()
    
    # Process each file
    successful = 0
    failed = 0
    
    for i, original_filename in enumerate(FILES_TO_DUPLICATE, 21):
        new_filename = f"Sample{i}.mp3"
        
        result = duplicate_file(collection, s3_client, original_filename, new_filename, i)
        
        if result:
            successful += 1
        else:
            failed += 1
    
    # Print summary
    print()
    print("=" * 80)
    print("📊 DUPLICATION SUMMARY")
    print("=" * 80)
    print(f"   Total files: {len(FILES_TO_DUPLICATE)}")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    print()
    print("✨ Duplication completed!")
    
    client.close()


if __name__ == '__main__':
    main()

