#!/usr/bin/env python3
"""
Script to delete flagged files from MongoDB and S3 bucket based on filename prefixes

Environment Variables Required:
- MONGODB_URI: MongoDB connection URI (default: mongodb://localhost:27017/)
- MONGODB_DATABASE: MongoDB database name (default: transcription_db)
- MONGODB_COLLECTION: MongoDB collection name (default: transcriptions)
- S3_BUCKET_NAME: S3 bucket name (default: transcription-audio-files)
- S3_REGION: S3 region (default: us-east-1)
- AWS_ACCESS_KEY_ID or ACCESS_KEY_ID: AWS access key
- AWS_SECRET_ACCESS_KEY or SECRET_ACCESS_KEY: AWS secret key
"""
import os
import sys
from pathlib import Path

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv is optional, continue without it
    pass

# Add project root to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.storage import StorageManager

# Filenames provided by user
filenames = [
    "5191919_audio.mp3",
"7242936_audio.mp3",
"7238581_audio.mp3",
"5326330_audio.mp3",
"5439399_audio.mp3",
"5241317_audio.mp3",
"5534960_audio.mp3",
"5175503_audio.mp3",
"6576242_audio.mp3",
"7238783_audio.mp3",
"5195921_audio.mp3",
"5194458_audio.mp3",
"5178165_audio.mp3",
"5540130_audio.mp3",
"5497812_audio.mp3",
"5632958_audio.mp3",
"7239384_audio.mp3",
"5313169_audio.mp3",
"5269009_audio.mp3",
"5225319_audio.mp3",

"5143775_audio.mp3",
"5817954_audio.mp3",
"5764023_audio.mp3",
"7242832_audio.mp3",
"5625363_audio.mp3",
"5193740_audio.mp3",
"6989786_audio.mp3",
"6106493_audio.mp3",
"5702005_audio.mp3",
"5196097_audio.mp3",
"5143758_audio.mp3",
"5191507_audio.mp3",
"5755425_audio.mp3",
"5175383_audio.mp3",
"5289317_audio.mp3",
"7231410_audio.mp3",
"5630791_audio.mp3",
"5143282_audio.mp3",
"7235847_audio.mp3",
"7242129_audio.mp3"

]

# Extract prefixes (remove _audio.mp3 or .mp3)
prefixes = set()
for filename in filenames:
    if "_audio.mp3" in filename:
        prefix = filename.replace("_audio.mp3", "")
    elif filename.endswith(".mp3"):
        prefix = filename.replace(".mp3", "")
    else:
        prefix = filename
    prefixes.add(prefix)


def find_documents_by_prefix(prefix: str, storage_manager: StorageManager):
    """
    Find MongoDB documents matching a prefix.
    Searches in multiple fields to find documents.
    """
    if not storage_manager.collection:
        return []
    
    documents = []
    
    # Search patterns:
    # 1. metadata.filename = "{prefix}_audio.mp3"
    # 2. metadata.filename contains prefix
    # 3. s3_metadata.key contains prefix
    # 4. transcription_data.audio_path contains prefix
    
    search_patterns = [
        {'transcription_data.metadata.filename': f"{prefix}_audio.mp3"},
        {'transcription_data.metadata.filename': {'$regex': f"^{prefix}", '$options': 'i'}},
        {'s3_metadata.key': {'$regex': prefix, '$options': 'i'}},
        {'transcription_data.audio_path': {'$regex': prefix, '$options': 'i'}}
    ]
    
    found_ids = set()
    for pattern in search_patterns:
        results = storage_manager.collection.find(pattern)
        for doc in results:
            doc_id = str(doc['_id'])
            if doc_id not in found_ids:
                found_ids.add(doc_id)
                documents.append(doc)
    
    return documents


def delete_flagged_files():
    """Delete flagged files from MongoDB and S3"""
    
    print("🚀 Starting Flagged Files Deletion")
    print()
    
    # Display configuration (without sensitive values)
    print("📋 Configuration:")
    print(f"   MongoDB URI: {os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')}")
    print(f"   MongoDB Database: {os.getenv('MONGODB_DATABASE', 'transcription_db')}")
    print(f"   MongoDB Collection: {os.getenv('MONGODB_COLLECTION', 'transcriptions')}")
    print(f"   S3 Bucket: {os.getenv('S3_BUCKET_NAME', 'transcription-audio-files')}")
    print(f"   S3 Region: {os.getenv('S3_REGION', 'us-east-1')}")
    aws_key = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('ACCESS_KEY_ID')
    print(f"   AWS Access Key: {'✅ Set' if aws_key else '❌ Not set'}")
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('SECRET_ACCESS_KEY')
    print(f"   AWS Secret Key: {'✅ Set' if aws_secret else '❌ Not set'}")
    print()
    
    # Initialize storage manager
    print("📦 Initializing StorageManager...")
    storage_manager = StorageManager()
    
    if not storage_manager.s3_client:
        print("⚠️  Warning: S3 client not initialized. S3 deletions will be skipped.")
        print("   Please check AWS credentials if you want to delete from S3.")
    
    if not storage_manager.collection:
        print("❌ Error: MongoDB not initialized. Please check MongoDB connection.")
        sys.exit(1)
    
    print("✅ StorageManager initialized successfully")
    print()
    
    # Get all prefixes sorted
    sorted_prefixes = sorted(prefixes)
    print(f"📋 Found {len(sorted_prefixes)} unique prefixes to delete\n")
    
    total_deleted_mongo = 0
    total_deleted_s3 = 0
    total_not_found = 0
    total_errors = 0
    errors = []
    
    for idx, prefix in enumerate(sorted_prefixes, 1):
        print(f"[{idx}/{len(sorted_prefixes)}] Processing: {prefix}")
        
        # Find documents matching this prefix
        documents = find_documents_by_prefix(prefix, storage_manager)
        
        if not documents:
            print(f"  ⚠️  No documents found for prefix: {prefix}")
            total_not_found += 1
            continue
        
        print(f"  📄 Found {len(documents)} document(s) for prefix: {prefix}")
        
        # Delete each document
        for doc in documents:
            doc_id = str(doc['_id'])
            s3_metadata = doc.get('s3_metadata', {})
            s3_key = s3_metadata.get('key', '')
            filename = doc.get('transcription_data', {}).get('metadata', {}).get('filename', 'N/A')
            
            print(f"    Deleting document: {doc_id} (filename: {filename})")
            
            # Use the delete_transcription method which handles both MongoDB and S3
            result = storage_manager.delete_transcription(doc_id)
            
            if result.get('success'):
                total_deleted_mongo += 1
                if result.get('s3_deleted'):
                    total_deleted_s3 += 1
                print(f"    ✅ Successfully deleted: {doc_id}")
            else:
                total_errors += 1
                error_msg = result.get('error', 'Unknown error')
                errors.append(f"{prefix} ({doc_id}): {error_msg}")
                print(f"    ❌ Error deleting {doc_id}: {error_msg}")
    
    # Print summary
    print(f"\n{'='*80}")
    print("📊 DELETION SUMMARY")
    print(f"{'='*80}")
    print(f"Total prefixes: {len(sorted_prefixes)}")
    print(f"✅ Successfully deleted from MongoDB: {total_deleted_mongo}")
    print(f"✅ Successfully deleted from S3: {total_deleted_s3}")
    print(f"⚠️  Not found in MongoDB: {total_not_found}")
    print(f"❌ Errors: {total_errors}")
    print(f"{'='*80}\n")
    
    if errors:
        print("❌ Files with errors:")
        for error in errors:
            print(f"  - {error}")
        print()
    
    return total_deleted_mongo, total_deleted_s3, total_not_found, total_errors


if __name__ == "__main__":
    try:
        # Ask for confirmation
        print("⚠️  WARNING: This will permanently delete files from MongoDB and S3!")
        print("   Make sure you have backed up the data if needed.")
        print()
        response = input("Do you want to continue? (yes/no): ").strip().lower()
        
        if response not in ['yes', 'y']:
            print("❌ Deletion cancelled by user.")
            sys.exit(0)
        
        print()
        deleted_mongo, deleted_s3, not_found, errors = delete_flagged_files()
        
        if errors == 0:
            print("✅ All deletions completed successfully!")
            sys.exit(0)
        else:
            print(f"⚠️  Completed with {errors} error(s).")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Deletion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

