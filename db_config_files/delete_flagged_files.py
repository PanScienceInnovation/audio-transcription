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

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.storage import StorageManager

# Filenames provided by user
filenames = [
    "5540130_audio.mp3", "5497812_audio.mp3", "5632958_audio.mp3", "7239384_audio.mp3",
    "5313169_audio.mp3", "5269009_audio.mp3", "5225319_audio.mp3", "5817954_audio.mp3",
    "7231304_audio.mp3", "5764023_audio.mp3", "5755425_audio.mp3", "5289317_audio.mp3",
    "7231410_audio.mp3", "5630791_audio.mp3", "7235847_audio.mp3", "7242129_audio.mp3",
    "7232136_audio.mp3", "5294171_audio.mp3", "Sample15.mp3", "Sample8.mp3",
    "test9_audio.mp3", "test5_audio.mp3", "7242936_audio.mp3", "7238581_audio.mp3",
    "5326330_audio.mp3", "5439399_audio.mp3", "5241317_audio.mp3", "5534960_audio.mp3",
    "6576242_audio.mp3", "7241742_audio.mp3", "7238783_audio.mp3", "5241240_audio.mp3",
    "7242832_audio.mp3", "6704098_audio.mp3", "7121322_audio.mp3", "5625363_audio.mp3",
    "6989786_audio.mp3", "6106493_audio.mp3", "7238023_audio.mp3", "5702005_audio.mp3",
    "5573799_audio.mp3", "5568276_audio.mp3", "5652592_audio.mp3", "7243018_audio.mp3",
    "5548475_audio.mp3", "5543432_audio.mp3", "5702963_audio.mp3", "5389423_audio.mp3",
    "5491865_audio.mp3", "5826896_audio.mp3", "6515465_audio.mp3", "5275030_audio.mp3",
    "7107232_audio.mp3", "5537890_audio.mp3", "6497925_audio.mp3", "5359711_audio.mp3",
    "5329654_audio.mp3", "7237846_audio.mp3", "5738313_audio.mp3", "5782327_audio.mp3",
    "5774494_audio.mp3", "5681423_audio.mp3", "5454621_audio.mp3", "6348357_audio.mp3",
    "7231979_audio.mp3", "7070074_audio.mp3", "5234692_audio.mp3", "7240313_audio.mp3",
    "5699445_audio.mp3", "7235586_audio.mp3", "7193756_audio.mp3", "5207416_audio.mp3",
    "5309899_audio.mp3", "6117174_audio.mp3", "5698775_audio.mp3", "7242631_audio.mp3",
    "7242056_audio.mp3", "5619827_audio.mp3", "5278812_audio.mp3", "7231245_audio.mp3",
    "5306717_audio.mp3", "6626590_audio.mp3", "5283488_audio.mp3", "7222632_audio.mp3",
    "7234757_audio.mp3", "5799602_audio.mp3", "5619420_audio.mp3", "7237573_audio.mp3",
    "7231849_audio.mp3", "7115832_audio.mp3", "5264473_audio.mp3", "5548031_audio.mp3",
    "7235031_audio.mp3", "5529276_audio.mp3", "5751493_audio.mp3", "5264258_audio.mp3",
    "5512864_audio.mp3", "5240293_audio.mp3", "7237351_audio.mp3", "5723221_audio.mp3",
    "7071609_audio.mp3", "5601684_audio.mp3", "5230569_audio.mp3", "6176431_audio.mp3",
    "6712828_audio.mp3", "5678820_audio.mp3", "5765241_audio.mp3", "7240023_audio.mp3",
    "5485825_audio.mp3", "7241561_audio.mp3", "5508716_audio.mp3", "7239444_audio.mp3",
    "7044344_audio.mp3", "5721959_audio.mp3", "5229624_audio.mp3", "7236456_audio.mp3",
    "5562703_audio.mp3", "5198847_audio.mp3", "5198572_audio.mp3", "5197967_audio.mp3",
    "5196097_audio.mp3", "5195985_audio.mp3", "5195921_audio.mp3", "5195013_audio.mp3",
    "5194468_audio.mp3", "5194458_audio.mp3", "5194424_audio.mp3", "5193918_audio.mp3",
    "5193740_audio.mp3", "5191919_audio.mp3", "5191507_audio.mp3", "5191390_audio.mp3",
    "5186252_audio.mp3", "5184199_audio.mp3", "5182357_audio.mp3", "5178165_audio.mp3",
    "5175503_audio.mp3", "5175383_audio.mp3", "5175374_audio.mp3", "5169582_audio.mp3",
    "5169456_audio.mp3", "5158612_audio.mp3", "5143857_audio.mp3", "5143844_audio.mp3",
    "5143801_audio.mp3", "5143775_audio.mp3", "5143758_audio.mp3", "5143331_audio.mp3",
    "5143282_audio.mp3"
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

