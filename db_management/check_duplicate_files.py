#!/usr/bin/env python3
"""
Script to check for duplicate files in MongoDB by filename.

Checks for duplicates based on filename extracted from:
- transcription_data.metadata.filename (primary)
- transcription_data.audio_path (fallback)
- s3_metadata.key (fallback)

Environment Variables Required:
- MONGODB_URI: MongoDB connection URI (default: mongodb://localhost:27017/)
- MONGODB_DATABASE: MongoDB database name (default: transcription_db)
- MONGODB_COLLECTION: MongoDB collection name (default: transcriptions)
"""
import os
import sys
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Optional

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv is optional, continue without it
    pass

try:
    from pymongo import MongoClient
except ImportError:
    print("❌ Error: pymongo is not installed. Please install it with: pip install pymongo")
    sys.exit(1)


def get_mongodb_connection():
    """Connect to MongoDB and return collection."""
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    mongodb_database = os.getenv('MONGODB_DATABASE', 'transcription_db')
    mongodb_collection = os.getenv('MONGODB_COLLECTION', 'transcriptions')
    
    try:
        client = MongoClient(mongodb_uri)
        # Test connection
        client.admin.command('ping')
        db = client[mongodb_database]
        collection = db[mongodb_collection]
        
        print(f"✅ Connected to MongoDB: {mongodb_database}")
        print(f"   Collection: {mongodb_collection}")
        return client, collection
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        sys.exit(1)


def extract_filename(doc: Dict[str, Any]) -> Optional[str]:
    """Extract filename from document."""
    # Priority order:
    # 1. transcription_data.metadata.filename
    # 2. transcription_data.audio_path (extract filename)
    # 3. s3_metadata.key (extract filename)
    
    transcription_data = doc.get('transcription_data', {})
    metadata = transcription_data.get('metadata', {})
    
    # First priority: metadata.filename
    filename = metadata.get('filename')
    if filename:
        return filename
    
    # Second priority: audio_path
    audio_path = transcription_data.get('audio_path') or metadata.get('audio_path')
    if audio_path:
        # Extract filename from path
        if '/' in audio_path:
            return audio_path.split('/')[-1]
        return audio_path
    
    # Third priority: S3 key
    s3_metadata = doc.get('s3_metadata', {})
    s3_key = s3_metadata.get('key', '')
    if s3_key:
        # Extract filename from S3 key (remove timestamp prefix if present)
        s3_filename = s3_key.split('/')[-1] if '/' in s3_key else s3_key
        # Try to remove timestamp prefix (format: YYYYMMDD_HHMMSS_filename)
        import re
        match = re.match(r'^\d{8}_\d{6}_(.+)$', s3_filename)
        if match:
            return match.group(1)
        return s3_filename
    
    return None


def find_duplicates_by_filename(collection) -> Dict[str, List[Dict[str, Any]]]:
    """Find duplicates based on filename."""
    print("\n🔍 Checking for duplicates by filename...")
    
    filename_to_docs = defaultdict(list)
    
    # Get all documents
    cursor = collection.find({})
    total_docs = collection.count_documents({})
    
    processed = 0
    for doc in cursor:
        processed += 1
        if processed % 100 == 0:
            print(f"   Processed {processed}/{total_docs} documents...", end='\r')
        
        filename = extract_filename(doc)
        if filename:
            filename_to_docs[filename].append({
                '_id': str(doc['_id']),
                'created_at': doc.get('created_at', 'N/A'),
                's3_key': doc.get('s3_metadata', {}).get('key', 'N/A'),
                'audio_path': doc.get('transcription_data', {}).get('audio_path', 'N/A'),
                'user_id': doc.get('user_id', 'N/A'),
                'assigned_user_id': doc.get('assigned_user_id', 'N/A'),
                'is_flagged': doc.get('is_flagged', False),
                'manual_status': doc.get('manual_status', 'N/A')
            })
    
    print(f"   Processed {processed}/{total_docs} documents...")
    
    # Filter to only duplicates (more than one document per filename)
    duplicates = {filename: docs for filename, docs in filename_to_docs.items() if len(docs) > 1}
    
    return duplicates


def find_duplicates_by_s3_key(collection) -> Dict[str, List[Dict[str, Any]]]:
    """Find duplicates based on S3 key."""
    print("\n🔍 Checking for duplicates by S3 key...")
    
    s3_key_to_docs = defaultdict(list)
    
    # Get all documents
    cursor = collection.find({})
    total_docs = collection.count_documents({})
    
    processed = 0
    for doc in cursor:
        processed += 1
        if processed % 100 == 0:
            print(f"   Processed {processed}/{total_docs} documents...", end='\r')
        
        s3_key = doc.get('s3_metadata', {}).get('key', '')
        if s3_key:
            filename = extract_filename(doc)
            s3_key_to_docs[s3_key].append({
                '_id': str(doc['_id']),
                'filename': filename or 'N/A',
                'created_at': doc.get('created_at', 'N/A'),
                'audio_path': doc.get('transcription_data', {}).get('audio_path', 'N/A'),
                'user_id': doc.get('user_id', 'N/A'),
                'assigned_user_id': doc.get('assigned_user_id', 'N/A'),
                'is_flagged': doc.get('is_flagged', False),
                'manual_status': doc.get('manual_status', 'N/A')
            })
    
    print(f"   Processed {processed}/{total_docs} documents...")
    
    # Filter to only duplicates
    duplicates = {s3_key: docs for s3_key, docs in s3_key_to_docs.items() if len(docs) > 1}
    
    return duplicates


def find_duplicates_by_audio_path(collection) -> Dict[str, List[Dict[str, Any]]]:
    """Find duplicates based on audio path."""
    print("\n🔍 Checking for duplicates by audio path...")
    
    audio_path_to_docs = defaultdict(list)
    
    # Get all documents
    cursor = collection.find({})
    total_docs = collection.count_documents({})
    
    processed = 0
    for doc in cursor:
        processed += 1
        if processed % 100 == 0:
            print(f"   Processed {processed}/{total_docs} documents...", end='\r')
        
        transcription_data = doc.get('transcription_data', {})
        audio_path = transcription_data.get('audio_path') or transcription_data.get('metadata', {}).get('audio_path', '')
        
        if audio_path:
            filename = extract_filename(doc)
            audio_path_to_docs[audio_path].append({
                '_id': str(doc['_id']),
                'filename': filename or 'N/A',
                'created_at': doc.get('created_at', 'N/A'),
                's3_key': doc.get('s3_metadata', {}).get('key', 'N/A'),
                'user_id': doc.get('user_id', 'N/A'),
                'assigned_user_id': doc.get('assigned_user_id', 'N/A'),
                'is_flagged': doc.get('is_flagged', False),
                'manual_status': doc.get('manual_status', 'N/A')
            })
    
    print(f"   Processed {processed}/{total_docs} documents...")
    
    # Filter to only duplicates
    duplicates = {audio_path: docs for audio_path, docs in audio_path_to_docs.items() if len(docs) > 1}
    
    return duplicates


def format_datetime(dt) -> str:
    """Format datetime object to string."""
    if isinstance(dt, datetime):
        return dt.isoformat()
    return str(dt)


def print_duplicates(duplicates: Dict[str, List[Dict[str, Any]]], title: str):
    """Print duplicate results."""
    if not duplicates:
        print(f"\n✅ No duplicates found by {title}")
        return
    
    print(f"\n{'='*80}")
    print(f"📋 Duplicates by {title}: {len(duplicates)} unique {title}(s) with duplicates")
    print(f"{'='*80}")
    
    total_duplicate_docs = 0
    for key, docs in sorted(duplicates.items()):
        duplicate_count = len(docs) - 1  # Number of extra documents (duplicates)
        total_duplicate_docs += duplicate_count
        
        print(f"\n🔴 {title}: {key}")
        print(f"   Found {len(docs)} document(s) ({(duplicate_count)} duplicate(s)):")
        
        for i, doc in enumerate(docs, 1):
            print(f"   [{i}] Document ID: {doc['_id']}")
            if 'filename' in doc:
                print(f"       Filename: {doc['filename']}")
            if 's3_key' in doc:
                print(f"       S3 Key: {doc['s3_key']}")
            if 'audio_path' in doc:
                print(f"       Audio Path: {doc['audio_path']}")
            print(f"       Created: {format_datetime(doc['created_at'])}")
            print(f"       User ID: {doc['user_id']}")
            print(f"       Assigned User ID: {doc['assigned_user_id']}")
            print(f"       Flagged: {doc['is_flagged']}")
            print(f"       Status: {doc['manual_status']}")
    
    print(f"\n📊 Summary: {len(duplicates)} unique {title}(s) with duplicates")
    print(f"   Total duplicate documents: {total_duplicate_docs}")


def export_duplicates_to_json(duplicates: Dict[str, List[Dict[str, Any]]], 
                               title: str, output_dir: Path):
    """Export duplicates to JSON file."""
    if not duplicates:
        return None
    
    # Format data for JSON export
    export_data = {
        'check_type': title,
        'timestamp': datetime.now().isoformat(),
        'total_duplicate_groups': len(duplicates),
        'duplicates': {}
    }
    
    for key, docs in duplicates.items():
        export_data['duplicates'][key] = []
        for doc in docs:
            export_data['duplicates'][key].append({
                'document_id': doc['_id'],
                'filename': doc.get('filename', 'N/A'),
                's3_key': doc.get('s3_key', 'N/A'),
                'audio_path': doc.get('audio_path', 'N/A'),
                'created_at': format_datetime(doc['created_at']),
                'user_id': doc.get('user_id', 'N/A'),
                'assigned_user_id': doc.get('assigned_user_id', 'N/A'),
                'is_flagged': doc.get('is_flagged', False),
                'manual_status': doc.get('manual_status', 'N/A')
            })
    
    # Create output filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_title = title.lower().replace(' ', '_')
    output_file = output_dir / f"duplicates_by_{safe_title}.json"
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    return output_file


def main():
    """Main function to check for duplicates by filename only."""
    print("="*80)
    print("🔍 MongoDB Duplicate Files Checker (Filename Only)")
    print("="*80)
    print()
    
    # Display configuration
    print("📋 Configuration:")
    print(f"   MongoDB URI: {os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')}")
    print(f"   MongoDB Database: {os.getenv('MONGODB_DATABASE', 'transcription_db')}")
    print(f"   MongoDB Collection: {os.getenv('MONGODB_COLLECTION', 'transcriptions')}")
    print()
    
    # Connect to MongoDB
    client, collection = get_mongodb_connection()
    
    # Get total document count
    total_docs = collection.count_documents({})
    print(f"📊 Total documents in collection: {total_docs}")
    print()
    
    if total_docs == 0:
        print("⚠️  No documents found in collection.")
        client.close()
        return
    
    # Check for duplicates by filename only
    duplicates_by_filename = find_duplicates_by_filename(collection)
    
    # Print results
    print_duplicates(duplicates_by_filename, "Filename")
    
    # Export to JSON if any duplicates found
    output_dir = Path(__file__).parent
    exported_files = []
    
    if duplicates_by_filename:
        file = export_duplicates_to_json(duplicates_by_filename, "Filename", output_dir)
        if file:
            exported_files.append(file)
    
    # Print summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    
    if len(duplicates_by_filename) == 0:
        print("✅ No filename duplicates found!")
    else:
        print(f"🔴 Found {len(duplicates_by_filename)} filename(s) with duplicates")
        total_duplicate_docs = sum(len(docs) - 1 for docs in duplicates_by_filename.values())
        print(f"   Total duplicate documents: {total_duplicate_docs}")
    
    if exported_files:
        print(f"\n💾 Exported results to:")
        for file in exported_files:
            print(f"   - {file}")
    
    print("\n✨ Duplicate check completed!")
    
    client.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Check interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

