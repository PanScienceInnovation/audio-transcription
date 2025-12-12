#!/usr/bin/env python3
"""
Script to find missing audio files in S3 bucket compared to MongoDB records.

This script:
1. Fetches all transcriptions from MongoDB with their S3 keys
2. Lists all objects in the S3 bucket
3. Compares them to find which files are missing from S3
4. Reports the results
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import boto3
from pymongo import MongoClient
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables
backend_dir = Path(__file__).parent / 'backend'
env_path = backend_dir / '.env'
load_dotenv(dotenv_path=env_path)

# Also load from root .env if it exists
root_env = Path(__file__).parent / '.env'
if root_env.exists():
    load_dotenv(dotenv_path=root_env)

def get_mongodb_transcriptions():
    """Fetch all transcriptions from MongoDB with their S3 keys."""
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    mongodb_database = os.getenv('MONGODB_DATABASE', 'transcription_db')
    mongodb_collection = os.getenv('MONGODB_COLLECTION', 'transcriptions')
    
    print(f"📊 Connecting to MongoDB...")
    print(f"   URI: {mongodb_uri}")
    print(f"   Database: {mongodb_database}")
    print(f"   Collection: {mongodb_collection}")
    
    try:
        client = MongoClient(mongodb_uri)
        db = client[mongodb_database]
        collection = db[mongodb_collection]
        
        # Test connection
        client.admin.command('ping')
        print("✅ Connected to MongoDB")
        
        # Fetch all transcriptions with S3 metadata
        print("\n📥 Fetching all transcriptions from MongoDB...")
        cursor = collection.find(
            {},
            {
                '_id': 1,
                's3_metadata.key': 1,
                's3_metadata.url': 1,
                's3_metadata.bucket': 1,
                'transcription_data.metadata.filename': 1,
                'created_at': 1
            }
        )
        
        transcriptions = []
        for doc in cursor:
            s3_metadata = doc.get('s3_metadata', {})
            s3_key = s3_metadata.get('key', '')
            
            if s3_key:
                transcriptions.append({
                    '_id': str(doc['_id']),
                    's3_key': s3_key,
                    's3_url': s3_metadata.get('url', ''),
                    's3_bucket': s3_metadata.get('bucket', ''),
                    'filename': doc.get('transcription_data', {}).get('metadata', {}).get('filename', ''),
                    'created_at': doc.get('created_at', '')
                })
        
        print(f"✅ Found {len(transcriptions)} transcriptions with S3 keys")
        return transcriptions
        
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {str(e)}")
        sys.exit(1)


def get_s3_objects():
    """List all objects in the S3 bucket."""
    s3_bucket_name = os.getenv('S3_BUCKET_NAME', 'transcription-audio-files')
    s3_region = os.getenv('S3_REGION', 'us-east-1')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('SECRET_ACCESS_KEY')
    
    print(f"\n📦 Connecting to S3...")
    print(f"   Bucket: {s3_bucket_name}")
    print(f"   Region: {s3_region}")
    
    try:
        if aws_access_key_id and aws_secret_access_key:
            s3_client = boto3.client(
                's3',
                region_name=s3_region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )
            print("✅ Connected to S3 with credentials")
        else:
            s3_client = boto3.client('s3', region_name=s3_region)
            print("✅ Connected to S3 with default credentials")
        
        # List all objects in the bucket
        print("\n📥 Listing all objects in S3 bucket...")
        s3_objects = []
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=s3_bucket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    s3_objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
        
        print(f"✅ Found {len(s3_objects)} objects in S3 bucket")
        return s3_objects, s3_bucket_name
        
    except Exception as e:
        print(f"❌ Error connecting to S3: {str(e)}")
        sys.exit(1)


def find_missing_files(transcriptions, s3_objects):
    """Compare MongoDB transcriptions with S3 objects to find missing files."""
    print("\n🔍 Comparing MongoDB records with S3 objects...")
    
    # Create sets for comparison
    mongo_s3_keys = {t['s3_key'] for t in transcriptions if t['s3_key']}
    s3_keys = {obj['key'] for obj in s3_objects}
    
    # Find missing files (in MongoDB but not in S3)
    missing_from_s3 = mongo_s3_keys - s3_keys
    
    # Find orphaned files (in S3 but not in MongoDB)
    orphaned_in_s3 = s3_keys - mongo_s3_keys
    
    # Create detailed missing files list
    missing_files_details = []
    for trans in transcriptions:
        if trans['s3_key'] in missing_from_s3:
            missing_files_details.append({
                '_id': trans['_id'],
                's3_key': trans['s3_key'],
                'filename': trans['filename'] or trans['s3_key'].split('/')[-1],
                's3_url': trans['s3_url'],
                'created_at': trans['created_at']
            })
    
    # Create detailed orphaned files list
    orphaned_files_details = []
    for obj in s3_objects:
        if obj['key'] in orphaned_in_s3:
            orphaned_files_details.append({
                'key': obj['key'],
                'size': obj['size'],
                'last_modified': obj['last_modified']
            })
    
    return {
        'missing_from_s3': missing_files_details,
        'orphaned_in_s3': orphaned_files_details,
        'mongo_count': len(transcriptions),
        's3_count': len(s3_objects),
        'mongo_with_s3_key': len(mongo_s3_keys),
        'missing_count': len(missing_from_s3),
        'orphaned_count': len(orphaned_in_s3)
    }


def print_results(results):
    """Print the comparison results in a readable format."""
    print("\n" + "="*80)
    print("📊 COMPARISON RESULTS")
    print("="*80)
    
    print(f"\n📈 Summary:")
    print(f"   MongoDB transcriptions with S3 keys: {results['mongo_with_s3_key']}")
    print(f"   Total MongoDB transcriptions: {results['mongo_count']}")
    print(f"   S3 objects: {results['s3_count']}")
    print(f"   Missing from S3: {results['missing_count']}")
    print(f"   Orphaned in S3 (not in MongoDB): {results['orphaned_count']}")
    
    if results['missing_count'] > 0:
        print(f"\n❌ MISSING FILES FROM S3 ({results['missing_count']} files):")
        print("-" * 80)
        for i, file_info in enumerate(results['missing_from_s3'], 1):
            print(f"\n{i}. MongoDB ID: {file_info['_id']}")
            print(f"   S3 Key: {file_info['s3_key']}")
            print(f"   Filename: {file_info['filename']}")
            print(f"   S3 URL: {file_info['s3_url']}")
            if file_info['created_at']:
                print(f"   Created: {file_info['created_at']}")
    
    if results['orphaned_count'] > 0:
        print(f"\n⚠️  ORPHANED FILES IN S3 (not in MongoDB) ({results['orphaned_count']} files):")
        print("-" * 80)
        for i, file_info in enumerate(results['orphaned_in_s3'], 1):
            print(f"\n{i}. S3 Key: {file_info['key']}")
            print(f"   Size: {file_info['size']:,} bytes ({file_info['size'] / 1024 / 1024:.2f} MB)")
            print(f"   Last Modified: {file_info['last_modified']}")
    
    print("\n" + "="*80)
    
    # Save results to file
    output_file = Path(__file__).parent / 'missing_s3_files_report.txt'
    with open(output_file, 'w') as f:
        f.write("MISSING S3 FILES REPORT\n")
        f.write("="*80 + "\n\n")
        f.write(f"Summary:\n")
        f.write(f"  MongoDB transcriptions with S3 keys: {results['mongo_with_s3_key']}\n")
        f.write(f"  Total MongoDB transcriptions: {results['mongo_count']}\n")
        f.write(f"  S3 objects: {results['s3_count']}\n")
        f.write(f"  Missing from S3: {results['missing_count']}\n")
        f.write(f"  Orphaned in S3: {results['orphaned_count']}\n\n")
        
        if results['missing_count'] > 0:
            f.write(f"MISSING FILES FROM S3 ({results['missing_count']} files):\n")
            f.write("-" * 80 + "\n")
            for file_info in results['missing_from_s3']:
                f.write(f"\nMongoDB ID: {file_info['_id']}\n")
                f.write(f"S3 Key: {file_info['s3_key']}\n")
                f.write(f"Filename: {file_info['filename']}\n")
                f.write(f"S3 URL: {file_info['s3_url']}\n")
                if file_info['created_at']:
                    f.write(f"Created: {file_info['created_at']}\n")
        
        if results['orphaned_count'] > 0:
            f.write(f"\n\nORPHANED FILES IN S3 ({results['orphaned_count']} files):\n")
            f.write("-" * 80 + "\n")
            for file_info in results['orphaned_in_s3']:
                f.write(f"\nS3 Key: {file_info['key']}\n")
                f.write(f"Size: {file_info['size']:,} bytes ({file_info['size'] / 1024 / 1024:.2f} MB)\n")
                f.write(f"Last Modified: {file_info['last_modified']}\n")
    
    print(f"\n💾 Detailed report saved to: {output_file}")


def main():
    """Main function to run the comparison."""
    print("="*80)
    print("🔍 S3 Missing Files Checker")
    print("="*80)
    
    # Get MongoDB transcriptions
    transcriptions = get_mongodb_transcriptions()
    
    # Get S3 objects
    s3_objects, s3_bucket_name = get_s3_objects()
    
    # Compare and find missing files
    results = find_missing_files(transcriptions, s3_objects)
    
    # Print results
    print_results(results)
    
    print("\n✅ Analysis complete!")


if __name__ == '__main__':
    main()
