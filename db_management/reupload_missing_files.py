#!/usr/bin/env python3
"""
Script to re-upload missing audio files to S3.

This script:
1. Reads the missing files report
2. Finds the corresponding audio files in the data_3 directory
3. Uploads them to S3 with the same key/filename
"""

import os
import sys
import re
from pathlib import Path
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

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


def parse_missing_files_report(report_path):
    """Parse the missing files report and extract file information."""
    missing_files = []
    
    with open(report_path, 'r') as f:
        content = f.read()
    
    # Pattern to match file entries
    pattern = r'MongoDB ID: ([^\n]+)\nS3 Key: ([^\n]+)\nFilename: ([^\n]+)'
    matches = re.findall(pattern, content)
    
    for mongo_id, s3_key, filename in matches:
        # Extract numeric ID from filename (e.g., "5242690_audio.mp3" -> "5242690")
        # Also handle cases where filename might just be the number
        match = re.match(r'^(\d+)(?:_audio)?\.mp3$', filename)
        if match:
            numeric_id = match.group(1)
        else:
            # Try to extract from S3 key
            key_match = re.search(r'/(\d+)_audio\.mp3$', s3_key)
            if key_match:
                numeric_id = key_match.group(1)
            else:
                print(f"⚠️  Warning: Could not extract numeric ID from {filename}")
                continue
        
        missing_files.append({
            'mongo_id': mongo_id.strip(),
            's3_key': s3_key.strip(),
            'filename': filename.strip(),
            'numeric_id': numeric_id
        })
    
    return missing_files


def find_audio_file(base_dir, numeric_id):
    """Find the audio file in the data_3 directory."""
    # Look for folder with matching numeric ID
    folder_path = Path(base_dir) / numeric_id
    
    if not folder_path.exists():
        return None
    
    # Look for audio.mp3 in that folder
    audio_file = folder_path / 'audio.mp3'
    
    if audio_file.exists():
        return str(audio_file)
    
    return None


def upload_to_s3(local_file_path, s3_key, s3_client, bucket_name):
    """Upload a file to S3."""
    try:
        # Get content type
        content_type = 'audio/mpeg' if local_file_path.endswith('.mp3') else 'audio/wav'
        
        # Get file size for progress tracking
        file_size = os.path.getsize(local_file_path)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"   📤 Uploading {file_size_mb:.2f} MB...")
        
        # Upload file
        s3_client.upload_file(
            local_file_path,
            bucket_name,
            s3_key,
            ExtraArgs={'ContentType': content_type}
        )
        
        return {'success': True, 'message': f'Uploaded {s3_key}'}
    except ClientError as e:
        return {'success': False, 'error': f"S3 upload error: {str(e)}"}
    except Exception as e:
        return {'success': False, 'error': f"Unexpected error: {str(e)}"}


def main():
    """Main function to re-upload missing files."""
    print("="*80)
    print("📤 Re-upload Missing Files to S3")
    print("="*80)
    
    # Configuration
    report_path = Path(__file__).parent / 'missing_s3_files_report.txt'
    base_dir = Path('/Users/ayush/Desktop/Wadhwani_bucket_data/data/bucket-prod-orf-asso1-indikaai/gujrati/batch1/annotation_data')
    
    s3_bucket_name = os.getenv('S3_BUCKET_NAME', 'audio-files-transcripn')
    s3_region = os.getenv('S3_REGION', 'ap-south-1')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('SECRET_ACCESS_KEY')
    
    # Check if report exists
    if not report_path.exists():
        print(f"❌ Error: Report file not found at {report_path}")
        sys.exit(1)
    
    # Check if base directory exists
    if not base_dir.exists():
        print(f"❌ Error: Base directory not found at {base_dir}")
        sys.exit(1)
    
    # Parse missing files
    print(f"\n📖 Reading missing files report...")
    missing_files = parse_missing_files_report(report_path)
    print(f"✅ Found {len(missing_files)} missing files in report")
    
    # Initialize S3 client
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
    except Exception as e:
        print(f"❌ Error connecting to S3: {str(e)}")
        sys.exit(1)
    
    # Process each missing file
    print(f"\n🔄 Processing {len(missing_files)} files...")
    print("-" * 80)
    
    success_count = 0
    not_found_count = 0
    error_count = 0
    not_found_files = []
    
    for i, file_info in enumerate(missing_files, 1):
        numeric_id = file_info['numeric_id']
        s3_key = file_info['s3_key']
        filename = file_info['filename']
        
        print(f"\n[{i}/{len(missing_files)}] Processing {filename}...")
        print(f"   Numeric ID: {numeric_id}")
        print(f"   S3 Key: {s3_key}")
        
        # Find the audio file
        audio_file_path = find_audio_file(base_dir, numeric_id)
        
        if not audio_file_path:
            print(f"   ❌ Audio file not found in {base_dir}/{numeric_id}/")
            not_found_count += 1
            not_found_files.append({
                'filename': filename,
                'numeric_id': numeric_id,
                's3_key': s3_key
            })
            continue
        
        print(f"   ✅ Found audio file: {audio_file_path}")
        
        # Upload to S3
        result = upload_to_s3(audio_file_path, s3_key, s3_client, s3_bucket_name)
        
        if result['success']:
            print(f"   ✅ Successfully uploaded to S3")
            success_count += 1
        else:
            print(f"   ❌ Upload failed: {result.get('error', 'Unknown error')}")
            error_count += 1
    
    # Summary
    print("\n" + "="*80)
    print("📊 UPLOAD SUMMARY")
    print("="*80)
    print(f"   Total files processed: {len(missing_files)}")
    print(f"   ✅ Successfully uploaded: {success_count}")
    print(f"   ❌ Files not found: {not_found_count}")
    print(f"   ❌ Upload errors: {error_count}")
    print("="*80)
    
    if success_count > 0:
        print(f"\n✅ Successfully re-uploaded {success_count} files to S3!")
    if not_found_count > 0:
        print(f"\n⚠️  {not_found_count} files were not found in the source directory:")
        for nf in not_found_files[:10]:  # Show first 10
            print(f"      - {nf['filename']} (ID: {nf['numeric_id']})")
        if len(not_found_files) > 10:
            print(f"      ... and {len(not_found_files) - 10} more")
    if error_count > 0:
        print(f"\n❌ {error_count} files had upload errors.")


if __name__ == '__main__':
    main()

