#!/usr/bin/env python3
"""
Script to copy folders based on filename prefixes
"""
import os
import shutil
from pathlib import Path

# Filenames provided by user
filenames = [
    "5494531_audio.mp3", "6848876_audio.mp3", "7234398_audio.mp3", "7239089_audio.mp3",
"7235476_audio.mp3", "5254889_audio.mp3", "5189362_audio.mp3", "5172392_audio.mp3",
"5156355_audio.mp3", "5143424_audio.mp3"
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

# Source and destination paths
source_dir = Path("/Users/ayush/Desktop/Wadhwani_bucket_data/data/bucket-prod-orf-asso1-indikaai/gujrati/batch1/annotation_data")
dest_dir = Path("/Users/ayush/Desktop/transcription/data/flagged_data/data_4/1")

def copy_folders():
    """Copy folders from source to destination based on prefixes"""
    
    # Check if source directory exists
    if not source_dir.exists():
        print(f"❌ ERROR: Source directory not found: {source_dir}")
        return
    
    # Create destination directory if it doesn't exist
    dest_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Destination directory: {dest_dir}")
    
    # Get all prefixes sorted
    sorted_prefixes = sorted(prefixes)
    print(f"\n📋 Found {len(sorted_prefixes)} unique prefixes to copy\n")
    
    copied_count = 0
    not_found_count = 0
    error_count = 0
    
    for idx, prefix in enumerate(sorted_prefixes, 1):
        source_folder = source_dir / prefix
        dest_folder = dest_dir / prefix
        
        print(f"[{idx}/{len(sorted_prefixes)}] Processing: {prefix}")
        
        # Check if source folder exists
        if not source_folder.exists():
            print(f"  ⚠️  Folder not found: {source_folder}")
            not_found_count += 1
            continue
        
        # Check if destination already exists
        if dest_folder.exists():
            print(f"  ⚠️  Destination already exists, skipping: {dest_folder}")
            continue
        
        try:
            # Copy the entire folder
            shutil.copytree(source_folder, dest_folder)
            print(f"  ✅ Copied: {prefix}")
            copied_count += 1
        except Exception as e:
            print(f"  ❌ Error copying {prefix}: {e}")
            error_count += 1
    
    # Print summary
    print(f"\n{'='*80}")
    print("📊 COPY SUMMARY")
    print(f"{'='*80}")
    print(f"Total prefixes: {len(sorted_prefixes)}")
    print(f"✅ Successfully copied: {copied_count}")
    print(f"⚠️  Not found in source: {not_found_count}")
    print(f"❌ Errors: {error_count}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    copy_folders()

