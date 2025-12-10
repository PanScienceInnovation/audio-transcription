#!/usr/bin/env python3
"""
Script to copy folders based on filename prefixes
"""
import os
import shutil
from pathlib import Path

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

# Source and destination paths
source_dir = Path("/Users/ayush/Desktop/Wadhwani_bucket_data/data/bucket-prod-orf-asso1-indikaai/gujrati/batch1/annotation_data")
dest_dir = Path("/Users/ayush/Desktop/transcription/data/flagged_data/data_4/2")

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

