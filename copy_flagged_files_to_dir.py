#!/usr/bin/env python3
"""
Script to copy folders based on filename prefixes
"""
import os
import shutil
from pathlib import Path

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

# Source and destination paths
source_dir = Path("/Users/ayush/Desktop/Wadhwani_bucket_data/data/bucket-prod-orf-asso1-indikaai/gujrati/batch1/annotation_data")
dest_dir = Path("/Users/ayush/Desktop/transcription/data/flagged_data/data_3")

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

