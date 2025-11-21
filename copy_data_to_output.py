#!/usr/bin/env python3
"""
Script to copy audio files, txt files, and JSON files from data/data_3 to output folder.
Structure:
- data/data_3/[number]/audio.mp3 -> output/[number]/audio.mp3
- data/data_3/[number]/ref_text.txt -> output/[number]/ref_text.txt
- data/data_3/[number]/transcriptions/[number].json -> output/[number]/[number].json
"""

import os
import shutil
from pathlib import Path

def copy_files_to_output():
    # Define paths
    base_dir = Path(__file__).parent
    data_3_dir = base_dir / "data" / "data_3"
    output_dir = base_dir / "output"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(exist_ok=True)
    
    # Get all number folders in data_3
    if not data_3_dir.exists():
        print(f"Error: {data_3_dir} does not exist!")
        return
    
    number_folders = [d for d in data_3_dir.iterdir() if d.is_dir() and d.name.isdigit()]
    print(f"Found {len(number_folders)} number folders in {data_3_dir}")
    
    copied_count = 0
    skipped_count = 0
    
    for number_folder in number_folders:
        number = number_folder.name
        output_number_dir = output_dir / number
        output_number_dir.mkdir(exist_ok=True)
        
        # Find audio file (could be .mp3, .wav, etc.)
        audio_files = list(number_folder.glob("*.mp3")) + list(number_folder.glob("*.wav")) + \
                     list(number_folder.glob("*.m4a")) + list(number_folder.glob("*.flac"))
        audio_files = [f for f in audio_files if f.is_file() and f.parent == number_folder]
        
        # Find txt file
        txt_files = list(number_folder.glob("*.txt"))
        txt_files = [f for f in txt_files if f.is_file() and f.parent == number_folder]
        
        # Find JSON file in transcriptions folder
        transcriptions_dir = number_folder / "transcriptions"
        json_files = []
        if transcriptions_dir.exists() and transcriptions_dir.is_dir():
            json_files = list(transcriptions_dir.glob("*.json"))
        
        copied = False
        
        # Copy audio file
        if audio_files:
            audio_file = audio_files[0]  # Take the first audio file found
            dest_audio = output_number_dir / audio_file.name
            if not dest_audio.exists():
                shutil.copy2(audio_file, dest_audio)
                print(f"Copied: {audio_file.name} -> output/{number}/")
                copied = True
            else:
                print(f"Skipped (exists): output/{number}/{audio_file.name}")
        else:
            print(f"Warning: No audio file found in {number_folder}")
        
        # Copy txt file
        if txt_files:
            txt_file = txt_files[0]  # Take the first txt file found
            dest_txt = output_number_dir / txt_file.name
            if not dest_txt.exists():
                shutil.copy2(txt_file, dest_txt)
                print(f"Copied: {txt_file.name} -> output/{number}/")
                copied = True
            else:
                print(f"Skipped (exists): output/{number}/{txt_file.name}")
        else:
            print(f"Warning: No txt file found in {number_folder}")
        
        # Copy JSON file
        if json_files:
            json_file = json_files[0]  # Take the first JSON file found
            dest_json = output_number_dir / json_file.name
            if not dest_json.exists():
                shutil.copy2(json_file, dest_json)
                print(f"Copied: {json_file.name} -> output/{number}/")
                copied = True
            else:
                print(f"Skipped (exists): output/{number}/{json_file.name}")
        else:
            print(f"Warning: No JSON file found in {number_folder}/transcriptions/")
        
        if copied:
            copied_count += 1
        else:
            skipped_count += 1
    
    print(f"\n=== Summary ===")
    print(f"Total folders processed: {len(number_folders)}")
    print(f"Folders with new files copied: {copied_count}")
    print(f"Folders skipped (all files already exist): {skipped_count}")
    print(f"\nOutput directory: {output_dir}")

if __name__ == "__main__":
    copy_files_to_output()

