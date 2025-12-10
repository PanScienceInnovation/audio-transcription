#!/usr/bin/env python3
"""
Script to process all audio files in the telugu_sample directory using audio_diarization.py
Each audio file will get its own separate JSON output file.
"""
import os
import sys
import subprocess
import shutil
from pathlib import Path

# Get the script directory
script_dir = Path(__file__).parent
# Updated path for Telugu samples
data_dir = script_dir / "data" / "telugu_sample"
backend_dir = script_dir / "backend"
audio_diarization_script = backend_dir / "audio_diarization.py"

def process_all_audio_files():
    """Process all audio files in the telugu_sample directory."""
    
    if not audio_diarization_script.exists():
        print(f"❌ ERROR: audio_diarization.py not found at {audio_diarization_script}")
        sys.exit(1)
    
    if not data_dir.exists():
        print(f"❌ ERROR: {data_dir} directory not found at {data_dir}")
        print(f"   Please ensure the directory exists at: {data_dir.absolute()}")
        sys.exit(1)
    
    # Find all .wav files in the directory
    audio_files = sorted([f for f in data_dir.iterdir() if f.is_file() and f.suffix.lower() == '.wav'])
    
    total = len(audio_files)
    print(f"📁 Found {total} audio files to process\n")
    
    if total == 0:
        print(f"⚠️  WARNING: No .wav files found in {data_dir}")
        sys.exit(0)
    
    success_count = 0
    error_count = 0
    skipped_count = 0
    errors = []
    
    for idx, audio_file in enumerate(audio_files, 1):
        print(f"\n{'='*80}")
        print(f"[{idx}/{total}] Processing: {audio_file.name}")
        print(f"{'='*80}")
        
        # Check if audio file exists (should always be true, but just in case)
        if not audio_file.exists():
            print(f"⚠️  WARNING: {audio_file.name} not found")
            skipped_count += 1
            continue
        
        # Create a subdirectory for this audio file to ensure separate JSON output
        # Use the filename without extension as the subdirectory name
        file_stem = audio_file.stem  # filename without extension
        temp_subdir = data_dir / file_stem
        temp_audio_path = temp_subdir / audio_file.name
        
        try:
            # Create subdirectory
            temp_subdir.mkdir(exist_ok=True)
            
            # Copy audio file to subdirectory
            shutil.copy2(audio_file, temp_audio_path)
            print(f"📁 Created temporary subdirectory: {temp_subdir.name}")
            
            # Run the transcription command
            # Note: No reference text file for Telugu samples
            cmd = [
                "python3",
                str(audio_diarization_script),
                str(temp_audio_path),
                "Telugu",
                "English"
            ]
            
            print(f"🎵 Processing audio: {audio_file.name}")
            print(f"🌐 Source Language: Telugu")
            print(f"🌐 Target Language: English")
            print(f"▶️  Running: {' '.join(cmd)}\n")
            
            result = subprocess.run(
                cmd,
                cwd=str(script_dir),
                capture_output=False,  # Show output in real-time
                text=True,
                check=True
            )
            
            # The output JSON will be in temp_subdir/transcriptions/temp_subdir.json
            output_json = temp_subdir / "transcriptions" / f"{temp_subdir.name}.json"
            if output_json.exists():
                # Optionally, you could move it to a central location or rename it
                # For now, it stays in the subdirectory
                print(f"📄 Output JSON: {output_json}")
            
            print(f"\n✅ Successfully processed {audio_file.name}")
            success_count += 1
            
        except subprocess.CalledProcessError as e:
            print(f"\n❌ ERROR processing {audio_file.name}: {e}")
            error_count += 1
            errors.append((audio_file.name, str(e)))
            
        except Exception as e:
            print(f"\n❌ Unexpected error processing {audio_file.name}: {e}")
            error_count += 1
            errors.append((audio_file.name, str(e)))
            
        finally:
            # Clean up: remove the copied audio file from temp subdirectory
            # Keep the subdirectory and transcription output
            if temp_audio_path.exists():
                try:
                    temp_audio_path.unlink()
                    print(f"🧹 Cleaned up temporary audio file: {temp_audio_path.name}")
                except Exception as e:
                    print(f"⚠️  Warning: Could not remove temporary file {temp_audio_path}: {e}")
    
    # Print summary
    print(f"\n{'='*80}")
    print("📊 PROCESSING SUMMARY")
    print(f"{'='*80}")
    print(f"Total audio files: {total}")
    print(f"✅ Successfully processed: {success_count}")
    print(f"❌ Errors: {error_count}")
    print(f"⚠️  Skipped: {skipped_count}")
    print(f"{'='*80}\n")
    
    if errors:
        print("❌ Files with errors:")
        for file_name, error_msg in errors:
            print(f"  - {file_name}: {error_msg}")
        print()
    
    return success_count, error_count, skipped_count

if __name__ == "__main__":
    try:
        success, errors, skipped = process_all_audio_files()
        sys.exit(0 if errors == 0 else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

