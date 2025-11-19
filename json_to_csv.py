import json
import csv
import sys
import os
import glob

def ts_to_seconds(ts):
    h, m, s = ts.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)

def json_to_csv(json_file, csv_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    annotations = data["annotations"]

    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(["start", "end", "duration_seconds", "word"])

        for ann in annotations:
            start = ann["start"]
            end = ann["end"]

            # PREVENT GOOGLE SHEETS FROM CONVERTING TIME FORMAT
            start_txt = "'" + start
            end_txt = "'" + end

            duration = ts_to_seconds(end) - ts_to_seconds(start)
            word = ann["Transcription"][0]

            writer.writerow([start_txt, end_txt, duration, word])

    print("CSV created:", csv_file)


def convert_all_json_files(base_path):
    """Convert all JSON files in directory structure to CSV"""
    # Pattern to match: base_path/{directory}/transcriptions/{json_file}
    pattern = os.path.join(base_path, "*", "transcriptions", "*.json")
    json_files = glob.glob(pattern)
    
    if not json_files:
        print(f"No JSON files found matching pattern: {pattern}")
        return
    
    print(f"Found {len(json_files)} JSON file(s) to convert...")
    
    for json_file in json_files:
        # Extract parent folder name (the number) from path
        # Path structure: base_path/{number}/transcriptions/{json_file}
        # Get the parent of transcriptions folder, which is the number
        transcriptions_dir = os.path.dirname(json_file)  # .../{number}/transcriptions
        parent_folder = os.path.basename(os.path.dirname(transcriptions_dir))  # {number}
        
        # Create CSV file in the same transcriptions directory with parent folder name
        csv_filename = f"{parent_folder}.csv"
        csv_file = os.path.join(transcriptions_dir, csv_filename)
        
        try:
            json_to_csv(json_file, csv_file)
        except Exception as e:
            print(f"Error converting {json_file}: {e}")
    
    print(f"\nConversion complete! Processed {len(json_files)} file(s).")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No arguments: convert all files in Sample Part 3
        base_path = "/Users/ayush/Desktop/transcription/data"
        convert_all_json_files(base_path)
    elif len(sys.argv) == 2:
        # One argument: base path to search for JSON files
        base_path = sys.argv[1]
        convert_all_json_files(base_path)
    elif len(sys.argv) == 3:
        # Two arguments: single file conversion (original behavior)
        json_to_csv(sys.argv[1], sys.argv[2])
    else:
        print("Usage:")
        print("  python json_to_csv.py                              # Convert all JSON files in Sample Part 3")
        print("  python json_to_csv.py <base_path>                  # Convert all JSON files in specified path")
        print("  python json_to_csv.py <json_file> <csv_file>       # Convert single file")
