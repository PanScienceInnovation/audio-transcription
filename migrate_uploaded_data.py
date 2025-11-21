"""
Migration script to update documents uploaded on 2025-11-21 to match the correct schema.
Converts annotations format to words format with proper structure.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timezone
import re
from bson import ObjectId

# Load environment variables
script_dir = Path(__file__).parent
env_path = script_dir / '.env'
load_dotenv(dotenv_path=env_path)

# MongoDB Configuration
mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
mongodb_database = os.getenv('MONGODB_DATABASE', 'transcription_db')
mongodb_collection = os.getenv('MONGODB_COLLECTION', 'transcriptions')


def parse_timestamp(timestamp_str: str) -> float:
    """Convert timestamp string (HH:MM:SS.microseconds) to seconds."""
    try:
        parts = timestamp_str.split(':')
        hours = float(parts[0])
        minutes = float(parts[1])
        sec_parts = parts[2].split('.')
        seconds = float(sec_parts[0])
        microseconds = float(sec_parts[1]) if len(sec_parts) > 1 else 0.0
        total_seconds = hours * 3600 + minutes * 60 + seconds + microseconds / 1000000
        return total_seconds
    except (ValueError, IndexError) as e:
        print(f"⚠️  Warning: Could not parse timestamp '{timestamp_str}': {e}")
        return 0.0


def calculate_duration(start_str: str, end_str: str) -> float:
    """Calculate duration in seconds from start and end timestamps."""
    start_sec = parse_timestamp(start_str)
    end_sec = parse_timestamp(end_str)
    return end_sec - start_sec


def clean_word(text: str) -> str:
    """Clean word text by removing HTML-like tags."""
    if not text:
        return ""
    # Remove <AI>...</AI> tags and similar
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


def extract_word(transcription_list: list) -> str:
    """Extract word from Transcription array."""
    if not transcription_list or len(transcription_list) == 0:
        return ""
    # Get first element and clean it
    word = str(transcription_list[0]) if transcription_list[0] else ""
    return clean_word(word)


def convert_annotations_to_words(annotations: list, language: str = "Gujarati") -> list:
    """
    Convert annotations array to words array format.
    
    Args:
        annotations: List of annotation objects with start, end, Transcription
        language: Language for all words
        
    Returns:
        List of word objects in the target format
    """
    words = []
    
    for ann in annotations:
        start = ann.get('start', '')
        end = ann.get('end', '')
        transcription = ann.get('Transcription', [])
        
        if not start or not end or not transcription:
            continue
        
        word_text = extract_word(transcription)
        
        # Skip empty words
        if not word_text:
            continue
        
        # Calculate duration
        duration = calculate_duration(start, end)
        
        # Create word object
        word_obj = {
            'start': start,
            'end': end,
            'duration': duration,
            'word': word_text,
            'language': language
        }
        
        words.append(word_obj)
    
    return words


def update_document(doc: dict, collection) -> bool:
    """
    Update a document to match the target schema.
    
    Args:
        doc: MongoDB document to update
        collection: MongoDB collection object
        
    Returns:
        True if successful, False otherwise
    """
    try:
        transcription_data = doc.get('transcription_data', {})
        
        # Check if already in correct format (has words and metadata)
        if 'words' in transcription_data and 'metadata' in transcription_data:
            print(f"   ⏭️  Document {doc['_id']} already in correct format, skipping")
            return True
        
        # Get annotations - handle different possible locations
        annotations = transcription_data.get('annotations', [])
        
        # If no annotations found, try to check if already converted
        if not annotations:
            words = transcription_data.get('words', [])
            if words:
                # Already has words, might be already converted but needs metadata fix
                print(f"   ℹ️  Document {doc['_id']} already has words, checking metadata...")
            else:
                print(f"   ⚠️  Document {doc['_id']} has no annotations or words, skipping")
                return False
        
        # Language is always Gujarati
        language = "Gujarati"
        
        # Convert annotations to words
        if annotations:
            words = convert_annotations_to_words(annotations, language)
            
            if not words:
                print(f"   ⚠️  Document {doc['_id']} produced no words after conversion, creating empty words array")
                words = []  # Create empty array to maintain schema structure
        else:
            # Document already has words, just need to fix metadata structure
            words = transcription_data.get('words', [])
            if not words:
                print(f"   ⚠️  Document {doc['_id']} has no words or annotations, creating empty words array")
                words = []  # Create empty array to maintain schema structure
        
        # Get audio_path and filename for metadata
        audio_path = transcription_data.get('audio_path', '')
        filename = transcription_data.get('filename', 'audio.mp3')
        
        # If audio_path doesn't start with /api/audio/, add it
        if audio_path and not audio_path.startswith('/api/audio/'):
            # Extract just the filename from audio_path
            if '/' in audio_path:
                filename_from_path = audio_path.split('/')[-1]
            else:
                filename_from_path = audio_path
            audio_path = f"/api/audio/{filename_from_path}"
        
        # Ensure filename matches audio_path
        if not filename or filename == 'audio.mp3':
            if audio_path:
                filename = audio_path.split('/')[-1]
        
        # Build new transcription_data structure
        new_transcription_data = {
            'words': words,
            'language': language,
            'audio_duration': transcription_data.get('audio_duration'),
            'total_words': len(words),
            'transcription_type': 'words',
            'metadata': {
                'audio_path': audio_path,
                'filename': filename
            }
        }
        
        # Update document - replace entire transcription_data object
        # This will automatically remove old fields that aren't in new_transcription_data
        update_result = collection.update_one(
            {'_id': doc['_id']},
            {
                '$set': {
                    'transcription_data': new_transcription_data,
                    'updated_at': datetime.now(timezone.utc)
                }
            }
        )
        
        if update_result.modified_count > 0:
            print(f"   ✅ Updated document {doc['_id']} - {len(words)} words")
            return True
        else:
            print(f"   ⚠️  Document {doc['_id']} not modified (may already be updated)")
            return True
            
    except Exception as e:
        print(f"   ❌ Error updating document {doc['_id']}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main migration function."""
    print("🔄 Starting Migration of Uploaded Documents (2025-11-21)")
    print("=" * 80)
    
    try:
        # Connect to MongoDB
        client = MongoClient(mongodb_uri)
        db = client[mongodb_database]
        collection = db[mongodb_collection]
        
        print(f"✅ Connected to MongoDB: {mongodb_database}")
        print(f"   Collection: {mongodb_collection}")
        print()
        
        # Find documents uploaded on 2025-11-21
        # Documents created between 2025-11-21 00:00:00 and 2025-11-21 23:59:59
        start_date = datetime(2025, 11, 21, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2025, 11, 22, 0, 0, 0, tzinfo=timezone.utc)
        
        query = {
            'created_at': {
                '$gte': start_date,
                '$lt': end_date
            }
        }
        
        # Get count
        total_count = collection.count_documents(query)
        print(f"📊 Found {total_count} documents uploaded on 2025-11-21")
        print()
        
        if total_count == 0:
            print("⚠️  No documents found for migration.")
            client.close()
            return
        
        # Process documents
        cursor = collection.find(query).sort('created_at', 1)
        
        successful = 0
        failed = 0
        skipped = 0
        
        for i, doc in enumerate(cursor, 1):
            doc_id = doc['_id']
            source_folder_id = doc.get('transcription_data', {}).get('source_folder_id', 'N/A')
            
            print(f"[{i}/{total_count}] Processing document {doc_id} (folder: {source_folder_id})")
            
            result = update_document(doc, collection)
            
            if result:
                successful += 1
            else:
                failed += 1
        
        # Print summary
        print()
        print("=" * 80)
        print("📊 MIGRATION SUMMARY")
        print("=" * 80)
        print(f"   Total documents: {total_count}")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   ⏭️  Skipped: {skipped}")
        print()
        print("✨ Migration completed!")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

