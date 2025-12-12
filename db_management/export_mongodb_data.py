#!/usr/bin/env python3
"""
Script to export all data from MongoDB to JSON files.

This script exports all collections from the MongoDB database to separate JSON files.
It handles ObjectId serialization and provides progress feedback.

Usage:
    python export_mongodb_data.py [--output-dir OUTPUT_DIR] [--collections COLLECTION1,COLLECTION2]

Environment Variables:
    MONGODB_URI: MongoDB connection URI (default: mongodb://localhost:27017/)
    MONGODB_DATABASE: Database name (default: transcription_db)
"""

import os
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from pymongo import MongoClient
from bson import ObjectId, json_util
from bson.errors import InvalidId
from dotenv import load_dotenv


class MongoDBExporter:
    """Export MongoDB collections to JSON files."""
    
    def __init__(self, mongodb_uri: str, mongodb_database: str, output_dir: str = "mongodb_export"):
        """
        Initialize MongoDB exporter.
        
        Args:
            mongodb_uri: MongoDB connection URI
            mongodb_database: Database name
            output_dir: Output directory for JSON files
        """
        self.mongodb_uri = mongodb_uri
        self.mongodb_database = mongodb_database
        self.output_dir = output_dir
        self.client = None
        self.db = None
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
    
    def connect(self):
        """Connect to MongoDB."""
        try:
            print(f"📊 Connecting to MongoDB...")
            print(f"   URI: {self.mongodb_uri}")
            print(f"   Database: {self.mongodb_database}")
            
            self.client = MongoClient(self.mongodb_uri)
            self.db = self.client[self.mongodb_database]
            
            # Test connection
            self.client.admin.command('ping')
            print(f"✅ Connected to MongoDB successfully")
            
            return True
        except Exception as e:
            print(f"❌ Error connecting to MongoDB: {str(e)}")
            return False
    
    def get_collections(self) -> List[str]:
        """Get list of all collections in the database."""
        try:
            collections = self.db.list_collection_names()
            # Filter out system collections (they start with system.)
            collections = [c for c in collections if not c.startswith('system.')]
            return sorted(collections)
        except Exception as e:
            print(f"❌ Error listing collections: {str(e)}")
            return []
    
    def export_collection(self, collection_name: str) -> Dict[str, Any]:
        """
        Export a single collection to JSON.
        
        Args:
            collection_name: Name of the collection to export
            
        Returns:
            Dictionary with export statistics
        """
        try:
            collection = self.db[collection_name]
            
            # Count documents
            total_docs = collection.count_documents({})
            
            if total_docs == 0:
                print(f"   ⚠️  Collection '{collection_name}' is empty, skipping...")
                return {
                    'collection': collection_name,
                    'count': 0,
                    'success': True,
                    'message': 'Collection is empty'
                }
            
            print(f"   📥 Exporting collection '{collection_name}' ({total_docs} documents)...")
            
            # Fetch all documents
            documents = list(collection.find({}))
            
            # Convert ObjectId and other BSON types to JSON-serializable format
            # Using json_util.dumps and loads to handle BSON types properly
            json_data = json.loads(json_util.dumps(documents))
            
            # Create output filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_dir, f"{collection_name}_{timestamp}.json")
            
            # Write to JSON file with pretty formatting
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            
            file_size = os.path.getsize(output_file)
            file_size_mb = file_size / (1024 * 1024)
            
            print(f"   ✅ Exported {total_docs} documents to '{output_file}' ({file_size_mb:.2f} MB)")
            
            return {
                'collection': collection_name,
                'count': total_docs,
                'file': output_file,
                'file_size': file_size,
                'file_size_mb': file_size_mb,
                'success': True
            }
            
        except Exception as e:
            print(f"   ❌ Error exporting collection '{collection_name}': {str(e)}")
            return {
                'collection': collection_name,
                'count': 0,
                'success': False,
                'error': str(e)
            }
    
    def export_all(self, collection_names: List[str] = None) -> Dict[str, Any]:
        """
        Export all collections or specified collections.
        
        Args:
            collection_names: Optional list of specific collections to export.
                             If None, exports all collections.
        
        Returns:
            Dictionary with export summary
        """
        if not self.connect():
            return {'success': False, 'error': 'Failed to connect to MongoDB'}
        
        # Get collections to export
        if collection_names is None:
            collections_to_export = self.get_collections()
            if not collections_to_export:
                print("⚠️  No collections found in database")
                return {'success': False, 'error': 'No collections found'}
        else:
            collections_to_export = collection_names
        
        print(f"\n📦 Found {len(collections_to_export)} collection(s) to export:")
        for col in collections_to_export:
            print(f"   - {col}")
        
        print(f"\n🚀 Starting export...\n")
        
        # Export each collection
        results = []
        total_docs = 0
        total_size = 0
        
        for collection_name in collections_to_export:
            result = self.export_collection(collection_name)
            results.append(result)
            if result['success']:
                total_docs += result['count']
                total_size += result.get('file_size', 0)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 Export Summary")
        print(f"{'='*60}")
        print(f"Collections exported: {len([r for r in results if r['success']])}/{len(results)}")
        print(f"Total documents: {total_docs:,}")
        print(f"Total size: {total_size / (1024 * 1024):.2f} MB")
        print(f"Output directory: {os.path.abspath(self.output_dir)}")
        
        failed = [r for r in results if not r['success']]
        if failed:
            print(f"\n⚠️  Failed collections ({len(failed)}):")
            for r in failed:
                print(f"   - {r['collection']}: {r.get('error', 'Unknown error')}")
        
        print(f"\n✅ Export completed!")
        
        return {
            'success': True,
            'results': results,
            'total_documents': total_docs,
            'total_size_mb': total_size / (1024 * 1024),
            'output_dir': os.path.abspath(self.output_dir)
        }
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            print("🔌 MongoDB connection closed")


def main():
    """Main function to run the export script."""
    # Load environment variables from .env file
    script_dir = Path(__file__).parent
    env_loaded = False
    
    # Try root .env first
    root_env = script_dir / '.env'
    if root_env.exists():
        load_dotenv(dotenv_path=root_env)
        print(f"✅ Loaded environment variables from: {root_env}")
        env_loaded = True
    
    # Also try backend/.env as fallback
    backend_env = script_dir / 'backend' / '.env'
    if backend_env.exists():
        load_dotenv(dotenv_path=backend_env, override=False)  # Don't override if root .env was loaded
        if not env_loaded:
            print(f"✅ Loaded environment variables from: {backend_env}")
        env_loaded = True
    
    # If no .env files found, try loading from any .env file
    if not env_loaded:
        load_dotenv()  # This will look for .env in current directory and parent directories
    
    parser = argparse.ArgumentParser(
        description='Export all data from MongoDB to JSON files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export all collections (uses environment variables or defaults)
  python export_mongodb_data.py
  
  # Export to specific directory
  python export_mongodb_data.py --output-dir ./backup
  
  # Export specific collections only
  python export_mongodb_data.py --collections users,transcriptions
  
  # Custom MongoDB connection
  MONGODB_URI="mongodb://user:pass@host:27017/" MONGODB_DATABASE="my_db" python export_mongodb_data.py
        """
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='mongodb_export',
        help='Output directory for JSON files (default: mongodb_export)'
    )
    
    parser.add_argument(
        '--collections',
        type=str,
        help='Comma-separated list of collections to export (default: all collections)'
    )
    
    parser.add_argument(
        '--uri',
        type=str,
        help='MongoDB connection URI (overrides MONGODB_URI environment variable)'
    )
    
    parser.add_argument(
        '--database',
        type=str,
        help='MongoDB database name (overrides MONGODB_DATABASE environment variable)'
    )
    
    args = parser.parse_args()
    
    # Get MongoDB configuration
    mongodb_uri = args.uri or os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    mongodb_database = args.database or os.getenv('MONGODB_DATABASE', 'transcription_db')
    
    # Parse collections if provided
    collection_names = None
    if args.collections:
        collection_names = [c.strip() for c in args.collections.split(',')]
    
    # Create exporter and run
    exporter = MongoDBExporter(mongodb_uri, mongodb_database, args.output_dir)
    
    try:
        result = exporter.export_all(collection_names)
        if not result.get('success'):
            print(f"\n❌ Export failed: {result.get('error', 'Unknown error')}")
            return 1
    except KeyboardInterrupt:
        print("\n\n⚠️  Export interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        exporter.close()
    
    return 0


if __name__ == '__main__':
    exit(main())

