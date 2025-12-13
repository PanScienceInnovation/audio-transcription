"""
Storage utilities for S3 and MongoDB operations.
"""
import os
import time
import boto3
from pymongo import MongoClient
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError
import json


class StorageManager:
    """Manages S3 and MongoDB storage operations."""
    
    def __init__(self):
        """Initialize S3 and MongoDB connections."""
        # S3 Configuration - support both AWS_ACCESS_KEY_ID and ACCESS_KEY_ID
        self.s3_bucket_name = os.getenv('S3_BUCKET_NAME', 'transcription-audio-files')
        self.s3_region = os.getenv('S3_REGION', 'us-east-1')
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('SECRET_ACCESS_KEY')
        
        # MongoDB Configuration
        self.mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.mongodb_database = os.getenv('MONGODB_DATABASE', 'transcription_db')
        self.mongodb_collection = os.getenv('MONGODB_COLLECTION', 'transcriptions')
        
        # Initialize S3 client
        try:
            if self.aws_access_key_id and self.aws_secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    region_name=self.s3_region,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
                )
                print(f"✅ S3 client initialized with credentials")
                print(f"   Bucket: {self.s3_bucket_name}, Region: {self.s3_region}")
            else:
                # Use default credentials (IAM role, environment, or ~/.aws/credentials)
                self.s3_client = boto3.client('s3', region_name=self.s3_region)
                print(f"✅ S3 client initialized with default credentials")
                print(f"   Bucket: {self.s3_bucket_name}, Region: {self.s3_region}")
        except Exception as e:
            print(f"❌ Warning: Could not initialize S3 client: {str(e)}")
            self.s3_client = None
        
        # Initialize MongoDB client
        try:
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.db = self.mongo_client[self.mongodb_database]
            
            # Test connection with ping first
            self.mongo_client.admin.command('ping')
            
            # List existing collections
            collections = self.db.list_collection_names()
            
            # Get collection (MongoDB creates it automatically on first insert)
            self.collection = self.db[self.mongodb_collection]
            
            # Create version history collection
            self.version_history_collection = self.db['transcription_version_history']
            
            # Create reprocessed files collection
            self.reprocessed_collection = self.db['reprocessed_files']
            
            # Get telugu_transcriptions collection for indexing
            telugu_collection = self.db.get_collection('telugu_transcriptions')
            
            # Create indexes for better query performance
            try:
                # Helper function to create indexes on a collection
                def create_collection_indexes(collection, collection_name):
                    # Basic indexes
                    collection.create_index('created_at')
                    collection.create_index('updated_at')
                    collection.create_index('user_id')
                    collection.create_index('assigned_user_id')  # Index for filtering by assigned user
                    
                    # Flag and reprocess indexes
                    collection.create_index('is_flagged')
                    collection.create_index('is_double_flagged')
                    collection.create_index('has_been_reprocessed')
                    collection.create_index('manual_status')
                    
                    # Compound indexes for common query patterns
                    collection.create_index([('user_id', 1), ('created_at', -1)])  # User's transcriptions by date
                    collection.create_index([('assigned_user_id', 1), ('created_at', -1)])  # Assigned user queries
                    collection.create_index([('is_flagged', 1), ('created_at', -1)])  # Flagged files by date
                    collection.create_index([('is_double_flagged', 1), ('created_at', -1)])  # Double flagged by date
                    collection.create_index([('has_been_reprocessed', 1), ('created_at', -1)])  # Reprocessed by date
                    collection.create_index([('manual_status', 1), ('created_at', -1)])  # Status queries
                    collection.create_index([('transcription_data.language', 1)])  # Language filter
                    collection.create_index([('transcription_data.transcription_type', 1)])  # Type filter
                    
                    # Search indexes for filename fields (for regex search)
                    collection.create_index([('transcription_data.metadata.filename', 1)])  # Filename search
                    collection.create_index([('s3_metadata.key', 1)])  # S3 key search
                    collection.create_index([('transcription_data.audio_path', 1)])  # Audio path search
                    
                    # Additional compound indexes for common filter combinations
                    collection.create_index([('transcription_data.language', 1), ('created_at', -1)])  # Language + date
                    collection.create_index([('assigned_user_id', 1), ('manual_status', 1), ('created_at', -1)])  # User + status + date
                    collection.create_index([('is_flagged', 1), ('manual_status', 1), ('created_at', -1)])  # Flagged + status + date
                    collection.create_index([('transcription_data.language', 1), ('manual_status', 1)])  # Language + status
                    
                    print(f"   {collection_name}: All indexes created")
                
                # Create indexes for main collection
                create_collection_indexes(self.collection, 'Main collection (transcriptions)')
                
                # Create indexes for telugu_transcriptions collection
                create_collection_indexes(telugu_collection, 'Telugu collection (telugu_transcriptions)')
                
                # Create indexes for version history collection
                self.version_history_collection.create_index('transcription_id')
                self.version_history_collection.create_index([('transcription_id', 1), ('timestamp', -1)])  # Compound index for efficient queries
                
                # Create indexes for reprocessed files collection
                self.reprocessed_collection.create_index('created_at')
                self.reprocessed_collection.create_index('original_transcription_id')
                self.reprocessed_collection.create_index([('original_transcription_id', 1), ('created_at', -1)])
                
                print(f"✅ Created indexes on all key fields for fast queries")
                print(f"   Main collection: created_at, user_id, assigned_user_id, is_flagged, is_double_flagged, has_been_reprocessed, manual_status + compound indexes")
                print(f"   Telugu collection: Same indexes as main collection for optimal performance")
                print(f"   Version history: transcription_id + compound indexes")
                print(f"   Reprocessed files: created_at, original_transcription_id + compound indexes")
            except Exception as e:
                # Index might already exist, which is fine
                print(f"⚠️  Note: Some indexes may already exist (this is normal): {str(e)}")
                pass
            
            print(f"✅ Connected to MongoDB: {self.mongodb_database}")
            print(f"   Collection: {self.mongodb_collection}")
            print(f"   Version History Collection: transcription_version_history")
            print(f"   Reprocessed Files Collection: reprocessed_files")
            print(f"   Existing collections: {collections if collections else 'None (will be created on first insert)'}")
            
        except Exception as e:
            print(f"❌ Warning: Could not connect to MongoDB: {str(e)}")
            self.mongo_client = None
            self.db = None
            self.collection = None
            self.version_history_collection = None
            self.reprocessed_collection = None
    
    def _get_content_type(self, file_path: str) -> str:
        """Get content type based on file extension."""
        extension = os.path.splitext(file_path)[1].lower()
        content_types = {
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav',
            '.m4a': 'audio/mp4',
            '.ogg': 'audio/ogg',
            '.flac': 'audio/flac',
            '.aac': 'audio/aac',
        }
        return content_types.get(extension, 'audio/mpeg')
    
    def delete_audio_from_s3(self, s3_key: str) -> Dict[str, Any]:
        """
        Delete audio file from S3 bucket.
        
        Args:
            s3_key: S3 object key (path in bucket)
            
        Returns:
            Dictionary with deletion result
        """
        try:
            if not self.s3_client:
                return {
                    'success': False,
                    'error': 'S3 client not initialized. Please check AWS credentials.'
                }
            
            # Delete object from S3
            self.s3_client.delete_object(
                Bucket=self.s3_bucket_name,
                Key=s3_key
            )
            
            print(f"✅ Deleted S3 object: {s3_key}")
            
            return {
                'success': True,
                'message': f'S3 object deleted successfully: {s3_key}'
            }
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchKey':
                # Object doesn't exist, but that's okay - consider it deleted
                print(f"⚠️ S3 object not found (may already be deleted): {s3_key}")
                return {
                    'success': True,
                    'message': f'S3 object not found (may already be deleted): {s3_key}'
                }
            return {
                'success': False,
                'error': f"S3 deletion error: {str(e)}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error during S3 deletion: {str(e)}"
            }
    
    def upload_audio_to_s3(self, local_file_path: str, s3_key: str) -> Dict[str, Any]:
        """
        Upload audio file to S3 bucket.
        
        Args:
            local_file_path: Path to local audio file
            s3_key: S3 object key (path in bucket)
            
        Returns:
            Dictionary with S3 metadata including URL, bucket, key, etc.
        """
        try:
            if not self.s3_client:
                return {
                    'success': False,
                    'error': 'S3 client not initialized. Please check AWS credentials.'
                }
            
            # Get file size
            file_size = os.path.getsize(local_file_path)
            
            # Get content type based on file extension
            content_type = self._get_content_type(local_file_path)
            
            # Upload file to S3
            self.s3_client.upload_file(
                local_file_path,
                self.s3_bucket_name,
                s3_key,
                ExtraArgs={'ContentType': content_type}
            )
            
            # Generate S3 URL
            s3_url = f"https://{self.s3_bucket_name}.s3.{self.s3_region}.amazonaws.com/{s3_key}"
            
            # Get object metadata
            s3_metadata = {
                'bucket': self.s3_bucket_name,
                'key': s3_key,
                'url': s3_url,
                'region': self.s3_region,
                'size_bytes': file_size,
                'uploaded_at': datetime.now(timezone.utc).isoformat()
            }
            
            return {
                'success': True,
                'metadata': s3_metadata
            }
            
        except ClientError as e:
            return {
                'success': False,
                'error': f"S3 upload error: {str(e)}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error during S3 upload: {str(e)}"
            }
    
    def save_to_mongodb(self, transcription_data: Dict[str, Any], s3_metadata: Dict[str, Any], user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Save transcription data and S3 metadata to MongoDB.
        User ID is optional - if not provided, defaults to 'anonymous'.
        
        Args:
            transcription_data: Transcription JSON data
            s3_metadata: S3 metadata from upload
            user_id: User ID to associate with this transcription (optional, defaults to 'anonymous')
            
        Returns:
            Dictionary with MongoDB operation result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized. Please check MongoDB connection.'
                }
            
            # Use 'anonymous' if user_id is not provided
            if not user_id:
                user_id = 'anonymous'
            
            # Calculate edited_words_count if words are present
            if 'words' in transcription_data:
                edited_count = sum(1 for w in transcription_data['words'] if w.get('is_edited', False))
                transcription_data['edited_words_count'] = edited_count
            
            # Prepare document
            # assigned_user_id is None by default - admin will assign it later
            document = {
                'transcription_data': transcription_data,
                's3_metadata': s3_metadata,
                'user_id': user_id,  # Creator/owner of the transcription
                'assigned_user_id': None,  # Assigned to a specific user (managed by admin)
                'review_round': 0,  # Review round: 0 = first review, 1 = second review
                'review_history': [],  # History of review actions
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc)
            }
            
            # Insert document (MongoDB will create collection automatically if it doesn't exist)
            result = self.collection.insert_one(document)
            
            print(f"✅ Document saved to MongoDB collection '{self.mongodb_collection}'")
            print(f"   Document ID: {result.inserted_id}")
            
            return {
                'success': True,
                'document_id': str(result.inserted_id),
                'message': 'Data saved to MongoDB successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"MongoDB save error: {str(e)}"
            }
    
    def save_transcription(self, local_audio_path: str, transcription_data: Dict[str, Any], 
                          original_filename: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Complete save operation: upload audio to S3 and save transcription to MongoDB.
        User ID is optional - if not provided, defaults to 'anonymous'.
        
        Args:
            local_audio_path: Path to local audio file
            transcription_data: Transcription JSON data
            original_filename: Original audio filename
            user_id: User ID to associate with this transcription (optional, defaults to 'anonymous')
            
        Returns:
            Dictionary with complete operation result
        """
        try:
            # Use 'anonymous' if user_id is not provided
            if not user_id:
                user_id = 'anonymous'
            
            # Generate S3 key (path in bucket)
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
            file_extension = os.path.splitext(original_filename)[1]
            s3_key = f"audio/{timestamp}_{original_filename}"
            
            # Upload to S3
            s3_result = self.upload_audio_to_s3(local_audio_path, s3_key)
            
            if not s3_result['success']:
                return s3_result
            
            s3_metadata = s3_result['metadata']
            
            # Save to MongoDB
            mongo_result = self.save_to_mongodb(transcription_data, s3_metadata, user_id)
            
            if not mongo_result['success']:
                return mongo_result
            
            return {
                'success': True,
                's3_metadata': s3_metadata,
                'mongodb_id': mongo_result['document_id'],
                'message': 'Audio and transcription saved successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Save operation error: {str(e)}"
            }
    
    def save_reprocessed_transcription(self, original_transcription: Dict[str, Any], 
                                      new_transcription_data: Dict[str, Any], 
                                      user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Save reprocessed transcription to reprocessed_files collection.
        Uses the S3 metadata from the original transcription (doesn't re-upload audio).
        
        Args:
            original_transcription: The original transcription document from MongoDB
            new_transcription_data: The new transcription data from reprocessing
            user_id: User ID who initiated the reprocessing (optional)
            
        Returns:
            Dictionary with operation result including new document ID
        """
        try:
            if not self.reprocessed_collection:
                return {
                    'success': False,
                    'error': 'Reprocessed files collection not initialized. Please check MongoDB connection.'
                }
            
            # Use 'anonymous' if user_id is not provided
            if not user_id:
                user_id = 'anonymous'
            
            # Get S3 metadata from original transcription
            s3_metadata = original_transcription.get('s3_metadata', {})
            
            if not s3_metadata:
                return {
                    'success': False,
                    'error': 'Original transcription does not have S3 metadata'
                }
            
            # Calculate edited_words_count if words are present
            if 'words' in new_transcription_data:
                edited_count = sum(1 for w in new_transcription_data['words'] if w.get('is_edited', False))
                new_transcription_data['edited_words_count'] = edited_count
            
            # Prepare document with same schema as original collection
            document = {
                'transcription_data': new_transcription_data,
                's3_metadata': s3_metadata,  # Use S3 metadata from original
                'user_id': user_id,  # User who initiated reprocessing
                'original_transcription_id': str(original_transcription.get('_id', '')),  # Reference to original
                'assigned_user_id': original_transcription.get('assigned_user_id'),  # Keep same assignment
                'review_round': original_transcription.get('review_round', 0),  # Keep same review round
                'review_history': original_transcription.get('review_history', []),  # Copy review history
                'created_at': datetime.now(timezone.utc),  # New creation time for reprocessed version
                'updated_at': datetime.now(timezone.utc),
                'reprocessed_at': datetime.now(timezone.utc),  # Track when reprocessing happened
                'is_flagged': False,  # Reprocessed files are not flagged by default
                'flag_reason': None,
                'original_flag_reason': original_transcription.get('flag_reason'),  # Store original flag reason for reference
                'reprocessed_with_context': new_transcription_data.get('metadata', {}).get('reprocessed_with_context', False)
            }
            
            # Insert document into reprocessed_files collection
            result = self.reprocessed_collection.insert_one(document)
            
            print(f"✅ Reprocessed transcription saved to 'reprocessed_files' collection")
            print(f"   New Document ID: {result.inserted_id}")
            print(f"   Original Transcription ID: {document['original_transcription_id']}")
            
            return {
                'success': True,
                'document_id': str(result.inserted_id),
                'original_transcription_id': document['original_transcription_id'],
                'message': 'Reprocessed transcription saved successfully'
            }
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"❌ Error saving reprocessed transcription: {str(e)}")
            print(error_trace)
            return {
                'success': False,
                'error': f"Error saving reprocessed transcription: {str(e)}"
            }
    
    def get_transcription(self, document_id: str, user_id: Optional[str] = None, is_admin: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieve transcription from MongoDB by document ID.
        Regular users can only access transcriptions assigned to them.
        Admins can access all transcriptions.
        
        Args:
            document_id: MongoDB document ID
            user_id: User ID to check access (if not admin)
            is_admin: Whether the user is an admin
            
        Returns:
            Document data or None if not found or access denied
        """
        try:
            if not self.collection:
                return None
                
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate ObjectId format
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                print(f"❌ Invalid transcription ID format: {document_id}")
                return None
            
            # Get document by ID (check both collections)
            document = self.collection.find_one({'_id': obj_id})
            
            # If not found in main collection, check telugu_transcriptions collection
            if not document:
                telugu_collection = self.db.get_collection('telugu_transcriptions')
                document = telugu_collection.find_one({'_id': obj_id})
            
            if not document:
                return None
            
            # Check access: admins can see all, regular users only assigned ones
            if not is_admin and user_id:
                assigned_user_id = document.get('assigned_user_id')
                # If assigned_user_id doesn't exist (old data), deny access
                # If assigned_user_id exists but doesn't match, deny access
                if assigned_user_id is None or str(assigned_user_id) != str(user_id):
                    # User doesn't have access to this transcription
                    print(f"🚫 Access denied: user {user_id} trying to access transcription assigned to {assigned_user_id}")
                    return None
            
            # Convert ObjectId to string for JSON serialization (for all cases)
            document['_id'] = str(document['_id'])
            # Convert datetime to ISO format (for all cases)
            if 'created_at' in document and isinstance(document['created_at'], datetime):
                document['created_at'] = document['created_at'].isoformat()
            if 'updated_at' in document and isinstance(document['updated_at'], datetime):
                document['updated_at'] = document['updated_at'].isoformat()
            
            return document
        except Exception as e:
            print(f"❌ Error retrieving transcription: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def assign_transcription(self, document_id: str, assigned_user_id: str) -> Dict[str, Any]:
        """
        Assign a transcription to a specific user (admin only operation).
        
        Args:
            document_id: MongoDB document ID
            assigned_user_id: User ID to assign the transcription to
            
        Returns:
            Dictionary with assignment result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate and convert ObjectId
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                return {
                    'success': False,
                    'error': f'Invalid transcription ID format: {str(e)}'
                }
            
            # Ensure assigned_user_id is stored as string for consistent filtering
            assigned_user_id_str = str(assigned_user_id)
            
            # Find the document in any collection (transcriptions or telugu_transcriptions)
            current_doc = None
            target_collection = None
            collections_to_check = ['transcriptions', 'telugu_transcriptions']
            
            for coll_name in collections_to_check:
                coll = self.db[coll_name]
                doc = coll.find_one({'_id': obj_id})
                if doc:
                    current_doc = doc
                    target_collection = coll
                    if coll_name != self.mongodb_collection:
                        print(f"📝 Found transcription in collection '{coll_name}' (current default: '{self.mongodb_collection}')")
                    break
            
            # Check if document exists
            if not current_doc:
                print(f"❌ Transcription not found: {document_id} (ObjectId: {obj_id})")
                print(f"   Checked collections: {collections_to_check}")
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id} in any collection'
                }
            
            # Check if this is a first assignment (previous assigned_user_id is None or doesn't exist)
            previous_assigned_user_id = current_doc.get('assigned_user_id')
            is_first_assignment = previous_assigned_user_id is None
            
            # Prepare update fields
            update_fields = {
                'assigned_user_id': assigned_user_id_str,
                'updated_at': datetime.now(timezone.utc)
            }
            
            # Initialize review_round to 0 if not set (first assignment)
            if 'review_round' not in current_doc:
                update_fields['review_round'] = 0
            
            # Initialize review_history to empty array if not set
            if 'review_history' not in current_doc:
                update_fields['review_history'] = []
            
            # Update the assigned_user_id field in the correct collection
            update_result = target_collection.update_one(
                {'_id': obj_id},
                {
                    '$set': update_fields
                }
            )
            
            if update_result.matched_count == 0:
                print(f"❌ Update failed: Transcription {document_id} not found during update")
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id}'
                }
            
            # Verify the assignment was saved correctly (using the correct collection)
            updated_doc = target_collection.find_one({'_id': obj_id})
            saved_assigned_id = updated_doc.get('assigned_user_id') if updated_doc else None
            
            collection_name = target_collection.name
            print(f"✅ Assigned transcription {document_id} to user {assigned_user_id_str} in collection '{collection_name}'")
            print(f"   Verification: saved assigned_user_id = {saved_assigned_id}")
            
            if str(saved_assigned_id) != assigned_user_id_str:
                print(f"⚠️  Warning: Assignment mismatch! Expected {assigned_user_id_str}, got {saved_assigned_id}")
            
            return {
                'success': True,
                'document_id': document_id,
                'assigned_user_id': assigned_user_id_str,  # Return the string version for consistency
                'message': 'Transcription assigned successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error assigning transcription: {str(e)}"
            }
    
    def unassign_transcription(self, document_id: str) -> Dict[str, Any]:
        """
        Unassign a transcription (set assigned_user_id to None).
        
        Args:
            document_id: MongoDB document ID
            
        Returns:
            Dictionary with unassignment result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate and convert ObjectId
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                return {
                    'success': False,
                    'error': f'Invalid transcription ID format: {str(e)}'
                }
            
            # Find the document in any collection (transcriptions or telugu_transcriptions)
            current_doc = None
            target_collection = None
            collections_to_check = ['transcriptions', 'telugu_transcriptions']
            
            for coll_name in collections_to_check:
                coll = self.db[coll_name]
                doc = coll.find_one({'_id': obj_id})
                if doc:
                    current_doc = doc
                    target_collection = coll
                    if coll_name != self.mongodb_collection:
                        print(f"📝 Found transcription in collection '{coll_name}' (current default: '{self.mongodb_collection}')")
                    break
            
            if not current_doc:
                print(f"❌ Transcription not found: {document_id} (ObjectId: {obj_id})")
                print(f"   Checked collections: {collections_to_check}")
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id} in any collection'
                }
            
            # Remove the assigned_user_id (set to None) in the correct collection
            update_result = target_collection.update_one(
                {'_id': obj_id},
                {
                    '$set': {
                        'assigned_user_id': None,
                        'updated_at': datetime.now(timezone.utc)
                    }
                }
            )
            
            if update_result.matched_count == 0:
                print(f"❌ Update failed: Transcription {document_id} not found during update")
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id}'
                }
            
            collection_name = target_collection.name
            print(f"✅ Unassigned transcription {document_id} from collection '{collection_name}'")
            
            return {
                'success': True,
                'document_id': document_id,
                'message': 'Transcription unassigned successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error unassigning transcription: {str(e)}"
            }
    
    def flag_transcription(self, document_id: str, is_flagged: bool = True, flag_reason: Optional[str] = None, is_double_flagged: bool = False) -> Dict[str, Any]:
        """
        Flag or unflag a transcription.
        
        Args:
            document_id: MongoDB document ID
            is_flagged: Boolean to set flag state
            flag_reason: Optional reason for flagging
            is_double_flagged: Boolean to set double flag state (for reprocessed files that still have issues)
            
        Returns:
            Dictionary with update result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate and convert ObjectId
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                return {
                    'success': False,
                    'error': f'Invalid transcription ID format: {str(e)}'
                }
            
            # Update fields
            update_fields = {
                'is_flagged': is_flagged,
                'is_double_flagged': is_double_flagged,
                'updated_at': datetime.now(timezone.utc)
            }
            
            if is_flagged and flag_reason:
                update_fields['flag_reason'] = flag_reason
            elif not is_flagged:
                # Remove flag_reason if unflagging
                update_fields['flag_reason'] = None
                update_fields['is_double_flagged'] = False  # Also remove double flag when unflagging
            
            # Update the document
            update_result = self.collection.update_one(
                {'_id': obj_id},
                {
                    '$set': update_fields
                }
            )
            
            if update_result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Transcription not found'
                }
            
            flag_status = 'Double flagged' if is_double_flagged else ('Flagged' if is_flagged else 'Unflagged')
            print(f"✅ {flag_status} transcription {document_id}")
            print(f"   MongoDB update result: matched={update_result.matched_count}, modified={update_result.modified_count}")
            print(f"   Updated fields: {update_fields}")
            
            return {
                'success': True,
                'document_id': document_id,
                'is_flagged': is_flagged,
                'is_double_flagged': is_double_flagged,
                'flag_reason': flag_reason if is_flagged else None,
                'message': f"Transcription {flag_status.lower()} successfully"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error flagging transcription: {str(e)}"
            }
    
    def update_transcription_status(self, document_id: str, status: str) -> Dict[str, Any]:
        """
        Update transcription status (admin only operation).
        
        Args:
            document_id: MongoDB document ID
            status: Status to set ('done', 'pending', 'flagged', 'completed', 'validating', or 'passed')
            
        Returns:
            Dictionary with update result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            if status not in ['done', 'pending', 'flagged', 'completed', 'validating', 'passed']:
                return {
                    'success': False,
                    'error': f'Invalid status: {status}. Must be one of: done, pending, flagged, completed, validating, passed'
                }
            
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate and convert ObjectId
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                return {
                    'success': False,
                    'error': f'Invalid transcription ID format: {str(e)}'
                }
            
            # Find the document in any collection (transcriptions or telugu_transcriptions)
            current_doc = None
            target_collection = None
            collections_to_check = ['transcriptions', 'telugu_transcriptions']
            
            for coll_name in collections_to_check:
                coll = self.db[coll_name]
                doc = coll.find_one({'_id': obj_id})
                if doc:
                    current_doc = doc
                    target_collection = coll
                    if coll_name != self.mongodb_collection:
                        print(f"📝 Found transcription in collection '{coll_name}' for status update")
                    break
            
            if not current_doc:
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id} in any collection'
                }
            
            # Prepare update data
            update_fields = {
                'manual_status': status,
                'updated_at': datetime.now(timezone.utc)
            }
            
            # Set done_at or completed_at timestamp only if transitioning to that status for the first time
            current_manual_status = current_doc.get('manual_status')
            if status == 'done' and current_manual_status != 'done':
                # Only set done_at if not already set (first time marking as done)
                if not current_doc.get('done_at'):
                    update_fields['done_at'] = datetime.now(timezone.utc)
            elif status == 'completed' and current_manual_status != 'completed':
                # Only set completed_at if not already set (first time marking as completed)
                if not current_doc.get('completed_at'):
                    update_fields['completed_at'] = datetime.now(timezone.utc)
            
            # Update the manual_status field (admin override) in the correct collection
            update_result = target_collection.update_one(
                {'_id': obj_id},
                {'$set': update_fields}
            )
            
            if update_result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Transcription not found'
                }
            
            print(f"✅ Updated transcription status to '{status}': {document_id}")
            
            return {
                'success': True,
                'document_id': document_id,
                'status': status,
                'message': f"Transcription status updated to '{status}'"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error updating transcription status: {str(e)}"
            }
    
    def update_transcription_remarks(self, document_id: str, remarks: str) -> Dict[str, Any]:
        """
        Update transcription remarks (admin only operation).
        
        Args:
            document_id: MongoDB document ID
            remarks: Remarks text
            
        Returns:
            Dictionary with update result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate and convert ObjectId
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                return {
                    'success': False,
                    'error': f'Invalid transcription ID format: {str(e)}'
                }
            
            # Find the document in any collection (transcriptions or telugu_transcriptions)
            current_doc = None
            target_collection = None
            collections_to_check = ['transcriptions', 'telugu_transcriptions']
            
            for coll_name in collections_to_check:
                coll = self.db[coll_name]
                doc = coll.find_one({'_id': obj_id})
                if doc:
                    current_doc = doc
                    target_collection = coll
                    if coll_name != self.mongodb_collection:
                        print(f"📝 Found transcription in collection '{coll_name}' for remarks update")
                    break
            
            if not current_doc:
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id} in any collection'
                }
            
            # Update the remarks field in the correct collection
            update_result = target_collection.update_one(
                {'_id': obj_id},
                {
                    '$set': {
                        'remarks': remarks,
                        'updated_at': datetime.now(timezone.utc)
                    }
                }
            )
            
            if update_result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Transcription not found'
                }
            
            print(f"✅ Updated transcription remarks: {document_id}")
            
            return {
                'success': True,
                'document_id': document_id,
                'remarks': remarks,
                'message': 'Remarks updated successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error updating transcription remarks: {str(e)}"
            }
    
    def list_transcriptions(self, limit: int = 100, skip: int = 0, user_id: Optional[str] = None, is_admin: bool = False,
                           search: Optional[str] = None, language: Optional[str] = None, 
                           date: Optional[str] = None, status: Optional[str] = None,
                           assigned_user: Optional[str] = None, original_assignee: Optional[str] = None,
                           flagged: Optional[str] = None, transcription_type: Optional[str] = None) -> Dict[str, Any]:
        """
        List transcriptions from MongoDB with filtering and pagination.
        Regular users can only see transcriptions assigned to them.
        Admins can see all transcriptions.
        
        Args:
            limit: Maximum number of documents to return
            skip: Number of documents to skip
            user_id: User ID to filter transcriptions (if not admin)
            is_admin: Whether the user is an admin (admins see all transcriptions)
            search: Search term for filename (case-insensitive partial match)
            language: Filter by language (exact match)
            date: Filter by date (YYYY-MM-DD format, matches created_at date)
            status: Filter by status ('done', 'pending', 'flagged')
            assigned_user: Filter by current assigned user ID (or 'unassigned' for None)
            original_assignee: Filter by original assignee user ID (or 'unassigned' for None)
            flagged: Filter by flagged status ('flagged' or 'not-flagged')
            transcription_type: Filter by transcription type ('words' or 'phrases')
            
        Returns:
            Dictionary with list of transcriptions and metadata
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            # Build base query filter
            # Admins see all transcriptions, regular users only see assigned ones
            if is_admin:
                query_filter = {}
                print(f"👑 Admin user - showing all transcriptions")
            else:
                if user_id:
                    # Regular users see transcriptions assigned to them
                    # Ensure user_id is a string for comparison (MongoDB stores assigned_user_id as string)
                    user_id_str = str(user_id)
                    # Match documents where assigned_user_id equals user_id
                    # This will only match documents that have assigned_user_id field set to this user
                    query_filter = {'assigned_user_id': user_id_str}
                    print(f"🔍 Filtering transcriptions for user: {user_id_str} (is_admin: {is_admin})")
                else:
                    # If no user_id provided and not admin, return empty
                    # Match unassigned transcriptions (assigned_user_id is None or doesn't exist)
                    query_filter = {
                        '$or': [
                            {'assigned_user_id': None},
                            {'assigned_user_id': {'$exists': False}}
                        ]
                    }
                    print("⚠️  No user_id provided for non-admin user, showing unassigned only")
            
            # Apply additional filters
            additional_filters = []
            
            # Search filter (database-level using regex for better performance)
            # Search in filename, audio_path, and S3 key
            if search:
                search_regex = {'$regex': search, '$options': 'i'}  # Case-insensitive search
                additional_filters.append({
                    '$or': [
                        {'transcription_data.metadata.filename': search_regex},
                        {'transcription_data.audio_path': search_regex},
                        {'transcription_data.metadata.audio_path': search_regex},
                        {'s3_metadata.key': search_regex},
                    ]
                })
            
            # Language filter
            if language:
                additional_filters.append({'transcription_data.language': language})
            
            # Date filter
            # Use done_at for done files, completed_at for completed files, created_at for others
            if date:
                try:
                    from datetime import datetime, timezone, timedelta
                    # Parse date string (YYYY-MM-DD)
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    
                    # Create date range for the entire day in IST (UTC+5:30)
                    # IST is 5 hours 30 minutes ahead of UTC
                    # So to get the UTC equivalent of IST midnight, subtract 5:30
                    ist_offset = timedelta(hours=5, minutes=30)
                    
                    # Start of day in IST = midnight IST = previous day 18:30 UTC
                    start_of_day_ist = datetime.combine(date_obj.date(), datetime.min.time())
                    start_of_day_utc = (start_of_day_ist - ist_offset).replace(tzinfo=timezone.utc)
                    
                    # End of day in IST = 23:59:59 IST = same day 18:29:59 UTC
                    end_of_day_ist = datetime.combine(date_obj.date(), datetime.max.time())
                    end_of_day_utc = (end_of_day_ist - ist_offset).replace(tzinfo=timezone.utc)
                    
                    # Choose the appropriate date field based on status
                    # done_at: when file was first marked as done
                    # completed_at: when file was first marked as completed
                    # For files without done_at/completed_at (legacy), fall back to updated_at
                    if status == 'done':
                        # Filter by done_at if exists, otherwise fall back to updated_at
                        additional_filters.append({
                            '$or': [
                                {'done_at': {'$gte': start_of_day_utc, '$lte': end_of_day_utc}},
                                # Fallback for legacy files without done_at
                                {'$and': [
                                    {'done_at': {'$exists': False}},
                                    {'updated_at': {'$gte': start_of_day_utc, '$lte': end_of_day_utc}}
                                ]}
                            ]
                        })
                    elif status == 'completed':
                        # Filter by completed_at if exists, otherwise fall back to updated_at
                        additional_filters.append({
                            '$or': [
                                {'completed_at': {'$gte': start_of_day_utc, '$lte': end_of_day_utc}},
                                # Fallback for legacy files without completed_at
                                {'$and': [
                                    {'completed_at': {'$exists': False}},
                                    {'updated_at': {'$gte': start_of_day_utc, '$lte': end_of_day_utc}}
                                ]}
                            ]
                        })
                    else:
                        # For other statuses, filter by created_at
                        additional_filters.append({
                            'created_at': {
                                '$gte': start_of_day_utc,
                                '$lte': end_of_day_utc
                            }
                        })
                except ValueError:
                    print(f"⚠️  Invalid date format: {date}, ignoring date filter")
            
            # Status filter
            if status:
                if status == 'flagged':
                    # Flagged if is_flagged is True OR manual_status is 'flagged'
                    # BUT exclude double flagged files (double flagged has higher priority)
                    additional_filters.append({
                        '$and': [
                            {
                                '$or': [
                                    {'is_flagged': True},
                                    {'manual_status': 'flagged'}
                                ]
                            },
                            {'$or': [
                                {'is_double_flagged': False},
                                {'is_double_flagged': {'$exists': False}}
                            ]}
                        ]
                    })
                elif status == 'double_flagged':
                    # Double flagged files (is_double_flagged is True)
                    additional_filters.append({'is_double_flagged': True})
                elif status == 'reprocessed':
                    # Files that have been reprocessed (has_been_reprocessed is True)
                    # BUT exclude double flagged files (double flagged has higher priority)
                    additional_filters.append({
                        '$and': [
                            {'has_been_reprocessed': True},
                            {'$or': [
                                {'is_double_flagged': False},
                                {'is_double_flagged': {'$exists': False}}
                            ]}
                        ]
                    })
                elif status == 'done':
                    # Done if NOT flagged AND NOT completed AND (manual_status='done' OR (manual_status unset/invalid AND assigned==user))
                    additional_filters.append({
                        '$and': [
                            # Not flagged
                            {'$or': [{'is_flagged': False}, {'is_flagged': {'$exists': False}}]},
                            {'manual_status': {'$ne': 'flagged'}},
                            # Not completed, validating, or passed
                            {'manual_status': {'$nin': ['completed', 'validating', 'passed']}},
                            # Done condition
                            {'$or': [
                                {'manual_status': 'done'},
                                {
                                    '$and': [
                                        # manual_status not active (not done/pending/flagged/completed/validating/passed)
                                        {'manual_status': {'$nin': ['done', 'pending', 'flagged', 'completed', 'validating', 'passed']}},
                                        {'assigned_user_id': {'$ne': None}},
                                        {'user_id': {'$ne': None}},
                                        {'$expr': {'$eq': ['$assigned_user_id', '$user_id']}}
                                    ]
                                }
                            ]}
                        ]
                    })
                elif status == 'pending':
                    # Pending if NOT flagged AND NOT done AND NOT completed
                    additional_filters.append({
                        '$and': [
                            # Not flagged
                            {'$or': [{'is_flagged': False}, {'is_flagged': {'$exists': False}}]},
                            {'manual_status': {'$ne': 'flagged'}},
                            # Not completed, validating, or passed
                            {'manual_status': {'$nin': ['completed', 'validating', 'passed']}},
                            # Not done condition
                            {'$nor': [
                                {'manual_status': 'done'},
                                {
                                    '$and': [
                                        {'manual_status': {'$nin': ['done', 'pending', 'flagged', 'completed', 'validating', 'passed']}},
                                        {'assigned_user_id': {'$ne': None}},
                                        {'user_id': {'$ne': None}},
                                        {'$expr': {'$eq': ['$assigned_user_id', '$user_id']}}
                                    ]
                                }
                            ]}
                        ]
                    })
                elif status == 'completed':
                    # Completed if manual_status is 'completed' AND NOT flagged
                    # Flagged files have higher priority and should not appear as completed
                    additional_filters.append({
                        '$and': [
                            {'manual_status': 'completed'},
                            # Not flagged
                            {'$or': [{'is_flagged': False}, {'is_flagged': {'$exists': False}}]}
                        ]
                    })
                elif status == 'assigned_for_review':
                    # Assigned for review: has reassign action in review_history and assigned_user_id != user_id
                    # This will be handled in post-filtering since it requires checking review_history
                    pass
                elif status == 'validating':
                    # Validating: manual_status is 'validating'
                    additional_filters.append({'manual_status': 'validating'})
                elif status == 'passed':
                    # Passed: manual_status is 'passed'
                    additional_filters.append({'manual_status': 'passed'})
            
            # Assigned user filter (current assignee)
            if assigned_user:
                if assigned_user == 'unassigned':
                    additional_filters.append({
                        '$or': [
                            {'assigned_user_id': None},
                            {'assigned_user_id': {'$exists': False}}
                        ]
                    })
                else:
                    additional_filters.append({'assigned_user_id': str(assigned_user)})
            
            # Original assignee filter - this requires post-filtering since we need to check review_history
            # We'll handle this after fetching documents
            needs_original_assignee_filter = bool(original_assignee)
            
            # Flagged filter
            if flagged == 'flagged':
                additional_filters.append({'is_flagged': True})
            elif flagged == 'not-flagged':
                additional_filters.append({
                    '$or': [
                        {'is_flagged': False},
                        {'is_flagged': {'$exists': False}}
                    ]
                })
            
            # Transcription type filter
            if transcription_type:
                additional_filters.append({'transcription_data.transcription_type': transcription_type})
            
            # Combine all filters
            if additional_filters:
                if query_filter:
                    query_filter = {'$and': [query_filter] + additional_filters}
                else:
                    query_filter = {'$and': additional_filters} if len(additional_filters) > 1 else additional_filters[0]
            
            # Note: Search is now handled at database level via regex filter
            # Some statuses require post-filtering/sorting:
            # - 'assigned_for_review': needs to check review_history
            # - 'done': needs sorting to prioritize actual 'done' over 'assigned_for_review'
            # - 'original_assignee': needs to check review_history to find original assignee
            needs_post_filtering = (status == 'assigned_for_review') or needs_original_assignee_filter
            needs_post_sorting = (status == 'done')  # Need to sort after computing status
            
            # Calculate fetch size: if we need post-filtering or post-sorting, fetch all matching documents
            # For assigned_for_review, fetch 3x the limit (capped at 200)
            # For done status, fetch all documents to enable proper sorting across pages
            if needs_post_sorting:
                # For 'done' status, we need to fetch ALL matching documents to sort properly
                # Get the count first to know how many to fetch (from both collections)
                telugu_collection = self.db.get_collection('telugu_transcriptions')
                count_for_done = self.collection.count_documents(query_filter) + telugu_collection.count_documents(query_filter)
                fetch_limit = count_for_done  # Fetch all matching documents
                fetch_skip = 0
                print(f"📋 'done' filter detected - fetching all {fetch_limit} matching documents for proper sorting")
            elif needs_post_filtering:
                fetch_limit = min(limit * 3, 200)
                fetch_skip = 0  # Start from beginning if post-filtering
            else:
                fetch_limit = limit
                fetch_skip = skip
            
            # Get documents sorted by created_at descending (newest first)
            # Use projection to exclude large fields we don't need for list view
            # This significantly reduces data transfer and processing time
            # Excluding 'words' and 'phrases' arrays which can be very large
            projection = {
                '_id': 1,
                'created_at': 1,
                'updated_at': 1,
                'done_at': 1,  # When file was first marked as done
                'completed_at': 1,  # When file was first marked as completed
                'user_id': 1,
                'assigned_user_id': 1,
                'is_flagged': 1,
                'is_double_flagged': 1,
                'flag_reason': 1,
                'has_been_reprocessed': 1,
                'reprocessed_document_id': 1,
                'reprocessed_at': 1,
                'manual_status': 1,
                'review_round': 1,
                'review_history': 1,
                'transcription_data.transcription_type': 1,
                'transcription_data.language': 1,
                'transcription_data.total_words': 1,
                'transcription_data.total_phrases': 1,
                'transcription_data.audio_duration': 1,
                'transcription_data.audio_path': 1,
                'transcription_data.metadata.filename': 1,
                'transcription_data.metadata.audio_path': 1,
                'transcription_data.edited_words_count': 1,  # Use stored count if available
                'transcription_data.review_round_edited_words_count': 1,  # Use stored count for review round
                # Exclude words array for performance - it can be very large (100+ words)
                # 'transcription_data.words': 1,  # REMOVED - causes 16 second delays!
                's3_metadata.url': 1,
                's3_metadata.key': 1,
                'remarks': 1
            }
            
            find_start = time.time()
            
            # Query both collections: transcriptions and telugu_transcriptions
            telugu_collection = self.db.get_collection('telugu_transcriptions')
            
            # Query main collection
            cursor_main = self.collection.find(query_filter, projection).sort('created_at', -1)
            # Query telugu collection
            cursor_telugu = telugu_collection.find(query_filter, projection).sort('created_at', -1)
            
            find_time = (time.time() - find_start) * 1000
            print(f"⏱️  [TIMING] MongoDB find() query took {find_time:.2f}ms (fetch_limit={fetch_limit}, fetch_skip={fetch_skip})")
            
            # Process documents from both collections
            process_start = time.time()
            transcriptions = []
            
            # Process documents from main collection
            for doc in cursor_main:
                # Convert ObjectId to string
                doc['_id'] = str(doc['_id'])
                # Convert datetime to ISO format
                if 'created_at' in doc:
                    doc['created_at'] = doc['created_at'].isoformat()
                if 'updated_at' in doc:
                    doc['updated_at'] = doc['updated_at'].isoformat()
                
                # Extract summary info
                transcription_data = doc.get('transcription_data', {})
                s3_metadata = doc.get('s3_metadata', {})
                metadata = transcription_data.get('metadata', {})
                
                # Priority order for filename:
                # 1. metadata.filename (the actual filename - highest priority, preserves user's filename)
                # 2. audio_path from metadata.audio_path or transcription_data.audio_path
                # 3. S3 key (contains timestamped filename - strip timestamp prefix if needed)
                display_filename = ''
                
                # First priority: Use metadata.filename if it exists (this is the user's actual filename)
                if metadata.get('filename'):
                    display_filename = metadata.get('filename')
                else:
                    # Second priority: Check audio_path in both locations
                    audio_path = transcription_data.get('audio_path') or metadata.get('audio_path', '')
                    
                    if audio_path:
                        # Extract filename from audio_path (handle paths like "/api/audio/5143282_audio.mp3" or "5143282_audio.mp3")
                        if '/' in audio_path:
                            display_filename = audio_path.split('/')[-1]
                        else:
                            display_filename = audio_path
                    elif s3_metadata.get('key'):
                        # Last resort: Use S3 key which contains timestamped filename (e.g., "audio/20250120_123456_audio.mp3")
                        # Try to extract original filename by removing timestamp prefix
                        s3_key = s3_metadata.get('key', '')
                        s3_filename = s3_key.split('/')[-1] if '/' in s3_key else s3_key
                        
                        # Try to remove timestamp prefix (format: YYYYMMDD_HHMMSS_filename)
                        # Pattern: digits_underscore_digits_underscore_rest
                        import re
                        # Match pattern like "20251123_135107_test2_audio.mp3" and extract "test2_audio.mp3"
                        match = re.match(r'^\d{8}_\d{6}_(.+)$', s3_filename)
                        if match:
                            # Extract the original filename after timestamp
                            display_filename = match.group(1)
                        else:
                            # If pattern doesn't match, use the S3 filename as-is
                            display_filename = s3_filename
                    else:
                        # Final fallback: empty string
                        display_filename = ''
                
                # Calculate edited words count (for words type transcriptions)
                # Use stored count if available (set when document is saved/updated)
                # Otherwise default to 0 (words array excluded from projection for performance)
                edited_words_count = transcription_data.get('edited_words_count', 0)
                
                # Use stored review_round_edited_words_count instead of calculating
                # This avoids loading the entire words array which causes massive slowdown
                review_round_edited_words_count = transcription_data.get('review_round_edited_words_count', 0)
                
                # Determine status:
                # Priority order:
                # 1. "flagged" if is_flagged is True (highest priority - flagged files stay flagged)
                # 2. manual_status (if set by admin or when saving changes)
                # 3. "done" only if file is assigned AND the assigned user has saved changes (user_id matches assigned_user_id)
                # 4. "pending" if not assigned, or assigned but assigned user hasn't saved changes yet
                is_flagged = doc.get('is_flagged', False)
                assigned_user_id = doc.get('assigned_user_id')
                doc_user_id = doc.get('user_id')
                manual_status = doc.get('manual_status')  # Admin-set status override or set when saving changes
                
                # Flagged status has highest priority - flagged files stay flagged
                if is_flagged:
                    computed_status = 'flagged'
                # Special statuses (validating, passed, completed) have high priority - they should always be respected
                # This must be checked BEFORE reassignment logic to prevent these statuses from
                # being overridden by "assigned_for_review" when reassigned
                elif manual_status in ['validating', 'passed']:
                    computed_status = manual_status
                elif manual_status == 'completed':
                    computed_status = 'completed'
                # Check if file has been reassigned (has reassign action in review_history)
                # and new assignee hasn't saved yet (assigned_user_id != user_id)
                # This check should happen before other manual_status checks to override them when reassigned
                # IMPORTANT: This logic should NOT apply if the last person who saved is an admin
                # IMPORTANT: This logic should NOT override manual_status if it's explicitly set by admin
                elif assigned_user_id:
                    # PRIORITY: Check for special statuses (validating, passed, completed) FIRST before reassignment logic
                    # These statuses should always be respected when manually set
                    if manual_status in ['validating', 'passed', 'completed']:
                        computed_status = manual_status
                    # If manual_status is explicitly set, always respect it (admin override or user save)
                    # This ensures admin status changes take priority over reassignment logic
                    elif manual_status and manual_status in ['done', 'pending', 'flagged']:
                        # Always respect manual_status when it's explicitly set
                        computed_status = manual_status
                    else:
                        # No manual_status set, check reassignment status
                        review_history = doc.get('review_history', [])
                        has_reassign_action = any(entry.get('action') == 'reassign' for entry in review_history)
                        
                        # Check if last saver (user_id) is an admin
                        is_last_saver_admin = False
                        if doc_user_id:
                            try:
                                from bson import ObjectId
                                users_collection = self.db['users']
                                user_doc = users_collection.find_one({'_id': ObjectId(doc_user_id)})
                                if user_doc:
                                    is_last_saver_admin = user_doc.get('is_admin', False)
                            except Exception:
                                # If we can't check, assume not admin
                                pass
                        
                        # Only show "assigned_for_review" if:
                        # 1. File was reassigned AND
                        # 2. assigned_user_id != user_id AND
                        # 3. Last person who saved (user_id) is NOT an admin
                        if has_reassign_action and doc_user_id and str(assigned_user_id) != str(doc_user_id) and not is_last_saver_admin:
                            # File was reassigned and new assignee hasn't saved yet
                            # This overrides manual_status to show "Assigned for Review"
                            computed_status = 'assigned_for_review'
                        elif doc_user_id and str(assigned_user_id) == str(doc_user_id):
                            # Assigned and assigned user has saved changes
                            computed_status = 'done'
                        else:
                            # Assigned but assigned user hasn't saved yet (first assignment)
                            computed_status = 'pending'
                # Use manual_status if set and file is not assigned
                elif manual_status and manual_status in ['done', 'pending', 'flagged', 'completed', 'validating', 'passed']:
                    computed_status = manual_status
                else:
                    computed_status = 'pending'  # Not assigned
                
                # Apply status filter (if specified and doesn't match, skip this document)
                # Only apply application-level status filtering for 'assigned_for_review' since
                # all other statuses are already filtered at the database level
                # Note: double_flagged and reprocessed use separate DB fields and don't need this check
                if status == 'assigned_for_review' and computed_status != status:
                    continue
                
                # Apply original assignee filter (post-filtering)
                if needs_original_assignee_filter:
                    # Get original assignee from review_history
                    review_history = doc.get('review_history', [])
                    original_assignee_id = None
                    
                    # Find the first reassign action to get the original assignee
                    reassign_action = None
                    for entry in review_history:
                        if entry.get('action') == 'reassign':
                            reassign_action = entry
                            break
                    
                    if reassign_action and reassign_action.get('previous_assigned_user_id'):
                        original_assignee_id = str(reassign_action.get('previous_assigned_user_id'))
                    else:
                        # If no reassign action, current assigned_user_id is the original
                        current_assigned = doc.get('assigned_user_id')
                        original_assignee_id = str(current_assigned) if current_assigned else None
                    
                    # Filter by original assignee
                    if original_assignee == 'unassigned':
                        # Match if original assignee is None or doesn't exist
                        if original_assignee_id:
                            continue
                    else:
                        # Match if original assignee matches the filter
                        if not original_assignee_id or str(original_assignee_id) != str(original_assignee):
                            continue
                
                # Search filter is now handled at database level via regex
                # No need for post-filtering on search term
                
                summary = {
                    '_id': doc['_id'],
                    'created_at': doc.get('created_at'),
                    'updated_at': doc.get('updated_at'),
                    'transcription_type': transcription_data.get('transcription_type', 'words'),
                    'language': transcription_data.get('language', 'Unknown'),
                    'total_words': transcription_data.get('total_words', 0),
                    'total_phrases': transcription_data.get('total_phrases', 0),
                    'audio_duration': transcription_data.get('audio_duration', 0),
                    's3_url': s3_metadata.get('url', ''),
                    'filename': display_filename,
                    'user_id': doc.get('user_id'),  # Creator/saver
                    'assigned_user_id': doc.get('assigned_user_id'),  # Assigned user
                    'status': computed_status,  # 'done', 'pending', 'flagged', or 'completed'
                    'is_flagged': is_flagged,
                    'is_double_flagged': doc.get('is_double_flagged', False),
                    'flag_reason': doc.get('flag_reason'),
                    'has_been_reprocessed': doc.get('has_been_reprocessed', False),
                    'reprocessed_document_id': doc.get('reprocessed_document_id'),
                    'edited_words_count': edited_words_count,  # Number of words edited
                    'review_round_edited_words_count': review_round_edited_words_count,  # Number of words edited in review round
                    'remarks': doc.get('remarks'),
                    'review_round': doc.get('review_round', 0),
                    'review_history': doc.get('review_history', []),
                    'passed_by': doc.get('passed_by')
                }
                transcriptions.append(summary)
            
            # Process documents from telugu collection (same logic as above)
            for doc in cursor_telugu:
                # Convert ObjectId to string
                doc['_id'] = str(doc['_id'])
                # Convert datetime to ISO format
                if 'created_at' in doc:
                    doc['created_at'] = doc['created_at'].isoformat()
                if 'updated_at' in doc:
                    doc['updated_at'] = doc['updated_at'].isoformat()
                
                # Extract summary info
                transcription_data = doc.get('transcription_data', {})
                s3_metadata = doc.get('s3_metadata', {})
                metadata = transcription_data.get('metadata', {})
                
                # Priority order for filename:
                display_filename = ''
                if metadata.get('filename'):
                    display_filename = metadata.get('filename')
                else:
                    audio_path = transcription_data.get('audio_path') or metadata.get('audio_path', '')
                    if audio_path:
                        if '/' in audio_path:
                            display_filename = audio_path.split('/')[-1]
                        else:
                            display_filename = audio_path
                    elif s3_metadata.get('key'):
                        s3_key = s3_metadata.get('key', '')
                        s3_filename = s3_key.split('/')[-1] if '/' in s3_key else s3_key
                        import re
                        match = re.match(r'^\d{8}_\d{6}_(.+)$', s3_filename)
                        if match:
                            display_filename = match.group(1)
                        else:
                            display_filename = s3_filename
                    else:
                        display_filename = ''
                
                edited_words_count = transcription_data.get('edited_words_count', 0)
                review_round_edited_words_count = transcription_data.get('review_round_edited_words_count', 0)
                
                # Determine status (same logic as above)
                is_flagged = doc.get('is_flagged', False)
                assigned_user_id = doc.get('assigned_user_id')
                doc_user_id = doc.get('user_id')
                manual_status = doc.get('manual_status')
                
                if is_flagged:
                    computed_status = 'flagged'
                elif manual_status in ['validating', 'passed']:
                    # PRIORITY: validating and passed statuses should always be respected
                    computed_status = manual_status
                elif manual_status == 'completed':
                    computed_status = 'completed'
                elif assigned_user_id:
                    review_history = doc.get('review_history', [])
                    has_reassign_action = any(entry.get('action') == 'reassign' for entry in review_history)
                    
                    is_last_saver_admin = False
                    if doc_user_id:
                        try:
                            from bson import ObjectId
                            users_collection = self.db['users']
                            user_doc = users_collection.find_one({'_id': ObjectId(doc_user_id)})
                            if user_doc:
                                is_last_saver_admin = user_doc.get('is_admin', False)
                        except Exception:
                            pass
                    
                    if has_reassign_action and doc_user_id and str(assigned_user_id) != str(doc_user_id) and not is_last_saver_admin:
                        computed_status = 'assigned_for_review'
                    elif manual_status and manual_status in ['done', 'pending', 'flagged']:
                        computed_status = manual_status
                    elif doc_user_id and str(assigned_user_id) == str(doc_user_id):
                        computed_status = 'done'
                    else:
                        computed_status = 'pending'
                elif manual_status and manual_status in ['done', 'pending', 'flagged', 'completed', 'validating', 'passed']:
                    computed_status = manual_status
                else:
                    computed_status = 'pending'
                
                # Apply status filter
                if status == 'assigned_for_review' and computed_status != status:
                    continue
                
                # Apply original assignee filter
                if needs_original_assignee_filter:
                    review_history = doc.get('review_history', [])
                    original_assignee_id = None
                    
                    reassign_action = None
                    for entry in review_history:
                        if entry.get('action') == 'reassign':
                            reassign_action = entry
                            break
                    
                    if reassign_action and reassign_action.get('previous_assigned_user_id'):
                        original_assignee_id = str(reassign_action.get('previous_assigned_user_id'))
                    else:
                        current_assigned = doc.get('assigned_user_id')
                        original_assignee_id = str(current_assigned) if current_assigned else None
                    
                    if original_assignee == 'unassigned':
                        if original_assignee_id:
                            continue
                    else:
                        if not original_assignee_id or str(original_assignee_id) != str(original_assignee):
                            continue
                
                summary = {
                    '_id': doc['_id'],
                    'created_at': doc.get('created_at'),
                    'updated_at': doc.get('updated_at'),
                    'transcription_type': transcription_data.get('transcription_type', 'words'),
                    'language': transcription_data.get('language', 'Unknown'),
                    'total_words': transcription_data.get('total_words', 0),
                    'total_phrases': transcription_data.get('total_phrases', 0),
                    'audio_duration': transcription_data.get('audio_duration', 0),
                    's3_url': s3_metadata.get('url', ''),
                    'filename': display_filename,
                    'user_id': doc.get('user_id'),
                    'assigned_user_id': doc.get('assigned_user_id'),
                    'status': computed_status,
                    'is_flagged': is_flagged,
                    'is_double_flagged': doc.get('is_double_flagged', False),
                    'flag_reason': doc.get('flag_reason'),
                    'has_been_reprocessed': doc.get('has_been_reprocessed', False),
                    'reprocessed_document_id': doc.get('reprocessed_document_id'),
                    'edited_words_count': edited_words_count,
                    'review_round_edited_words_count': review_round_edited_words_count,
                    'remarks': doc.get('remarks'),
                    'review_round': doc.get('review_round', 0),
                    'review_history': doc.get('review_history', []),
                    'passed_by': doc.get('passed_by')
                }
                transcriptions.append(summary)
            
            process_time = (time.time() - process_start) * 1000
            print(f"⏱️  [TIMING] Document processing took {process_time:.2f}ms (processed {len(transcriptions)} documents before pagination)")
            
            # Sort all transcriptions by created_at descending (newest first) before applying status-based sorting
            transcriptions.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            
            # When filtering by 'done', sort to prioritize actual 'done' files over 'assigned_for_review' files
            # This ensures done files appear first across all pages
            if needs_post_sorting:
                sort_start = time.time()
                transcriptions.sort(key=lambda x: (
                    0 if x['status'] == 'done' else 1  # done files first (0), then assigned_for_review (1)
                ))
                sort_time = (time.time() - sort_start) * 1000
                done_count = sum(1 for t in transcriptions if t['status'] == 'done')
                assigned_for_review_count = sum(1 for t in transcriptions if t['status'] == 'assigned_for_review')
                print(f"📋 Sorted {len(transcriptions)} 'done' filter results in {sort_time:.2f}ms: {done_count} actual done files first, then {assigned_for_review_count} assigned_for_review")
            
            # Apply pagination if we did post-filtering or post-sorting
            if needs_post_filtering or needs_post_sorting:
                total_count = len(transcriptions)
                # Apply skip and limit to filtered/sorted results
                transcriptions = transcriptions[skip:skip + limit]
                operation = "post-sorting" if needs_post_sorting else "post-filtering"
                print(f"📄 Applied {operation} pagination: showing {len(transcriptions)} of {total_count} results (skip={skip}, limit={limit})")
            else:
                # Get total count for non-post-filtered queries (from both collections)
                count_start = time.time()
                telugu_collection = self.db.get_collection('telugu_transcriptions')
                if is_admin and query_filter == {}:
                    # For admin users querying all documents, use estimated_document_count (much faster, O(1))
                    total_count = self.collection.estimated_document_count() + telugu_collection.estimated_document_count()
                else:
                    # For filtered queries, use count_documents (exact count)
                    total_count = self.collection.count_documents(query_filter) + telugu_collection.count_documents(query_filter)
                count_time = (time.time() - count_start) * 1000
                print(f"📊 Total count: {total_count} (count took {count_time:.2f}ms)")
            
            return {
                'success': True,
                'transcriptions': transcriptions,
                'total': total_count,
                'limit': limit,
                'skip': skip
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error listing transcriptions: {str(e)}"
            }
    
    def get_transcription_statistics(self, user_id: Optional[str] = None, is_admin: bool = False, 
                                     transcription_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about transcriptions (total, done, pending, flagged counts).
        Regular users only see statistics for transcriptions assigned to them.
        Admins see statistics for all transcriptions.
        
        Args:
            user_id: User ID to filter transcriptions (if not admin)
            is_admin: Whether the user is an admin (admins see all transcriptions)
            transcription_type: Filter by transcription type ('words' or 'phrases')
            
        Returns:
            Dictionary with statistics
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            # Build base query filter (same as list_transcriptions)
            if is_admin:
                query_filter = {}
            else:
                if user_id:
                    user_id_str = str(user_id)
                    query_filter = {'assigned_user_id': user_id_str}
                else:
                    query_filter = {
                        '$or': [
                            {'assigned_user_id': None},
                            {'assigned_user_id': {'$exists': False}}
                        ]
                    }
            
            # Add transcription_type filter if specified
            if transcription_type:
                if query_filter:
                    query_filter = {'$and': [query_filter, {'transcription_data.transcription_type': transcription_type}]}
                else:
                    query_filter = {'transcription_data.transcription_type': transcription_type}
            
            # Get all matching documents (we need to process them to determine status)
            # Use projection to exclude large fields, but include audio_duration for duration calculation
            projection = {
                '_id': 1,
                'user_id': 1,
                'assigned_user_id': 1,
                'is_flagged': 1,
                'is_double_flagged': 1,
                'has_been_reprocessed': 1,
                'manual_status': 1,
                'review_round': 1,
                'review_history': 1,
                'transcription_data.transcription_type': 1,
                'transcription_data.audio_duration': 1
            }
            
            # Query both collections: transcriptions and telugu_transcriptions
            cursor_main = self.collection.find(query_filter, projection)
            telugu_collection = self.db.get_collection('telugu_transcriptions')
            cursor_telugu = telugu_collection.find(query_filter, projection)
            
            # Initialize counters
            total_count = 0
            done_count = 0
            pending_count = 0
            flagged_count = 0
            completed_count = 0
            validating_count = 0
            passed_count = 0
            double_flagged_count = 0
            reprocessed_count = 0
            total_done_duration = 0.0  # Total duration in seconds
            total_completed_duration = 0.0  # Total duration for completed files
            
            # Process each document to determine status (from main collection)
            for doc in cursor_main:
                total_count += 1
                
                # Get audio duration if available
                transcription_data = doc.get('transcription_data', {})
                audio_duration = transcription_data.get('audio_duration', 0) or 0
                
                # Track double flagged and reprocessed separately
                is_double_flagged = doc.get('is_double_flagged', False)
                has_been_reprocessed = doc.get('has_been_reprocessed', False)
                
                if is_double_flagged:
                    double_flagged_count += 1
                if has_been_reprocessed:
                    reprocessed_count += 1
                
                # Determine status (same logic as in list_transcriptions)
                is_flagged = doc.get('is_flagged', False)
                assigned_user_id = doc.get('assigned_user_id')
                doc_user_id = doc.get('user_id')
                manual_status = doc.get('manual_status')
                review_round = doc.get('review_round', 0)
                
                # Flagged status has highest priority - flagged files stay flagged
                if is_flagged:
                    status = 'flagged'
                # Special statuses (validating, passed, completed) have high priority - they should always be respected
                # This must be checked BEFORE other logic to prevent these statuses from being overridden
                elif manual_status in ['validating', 'passed']:
                    status = manual_status
                elif manual_status == 'completed':
                    status = 'completed'
                elif assigned_user_id:
                    # PRIORITY: Check for special statuses (validating, passed) FIRST
                    # These statuses should always be respected when manually set
                    if manual_status in ['validating', 'passed']:
                        status = manual_status
                    elif manual_status and manual_status in ['done', 'pending', 'flagged', 'completed']:
                        status = manual_status
                    elif doc_user_id and str(assigned_user_id) == str(doc_user_id):
                        # Assigned and assigned user has saved changes
                        status = 'done'
                    else:
                        # Assigned but assigned user hasn't saved yet (first assignment)
                        status = 'pending'
                # Use manual_status if set and file is not assigned
                elif manual_status and manual_status in ['done', 'pending', 'flagged', 'completed', 'validating', 'passed']:
                    status = manual_status
                else:
                    status = 'pending'  # Not assigned
                
                # Count by status and accumulate duration for done/completed files
                if status == 'completed':
                    completed_count += 1
                    total_completed_duration += float(audio_duration)
                elif status == 'done':
                    done_count += 1
                    total_done_duration += float(audio_duration)
                elif status == 'flagged':
                    flagged_count += 1
                elif status == 'validating':
                    validating_count += 1
                elif status == 'passed':
                    passed_count += 1
                else:
                    pending_count += 1
            
            # Process each document to determine status (from telugu collection)
            for doc in cursor_telugu:
                total_count += 1
                
                # Get audio duration if available
                transcription_data = doc.get('transcription_data', {})
                audio_duration = transcription_data.get('audio_duration', 0) or 0
                
                # Track double flagged and reprocessed separately
                is_double_flagged = doc.get('is_double_flagged', False)
                has_been_reprocessed = doc.get('has_been_reprocessed', False)
                
                if is_double_flagged:
                    double_flagged_count += 1
                if has_been_reprocessed:
                    reprocessed_count += 1
                
                # Determine status (same logic as in list_transcriptions)
                is_flagged = doc.get('is_flagged', False)
                assigned_user_id = doc.get('assigned_user_id')
                doc_user_id = doc.get('user_id')
                manual_status = doc.get('manual_status')
                review_round = doc.get('review_round', 0)
                
                # Flagged status has highest priority - flagged files stay flagged
                if is_flagged:
                    status = 'flagged'
                # Special statuses (validating, passed, completed) have high priority - they should always be respected
                # This must be checked BEFORE other logic to prevent these statuses from being overridden
                elif manual_status in ['validating', 'passed']:
                    status = manual_status
                elif manual_status == 'completed':
                    status = 'completed'
                elif assigned_user_id:
                    # PRIORITY: Check for special statuses (validating, passed) FIRST
                    # These statuses should always be respected when manually set
                    if manual_status in ['validating', 'passed']:
                        status = manual_status
                    elif manual_status and manual_status in ['done', 'pending', 'flagged', 'completed']:
                        status = manual_status
                    elif doc_user_id and str(assigned_user_id) == str(doc_user_id):
                        # Assigned and assigned user has saved changes
                        status = 'done'
                    else:
                        # Assigned but assigned user hasn't saved yet (first assignment)
                        status = 'pending'
                # Use manual_status if set and file is not assigned
                elif manual_status and manual_status in ['done', 'pending', 'flagged', 'completed', 'validating', 'passed']:
                    status = manual_status
                else:
                    status = 'pending'  # Not assigned
                
                # Count by status and accumulate duration for done/completed files
                if status == 'completed':
                    completed_count += 1
                    total_completed_duration += float(audio_duration)
                elif status == 'done':
                    done_count += 1
                    total_done_duration += float(audio_duration)
                elif status == 'flagged':
                    flagged_count += 1
                elif status == 'validating':
                    validating_count += 1
                elif status == 'passed':
                    passed_count += 1
                else:
                    pending_count += 1
            
            print(f"📊 Statistics calculation complete:")
            print(f"   Total: {total_count}")
            print(f"   Done: {done_count}")
            print(f"   Completed: {completed_count}")
            print(f"   Validating: {validating_count}")
            print(f"   Passed: {passed_count}")
            print(f"   Pending: {pending_count}")
            print(f"   Flagged: {flagged_count}")
            
            return {
                'success': True,
                'statistics': {
                    'total': total_count,
                    'done': done_count,
                    'pending': pending_count,
                    'flagged': flagged_count,
                    'completed': completed_count,
                    'validating': validating_count,
                    'passed': passed_count,
                    'double_flagged': double_flagged_count,
                    'reprocessed': reprocessed_count,
                    'total_done_duration': total_done_duration,
                    'total_completed_duration': total_completed_duration
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error getting statistics: {str(e)}"
            }
    
    def update_transcription(self, document_id: str, transcription_data: Dict[str, Any], user_id: Optional[str] = None, 
                            status: Optional[str] = None, review_round: Optional[int] = None) -> Dict[str, Any]:
        """
        Update transcription data in MongoDB (all users can update all data).
        Tracks version history of word and timestamp changes.
        
        Args:
            document_id: MongoDB document ID
            transcription_data: Updated transcription data
            user_id: User ID to mark who saved the changes (optional)
            status: Status to set (optional, e.g., 'done', 'completed')
            review_round: Review round number (optional)
            
        Returns:
            Dictionary with update result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            from bson import ObjectId
            
            # Find the document in any collection (transcriptions or telugu_transcriptions)
            current_doc = None
            target_collection = None
            collections_to_check = ['transcriptions', 'telugu_transcriptions']
            
            for coll_name in collections_to_check:
                coll = self.db[coll_name]
                doc = coll.find_one({'_id': ObjectId(document_id)})
                if doc:
                    current_doc = doc
                    target_collection = coll
                    if coll_name != self.mongodb_collection:
                        print(f"📝 Found transcription in collection '{coll_name}' for update")
                    break
            
            if not current_doc:
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id} in any collection'
                }
            
            # Get current transcription data
            current_transcription_data = current_doc.get('transcription_data', {})
            
            # Track version history - collect ALL changes (modifications, additions, deletions)
            all_changes = []
            
            # Compare words if transcription type is 'words'
            if transcription_data.get('transcription_type') == 'words':
                old_words = current_transcription_data.get('words', [])
                new_words = transcription_data.get('words', [])
                
                # Create a function to create a unique key for a word
                def word_key(word):
                    """Create a unique key for a word based on its content."""
                    return (word.get('word', ''), str(word.get('start', '')), str(word.get('end', '')))
                
                # Build sets of word keys for efficient lookup
                old_word_keys = {word_key(w) for w in old_words}
                new_word_keys = {word_key(w) for w in new_words}
                
                # Track modifications and keep track of which keys were modified to avoid double-counting
                modified_old_keys = set()
                modified_new_keys = set()
                
                # Track all modifications (words at same position that changed)
                # This must be checked before additions/deletions to avoid false positives
                for i in range(min(len(old_words), len(new_words))):
                    old_word = old_words[i]
                    new_word = new_words[i]
                    old_key = word_key(old_word)
                    new_key = word_key(new_word)
                    
                    if old_key != new_key:
                        # Check if this is a true modification (not a move)
                        # If old_word exists elsewhere in new_words, it was moved, not modified
                        # If new_word exists elsewhere in old_words, it was moved, not modified
                        old_exists_elsewhere = any(word_key(w) == old_key for j, w in enumerate(new_words) if j != i)
                        new_exists_elsewhere = any(word_key(w) == new_key for j, w in enumerate(old_words) if j != i)
                        
                        if not old_exists_elsewhere and not new_exists_elsewhere:
                            # True modification at this position
                            all_changes.append({
                                'before': {
                                    'word': old_word.get('word', ''),
                                    'start': old_word.get('start', ''),
                                    'end': old_word.get('end', '')
                                },
                                'after': {
                                    'word': new_word.get('word', ''),
                                    'start': new_word.get('start', ''),
                                    'end': new_word.get('end', '')
                                }
                            })
                            # Track these keys as modified to exclude from additions/deletions
                            modified_old_keys.add(old_key)
                            modified_new_keys.add(new_key)
                
                # Track all additions (words in new but not in old, excluding modifications)
                for new_word in new_words:
                    new_key = word_key(new_word)
                    if new_key not in old_word_keys and new_key not in modified_new_keys:
                        # This is a new word (not a modification)
                        all_changes.append({
                            'before': None,
                            'after': {
                                'word': new_word.get('word', ''),
                                'start': new_word.get('start', ''),
                                'end': new_word.get('end', '')
                            }
                        })
                
                # Track all deletions (words in old but not in new, excluding modifications)
                for old_word in old_words:
                    old_key = word_key(old_word)
                    if old_key not in new_word_keys and old_key not in modified_old_keys:
                        # This word was deleted (not a modification)
                        all_changes.append({
                            'before': {
                                'word': old_word.get('word', ''),
                                'start': old_word.get('start', ''),
                                'end': old_word.get('end', '')
                            },
                            'after': None
                        })
            
            # Compare phrases if transcription type is 'phrases'
            elif transcription_data.get('transcription_type') == 'phrases':
                old_phrases = current_transcription_data.get('phrases', [])
                new_phrases = transcription_data.get('phrases', [])
                
                # Create a function to create a unique key for a phrase
                def phrase_key(phrase):
                    """Create a unique key for a phrase based on its content."""
                    return (phrase.get('text', ''), str(phrase.get('start', '')), str(phrase.get('end', '')))
                
                # Build sets of phrase keys for efficient lookup
                old_phrase_keys = {phrase_key(p) for p in old_phrases}
                new_phrase_keys = {phrase_key(p) for p in new_phrases}
                
                # Track modifications and keep track of which keys were modified to avoid double-counting
                modified_old_keys = set()
                modified_new_keys = set()
                
                # Track all modifications (phrases at same position that changed)
                for i in range(min(len(old_phrases), len(new_phrases))):
                    old_phrase = old_phrases[i]
                    new_phrase = new_phrases[i]
                    old_key = phrase_key(old_phrase)
                    new_key = phrase_key(new_phrase)
                    
                    if old_key != new_key:
                        # Check if this is a true modification (not a move)
                        old_exists_elsewhere = any(phrase_key(p) == old_key for j, p in enumerate(new_phrases) if j != i)
                        new_exists_elsewhere = any(phrase_key(p) == new_key for j, p in enumerate(old_phrases) if j != i)
                        
                        if not old_exists_elsewhere and not new_exists_elsewhere:
                            # True modification at this position
                            all_changes.append({
                                'before': {
                                    'word': old_phrase.get('text', ''),
                                    'start': old_phrase.get('start', ''),
                                    'end': old_phrase.get('end', '')
                                },
                                'after': {
                                    'word': new_phrase.get('text', ''),
                                    'start': new_phrase.get('start', ''),
                                    'end': new_phrase.get('end', '')
                                }
                            })
                            # Track these keys as modified to exclude from additions/deletions
                            modified_old_keys.add(old_key)
                            modified_new_keys.add(new_key)
                
                # Track all additions (phrases in new but not in old, excluding modifications)
                for new_phrase in new_phrases:
                    new_key = phrase_key(new_phrase)
                    if new_key not in old_phrase_keys and new_key not in modified_new_keys:
                        # This is a new phrase (not a modification)
                        all_changes.append({
                            'before': None,
                            'after': {
                                'word': new_phrase.get('text', ''),
                                'start': new_phrase.get('start', ''),
                                'end': new_phrase.get('end', '')
                            }
                        })
                
                # Track all deletions (phrases in old but not in new, excluding modifications)
                for old_phrase in old_phrases:
                    old_key = phrase_key(old_phrase)
                    if old_key not in new_phrase_keys and old_key not in modified_old_keys:
                        # This phrase was deleted (not a modification)
                        all_changes.append({
                            'before': {
                                'word': old_phrase.get('text', ''),
                                'start': old_phrase.get('start', ''),
                                'end': old_phrase.get('end', '')
                            },
                            'after': None
                        })
            
            # Save all version history entries to separate collection
            if all_changes and self.version_history_collection:
                timestamp = datetime.now(timezone.utc)
                version_docs = []
                for change in all_changes:
                    version_doc = {
                        'transcription_id': document_id,
                        'timestamp': timestamp,
                        'user_id': str(user_id) if user_id else None,
                        'before': change['before'],
                        'after': change['after']
                    }
                    version_docs.append(version_doc)
                
                # Insert all changes as separate version history entries
                if version_docs:
                    self.version_history_collection.insert_many(version_docs)
                    print(f"✅ Saved {len(version_docs)} version history entries for transcription: {document_id}")
            
            # Prepare update data
            
            # Calculate edited_words_count and review_round_edited_words_count if words are present
            if transcription_data.get('transcription_type') == 'words' and 'words' in transcription_data:
                edited_count = sum(1 for w in transcription_data['words'] if w.get('is_edited', False))
                transcription_data['edited_words_count'] = edited_count
                
                # Also calculate review round edited words count and store it
                review_round_edited_count = sum(1 for w in transcription_data['words'] if w.get('edited_in_review_round', False))
                transcription_data['review_round_edited_words_count'] = review_round_edited_count
                
            update_data = {
                'transcription_data': transcription_data,
                'updated_at': datetime.now(timezone.utc)
            }
            
            # If user_id is provided, update it to mark who saved the changes
            if user_id:
                update_data['user_id'] = str(user_id)  # Ensure it's a string
            
            # Set status if provided, otherwise default to "done"
            # Note: If file is flagged, the status computation will still show "flagged" 
            # because flagged status has higher priority than manual_status
            # But we set manual_status so that when unflagged, it will show the correct status
            new_status = status if status else 'done'
            update_data['manual_status'] = new_status
            
            # Set done_at or completed_at timestamp only if transitioning to that status for the first time
            current_manual_status = current_doc.get('manual_status')
            if new_status == 'done' and current_manual_status != 'done':
                # Only set done_at if not already set (first time marking as done)
                if not current_doc.get('done_at'):
                    update_data['done_at'] = datetime.now(timezone.utc)
            elif new_status == 'completed' and current_manual_status != 'completed':
                # Only set completed_at if not already set (first time marking as completed)
                if not current_doc.get('completed_at'):
                    update_data['completed_at'] = datetime.now(timezone.utc)
            
            # Set review_round if provided
            if review_round is not None:
                update_data['review_round'] = review_round
            
            # Initialize review_history if it doesn't exist
            # current_doc was already fetched above, use it here
            if current_doc and 'review_history' not in current_doc:
                update_data['review_history'] = []
            
            # Update document by ID only (no user_id filtering)
            # Use the target_collection we found earlier
            update_result = target_collection.update_one(
                {'_id': ObjectId(document_id)},
                {
                    '$set': update_data
                }
            )
            
            if update_result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Transcription not found'
                }
            
            print(f"✅ Updated transcription in MongoDB: {document_id}")
            if all_changes:
                print(f"   Tracked {len(all_changes)} version history entries")
            
            return {
                'success': True,
                'document_id': document_id,
                'message': 'Transcription updated successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error updating transcription: {str(e)}"
            }
    
    def get_version_history(self, document_id: str, user_id: Optional[str] = None, is_admin: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieve version history for a transcription from separate collection.
        Regular users can only access version history for transcriptions assigned to them.
        Admins can access version history for all transcriptions.
        
        Args:
            document_id: MongoDB document ID
            user_id: User ID to check access (if not admin)
            is_admin: Whether the user is an admin
            
        Returns:
            Dictionary with version history or None if not found or access denied
        """
        try:
            if not self.collection or not self.version_history_collection:
                return None
                
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate ObjectId format
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                print(f"❌ Invalid transcription ID format: {document_id}")
                return None
            
            # Find the document in any collection (transcriptions or telugu_transcriptions)
            document = None
            collections_to_check = ['transcriptions', 'telugu_transcriptions']
            
            for coll_name in collections_to_check:
                coll = self.db[coll_name]
                doc = coll.find_one({'_id': obj_id})
                if doc:
                    document = doc
                    if coll_name != self.mongodb_collection:
                        print(f"📝 Found transcription in collection '{coll_name}' for version history")
                    break
            
            if not document:
                return None
            
            # Check access: admins can see all, regular users only assigned ones
            if not is_admin and user_id:
                assigned_user_id = document.get('assigned_user_id')
                if assigned_user_id is None or str(assigned_user_id) != str(user_id):
                    print(f"🚫 Access denied: user {user_id} trying to access version history for transcription assigned to {assigned_user_id}")
                    return None
            
            # Get version history from separate collection, sorted by timestamp descending (newest first)
            version_history_cursor = self.version_history_collection.find(
                {'transcription_id': document_id}
            ).sort('timestamp', -1)
            
            # Convert to list and format
            formatted_history = []
            for entry in version_history_cursor:
                formatted_entry = {
                    'timestamp': entry['timestamp'].isoformat() if isinstance(entry.get('timestamp'), datetime) else entry.get('timestamp'),
                    'user_id': entry.get('user_id'),
                    'before': entry.get('before'),
                    'after': entry.get('after')
                }
                formatted_history.append(formatted_entry)
            
            return {
                'transcription_id': document_id,
                'version_history': formatted_history,
                'total_versions': len(formatted_history)
            }
            
        except Exception as e:
            print(f"❌ Error retrieving version history: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def clear_version_history(self, document_id: str, user_id: Optional[str] = None, is_admin: bool = False) -> Dict[str, Any]:
        """
        Clear version history for a transcription.
        Regular users can only clear version history for transcriptions assigned to them.
        Admins can clear version history for all transcriptions.
        
        Args:
            document_id: MongoDB document ID
            user_id: User ID to check access (if not admin)
            is_admin: Whether the user is an admin
            
        Returns:
            Dictionary with operation result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            from bson import ObjectId
            from bson.errors import InvalidId
            
            # Validate ObjectId format
            try:
                obj_id = ObjectId(document_id)
            except (InvalidId, ValueError) as e:
                return {
                    'success': False,
                    'error': f'Invalid transcription ID format: {str(e)}'
                }
            
            # Find the document in any collection (transcriptions or telugu_transcriptions)
            document = None
            collections_to_check = ['transcriptions', 'telugu_transcriptions']
            
            for coll_name in collections_to_check:
                coll = self.db[coll_name]
                doc = coll.find_one({'_id': obj_id})
                if doc:
                    document = doc
                    if coll_name != self.mongodb_collection:
                        print(f"📝 Found transcription in collection '{coll_name}' for clearing version history")
                    break
            
            if not document:
                return {
                    'success': False,
                    'error': f'Transcription not found with ID: {document_id} in any collection'
                }
            
            # Check access: admins can clear all, regular users only assigned ones
            if not is_admin and user_id:
                assigned_user_id = document.get('assigned_user_id')
                if assigned_user_id is None or str(assigned_user_id) != str(user_id):
                    return {
                        'success': False,
                        'error': 'Access denied: You can only clear version history for transcriptions assigned to you'
                    }
            
            # Delete all version history entries from separate collection
            if not self.version_history_collection:
                return {
                    'success': False,
                    'error': 'Version history collection not initialized'
                }
            
            delete_result = self.version_history_collection.delete_many(
                {'transcription_id': document_id}
            )
            
            print(f"✅ Cleared {delete_result.deleted_count} version history entries for transcription: {document_id}")
            
            return {
                'success': True,
                'document_id': document_id,
                'message': 'Version history cleared successfully'
            }
            
        except Exception as e:
            print(f"❌ Error clearing version history: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {
                'success': False,
                'error': f"Error clearing version history: {str(e)}"
            }
    
    def delete_transcription(self, document_id: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Delete a transcription from MongoDB and its associated S3 audio file (all users can delete all data).
        
        Args:
            document_id: MongoDB document ID
            user_id: Ignored (kept for backward compatibility)
            
        Returns:
            Dictionary with delete operation result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            from bson import ObjectId
            
            # Get the document to extract S3 metadata before deleting (no user_id filtering)
            document = self.collection.find_one({'_id': ObjectId(document_id)})
            
            if not document:
                return {
                    'success': False,
                    'error': 'Transcription not found'
                }
            
            # Extract S3 key from document
            s3_metadata = document.get('s3_metadata', {})
            s3_key = s3_metadata.get('key', '')
            
            # Delete S3 object if key exists
            s3_delete_result = None
            if s3_key:
                print(f"🗑️  Attempting to delete S3 object: {s3_key}")
                s3_delete_result = self.delete_audio_from_s3(s3_key)
                if not s3_delete_result.get('success'):
                    # Log warning but continue with MongoDB deletion
                    print(f"⚠️  Warning: Failed to delete S3 object: {s3_delete_result.get('error')}")
            else:
                print(f"⚠️  No S3 key found in document, skipping S3 deletion")
            
            # Delete document from MongoDB (no user_id filtering)
            delete_result = self.collection.delete_one({'_id': ObjectId(document_id)})
            
            if delete_result.deleted_count == 0:
                return {
                    'success': False,
                    'error': 'Transcription not found in MongoDB'
                }
            
            print(f"✅ Deleted transcription from MongoDB: {document_id}")
            
            # Prepare response message
            message = 'Transcription deleted successfully'
            if s3_key:
                if s3_delete_result and s3_delete_result.get('success'):
                    message += f'. S3 audio file ({s3_key}) also deleted.'
                else:
                    message += f'. Note: S3 audio file deletion had issues (check logs).'
            
            return {
                'success': True,
                'document_id': document_id,
                'message': message,
                's3_deleted': s3_delete_result.get('success') if s3_delete_result else False
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error deleting transcription: {str(e)}"
            }

