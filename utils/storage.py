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
            
            # Create indexes for better query performance
            try:
                self.collection.create_index('created_at')
                self.collection.create_index('user_id')
                self.collection.create_index('assigned_user_id')  # Index for filtering by assigned user
                self.collection.create_index([('user_id', 1), ('created_at', -1)])  # Compound index
                self.collection.create_index([('assigned_user_id', 1), ('created_at', -1)])  # Compound index for assigned user queries
                print(f"✅ Created indexes on 'created_at', 'user_id', and 'assigned_user_id' fields")
            except Exception as e:
                # Index might already exist, which is fine
                pass
            
            print(f"✅ Connected to MongoDB: {self.mongodb_database}")
            print(f"   Collection: {self.mongodb_collection}")
            print(f"   Existing collections: {collections if collections else 'None (will be created on first insert)'}")
            
        except Exception as e:
            print(f"❌ Warning: Could not connect to MongoDB: {str(e)}")
            self.mongo_client = None
            self.db = None
            self.collection = None
    
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
            
            # Prepare document
            # assigned_user_id is None by default - admin will assign it later
            document = {
                'transcription_data': transcription_data,
                's3_metadata': s3_metadata,
                'user_id': user_id,  # Creator/owner of the transcription
                'assigned_user_id': None,  # Assigned to a specific user (managed by admin)
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
            
            # Get document by ID
            document = self.collection.find_one({'_id': obj_id})
            
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
            
            # Update the assigned_user_id field
            update_result = self.collection.update_one(
                {'_id': obj_id},
                {
                    '$set': {
                        'assigned_user_id': assigned_user_id_str,
                        'updated_at': datetime.now(timezone.utc)
                    }
                }
            )
            
            if update_result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Transcription not found'
                }
            
            # Verify the assignment was saved correctly
            updated_doc = self.collection.find_one({'_id': obj_id})
            saved_assigned_id = updated_doc.get('assigned_user_id') if updated_doc else None
            
            print(f"✅ Assigned transcription {document_id} to user {assigned_user_id_str}")
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
            
            # Remove the assigned_user_id (set to None)
            update_result = self.collection.update_one(
                {'_id': obj_id},
                {
                    '$set': {
                        'assigned_user_id': None,
                        'updated_at': datetime.now(timezone.utc)
                    }
                }
            )
            
            if update_result.matched_count == 0:
                return {
                    'success': False,
                    'error': 'Transcription not found'
                }
            
            print(f"✅ Unassigned transcription {document_id}")
            
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
    
    def flag_transcription(self, document_id: str, is_flagged: bool = True, flag_reason: Optional[str] = None) -> Dict[str, Any]:
        """
        Flag or unflag a transcription.
        
        Args:
            document_id: MongoDB document ID
            is_flagged: Boolean to set flag state
            flag_reason: Optional reason for flagging
            
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
                'updated_at': datetime.now(timezone.utc)
            }
            
            if is_flagged and flag_reason:
                update_fields['flag_reason'] = flag_reason
            elif not is_flagged:
                # Remove flag_reason if unflagging
                update_fields['flag_reason'] = None
            
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
            
            print(f"✅ {'Flagged' if is_flagged else 'Unflagged'} transcription {document_id}")
            
            return {
                'success': True,
                'document_id': document_id,
                'is_flagged': is_flagged,
                'flag_reason': flag_reason if is_flagged else None,
                'message': f"Transcription {'flagged' if is_flagged else 'unflagged'} successfully"
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
            status: Status to set ('done', 'pending', or 'flagged')
            
        Returns:
            Dictionary with update result
        """
        try:
            if not self.collection:
                return {
                    'success': False,
                    'error': 'MongoDB not initialized'
                }
            
            if status not in ['done', 'pending', 'flagged']:
                return {
                    'success': False,
                    'error': f'Invalid status: {status}. Must be one of: done, pending, flagged'
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
            
            # Update the manual_status field (admin override)
            update_result = self.collection.update_one(
                {'_id': obj_id},
                {
                    '$set': {
                        'manual_status': status,
                        'updated_at': datetime.now(timezone.utc)
                    }
                }
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
    
    def list_transcriptions(self, limit: int = 100, skip: int = 0, user_id: Optional[str] = None, is_admin: bool = False,
                           search: Optional[str] = None, language: Optional[str] = None, 
                           date: Optional[str] = None, status: Optional[str] = None,
                           assigned_user: Optional[str] = None, flagged: Optional[str] = None,
                           transcription_type: Optional[str] = None) -> Dict[str, Any]:
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
            assigned_user: Filter by assigned user ID (or 'unassigned' for None)
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
            
            # Language filter
            if language:
                additional_filters.append({'transcription_data.language': language})
            
            # Date filter (match created_at date)
            if date:
                try:
                    from datetime import datetime, timezone
                    # Parse date string (YYYY-MM-DD)
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    # Create date range for the entire day
                    start_of_day = datetime.combine(date_obj.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
                    end_of_day = datetime.combine(date_obj.date(), datetime.max.time()).replace(tzinfo=timezone.utc)
                    additional_filters.append({
                        'created_at': {
                            '$gte': start_of_day,
                            '$lte': end_of_day
                        }
                    })
                except ValueError:
                    print(f"⚠️  Invalid date format: {date}, ignoring date filter")
            
            # Status filter (needs to be applied after document processing, but we can pre-filter some cases)
            # Note: Status is computed from multiple fields, so we'll filter after processing
            # But we can filter by manual_status if it exists
            if status:
                # If status is 'flagged', we can filter by is_flagged
                if status == 'flagged':
                    additional_filters.append({'is_flagged': True})
                elif status == 'done':
                    # For 'done', we need both assigned_user_id and user_id to match
                    # This is complex, so we'll filter after processing
                    pass
                elif status == 'pending':
                    # For 'pending', we can filter out flagged ones
                    additional_filters.append({'$or': [
                        {'is_flagged': False},
                        {'is_flagged': {'$exists': False}}
                    ]})
            
            # Assigned user filter
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
            
            # Note: If search or status filters are active, we need to process more documents
            # to apply these filters correctly, then paginate. For now, we'll fetch a larger batch
            # when these filters are active, filter in memory, then paginate.
            # TODO: Optimize by storing computed status and adding text index for filename search
            needs_post_filtering = bool(search or (status and status in ['done', 'pending']))
            
            # Calculate fetch size: if we need post-filtering, fetch more to account for filtering
            fetch_limit = limit * 10 if needs_post_filtering else limit
            fetch_skip = skip if not needs_post_filtering else 0  # Start from beginning if post-filtering
            
            # Get documents sorted by created_at descending (newest first)
            # Use projection to exclude large fields we don't need for list view
            # This significantly reduces data transfer and processing time
            # Excluding 'words' and 'phrases' arrays which can be very large
            projection = {
                '_id': 1,
                'created_at': 1,
                'updated_at': 1,
                'user_id': 1,
                'assigned_user_id': 1,
                'is_flagged': 1,
                'flag_reason': 1,
                'manual_status': 1,
                'transcription_data.transcription_type': 1,
                'transcription_data.language': 1,
                'transcription_data.total_words': 1,
                'transcription_data.total_phrases': 1,
                'transcription_data.audio_duration': 1,
                'transcription_data.audio_path': 1,
                'transcription_data.metadata.filename': 1,
                'transcription_data.metadata.audio_path': 1,
                'transcription_data.edited_words_count': 1,  # Use stored count if available
                's3_metadata.url': 1,
                's3_metadata.key': 1
            }
            
            find_start = time.time()
            cursor = self.collection.find(query_filter, projection).sort('created_at', -1).skip(fetch_skip).limit(fetch_limit)
            find_time = (time.time() - find_start) * 1000
            print(f"⏱️  [TIMING] MongoDB find() query took {find_time:.2f}ms (fetch_limit={fetch_limit}, fetch_skip={fetch_skip})")
            
            # Process documents
            process_start = time.time()
            transcriptions = []
            for doc in cursor:
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
                
                # Determine status:
                # Priority order:
                # 1. manual_status (if set by admin - highest priority)
                # 2. "flagged" if is_flagged is True
                # 3. "done" only if file is assigned AND the assigned user has saved changes (user_id matches assigned_user_id)
                # 4. "pending" if not assigned, or assigned but assigned user hasn't saved changes yet
                is_flagged = doc.get('is_flagged', False)
                assigned_user_id = doc.get('assigned_user_id')
                doc_user_id = doc.get('user_id')
                manual_status = doc.get('manual_status')  # Admin-set status override
                
                # Use manual_status if set by admin (highest priority)
                if manual_status and manual_status in ['done', 'pending', 'flagged']:
                    computed_status = manual_status
                elif is_flagged:
                    computed_status = 'flagged'
                # Status is "done" only if assigned AND the user_id matches assigned_user_id (meaning assigned user saved)
                elif assigned_user_id and doc_user_id and str(assigned_user_id) == str(doc_user_id):
                    computed_status = 'done'  # Assigned and assigned user has saved changes
                else:
                    computed_status = 'pending'  # Not assigned, or assigned but assigned user hasn't saved yet
                
                # Apply status filter (if specified and doesn't match, skip this document)
                if status and computed_status != status:
                    continue
                
                # Apply search filter (if specified, check filename, assigned user name would need user lookup)
                if search:
                    search_lower = search.lower()
                    # Check filename
                    if search_lower not in display_filename.lower():
                        # Could also check status, but we already filtered by status above
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
                    'user_id': doc.get('user_id'),  # Creator/saver
                    'assigned_user_id': doc.get('assigned_user_id'),  # Assigned user
                    'status': computed_status,  # 'done', 'pending', or 'flagged'
                    'is_flagged': is_flagged,
                    'flag_reason': doc.get('flag_reason'),
                    'edited_words_count': edited_words_count  # Number of words edited
                }
                transcriptions.append(summary)
            
            process_time = (time.time() - process_start) * 1000
            print(f"⏱️  [TIMING] Document processing took {process_time:.2f}ms (processed {len(transcriptions)} documents before pagination)")
            
            # Apply pagination if we did post-filtering
            if needs_post_filtering:
                total_count = len(transcriptions)
                # Apply skip and limit to filtered results
                transcriptions = transcriptions[skip:skip + limit]
                print(f"📄 Applied post-filtering pagination: showing {len(transcriptions)} of {total_count} filtered results")
            else:
                # Get total count for non-post-filtered queries
                count_start = time.time()
                if is_admin and query_filter == {}:
                    # For admin users querying all documents, use estimated_document_count (much faster, O(1))
                    total_count = self.collection.estimated_document_count()
                else:
                    # For filtered queries, use count_documents (exact count)
                    total_count = self.collection.count_documents(query_filter)
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
                'manual_status': 1,
                'transcription_data.transcription_type': 1,
                'transcription_data.audio_duration': 1
            }
            
            cursor = self.collection.find(query_filter, projection)
            
            # Initialize counters
            total_count = 0
            done_count = 0
            pending_count = 0
            flagged_count = 0
            total_done_duration = 0.0  # Total duration in seconds
            
            # Process each document to determine status
            for doc in cursor:
                total_count += 1
                
                # Get audio duration if available
                transcription_data = doc.get('transcription_data', {})
                audio_duration = transcription_data.get('audio_duration', 0) or 0
                
                # Determine status (same logic as in list_transcriptions)
                is_flagged = doc.get('is_flagged', False)
                assigned_user_id = doc.get('assigned_user_id')
                doc_user_id = doc.get('user_id')
                manual_status = doc.get('manual_status')
                
                # Use manual_status if set by admin (highest priority)
                if manual_status and manual_status in ['done', 'pending', 'flagged']:
                    status = manual_status
                elif is_flagged:
                    status = 'flagged'
                # Status is "done" only if assigned AND the user_id matches assigned_user_id
                elif assigned_user_id and doc_user_id and str(assigned_user_id) == str(doc_user_id):
                    status = 'done'
                else:
                    status = 'pending'
                
                # Count by status and accumulate duration for done files
                if status == 'done':
                    done_count += 1
                    total_done_duration += float(audio_duration)
                elif status == 'flagged':
                    flagged_count += 1
                else:
                    pending_count += 1
            
            return {
                'success': True,
                'statistics': {
                    'total': total_count,
                    'done': done_count,
                    'pending': pending_count,
                    'flagged': flagged_count,
                    'total_done_duration': total_done_duration
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error getting statistics: {str(e)}"
            }
    
    def update_transcription(self, document_id: str, transcription_data: Dict[str, Any], user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Update transcription data in MongoDB (all users can update all data).
        
        Args:
            document_id: MongoDB document ID
            transcription_data: Updated transcription data
            user_id: User ID to mark who saved the changes (optional)
            
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
            
            # Prepare update data
            update_data = {
                'transcription_data': transcription_data,
                'updated_at': datetime.now(timezone.utc)
            }
            
            # If user_id is provided, update it to mark who saved the changes
            if user_id:
                update_data['user_id'] = str(user_id)  # Ensure it's a string
            
            # Update document by ID only (no user_id filtering)
            update_result = self.collection.update_one(
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

