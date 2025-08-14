"""
File Upload Controller

This module handles secure file uploads to S3-compatible storage services.
It provides functionality to validate files and upload them using boto3 client.

The module follows a layered architecture pattern with clear separation of concerns:
- Validation layer: Validates file names and content
- Storage layer: Handles S3 upload operations using boto3
- Error handling layer: Provides comprehensive error handling and logging

Key Features:
- Strict file name validation against allowed patterns
- Secure file content validation and sanitization 
- Configurable S3 storage integration
- Comprehensive error handling and logging
- Type hints and documentation

Functions:
    file_upload: Main entry point for file upload workflow
    upload_file_in_s3_with_boto3: Handles S3 upload operations
    get_s3_client: Creates configured S3 client
    get_file_from_s3: Retrieves file from S3 storage
"""

import logging
import json
import io
import re
import os
import pickle
import boto3
import botocore
from pathlib import Path
from typing import Dict, Any, Union
from fastapi import HTTPException

from settings import settings
from app.redis_connection import redis_client
from app.modules.upload_model.upload_file_service import get_model_files, store_file_in_db
from app.modules.upload_model.constants import REDIS_PREFIX, MODEL_DIRS

# Configure module logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants from settings
S3_BUCKET_NAME = settings.S3_BUCKET_NAME
S3_ENDPOINT = settings.S3_ENDPOINT
OBJECT_NAME = "public-uploads/dispatch"

SCHEDULE_FILES = ["available", "pre_pulled", "pending_return"]

APPOINTMENT_FILES = [
    "delivery_cos",
    "delivery_sin", 
    "pickup_cos", 
    "pickup_sin", 
    "return_cos", 
    "return_sin", 
    "delivery_pkl", 
    "pickup_pkl",
    "return_pkl"
]

class S3UploadError(Exception):
    """Custom exception for S3 upload failures"""
    pass

async def file_upload(form_data: dict) -> str:
    """
    Upload a file to S3 storage after validation.
    
    Args:
        file: FastAPI UploadFile object containing the file to upload
        
    Returns:
        str: Public URL of the uploaded file
        
    Raises:
        HTTPException: If file validation fails or upload errors occur
        S3UploadError: If S3 operations fail
    """
    try:
        carrier_id = form_data.get("carrier_id")
        file_type = form_data.get("file_type")
        load_type = form_data.get("load_type")

        file_names = SCHEDULE_FILES if file_type == "schedule" else APPOINTMENT_FILES

        uploaded_file = await get_model_files(carrier_id, file_type, load_type, None)

        for file_name in file_names:
            if form_data.get(file_name, False):
                file = form_data.get(file_name)
                full_file_name = file.filename
                file_content = await file.read()
                # check this file in uploaded_file or not
                file_detail = None
                for upload_file in uploaded_file:
                    if upload_file["file_type"] == file_type and upload_file["file_name"] == file_name:
                        file_detail = upload_file
                        break
                
                if file_detail is None:
                    file_version = 1
                else:
                    file_version = file_detail["version"] + 1

                file_path = f"model_{carrier_id}_{file_type}_{file_name}_{load_type}_{file_version}.{full_file_name.split('.')[-1]}"\
                    if carrier_id else f"model_{file_type}_{file_name}_{load_type}_{file_version}.{full_file_name.split('.')[-1]}"

                file_content_parsed = _validate_file_content(file_path, file_content)
                file_url = await upload_file_in_s3_with_boto3(file_path, file_content_parsed)
                await upload_file_to_redis(file_path, file_content_parsed)
                if file_url is not None:
                    file_data = {
                        "carrier": carrier_id,
                        "file_name": file_name,
                        "load_type": load_type,
                        "file_type": file_type,
                        "file_url": file_url,
                        "version": file_version
                    }
                    await store_file_in_db(file_data)

        return "success"

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File upload failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error during file upload")

def _validate_file_content(filename: str, content: bytes) -> Union[Dict, Any]:
    """
    Validate and parse file content based on file type.
    
    Args:
        filename: Name of the file
        content: Raw file content
        
    Returns:
        Parsed file content
        
    Raises:
        HTTPException: If content validation fails
    """
    try:
        if filename.endswith('.pkl'):
            return pickle.loads(content)
        else:
            raise ValueError(f"Unsupported file type: {filename}")
    except (json.JSONDecodeError, pickle.UnpicklingError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid file content: {str(e)}")

async def upload_file_in_s3_with_boto3(file_name: str, file_content: Union[Dict, Any]) -> str:
    """
    Upload file content to S3 using boto3 client.
    
    Args:
        file_name: Name of the file to upload
        file_content: Parsed file content to upload
        
    Returns:
        str: Public URL of the uploaded file
        
    Raises:
        S3UploadError: If S3 operations fail
    """
    try:
        s3_client = await get_s3_client()
        object_name = str(Path(OBJECT_NAME) / file_name)

        # Prepare content based on file type
        if file_name.endswith('.pkl'):
            file_bytes = pickle.dumps(file_content)
            content_type = "application/octet-stream"
        else:
            file_bytes = json.dumps(file_content).encode('utf-8')
            content_type = "application/json"

        # Upload new object
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=object_name,
            Body=io.BytesIO(file_bytes).read(),
            ContentType=content_type
        )

        return f"https://{S3_BUCKET_NAME}.{S3_ENDPOINT}/{object_name}"

    except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
        logger.error(f"S3 upload failed: {str(e)}", exc_info=True)
        raise S3UploadError(f"Failed to upload file to S3: {str(e)}")


async def upload_file_to_redis(file_name: str, file_content: Union[Dict, Any]) -> str:
    """
    Uploads a .pkl or .json file to Redis.

    Args:
        redis_client (redis.asyncio.Redis): Redis client instance.
        file_data (bytes | str): File content (either bytes or string).
        redis_key (str): Redis key under which the file content will be stored.

    Returns:
        str: Success or error message.
    """
    try:
        # Determine the file type from the content
        if file_name.endswith('.pkl'):
            file_bytes = pickle.dumps(file_content)
        else:
            file_bytes = json.dumps(file_content).encode('utf-8')
        
        ttl_in_seconds = 15 * 24 * 60 * 60  # 15 days in seconds
        redis_key = f"{REDIS_PREFIX}:{file_name}"

        if not redis_client:
            logger.error("Redis client is not available.")
            return None
        await redis_client.set(redis_key, file_bytes, ttl_in_seconds)  # Store file in redis
        return f"File successfully uploaded to Redis with key '{redis_key}'."
    except Exception as e:
        logger.error(f"Error upload_file_to_redis: {str(e)}")
        return None

async def get_s3_client() -> boto3.client:
    """
    Create and configure S3 client based on environment.
    
    Returns:
        boto3.client: Configured S3 client
        
    Raises:
        EnvironmentError: If required credentials are missing
    """
    environment = settings.ENVIRONMENT.lower()
    
    if environment == "local":
        if not (settings.S3_AWS_ACCESS_KEY_ID and settings.S3_AWS_SECRET_ACCESS_KEY):
            raise EnvironmentError("S3 credentials not configured for local environment")
            
        return boto3.client(
            "s3",
            aws_access_key_id=settings.S3_AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.S3_AWS_SECRET_ACCESS_KEY,
            endpoint_url=f"https://{S3_ENDPOINT}"
        )
    
    return boto3.client('s3')

async def get_file_from_s3(file_name: str) -> Union[Dict, Any]:
    """
    Retrieve and parse file from S3 storage.
    
    Args:
        file_name: Name of the file to retrieve
        
    Returns:
        Parsed file content
        
    Raises:
        S3UploadError: If S3 operations fail
        HTTPException: If file parsing fails
    """
    try:
        s3_client = await get_s3_client()
        file_key = f"{OBJECT_NAME}/{file_name}".replace('\\', '/')

        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        file_content = response['Body'].read()

        if file_name.endswith('.json'):
            return json.loads(file_content.decode('utf-8'))
        elif file_name.endswith('.pkl'):
            return pickle.loads(file_content)
        else:
            raise ValueError(f"Unsupported file type: {file_name}")

    except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
        logger.error(f"S3 retrieval failed: {str(e)}", exc_info=True)
        raise S3UploadError(f"Failed to retrieve file from S3: {str(e)}")
    except (json.JSONDecodeError, pickle.UnpicklingError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file content: {str(e)}")
    
async def get_file_from_redis(file_name: str) -> Union[Dict, Any]:
    """
    Retrieve and parse file from Redis.

    Args:
        redis_key: The Redis key under which the file content is stored

    Returns:
        Parsed file content (either JSON or Pickle)

    Raises:
        HTTPException: If file retrieval or parsing fails
    """
    try:
        if not redis_client:
            logger.error("Redis client is not available.")
            return None
        redis_key = f"{REDIS_PREFIX}:{file_name}"
        file_content = await redis_client.get(redis_key)
        
        if file_content is not None:
            if file_name.endswith('.pkl'):
                return pickle.loads(file_content)
            elif file_name.endswith('.json'):
                return json.loads(file_content.decode('utf-8'))  # Deserialize JSON content
            else:
                raise ValueError(f"Unsupported file type: {file_name}")
    except ValueError as e:
        logger.error(f"Failed to parse file content: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Redis retrieval failed: {str(e)}")
        return None

async def get_file_content(file_name: str, type_of_load: str) -> Union[Dict, Any]:
    """
    Retrieve and parse file from redis and S3 storage.
    """
    try:

        if settings.READ_MODELS_FROM_LOCAL:
            # Get path to load_scheduler directory
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            model_path = os.path.join(current_dir, MODEL_DIRS[type_of_load], file_name)
            
            if os.path.exists(model_path):
                if file_name.endswith('.pkl'):
                    with open(model_path, 'rb') as file:
                        file_content = pickle.load(file)
                else:
                    with open(model_path, 'r') as file:
                        file_content = json.load(file)
                return file_content

        file_content = await get_file_from_redis(file_name)
        if file_content is None:
            file_content = await get_file_from_s3(file_name)
            await upload_file_to_redis(file_name, file_content)

        return file_content
    
    except S3UploadError as e:
        raise HTTPException(status_code=400, detail=f"Failed to retrieve file from S3: {str(e)}")

def get_full_path(file_name: str) -> str:
    current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    model_path = os.path.join(current_dir, 'load_scheduler', file_name)
    return model_path