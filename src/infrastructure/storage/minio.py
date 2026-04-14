"""
MinIO storage connector for interacting with object storage.
Handles upload and download of raw and processed data.
"""
import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src.shared.config.settings import settings


logger = logging.getLogger(__name__)


class MinIOConnector:
    """
    Connector for MinIO (S3-compatible) object storage.
    Handles all operations related to uploading and downloading data.
    """

    def __init__(self,
                 endpoint: str = settings.MINIO_ENDPOINT,
                 access_key: str = settings.MINIO_ACCESS_KEY,
                 secret_key: str = settings.MINIO_SECRET_KEY,
                 bucket: str = settings.MINIO_BUCKET):
        """
        Initialize MinIO connector.

        Args:
            endpoint: MinIO endpoint URL
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket: Default bucket name
        """
        self.endpoint = endpoint
        self.bucket = bucket

        # Create S3 client configured for MinIO
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )

        logger.info(f"MinIO connector initialized: {endpoint}, bucket: {bucket}")

    def upload_file(self, 
                   local_path: str,
                   object_name: str,
                   bucket: Optional[str] = None) -> bool:
        """
        Upload a file to MinIO.

        Args:
            local_path: Local file path
            object_name: Object name in MinIO (e.g., 'bronze/weather_raw_202501150000.json')
            bucket: Bucket name (uses default if not provided)

        Returns:
            True if upload successful, False otherwise
        """
        bucket = bucket or self.bucket

        try:
            self.client.upload_file(local_path, bucket, object_name)
            logger.info(f"Successfully uploaded {local_path} to s3://{bucket}/{object_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload file to MinIO: {e}")
            return False
        except FileNotFoundError:
            logger.error(f"Local file not found: {local_path}")
            return False

    def download_file(self,
                     object_name: str,
                     local_path: str,
                     bucket: Optional[str] = None) -> bool:
        """
        Download a file from MinIO.

        Args:
            object_name: Object name in MinIO
            local_path: Local file path to save
            bucket: Bucket name (uses default if not provided)

        Returns:
            True if download successful, False otherwise
        """
        bucket = bucket or self.bucket

        try:
            self.client.download_file(bucket, object_name, local_path)
            logger.info(f"Successfully downloaded s3://{bucket}/{object_name} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download file from MinIO: {e}")
            return False

    def list_objects(self,
                    prefix: str,
                    bucket: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List objects in MinIO with given prefix.

        Args:
            prefix: Prefix to filter objects (e.g., 'bronze/')
            bucket: Bucket name (uses default if not provided)

        Returns:
            List of object dictionaries
        """
        bucket = bucket or self.bucket
        objects = []

        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                objects = response['Contents']
                logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
            else:
                logger.info(f"No objects found with prefix '{prefix}'")
        except ClientError as e:
            logger.error(f"Failed to list objects from MinIO: {e}")

        return objects

    def create_bucket_if_not_exists(self, bucket_name: Optional[str] = None) -> bool:
        """
        Create bucket if it doesn't exist.

        Args:
            bucket_name: Bucket name (uses default if not provided)

        Returns:
            True if bucket exists or was created successfully
        """
        bucket_name = bucket_name or self.bucket

        try:
            # Check if bucket exists
            self.client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' already exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Bucket doesn't exist, create it
                try:
                    self.client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Successfully created bucket '{bucket_name}'")
                    return True
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket: {create_error}")
                    return False
            else:
                logger.error(f"Error checking bucket: {e}")
                return False

    def upload_dataframe_as_parquet(self,
                                   df,
                                   object_name: str,
                                   bucket: Optional[str] = None) -> bool:
        """
        Upload pandas DataFrame as Parquet file to MinIO.

        Args:
            df: pandas DataFrame
            object_name: Object name in MinIO
            bucket: Bucket name (uses default if not provided)

        Returns:
            True if upload successful, False otherwise
        """
        import tempfile

        bucket = bucket or self.bucket

        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                df.to_parquet(tmp_path, index=False)

                # Upload to MinIO
                success = self.upload_file(tmp_path, object_name, bucket)

                # Clean up temporary file
                Path(tmp_path).unlink()

                return success
        except Exception as e:
            logger.error(f"Failed to upload DataFrame as Parquet: {e}")
            return False

    def download_parquet_as_dataframe(self,
                                     object_name: str,
                                     bucket: Optional[str] = None):
        """
        Download Parquet file from MinIO as pandas DataFrame.

        Args:
            object_name: Object name in MinIO
            bucket: Bucket name (uses default if not provided)

        Returns:
            pandas DataFrame or None if download fails
        """
        import tempfile
        import pandas as pd

        bucket = bucket or self.bucket

        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                success = self.download_file(object_name, tmp_path, bucket)

                if not success:
                    return None

                df = pd.read_parquet(tmp_path)
                Path(tmp_path).unlink()

                return df
        except Exception as e:
            logger.error(f"Failed to download Parquet file: {e}")
            return None
