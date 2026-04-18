import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src.shared.config.settings import settings


logger = logging.getLogger(__name__)


class MinIOConnector:
    """Connector for MinIO-compatible object storage."""

    def __init__(
        self,
        endpoint: str = settings.MINIO_ENDPOINT,
        access_key: str = settings.MINIO_ACCESS_KEY,
        secret_key: str = settings.MINIO_SECRET_KEY,
        bucket: str = settings.MINIO_BUCKET,
    ):
        self.endpoint = endpoint
        self.bucket = bucket
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
        )

        logger.info(f"MinIO connector initialized: {endpoint}, bucket: {bucket}")

    def upload_file(
        self,
        local_path: str,
        object_name: str,
        bucket: Optional[str] = None,
    ) -> bool:
        bucket = bucket or self.bucket

        try:
            self.client.upload_file(local_path, bucket, object_name)
            logger.info(f"Successfully uploaded {local_path} to s3://{bucket}/{object_name}")
            return True
        except ClientError as exc:
            logger.error(f"Failed to upload file to MinIO: {exc}")
            return False
        except FileNotFoundError:
            logger.error(f"Local file not found: {local_path}")
            return False

    def download_file(
        self,
        object_name: str,
        local_path: str,
        bucket: Optional[str] = None,
    ) -> bool:
        bucket = bucket or self.bucket

        try:
            self.client.download_file(bucket, object_name, local_path)
            logger.info(f"Successfully downloaded s3://{bucket}/{object_name} to {local_path}")
            return True
        except ClientError as exc:
            logger.error(f"Failed to download file from MinIO: {exc}")
            return False

    def list_objects(
        self,
        prefix: str,
        bucket: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        bucket = bucket or self.bucket
        objects: List[Dict[str, Any]] = []

        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                objects = response['Contents']
                logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
            else:
                logger.info(f"No objects found with prefix '{prefix}'")
        except ClientError as exc:
            logger.error(f"Failed to list objects from MinIO: {exc}")

        return objects

    def create_bucket_if_not_exists(self, bucket_name: Optional[str] = None) -> bool:
        bucket_name = bucket_name or self.bucket

        try:
            self.client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' already exists")
            return True
        except ClientError as exc:
            if exc.response['Error']['Code'] == '404':
                try:
                    self.client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Successfully created bucket '{bucket_name}'")
                    return True
                except ClientError as create_exc:
                    logger.error(f"Failed to create bucket: {create_exc}")
                    return False

            logger.error(f"Error checking bucket: {exc}")
            return False

    def upload_dataframe_as_parquet(
        self,
        df,
        object_name: str,
        bucket: Optional[str] = None,
    ) -> bool:
        import tempfile

        bucket = bucket or self.bucket

        try:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                df.to_parquet(tmp_path, index=False)
                success = self.upload_file(tmp_path, object_name, bucket)
                Path(tmp_path).unlink()
                return success
        except Exception as exc:
            logger.error(f"Failed to upload DataFrame as Parquet: {exc}")
            return False

    def download_parquet_as_dataframe(
        self,
        object_name: str,
        bucket: Optional[str] = None,
    ):
        import tempfile

        import pandas as pd

        bucket = bucket or self.bucket

        try:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                success = self.download_file(object_name, tmp_path, bucket)

                if not success:
                    return None

                df = pd.read_parquet(tmp_path)
                Path(tmp_path).unlink()
                return df
        except Exception as exc:
            logger.error(f"Failed to download Parquet file: {exc}")
            return None
