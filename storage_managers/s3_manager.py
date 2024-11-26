import io
import os
import boto3
import botocore
from . import StorageManager
addressing_style = os.getenv('ADDRESS_STYLE', "virtual")


class S3Manager(StorageManager):
    def __init__(self):
        public_key = os.environ["S3_PUBLIC_KEY"]
        private_key = os.environ["S3_PRIVATE_KEY"]
        region = os.environ["S3_REGION_NAME"]
        endpoint_url = os.environ["S3_ENDPOINT"]
        self.bucket = os.environ["S3_BUCKET"]

        session = boto3.session.Session()
        self.client = session.client('s3',
                                     config=botocore.config.Config(s3={'addressing_style': addressing_style}),
                                     region_name=region,
                                     endpoint_url=endpoint_url,
                                     aws_access_key_id=public_key,
                                     aws_secret_access_key=private_key)

    def store_file(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def delete_file(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def get_file(self, key: str) -> bytes:
        bytes_buffer = io.BytesIO()
        self.client.download_fileobj(Bucket=self.bucket, Key=key, Fileobj=bytes_buffer)
        return bytes_buffer.getvalue()
