import io
import time
import boto3
import botocore
import os.path

public_key = os.environ["S3_PUBLIC_KEY"]
private_key = os.environ["S3_PRIVATE_KEY"]
region = os.environ["S3_REGION_NAME"]
endpoint_url = os.environ["S3_ENDPOINT"]
bucket = os.environ["S3_BUCKET"]

session = boto3.session.Session()
client = session.client('s3',
                        config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
                        region_name=region,
                        endpoint_url=endpoint_url,
                        aws_access_key_id=public_key,
                        aws_secret_access_key=private_key)


def save_work(work_id, updated_time, data):
    prev_updated_time = get_updated_time(work_id)
    if prev_updated_time != -1:  # Checks if there's an existing copy
        # Makes copy of old version & removes it
        client.copy_object(Bucket=bucket,
                           CopySource={"Bucket": bucket, "Key": f"{work_id}.pdf"},
                           Key=f"Old/{work_id}/{work_id}_{prev_updated_time}.pdf")
        client.delete_object(Bucket=bucket, Key=f"{work_id}.pdf")
    client.put_object(Bucket=bucket,
                      Key=f"{work_id}.pdf",
                      Body=data,
                      Metadata={"workupdatedtime": str(updated_time), "uploadedtime": str(int(time.time()))})


def get_updated_time(work_id):
    try:
        metadataRes = client.head_object(Bucket=bucket, Key=f"{work_id}.pdf")
        updatedTime = metadataRes["Metadata"]["workupdatedtime"]
        return int(updatedTime)
    except botocore.exceptions.ClientError:
        return -1
    except KeyError:
        return 0


def get_work(work_id):
    try:
        bytes_buffer = io.BytesIO()
        client.download_fileobj(Bucket=bucket, Key=f"{work_id}.pdf", Fileobj=bytes_buffer)
        return bytes_buffer.getvalue()
    except botocore.exceptions.ClientError:
        return False


if __name__ == "__main__":
    print(get_updated_time(46307245))
