import datetime
import io
import time
from stat import S_IFREG
from stream_zip import stream_zip, ZIP_64
import boto3
import botocore
import os.path
import bsdiff4

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


def save_work(work_id, updated_time, work_data):
    prev_updated_time = get_updated_time(work_id)
    if prev_updated_time != -1:  # Checks if there's an existing copy
        # Gets the old version
        bytes_buffer = io.BytesIO()
        client.download_fileobj(Bucket=bucket, Key=f"{work_id}.pdf", Fileobj=bytes_buffer)
        old_version = bytes_buffer.getvalue()
        old_metadata_res = client.head_object(Bucket=bucket, Key=f"{work_id}.pdf")
        old_metadata = old_metadata_res["Metadata"]
        # Saves a diff file so that the old version can be patched & recovered in the future
        diff = bsdiff4.diff(work_data, old_version)
        client.put_object(Bucket=bucket,
                          Key=f"diff_archive/{work_id}/{prev_updated_time}.diff",
                          Body=diff,
                          Metadata=old_metadata)
        # Cleans up old version to make way for new version
        client.delete_object(Bucket=bucket, Key=f"{work_id}.pdf")
    client.put_object(Bucket=bucket,
                      Key=f"{work_id}.pdf",
                      Body=work_data,
                      Metadata={"workupdatedtime": str(updated_time),
                                "uploadedtime": str(int(time.time())),
                                "prev_version": str(prev_updated_time)})


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


def get_work_versions(work_id, limit=100):
    response = client.list_objects_v2(Bucket=bucket, Prefix=f"diff_archive/{work_id}/", MaxKeys=limit)
    # Get timestamp from file names & return them
    versions = []
    for version in response.get("Contents", []):
        key: str = version["Key"]
        ending = key.rsplit("/", 1)[1]
        timestamp = int(ending[:-5])
        versions.append(timestamp)
    versions.sort(reverse=True)
    return versions


def get_archived_work(work_id, timestamp):
    # a list of patch files to apply to the original in order to recover the archived work
    patches = []
    # original work data
    metadataRes = client.head_object(Bucket=bucket, Key=f"{work_id}.pdf")
    prev_version = int(metadataRes["Metadata"]["prev-version"])
    patches.append(prev_version)

    # iterate through each previous patch file till the desired one is reached
    while True:
        if prev_version == timestamp:
            break
        # fail out if the desired timestamp is skipped over
        if prev_version < timestamp:
            raise Exception("Desired timestamp not found")

        metadataRes = client.head_object(Bucket=bucket, Key=f"diff_archive/{work_id}/{prev_version}.diff")
        prev_version = int(metadataRes["Metadata"]["prev-version"])
        patches.append(prev_version)

    # get the current file, this will be mutated through patches
    bytes_buffer = io.BytesIO()
    client.download_fileobj(Bucket=bucket, Key=f"{work_id}.pdf", Fileobj=bytes_buffer)
    master_file = bytes_buffer.getvalue()
    # Iteratively get and apply patches
    for version in patches:
        bytes_buffer = io.BytesIO()
        client.download_fileobj(Bucket=bucket, Key=f"diff_archive/{work_id}/{version}.diff", Fileobj=bytes_buffer)
        diff_bytes = bytes_buffer.getvalue()
        master_file = bsdiff4.patch(master_file, diff_bytes)

    return master_file


def get_bulk_works(work_ids):
    failed_works = []

    def work_files():
        for work_id in work_ids:
            work = get_work(work_id)
            if work is False:
                failed_works.append(work_id)
                continue

            def work_bytes_gen():
                yield work

            yield f"{work_id}.pdf", datetime.datetime.now(), S_IFREG | 0o600, ZIP_64, work_bytes_gen()

    return stream_zip(work_files())
