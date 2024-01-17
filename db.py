import datetime
import io
import random
import re
from stat import S_IFREG
from typing import List
from pydantic import BaseModel
from typing_extensions import TypedDict
from stream_zip import stream_zip, ZIP_64
import boto3
import botocore
import os.path
import bsdiff4
import psycopg2

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

conn = psycopg2.connect(database=os.environ["POSTGRESQL_DATABASE"],
                        host=os.environ["POSTGRESQL_HOST"],
                        user=os.environ["POSTGRESQL_USER"],
                        password=os.environ["POSTGRESQL_PASSWORD"],
                        port=os.environ["POSTGRESQL_PORT"])
conn.autocommit = True

# Check if db has been initialized. If it hasn't been, initialize it.
queue_table_cursor = conn.cursor()
queue_table_cursor.execute(
    "select exists(select * from information_schema.tables where table_name='queue')")
has_queue = queue_table_cursor.fetchone()[0]
queue_table_cursor.close()
if not has_queue:
    print("initializing database...")
    init_cursor = conn.cursor()
    with open("db_init.sql", "r") as f:
        init_cursor.execute(f.read())
        init_cursor.close()


def queue_work(work_id: int, updated_time: int, work_format: str, reporter_name: str, reporter_id: str):
    cursor = conn.cursor()

    cursor.execute("SELECT EXISTS(SELECT 1 FROM queue WHERE work_id=%s AND format=%s)", (work_id, work_format))
    queue_item_exists = cursor.fetchone()[0]
    if queue_item_exists:
        return

    cursor.execute(
        "INSERT INTO queue"
        "(work_id, submitted_time, updated, submitted_by_name, submitted_by_id, format)"
        " VALUES (%s, NOW(), %s, %s, %s, %s)",
        (work_id, updated_time, reporter_name, reporter_id, work_format)
    )
    cursor.close()


class JobOrder(BaseModel):
    job_id: int
    work_id: str
    work_format: str
    report_code: int


def get_job(client_name: str, client_id: str) -> None | JobOrder:
    cursor = conn.cursor()

    cursor.execute("""
    SELECT job_id, work_id, format
    FROM queue
    WHERE NOT EXISTS (
        SELECT *
        FROM dispatches
        -- Time should be current time minus delay to redistribute.
        WHERE dispatches.job_id = queue.job_id
        AND dispatches.dispatched_time > (NOW() - INTERVAL '00:01:00')
    )
    ORDER BY queue.submitted_time DESC
    LIMIT 1;
    """)
    queue_query = cursor.fetchall()
    print(queue_query)
    cursor.close()

    if not queue_query:
        return None

    job_id, work_id, work_format = queue_query[0]
    report_code = dispatch_job(job_id, client_name, client_id)
    job_order = JobOrder(job_id=job_id, work_id=work_id, work_format=work_format, report_code=report_code)
    return job_order


def dispatch_job(job_id: int, client_name: str, client_id: str) -> int:
    print("hit dispatch")
    cursor = conn.cursor()
    report_code = random.randrange(-32768, 32767)
    cursor.execute("INSERT INTO dispatches"
                   "(dispatched_time, dispatched_to_name, dispatched_to_id, job_id, report_code)"
                   "VALUES (NOW(), %s, %s, %s, %s)",
                   (client_name, client_id, job_id, report_code))
    cursor.close()
    return report_code


def clear_queue_by_attempts(attempts: int):
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM queue
        WHERE job_id IN (
            SELECT job_id
            FROM dispatches
            GROUP BY job_id
            HAVING COUNT(*) >= %s
        )
    """, (attempts,))
    cursor.close()


def mark_job_fail(job_id: int, fail_code: int):
    # TODO check use query to remove work if it's also had too many failed attempts
    cursor = conn.cursor()
    cursor.execute("""
        
    """)
    cursor.close()


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


class WorkBulkEntry(TypedDict):
    work_id: int
    title: str


re_clean_filename = re.compile(r"[/\\?%*:|\"<>\x7F\x00-\x1F]")


def get_bulk_works(works: List[WorkBulkEntry]):
    failed_works = []

    def work_files():
        for work in works:
            work_contents = get_work(work["work_id"])
            if work_contents is False:
                failed_works.append(work)
                continue

            def work_bytes_gen():
                yield work_contents

            file_name = re_clean_filename.sub('-', f"{work['title']} ({work['work_id']}).pdf")
            yield file_name, datetime.datetime.now(), S_IFREG | 0o600, ZIP_64, work_bytes_gen()

    return stream_zip(work_files())


def store_work(work_id, updated_time, data):
    return None