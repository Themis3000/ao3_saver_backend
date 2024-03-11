import random
from typing_extensions import TypedDict
from pydantic import BaseModel
import boto3
import botocore
import os.path
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
    dispatch_id: int
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
        WHERE dispatches.job_id = queue.job_id
        AND dispatches.dispatched_time > (NOW() - INTERVAL '00:01:00')
    )
    ORDER BY queue.submitted_time DESC
    LIMIT 1;
    """)
    queue_query = cursor.fetchone()
    cursor.close()

    if not queue_query:
        return None

    job_id, work_id, work_format = queue_query
    dispatch_id, report_code = dispatch_job(job_id, client_name, client_id)
    job_order = JobOrder(dispatch_id=dispatch_id,
                         job_id=job_id,
                         work_id=work_id,
                         work_format=work_format,
                         report_code=report_code)
    return job_order


def dispatch_job(job_id: int, client_name: str, client_id: str) -> tuple[int, int]:
    cursor = conn.cursor()
    report_code = random.randrange(-32768, 32767)
    cursor.execute("""
                    INSERT INTO dispatches
                    (dispatched_time, dispatched_to_name, dispatched_to_id, job_id, report_code)
                    VALUES (NOW(), %s, %s, %s, %s)
                    RETURNING dispatch_id;
                   """, (client_name, client_id, job_id, report_code))
    dispatch_id = cursor.fetchone()[0]
    cursor.close()
    return dispatch_id, report_code


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


class NotAuthorized(Exception):
    """
    This is used for when an update is not authorized based on the provided values.
    Specifically, when a provided authentication code/process was found invalid
    """


class JobNotFound(Exception):
    """This is used when a job can't be found based on the provided values."""


class AlreadyReported(Exception):
    """This is used when something is already reported and did not have any reason to be reported again"""


def mark_dispatch_fail(dispatch_id: int, fail_code: int, report_code: int):
    # TODO check use query to remove work if it's also had too many failed attempts
    cursor = conn.cursor()

    cursor.execute("""
        SELECT report_code, fail_reported, job_id FROM dispatches WHERE dispatch_id = %s
    """, (dispatch_id,))
    dispatch_data = cursor.fetchone()

    if dispatch_data is None:
        raise JobNotFound("invalid dispatch id provided")

    stored_report_code, fail_reported, job_id = dispatch_data

    if report_code != stored_report_code:
        raise NotAuthorized("Not authorized to update given dispatch id")

    if fail_reported:
        raise AlreadyReported(f"A fail has already been marked for dispatch id {dispatch_id}")

    cursor.execute("""
        UPDATE dispatches
        SET fail_reported = true, fail_status = %(fail_status)s
        WHERE dispatch_id = %(dispatch_id)s;
        
        DELETE FROM queue
        WHERE job_id = %(job_id)s
            AND (
                SELECT COUNT(*)
                FROM dispatches
                WHERE job_id = %(job_id)s AND fail_reported = true
                LIMIT 3
            ) >= 3;
    """, {"fail_status": fail_code, "dispatch_id": dispatch_id, "job_id": job_id})

    cursor.close()


class WorkBulkEntry(TypedDict):
    work_id: int
    title: str


def add_storage_entry(work_id: int, uploaded_time: int, updated_time: int, location: str, retrieved_from: str,
                      file_format: str, patch_of: int = None) -> int:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO storage
        (work_id, uploaded_time, updated_time, location, patch_of, retrieved_from, format)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING storage_id;
    """, [work_id, uploaded_time, updated_time, location, patch_of,
          retrieved_from, file_format])
    storage_id = cursor.fetchone()[0]
    cursor.close()
    return storage_id


def update_storage_patch(storage_id: int, patch_of: int, location: str):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE storage
        SET patch_of = %(patch_of)s, location = %(location)s
        WHERE storage_id = %(storage_id)s;
    """, {"patch_of": patch_of, "location": location, "storage_id": storage_id})
    cursor.close()


def add_work_entry(work_id: int, img_enabled: bool, title: str = None):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO works
        (work_id, title, img_enabled)
        VALUES (%s, %s, %s);
    """, [work_id, title, img_enabled])
    cursor.close()
