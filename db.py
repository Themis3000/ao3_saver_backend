import random
import time
from typing_extensions import TypedDict
from pydantic import BaseModel
import os.path
import psycopg2

valid_formats = ["pdf", "epub", "azw3", "mobi", "html"]

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


class InvalidFormat(Exception):
    pass


def queue_work(work_id: int, updated_time: int, work_format: str, reporter_id: str):
    if work_format not in valid_formats:
        raise InvalidFormat(f"{work_format} is not a valid format")

    cursor = conn.cursor()

    cursor.execute("SELECT EXISTS(SELECT 1 FROM queue WHERE work_id=%s AND format=%s)", (work_id, work_format))
    queue_item_exists = cursor.fetchone()[0]
    if queue_item_exists:
        return

    cursor.execute("""
        INSERT INTO queue
        (work_id, submitted_time, updated, submitted_by_id, format)
        VALUES (%(work_id)s, NOW(), %(updated)s, %(submitted_by_id)s, %(format)s);
    """, {"work_id": work_id, "updated": updated_time, "submitted_by_id": reporter_id})
    cursor.close()


class JobOrder(BaseModel):
    dispatch_id: int
    job_id: int
    work_id: str
    work_format: str
    report_code: int
    updated: int


def get_job(client_name: str, client_id: str) -> None | JobOrder:
    cursor = conn.cursor()

    cursor.execute("""
    SELECT job_id, work_id, format, updated
    FROM queue
    WHERE NOT EXISTS (
        SELECT *
        FROM dispatches
        WHERE dispatches.job_id = queue.job_id
        AND dispatches.dispatched_time > (NOW() - INTERVAL '00:03:00')
    )
    ORDER BY queue.submitted_time DESC
    LIMIT 1;
    """)
    queue_query = cursor.fetchone()
    cursor.close()

    if not queue_query:
        return None

    job_id, work_id, work_format, updated = queue_query
    dispatch_id, report_code = dispatch_job(job_id, client_name, client_id)
    job_order = JobOrder(dispatch_id=dispatch_id,
                         job_id=job_id,
                         work_id=work_id,
                         work_format=work_format,
                         report_code=report_code,
                         updated=updated)
    return job_order


def dispatch_job(job_id: int, client_name: str, client_id: str) -> tuple[int, int]:
    cursor = conn.cursor()
    report_code = random.randrange(-32768, 32767)
    cursor.execute("""
                    INSERT INTO dispatches
                    (dispatched_time, dispatched_to_name, dispatched_to_id, job_id, report_code)
                    VALUES (NOW(), %(client_name)s, %(client_id)s, %(job_id)s, %(report_code)s)
                    RETURNING dispatch_id;
                   """, {"client_name": client_name, "client_id": client_id, "job_id": job_id,
                         "report_code": report_code})
    dispatch_id = cursor.fetchone()[0]
    cursor.close()
    return dispatch_id, report_code


# TODO:Themis Remove this if there's still no use by the time everything is finished.
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
        SELECT report_code, fail_reported, job_id
        FROM dispatches
        WHERE dispatch_id = %s AND fail_reported = false
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


def update_storage_patch(storage_id: int, patch_of: int):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE storage
        SET patch_of = %(patch_of)s
        WHERE storage_id = %(storage_id)s;
    """, {"patch_of": patch_of, "storage_id": storage_id})
    cursor.close()


def add_work_entry(work_id: int, img_enabled: bool, title: str = None):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO works
        (work_id, title, img_enabled)
        VALUES (%s, %s, %s);
    """, [work_id, title, img_enabled])
    cursor.close()


class StorageData(BaseModel):
    storage_id: int
    work_id: int
    uploaded_time: int
    updated_time: int
    location: str
    patch_of: int | None
    retrieved_from: str
    format: str


def parse_storage_query(result) -> StorageData | None:
    if len(result) == 0:
        return None

    return StorageData(storage_id=result[0], work_id=result[1], uploaded_time=result[2], updated_time=result[3],
                       location=result[4], patch_of=result[5], retrieved_from=result[6], format=result[7])


def get_head_work_storage_data(work_id: int, file_format: str) -> StorageData | None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM storage
        WHERE work_id = %(work_id)s AND format = %(format)s AND patch_of IS NULL
        LIMIT 1;
    """, {"work_id": work_id, "format": file_format})
    result = cursor.fetchone()
    cursor.close()

    return parse_storage_query(result)


def get_work_storage_by_timestamp(work_id: int, timestamp: int, file_format: str) -> StorageData | None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM storage
        WHERE work_id = %(work_id)s AND format = %(format)s AND updated_time = %(timestamp)s
        LIMIT 1;
    """, {"work_id": work_id, "format": file_format, "timestamp": timestamp})
    result = cursor.fetchone()
    cursor.close()

    return parse_storage_query(result)


def get_storage_entry(storage_id: int) -> StorageData | None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM storage
        WHERE storage_id = %(storage_id)s
    """, {"storage_id": storage_id})
    result = cursor.fetchone()
    cursor.close()

    return parse_storage_query(result)


def remove_from_queue(job_id: int):
    cursor = conn.cursor()
    cursor.execute("""
        DELETE
        FROM queue
        WHERE job_id = %(job_id)s
    """, {"job_id": job_id})
    cursor.close()


def submit_dispatch(dispatch_id: int, report_code: int, work: bytes) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT report_code, job_id
        FROM dispatches
        WHERE dispatch_id = %(dispatch_id)s AND fail_reported = false
    """, {"dispatch_id": dispatch_id})
    result = cursor.fetchone()
    cursor.close()

    true_report_code, job_id = result[0]

    if true_report_code is None:
        raise JobNotFound("Invalid job_id provided")

    if report_code != true_report_code:
        raise NotAuthorized("You did not provide the proper report code for this work job")

    cursor = conn.cursor()
    cursor.execute("""
        SELECT work_id, updated, submitted_by_id, format
        FROM queue
        WHERE job_id = %(job_id)s
    """, {"job_id": job_id})
    result = cursor.fetchone()
    cursor.close()

    work_id, updated_time, submitted_by, file_format = result[0]

    from file_storage import storage
    storage.store_work(work_id, work, int(time.time()), updated_time, submitted_by, file_format)
    remove_from_queue(job_id)


def sideload_work(work_id, work, updated_time, submitted_by, file_format):
    from file_storage import storage
    storage.store_work(work_id, work, int(time.time()), updated_time, submitted_by, file_format)


def update_work_entry(work_id, img_enabled: bool = None, title: str = None):
    cursor = conn.cursor()
    cursor.execute("""
        do $$
        BEGIN
        
        IF (SELECT EXISTS(SELECT 1 FROM works WHERE work_id=%(work_id)s)) then
            UPDATE works
            SET img_enabled = %(img_enabled)s, title = %(title)s
            WHERE work_id=%(work_id)s;
        else
            INSERT INTO works (work_id, title, img_enabled)
            VALUES (%(work_id)s, %(title)s, %(img_enabled)s);
        end if;
        end $$
    """, {"work_id": work_id, "img_enabled": img_enabled, "title": title})
    cursor.close()
