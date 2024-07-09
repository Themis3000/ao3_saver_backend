import datetime
import hashlib
import random
import re
import time
from stat import S_IFREG
from typing import List, Dict
from stream_zip import stream_zip, ZIP_64
from typing_extensions import TypedDict
from pydantic import BaseModel
import os.path
import psycopg2

valid_formats = ["pdf", "epub", "azw3", "mobi", "html", "txt"]
format_mimetypes = {
    "pdf": "application/pdf",
    "epub": "application/epub+zip",
    "azw3": "application/vnd.amazon.ebook",
    "mobi": "application/x-mobipocket-ebook",
    "html": "text/html",
    "txt": "text/plain"
}

conn = psycopg2.connect(database=os.environ["POSTGRESQL_DATABASE"],
                        host=os.environ["POSTGRESQL_HOST"],
                        user=os.environ["POSTGRESQL_USER"],
                        password=os.environ["POSTGRESQL_PASSWORD"],
                        port=os.environ["POSTGRESQL_PORT"])

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
        conn.commit()


class ConnManager:
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            conn.rollback()
            return
        conn.commit()


class InvalidFormat(Exception):
    pass


class WorkNotFound(Exception):
    pass


def queue_work(work_id: int, updated_time: int, work_format: str, reporter_id: str):
    if work_format not in valid_formats:
        raise InvalidFormat(f"{work_format} is not a valid format")

    cursor = conn.cursor()

    cursor.execute("""
        SELECT EXISTS(
            SELECT FROM works_storage
            WHERE work_id=%(work_id)s AND format=%(work_format)s AND updated_time>=%(updated_time)s
        )
        OR EXISTS(
            SELECT FROM queue
            WHERE work_id=%(work_id)s AND format=%(work_format)s AND complete=false
        )
        """, {"work_id": work_id, "work_format": work_format, "updated_time": updated_time})
    queue_item_exists = cursor.fetchone()[0]
    if queue_item_exists:
        return

    cursor.execute("""
        INSERT INTO queue
        (work_id, submitted_time, updated, submitted_by_id, format)
        VALUES (%(work_id)s, NOW(), %(updated)s, %(submitted_by_id)s, %(format)s);
    """, {"work_id": work_id, "updated": updated_time, "submitted_by_id": reporter_id, "format": work_format})
    cursor.close()


class ObjectCacheInfo(BaseModel):
    etag: str | None
    time: datetime.datetime
    object_id: int
    url: str
    sha1: str


class JobOrder(BaseModel):
    dispatch_id: int
    job_id: int
    work_id: int
    work_format: str
    report_code: int
    updated: int
    get_img: bool = True
    cache_infos: Dict[str, ObjectCacheInfo] = {}

    def model_post_init(self, __context):
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                DISTINCT ON (request_url) request_url,
                etag, creation_time, object_id, sha1
            FROM object_index
            WHERE associated_work = %s
            ORDER BY request_url, creation_time DESC
            LIMIT 200;
        """, (self.work_id,))
        result = cursor.fetchall()
        cursor.close()
        self.cache_infos = {
            row[0]: ObjectCacheInfo(url=row[0], etag=row[1], time=row[2], object_id=row[3], sha1=row[4])
            for row in result}


def get_job(client_name: str) -> None | JobOrder:
    cursor = conn.cursor()

    cursor.execute("""
    SELECT job_id, work_id, format, updated
    FROM queue
    WHERE complete = false AND NOT EXISTS (
        SELECT
        FROM dispatches
        WHERE dispatches.job_id = queue.job_id
        AND dispatches.dispatched_time > (NOW() - INTERVAL '00:00:05')
    )
    ORDER BY queue.submitted_time DESC
    LIMIT 1;
    """)
    queue_query = cursor.fetchone()
    cursor.close()

    if not queue_query:
        return None

    job_id, work_id, work_format, updated = queue_query
    dispatch_id, report_code = dispatch_job(job_id, client_name)
    job_order = JobOrder(dispatch_id=dispatch_id,
                         job_id=job_id,
                         work_id=work_id,
                         work_format=work_format,
                         report_code=report_code,
                         updated=updated,
                         get_img=True)
    return job_order


def dispatch_job(job_id: int, client_name: str) -> tuple[int, int]:
    cursor = conn.cursor()
    report_code = random.randrange(-32768, 32767)
    cursor.execute("""
                    INSERT INTO dispatches
                    (dispatched_time, dispatched_to_name, job_id, report_code)
                    VALUES (NOW(), %(client_name)s, %(job_id)s, %(report_code)s)
                    RETURNING dispatch_id;
                   """, {"client_name": client_name, "job_id": job_id,
                         "report_code": report_code})
    dispatch_id = cursor.fetchone()[0]
    cursor.close()
    return dispatch_id, report_code


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
        SET fail_reported = true, fail_status = %(fail_status)s, complete = true
        WHERE dispatch_id = %(dispatch_id)s;
    """, {"fail_status": fail_code, "dispatch_id": dispatch_id, "job_id": job_id})

    fail_count = get_queue_fail_count(job_id)
    if fail_count >= 3:
        mark_queue_completed(job_id, False)

    cursor.close()


def get_queue_fail_count(job_id: int) -> int:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM dispatches
        WHERE job_id = %s AND fail_reported = true;
    """, (job_id,))
    fail_count = cursor.fetchone()[0]
    cursor.close()
    return fail_count


class WorkBulkEntry(TypedDict):
    work_id: int
    title: str


def add_storage_entry(work_id: int, uploaded_time: int, updated_time: int, location: str, retrieved_from: str,
                      file_format: str, patch_of: int = None) -> int:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO works_storage
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
        UPDATE works_storage
        SET patch_of = %(patch_of)s
        WHERE storage_id = %(storage_id)s;
    """, {"patch_of": patch_of, "storage_id": storage_id})
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
    if result is None:
        return None

    return StorageData(storage_id=result[0], work_id=result[1], uploaded_time=result[2], updated_time=result[3],
                       location=result[4], patch_of=result[5], retrieved_from=result[6], format=result[7])


def get_head_work_storage_data(work_id: int, file_format: str) -> StorageData | None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT storage_id, work_id, uploaded_time, updated_time, location, patch_of, retrieved_from, format
        FROM works_storage
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
        FROM works_storage
        WHERE work_id = %(work_id)s AND format = %(format)s AND uploaded_time = %(timestamp)s
        LIMIT 1;
    """, {"work_id": work_id, "format": file_format, "timestamp": timestamp})
    result = cursor.fetchone()
    cursor.close()

    return parse_storage_query(result)


def get_storage_entry(storage_id: int) -> StorageData | None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT *
        FROM works_storage
        WHERE storage_id = %(storage_id)s
    """, {"storage_id": storage_id})
    result = cursor.fetchone()
    cursor.close()

    return parse_storage_query(result)


def mark_queue_completed(job_id: int, success: bool):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE queue
        SET complete = true, success = %(success)s
        WHERE job_id = %(job_id)s
    """, {"job_id": job_id, "success": success})
    cursor.close()


class SupportingObject(BaseModel):
    url: str
    etag: str
    mimetype: str
    file_name: str
    data: bytes

    def data_sha1(self) -> str:
        return hashlib.sha1(self.data).hexdigest()


class SupportingCachedObject(BaseModel):
    url: str
    object_id: int


def submit_dispatch(dispatch_id: int, report_code: int, work: bytes,
                    supporting_objects: List[SupportingObject | SupportingCachedObject]) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT report_code, job_id
        FROM dispatches
        WHERE dispatch_id = %(dispatch_id)s AND fail_reported = false
    """, {"dispatch_id": dispatch_id})
    result = cursor.fetchone()
    cursor.close()

    true_report_code, job_id = result

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

    work_id, updated_time, submitted_by, file_format = result

    from file_storage import storage
    storage.store_work(work_id, work, int(time.time()), updated_time, submitted_by, file_format, supporting_objects)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE dispatches
        SET complete = true
        WHERE job_id = %(job_id)s
    """, {"job_id": job_id})
    cursor.close()
    mark_queue_completed(job_id, True)


def sideload_work(work_id, work, updated_time, submitted_by, file_format,
                  supporting_objects: List[SupportingObject | SupportingCachedObject]):
    from file_storage import storage
    storage.store_work(work_id, work, int(time.time()), updated_time, submitted_by, file_format, supporting_objects)


re_clean_filename = re.compile(r"[/\\?%*:|\"<>\x7F\x00-\x1F]")


def get_bulk_works(works: List[WorkBulkEntry]):
    from file_storage import storage
    failed_works = []

    def work_files():
        for work in works:
            work_contents = storage.get_work(work["work_id"])
            if work_contents is False:
                failed_works.append(work)
                continue

            def work_bytes_gen():
                yield work_contents

            file_name = re_clean_filename.sub('-', f"{work['title']} ({work['work_id']}).pdf")
            yield file_name, datetime.datetime.now(), S_IFREG | 0o600, ZIP_64, work_bytes_gen()

    return stream_zip(work_files())


class Work(BaseModel):
    storage_id: int
    work_id: int
    format: str
    uploaded_time: int
    updated_time: int
    location: str
    patch_of: int | None
    retrieved_from: str

    @property
    def archival_url(self) -> str:
        return f"/works/dl_historical/{self.work_id}/{self.uploaded_time}?file_format={self.format}"

    @property
    def newest_url(self) -> str:
        return f"/works/dl/{self.work_id}?file_format={self.format}"

    @property
    def formatted_upload(self) -> str:
        return datetime.datetime.fromtimestamp(self.uploaded_time).strftime('%c')


def get_work_versions(work_id: int) -> List[Work]:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT storage_id, work_id, format, uploaded_time, updated_time, location, patch_of, retrieved_from
        FROM works_storage
        WHERE work_id = %(work_id)s
        ORDER BY uploaded_time DESC;
    """, {"work_id": work_id})
    results = cursor.fetchall()
    cursor.close()
    works = [Work(
        storage_id=result[0],
        work_id=result[1],
        format=result[2],
        uploaded_time=result[3],
        updated_time=result[4],
        location=result[5],
        patch_of=result[6],
        retrieved_from=result[7]
    ) for result in results]
    return works


def object_exists(sha1: str):
    cursor = conn.cursor()
    cursor.execute("SELECT EXISTS(SELECT FROM object_store WHERE sha1 = %s)", (sha1,))
    result = cursor.fetchone()
    cursor.close()
    return result[0]


def create_object_entry(sha1: str, location: str):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO object_store (sha1, location) VALUES (%(sha1)s, %(location)s);
    """, {"sha1": sha1, "location": location})
    cursor.close()


def find_object_index_entry(sha1: str, request_url: str, etag: str | None, associated_work: int) -> int | None:
    """For checking to see if an entry already sufficiently describes what is to be inserted"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT object_id
        FROM object_index
        WHERE request_url = %(request_url)s AND etag=%(etag)s AND sha1=%(sha1)s AND associated_work=%(associated_work)s
    """, {"sha1": sha1, "request_url": request_url, "etag": etag, "associated_work": associated_work})
    result = cursor.fetchone()
    cursor.close()
    if result is None:
        return None
    return result[0]


def create_object_index_entry(sha1: str, request_url: str, etag: str | None, associated_work: int, mimetype: str) -> int:
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO object_index (request_url, sha1, etag, mimetype, associated_work)
        VALUES (%(request_url)s, %(sha1)s, %(etag)s, %(mimetype)s, %(associated_work)s)
        RETURNING object_id;
    """, {"sha1": sha1, "request_url": request_url, "etag": etag, "associated_work": associated_work, "mimetype": mimetype})
    result = cursor.fetchone()
    cursor.close()
    return result[0]


class SupportingObjectData(BaseModel):
    mimetype: str
    location: str
    data: bytes


def get_supporting_object_file(obj_id: int) -> SupportingObjectData | None:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT oi.mimetype, os.location
        FROM object_index oi
        INNER JOIN object_store os on os.sha1 = oi.sha1
        WHERE oi.object_id = %s
        LIMIT 1
    """, (obj_id,))
    result = cursor.fetchone()
    cursor.close()
    if result is None:
        return None
    from file_storage import storage
    data = storage.get_file(result[1])
    return SupportingObjectData(mimetype=result[0], location=result[1], data=data)
