import itertools
import uuid
import db
from fastapi import FastAPI, HTTPException, Request, status, File, Form, UploadFile, Depends
from fastapi.responses import Response, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List
from cacheout import Cache
from typing import Annotated
from auth import admin_token
from file_storage import storage

app = FastAPI()
bulk_dl_tasks_cache = Cache(maxsize=50)
templates = Jinja2Templates(directory="templates/")

origins = [
    "http://127.0.0.1:8000",
    "https://archiveofourown.org"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


class WorkReport(BaseModel):
    work_id: int
    updated_time: int
    format: str
    reporter: str = "Unknown"


@app.post("/report_work")
async def report_work(work: WorkReport):
    with db.ConnManager():
        db.queue_work(work.work_id, work.updated_time, work.format, work.reporter)
    return {"status": "queued"}


class JobRequest(BaseModel):
    client_name: str = "Unknown"


@app.post("/request_job", dependencies=[Depends(admin_token)])
async def request_job(job_request: JobRequest):
    with db.ConnManager():
        job = db.get_job(job_request.client_name)

    if job is None:
        return {"status": "queue empty"}

    return {"status": "job assigned", **job.dict()}


class JobFailure(BaseModel):
    dispatch_id: int
    fail_status: int
    report_code: int


@app.post("/job_fail", dependencies=[Depends(admin_token)])
async def fail_job(job: JobFailure):
    try:
        with db.ConnManager():
            db.mark_dispatch_fail(job.dispatch_id, job.fail_status, job.report_code)
    except db.NotAuthorized:
        raise HTTPException(status_code=403, detail="not authorized to report failure")
    except db.AlreadyReported:
        raise HTTPException(status_code=409, detail="a fail has already been reported")
    except db.JobNotFound:
        raise HTTPException(status_code=404, detail="the dispatch id is invalid")
    # I think it's funny I'm leaving it
    return {"status": "successfully failed!"}


async def extract_supporting_objects(form_data) -> List[db.SupportingObject | db.SupportingCachedObject]:
    supporting_data = []
    for i in itertools.count():
        file: UploadFile = form_data.get(f"supporting_objects_{i}")

        if file is None:
            cached_object_id = form_data.get(f"cached_{i}_object_id")
            cached_url = form_data.get(f"cached_{i}_url")

            if not cached_object_id or not cached_url:
                break

            supporting_data.append(db.SupportingCachedObject(url=cached_url, object_id=cached_object_id))
            continue

        url = form_data.get(f"supporting_objects_{i}_url")
        etag = form_data.get(f"supporting_objects_{i}_etag")
        if url is None or etag is None:
            raise HTTPException(status_code=400, detail="missing url or etag data")
        mimetype = file.headers.get("Content-Type", "")
        file_name = file.filename
        supporting_object = db.SupportingObject(url=url, etag=etag, mimetype=mimetype, file_name=file_name, data=await file.read())
        supporting_data.append(supporting_object)
    return supporting_data


@app.post("/submit_job", dependencies=[Depends(admin_token)])
async def complete_job(dispatch_id: Annotated[int, Form()],
                       report_code: Annotated[int, Form()],
                       work: Annotated[UploadFile, File()],
                       request: Request):
    """For submitting a completed job"""
    form_data = await request.form()
    supporting_objects = await extract_supporting_objects(form_data)

    try:
        with db.ConnManager():
            db.submit_dispatch(dispatch_id, report_code, await work.read(), supporting_objects)
    except db.NotAuthorized:
        raise HTTPException(status_code=403, detail="not authorized to submit job")
    except db.AlreadyReported:
        raise HTTPException(status_code=409, detail="this job has already been reported on")
    except db.JobNotFound:
        raise HTTPException(status_code=404, detail="the dispatch id is invalid")
    return {"status": "successfully submitted"}


@app.post("/submit_work", dependencies=[Depends(admin_token)])
async def submit_work(work_id: Annotated[int, Form()],
                      work: Annotated[UploadFile, File()],
                      file_format: Annotated[str, Form()],
                      updated_time: Annotated[int, Form()],
                      requester_id: Annotated[str, Form()],
                      request: Request):
    """For submitting a work that was never part of an assigned job"""
    form_data = await request.form()
    supporting_objects = await extract_supporting_objects(form_data)

    with db.ConnManager():
        db.sideload_work(work_id, await work.read(), updated_time, requester_id, file_format, supporting_objects)
    return {"status": "successfully submitted"}


@app.get("/works/{work_id}")
async def get_work(work_id: int, request: Request):
    with db.ConnManager():
        work_history = db.get_work_versions(work_id)
    if len(work_history) == 0:
        raise HTTPException(status_code=404, detail="work not found")
    newest_work = work_history.pop(0)
    if len(work_history) == 0:
        return RedirectResponse(url=newest_work.newest_url)

    return templates.TemplateResponse(
        "work_dl.jinja",
        context={"newest_work": newest_work, "work_history": work_history, "request": request},
    )


@app.get("/works/dl/{work_id}")
async def dl_work(work_id: int, file_format: str = "html"):
    with db.ConnManager():
        work = storage.get_work(work_id, file_format)
    if work is None:
        raise HTTPException(status_code=404, detail="work not found")
    return Response(content=work, media_type=db.format_mimetypes[file_format])


@app.get("/works/dl_historical/{work_id}/{timestamp}")
async def dl_historical_work(work_id: int, timestamp: int, file_format: str = "html"):
    try:
        with db.ConnManager():
            work = storage.get_archived_work(work_id, timestamp, file_format)
    except db.WorkNotFound:
        raise HTTPException(status_code=404, detail="work not found")
    return Response(content=work, media_type=db.format_mimetypes[file_format])


@app.get("/objects/{obj_id}")
async def get_object(obj_id: int):
    with db.ConnManager():
        supporting_object = db.get_supporting_object_file(obj_id)

    if supporting_object is None:
        raise HTTPException(status_code=404, detail="not found.")

    return Response(content=supporting_object.data,
                    media_type=supporting_object.mimetype,
                    headers={"Cache-Control": "max-age=31536000, immutable"})


class BulkRequest(BaseModel):
    works: List[db.WorkBulkEntry]


@app.post("/works/dl/bulk_prepare")
async def bulk_download_prep(work_requests: BulkRequest):
    dl_key = uuid.uuid4().hex
    bulk_dl_tasks_cache.set(dl_key, work_requests.works)
    return {"dl_id": dl_key}


@app.get("/works/dl/bulk_dl/{dl_id}")
async def bulk_download(dl_id: str):
    works = bulk_dl_tasks_cache.get(dl_id)
    if not works:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Download not valid, please initiate a new download or check that you have the right url.")
    zip_res = db.get_bulk_works(works)
    return StreamingResponse(content=zip_res, media_type="application/zip")
