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
    format: str = "html"
    reporter: str
    title: str = None
    author: str = None


@app.post("/report_work")
async def report_work(work: WorkReport):
    with db.ConnManager():
        job_id = db.queue_work(work.work_id, work.updated_time, work.format, work.reporter, work.title, work.author)
    if job_id is None:
        return {"status": "already fetched"}
    return {"status": "queued", "job_id": job_id}


@app.get("/work_exists/{work_id}")
async def work_exists(work_id: int):
    with db.ConnManager():
        return {"exists": db.work_exists(work_id)}


@app.get("/job_status")
async def job_status(job_id: int):
    try:
        with db.ConnManager():
            job_status = db.queue_item_status(job_id)
    except db.JobNotFound:
        raise HTTPException(status_code=404, detail="Job not found")

    if job_status == db.QueueStatus.IN_QUEUE:
        return {"status": "queued", "job_id": job_id}
    if job_status == db.QueueStatus.FAILED:
        return {"status": "failed", "job_id": job_id}
    if job_status == db.QueueStatus.COMPLETED:
        return {"status": "completed", "job_id": job_id}


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


@app.post("/submit_job", dependencies=[Depends(admin_token)])
async def complete_job(dispatch_id: Annotated[int, Form()],
                       report_code: Annotated[int, Form()],
                       work: Annotated[UploadFile, File()]):
    """For submitting a completed job"""
    try:
        with db.ConnManager():
            unfetched_objects = db.submit_dispatch(dispatch_id, report_code, await work.read())
    except db.NotAuthorized:
        raise HTTPException(status_code=403, detail="not authorized to submit job")
    except db.AlreadyReported:
        raise HTTPException(status_code=409, detail="this job has already been reported on")
    except db.JobNotFound:
        raise HTTPException(status_code=404, detail="the dispatch id is invalid")
    return {"status": "successfully submitted", "unfetched_objects": unfetched_objects}


@app.post("/submit_object", dependencies=[Depends(admin_token)])
async def submit_object(object_id: Annotated[int, Form()],
                        etag: Annotated[str, Form()],
                        mimetype: Annotated[str, Form()],
                        object_file: Annotated[UploadFile, File()]):
    """For submitting an unfetched object"""
    try:
        with db.ConnManager():
            db.store_unfetched_object(object_file=await object_file.read(),
                                      object_id=object_id,
                                      etag=etag,
                                      mimetype=mimetype)
    except db.ObjectNotFound:
        raise HTTPException(status_code=404, detail="the object id is invalid")
    return {"status": "successfully submitted"}


@app.post("/submit_object", dependencies=[Depends(admin_token)])
async def submit_object(object_id: Annotated[int, Form()],
                        submission_type: Annotated[str, Form()],
                        etag: Annotated[str, Form(None)],
                        mimetype: Annotated[str, Form(None)],
                        object_file: Annotated[UploadFile, File(None)]):
    """For submitting an unfetched object never part of a dispatch"""
    try:
        with db.ConnManager():
            if submission_type == 'file':
                if submission_type is None or etag is None or mimetype is None or object_file is None:
                    raise HTTPException(status_code=400, detail="missing submission values")
                db.store_unfetched_object(object_file=await object_file.read(),
                                          object_id=object_id,
                                          etag=etag,
                                          mimetype=mimetype)
            elif submission_type == 'cached':
                pass
            elif submission_type == 'failed':
                pass
    except db.ObjectNotFound:
        raise HTTPException(status_code=404, detail="the object id is invalid")
    return {"status": "successfully submitted"}


@app.post("/submit_work", dependencies=[Depends(admin_token)])
async def submit_work(work_id: Annotated[int, Form()],
                      work: Annotated[UploadFile, File()],
                      file_format: Annotated[str, Form()],
                      updated_time: Annotated[int, Form()],
                      requester_id: Annotated[str, Form()]):
    """For submitting a work that was never part of an assigned job"""
    with db.ConnManager():
        db.sideload_work(work_id, await work.read(), updated_time, requester_id, file_format)
    return {"status": "successfully submitted"}


@app.get("/works/{work_id}")
async def get_work(work_id: int, request: Request, version: int = None):
    if version is None:
        with db.ConnManager():
            work_history = db.get_work_versions(work_id)
        if len(work_history) == 0:
            raise HTTPException(status_code=404, detail="work not found")
        newest_work = work_history.pop(0)
        if len(work_history) == 0:
            return RedirectResponse(url=newest_work.permalink_url)
        return templates.TemplateResponse(
            "work_dl.jinja",
            context={"newest_work": newest_work, "work_history": work_history, "request": request},
        )

    with db.ConnManager():
        work, storage_data = storage.get_work(version)
    if work is None:
        raise HTTPException(status_code=404, detail="version not found")
    if storage_data.work_id != work_id:
        raise HTTPException(status_code=400, detail="invalid request")
    return Response(content=work,
                    media_type=db.format_mimetypes[storage_data.format],
                    headers={"Cache-Control": "max-age=31536000, immutable"})


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
