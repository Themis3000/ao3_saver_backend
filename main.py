import uuid
import db
from fastapi import FastAPI, HTTPException, Request, status, File, Form, UploadFile, Depends
from fastapi.responses import Response, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from typing import List
from cacheout import Cache
from typing import Annotated
from auth import admin_token
from file_storage import storage

app = FastAPI()
instrumentator = Instrumentator(should_group_status_codes=False, excluded_handlers=["/metrics"])
instrumentator.instrument(app).expose(app)
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
    db.queue_work(work.work_id, work.updated_time, work.format, work.reporter)
    return {"status": "queued"}


class JobRequest(BaseModel):
    client_name: str = "Unknown"


@app.post("/request_job", dependencies=[Depends(admin_token)])
async def request_job(job_request: JobRequest):
    job = db.get_job(job_request.client_name, job_request.client_name)

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
    db.submit_dispatch(dispatch_id, report_code, await work.read())


@app.post("/submit_work", dependencies=[Depends(admin_token)])
async def complete_job(work_id: Annotated[int, Form()],
                       work: Annotated[UploadFile, File()],
                       file_format: Annotated[str, Form()],
                       updated_time: Annotated[int, Form()],
                       requester_id: Annotated[str, Form()]):
    """For submitting a work that was never part of an assigned job"""
    db.sideload_work(work_id, await work.read(), updated_time, requester_id, file_format)
    return {"status": "successfully submitted"}


@app.get("/works/{work_id}")
async def get_work(work_id: int, request: Request):
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
async def dl_work(work_id: int, file_format: str = "pdf"):
    work = storage.get_work(work_id, file_format)
    if work is None:
        raise HTTPException(status_code=404, detail="work not found")
    return Response(content=work, media_type=db.format_mimetypes[file_format])


@app.get("/works/dl_historical/{work_id}/{timestamp}")
async def dl_historical_work(work_id: int, timestamp: int, file_format: str = "pdf"):
    try:
        work = storage.get_archived_work(work_id, timestamp, file_format)
    except db.WorkNotFound:
        raise HTTPException(status_code=404, detail="work not found")
    return Response(content=work, media_type=db.format_mimetypes[file_format])


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
