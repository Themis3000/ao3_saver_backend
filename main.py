import datetime
import uuid
import db
import ao3
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import Response, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from typing import List
from cacheout import Cache

app = FastAPI()
instrumentator = Instrumentator(should_group_status_codes=False, excluded_handlers=["/metrics"])
instrumentator.instrument(app).expose(app)
bulk_dl_tasks_cache = Cache(maxsize=5000)
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
    db.queue_work(work.work_id, work.updated_time, work.format, work.reporter, work.reporter)
    return {"status": "queued"}


class JobRequest(BaseModel):
    client_name: str = "Unknown"


@app.post("/request_job")
async def request_job(job_request: JobRequest):
    job = db.get_job(job_request.client_name, job_request.client_name)

    if job is None:
        return {"status": "queue empty"}

    return {"status": "job assigned", **job.dict()}


@app.get("/works/{work_id}")
async def get_work(work_id: int, request: Request):
    work_history = db.get_work_versions(work_id)
    most_recent_url = f"/works/dl/{work_id}"
    if len(work_history) == 0:
        return RedirectResponse(url=most_recent_url)

    archives = []
    for work_timestamp in work_history:
        url = f"/works/dl_historical/{work_id}/{work_timestamp}"
        date = datetime.datetime.fromtimestamp(work_timestamp).strftime('%c')
        archives.append({"url": url, "date": date})

    return templates.TemplateResponse(
        "work_dl.html",
        context={"most_recent_url": most_recent_url, "archives": archives, "request": request},
    )


@app.get("/works/dl/{work_id}")
async def dl_work(work_id: int):
    work = db.get_work(work_id)
    if work is False:
        raise HTTPException(status_code=404, detail="work not found")
    return Response(content=work, media_type="application/pdf")


@app.get("/works/dl_historical/{work_id}/{timestamp}")
async def dl_historical_work(work_id: int, timestamp: int):
    work = db.get_archived_work(work_id, timestamp)
    if work is False:
        raise HTTPException(status_code=404, detail="work not found")
    return Response(content=work, media_type="application/pdf")


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
