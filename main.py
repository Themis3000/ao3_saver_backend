import datetime
import uuid

import db
import ao3
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import Response, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List
from cacheout import Cache

app = FastAPI()
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


@app.post("/report_work")
async def report_work(work: WorkReport):
    print(f"work {work.work_id} updated at {work.updated_time} reported")
    stored_updated_time = db.get_updated_time(work.work_id)
    if work.updated_time > stored_updated_time:
        fetched_work = ao3.dl_work(work.work_id, work.updated_time)
        if not fetched_work:
            raise HTTPException(status_code=400, detail="work could not be fetched.")
    new_updated_time = db.get_updated_time(work.work_id)
    if new_updated_time == -1:
        raise HTTPException(status_code=500, detail="could not archive for an unknown reason.")
    return {"status": "success", "updated": new_updated_time}


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
    work_ids: List[int]


@app.post("/works/dl/bulk_prepare")
async def bulk_download_prep(work_requests: BulkRequest):
    dl_key = uuid.uuid4().hex
    bulk_dl_tasks_cache.set(dl_key, work_requests.work_ids)
    return {"dl_id": dl_key}


@app.get("/works/dl/bulk_dl/{dl_id}")
async def bulk_download(dl_id: str):
    work_ids = bulk_dl_tasks_cache.get(dl_id)
    if not work_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Download not valid, please initiate a new download or check that you have the right url.")
    zip_res = db.get_bulk_works(work_ids)
    return StreamingResponse(content=zip_res, media_type="application/zip")
