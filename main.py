import datetime
import db
import ao3
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

app = FastAPI()
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
