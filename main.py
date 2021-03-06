import db
import ao3
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

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
    if work.updated_time > db.get_updated_time(work.work_id):
        fetched_work = ao3.dl_work(work.work_id, work.updated_time)
        if not fetched_work:
            raise HTTPException(status_code=400, detail="work could not be fetched.")
    if db.get_updated_time(work.work_id) == -1:
        raise HTTPException(status_code=500, detail="could not archive for an unknown reason.")
    return {"status": "success", "updated": db.get_updated_time(work.work_id)}


@app.get("/works/{work_id}")
async def get_work(work_id: int):
    work = db.get_work(work_id)
    if work is False:
        raise HTTPException(status_code=404, detail="work not found")
    return StreamingResponse(work, media_type="application/pdf")
