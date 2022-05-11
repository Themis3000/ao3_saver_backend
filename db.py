from sqlitedict import SqliteDict
import os.path

db = SqliteDict("data/data.sqlite", autocommit=True)


def save_work(work_id, updated_time, data):
    with open(f"data/{work_id}.pdf", "wb") as f:
        f.write(data)
    db[work_id] = updated_time


def get_updated_time(work_id):
    if not os.path.isfile(f"data/{work_id}.pdf"):
        return -1
    return db.get(work_id, -1)


def get_work(work_id):
    updated_time = get_updated_time(work_id)
    if updated_time == -1:
        return False
    return open(f"data/{work_id}.pdf", "rb")
