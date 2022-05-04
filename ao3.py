import requests
import db


def dl_work(work_id, updated_time):
    response = requests.get("https://archiveofourown.org/downloads/38775681/file.pdf")
    data = response.content
    db.save_work(work_id, updated_time, data)
    return data
