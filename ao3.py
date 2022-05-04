import requests
import db


def dl_work(work_id, updated_time):
    response = requests.get(f"https://archiveofourown.org/downloads/{work_id}/file.pdf")
    data = response.content
    db.save_work(work_id, updated_time, data)
    return data
