import requests
import db


def dl_work(work_id, updated_time):
    print(f"downloading {work_id} updated at {updated_time}...")
    response = requests.get(f"https://archiveofourown.org/downloads/{work_id}/file.pdf")
    if not response.ok:
        return False
    data = response.content
    db.save_work(work_id, updated_time, data)
    return data
