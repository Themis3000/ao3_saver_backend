import requests
import db
import os


proxies = {}
if "PROXYADDRESS" in os.environ:
    proxies = {"https": os.environ["PROXYADDRESS"]}


def dl_work(work_id, updated_time):
    print(f"downloading {work_id} updated at {updated_time}...")
    response = requests.get(f"https://download.archiveofourown.org/downloads/{work_id}/file.pdf?updated_at={updated_time}", proxies=proxies)
    if not response.ok or response.headers["Content-Type"] != "application/pdf":
        print(f"got response {response.status_code} when requesting {work_id} updated at {updated_time}")
        return False
    data = response.content
    db.save_work(work_id, updated_time, data)
    return data
