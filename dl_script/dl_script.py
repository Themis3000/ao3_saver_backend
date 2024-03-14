import requests
import os

admin_token_str = os.environ.get("ADMIN_TOKEN", None)
auth_header = {"token": admin_token_str}
client_name = os.environ["DL_SCRIPT_NAME"]
server_address = os.environ["DL_SCRIPT_ADDRESS"]
request_endpoint = f"{server_address}/request_job"
failed_endpoint = f"{server_address}/job_fail"
submit_endpoint = f"{server_address}/submit_job"

proxies = {}
if "PROXYADDRESS" in os.environ:
    proxies = {"https": os.environ["PROXYADDRESS"]}

formats = {"pdf": "application/pdf",
           "epub": "application/epub+zip",
           "html": "text/html",
           "azw3": "application/vnd.amazon.ebook",
           "mobi": "application/x-mobipocket-ebook"}


def do_task():
    job_res = requests.post(request_endpoint, headers=auth_header, data={"client_name": auth_header})
    job_info = job_res.json()

    print(f"downloading {job_info['work_id']} updated at {job_info['updated']} in {job_info['format']} format...")
    response = requests.get(
        f"https://download.archiveofourown.org/downloads/{job_info['work_id']}/file.pdf?updated_at={job_info['updated']}",
        proxies=proxies)
    if not response.ok or response.headers["Content-Type"] != formats[job_info["format"]]:
        print(f"got response {response.status_code} when requesting {job_info['work_id']} updated at {job_info['updated']} in {job_info['format']} format, reporting to server...")
        return  # TODO: Report error to server here
    data = response.content
    print(f"successfully downloaded {job_info['work_id']} updated at {job_info['updated']}, reporting to server...")

    submit_res = requests.post(submit_endpoint,
                               headers=auth_header,
                               files={"work": data},
                               data={"job_id": job_info["job_id"], "report_code": job_info["report_code"]})

    if not response.ok:
        print(f"Work report has failed")
        return

    print("Work report success!")

do_task()
