import requests
import os

admin_token_str = os.environ.get("ADMIN_TOKEN", None)
auth_header = {"token": admin_token_str}
client_name = os.environ["DL_SCRIPT_NAME"]
server_address = os.environ["DL_SCRIPT_ADDRESS"]
request_endpoint = f"{server_address}/request_job"
failed_endpoint = f"{server_address}/job_fail"
submit_endpoint = f"{server_address}/submit_job"


def do_task():
    res = requests.post(request_endpoint, headers=auth_header, data={"client_name": auth_header})
    job_info = res.json()
