#!/usr/bin/env python3
import time

import requests

# pylint: disable=W0105
"""
Output example:
Workflow Execution ID: poll-test_2024-01-29T05:32:15.087284
{'detail': 'Could not find status file for step COPY with status STARTED in bucket <Bucket: clinvar-ingest> and file prefix executions/poll-test_2024-01-29T05:32:15.087284'}
201
{'workflow_execution_id': 'poll-test_2024-01-29T05:32:15.087284', 'step_name': 'COPY', 'timestamp': '2024-01-29T05:29:49.732983', 'step_status': 'STARTED', 'message': None}
Status Response: {'workflow_execution_id': 'poll-test_2024-01-29T05:32:15.087284', 'step_name': 'COPY', 'step_status': 'SUCCEEDED', 'timestamp': '2024-01-29T05:29:49.732983', 'message': None}
Step succeeded
"""


wf_input = {
    "Directory": "/clingen-data-model/clinvar-ingest/main/test/data",
    "Host": "https://raw.githubusercontent.com",
    "Last Modified": "2023-10-07 15:47:16",
    "Name": "OriginalTestDataSet.xml.gz",
    "Release Date": "2023-10-07",
    "Released": "2023-10-07 15:47:16",
    "Size": 46719,
}

wf_exec_id_seed = "poll-test"
baseurl = "http://localhost:8000"
wf_init_resp = requests.post(
    f"{baseurl}/create_workflow_execution_id/{wf_exec_id_seed}", timeout=60
)
assert wf_init_resp.status_code == 201
execution_id = wf_init_resp.json()["workflow_execution_id"]
print(f"Workflow Execution ID: {execution_id}")

# # execution id has been created but no steps have been started, assert that the status check a 404
# status_resp = requests.get(f"{baseurl}/step_status/{execution_id}/COPY", timeout=60)
# print(status_resp.json())
# assert status_resp.status_code == 404


# Run /copy step that writes a STARTED file, returns immediately,
# while running something and writing a SUCCEED file asynchronously
step1_resp = requests.post(f"{baseurl}/copy/{execution_id}", json=wf_input, timeout=60)
print(step1_resp.status_code)
print(step1_resp.json())
assert step1_resp.status_code == 201
step1_resp_json = step1_resp.json()

# poll for completion
while True:
    print("Sending status check request...")
    status_resp = requests.get(f"{baseurl}/step_status/{execution_id}/copy", timeout=60)
    assert status_resp.status_code == 200
    status_resp_json = status_resp.json()
    print(f"Status Response: {status_resp_json}")
    if status_resp_json["step_status"] == "SUCCEEDED":
        print("Step succeeded")
        break
    elif status_resp_json["step_status"] == "FAILED":
        raise RuntimeError(f"Step failed: {status_resp_json}")
    time.sleep(1)
