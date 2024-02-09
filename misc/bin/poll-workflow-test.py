#!/usr/bin/env python3
import time
import json
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


################################################################
# Run /copy step that writes a STARTED file, returns immediately,
# while running something and writing a SUCCEED file asynchronously
copy_resp = requests.post(f"{baseurl}/copy/{execution_id}", json=wf_input, timeout=60)
print(copy_resp.status_code)
print(copy_resp.json())
assert copy_resp.status_code == 201
copy_resp_json = copy_resp.json()

# poll for copy completion
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

################################################################
# Run /parse step that writes a STARTED file, returns immediately,
copy_output = json.loads(status_resp_json["message"])
parse_input = {
    "input_path": copy_output["gcs_path"],
}
parse_step_response = requests.post(
    f"{baseurl}/parse/{execution_id}", json=parse_input, timeout=60
)
print(parse_step_response.status_code)
print(parse_step_response.json())
assert parse_step_response.status_code == 201
parse_step_response_json = parse_step_response.json()

# poll for parse completion
while True:
    print("Sending status check request...")
    status_resp = requests.get(
        f"{baseurl}/step_status/{execution_id}/parse", timeout=60
    )
    assert status_resp.status_code == 200
    status_resp_json = status_resp.json()
    print(f"Status Response: {status_resp_json}")
    if status_resp_json["step_status"] == "SUCCEEDED":
        print("Step succeeded")
        break
    elif status_resp_json["step_status"] == "FAILED":
        raise RuntimeError(f"Step failed: {status_resp_json}")
    time.sleep(1)

################################################################
# Run /create_external_tables step that writes a STARTED file, returns immediately,
cet_input = json.loads(status_resp_json["message"])
cet_input["source_table_paths"] = cet_input["parsed_files"]
del cet_input["parsed_files"]
cet_input["destination_dataset"] = execution_id
print(f"{cet_input=}")
cet_step_response = requests.post(
    f"{baseurl}/create_external_tables/{execution_id}", json=cet_input, timeout=60
)
print(cet_step_response.status_code)
print(cet_step_response.json())
assert cet_step_response.status_code == 201
cet_step_response_json = cet_step_response.json()

# poll for create_external_tables completion
while True:
    print("Sending status check request...")
    status_resp = requests.get(
        f"{baseurl}/step_status/{execution_id}/create_external_tables", timeout=60
    )
    assert status_resp.status_code == 200
    status_resp_json = status_resp.json()
    print(f"Status Response: {status_resp_json}")
    if status_resp_json["step_status"] == "SUCCEEDED":
        print("Step succeeded")
        break
    elif status_resp_json["step_status"] == "FAILED":
        raise RuntimeError(f"Step failed: {status_resp_json}")
    time.sleep(1)

cet_final_output = json.loads(status_resp_json["message"])

print("All steps succeeded")
print("Create tables response:\n" + json.dumps(cet_final_output, indent=2))
