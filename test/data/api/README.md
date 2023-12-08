# Test requests


### Running a single cloud run service (step of a workflow)

This example uses as input a file in github, downloaded into a string using curl. You could also use cat on a local file.

```bash
curl -X POST -H "Authorization: Bearer `gcloud auth print-identity-token`" \
    --json "`curl -s https://raw.githubusercontent.com/clingen-data-model/clinvar-ingest/44-workflow-copy-job/test/data/api/copy-request-body-test.json`" \
    'https://clinvar-ingest-qmojsrhb3q-uc.a.run.app/copy'
```


### Runinng a whole workflow

For this, the input to the workflow is a wrapper object with the value passed to the first step of the workflow being a field called `argument` and the value being the JSON-encoded payload. For example the value passed as the POST payload to the `/copy` Cloud Run Service can be JSON-encoded as a JSON string in the `argument` field. It will not accept `argument` as being a JSON object, the value of that field itself, despite being JSON, must be JSON-encoded into a string.

Creating a workflow payload from a cloud run service payload:

```bash
$ python -c 'import json
with open("copy-request-body-test.json") as f:
    print(json.dumps({"argument": json.dumps(json.load(f))}))' > workflow-request-body-test.json
```

Workflow payload example:
```json
{
    "argument": "{\"Name\": \"OriginalTestDataSet.xml.gz\", \"Size\": 46719, \"Released\": \"2023-10-07 15:47:16\", \"Last Modified\": \"2023-10-07 15:47:16\", \"Directory\": \"\", \"Release Date\": \"2023-10-07\"}"
}

```

One other difference between the Cloud Run and the Workflow request is that the Cloud Run expects an OIDC identity token but the Workflow expects an OAuth2 access token. An access token for the currently activated account in gcloud can be retrieved with `gcloud auth print-access-token` (vs `print-identity-token`).

Example workflow invoke command:
```bash
curl -X POST \
    -H "Authorization: Bearer `gcloud auth print-access-token`" \
    --json "`curl -s https://raw.githubusercontent.com/clingen-data-model/clinvar-ingest/44-workflow-copy-job/test/data/api/workflow-request-body-test.json`" \
    "https://workflowexecutions.googleapis.com/v1/projects/clingen-dev/locations/us-central1/workflows/clinvar-ingest/executions"
```
