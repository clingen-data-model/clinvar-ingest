import json
import logging.config
from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from clinvar_ingest.api.main import app
from clinvar_ingest.api.model.requests import StepStartedResponse
from clinvar_ingest.status import StatusValue, StepName, StepStatus


@pytest.fixture
def log_conf():
    with open("log_conf.json", "r") as f:
        conf = json.load(f)
        logging.config.dictConfig(conf)


def test_status_check(log_conf, caplog) -> None:
    # per https://fastapi.tiangolo.com/advanced/testing-events/
    # perfer testing with the context manager...
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"health": "ok!"}
        assert len(caplog.records) == 3
        assert "starting" in caplog.records[0].msg
        assert "GET /health" in caplog.records[1].msg
        assert "elapsed_ms" in caplog.records[2].msg


@pytest.mark.integration
def test_copy_endpoint_success(log_conf, env_config, caplog) -> None:
    started_status_value = StatusValue(
        StepStatus.STARTED, StepName.COPY, datetime.utcnow().isoformat()
    )
    succeeded_status_value = StatusValue(
        StepStatus.SUCCEEDED, StepName.COPY, datetime.utcnow().isoformat()
    )
    with (
        patch("clinvar_ingest.api.main.http_download_curl", return_value=None),
        patch("clinvar_ingest.api.main.copy_file_to_bucket", return_value=None),
        patch("clinvar_ingest.api.main._get_gcs_client", return_value="not a client"),
        patch(
            "clinvar_ingest.api.main.write_status_file",
            return_value=started_status_value,
        ),
        patch(
            "clinvar_ingest.api.main.get_status_file",
            return_value=succeeded_status_value,
        ),
        TestClient(app) as client,
    ):
        wf_execution_id = "test-execution-id"
        body = {
            "Name": "ClinVarVariationRelease_2023-1104.xml.gz",
            "Size": 10,
            "Released": "2022-12-05 15:47:16",
            "Last Modified": "2023-12-05 15:47:16",
            "Directory": "/pub/clinvar/xml/clinvar_variation/weekly_release",
            "Host": "https://ftp.ncbi.nlm.nih.gov",
            "Release Date": "2023-12-04",
        }
        response = client.post(
            f"/copy/{wf_execution_id}",
            json=body,
        )
        assert response.status_code == 201

        expected_started_response = StepStartedResponse(
            workflow_execution_id=wf_execution_id,
            step_name=StepName.COPY,
            timestamp=datetime.utcnow(),
            step_status=StepStatus.STARTED,
        )
        actual_started_response = StepStartedResponse(**response.json())
        actual_started_response.timestamp = expected_started_response.timestamp
        assert expected_started_response == actual_started_response

        body["Released"] = "2022-12-05"
        response = client.post(
            f"/copy/{wf_execution_id}",
            json=body,
        )
        assert response.status_code == 422
        assert "Input should be a valid datetime" in response.text
        assert len(caplog.records) == 9
        # assert "status_code=422" in caplog.records[5].msg
