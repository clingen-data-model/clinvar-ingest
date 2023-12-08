import json
import logging.config
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from clinvar_ingest import config
from clinvar_ingest.api.main import app


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


def test_copy_endpoint_success(log_conf, caplog) -> None:
    with patch(
        "clinvar_ingest.api.main.http_upload_urllib", return_value=None
    ), TestClient(app) as client:
        body = {
            "Name": "ClinVarVariationRelease_2023-1104.xml.gz",
            "Size": 10,
            "Released": "2022-12-05 15:47:16",
            "Last Modified": "2023-12-05 15:47:16",
            "Directory": "/pub/clinvar/xml/clinvar_variation/weekly_release",
            "Release Date": "2023-12-04",
        }
        response = client.post(
            "/copy",
            json=body,
        )
        assert response.status_code == 201
        assert response.json() == {
            "ftp_path": f"{config.clinvar_ftp_base_url}/pub/clinvar/xml/clinvar_variation/weekly_release/ClinVarVariationRelease_2023-1104.xml.gz",
            "gcs_path": f"gs://{config.bucket_name}/{config.bucket_staging_prefix}/ClinVarVariationRelease_2023-1104.xml.gz",
        }

        body["Released"] = "2022-12-05"
        response = client.post(
            "/copy",
            json=body,
        )
        assert response.status_code == 422
        assert "Input should be a valid datetime" in response.text
        assert len(caplog.records) == 6
        assert "status_code=422" in caplog.records[5].msg
