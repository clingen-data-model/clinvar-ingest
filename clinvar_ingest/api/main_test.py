from fastapi.testclient import TestClient

from clinvar_ingest.api.main import app


def test_status_check(caplog) -> None:
    # per https://fastapi.tiangolo.com/advanced/testing-events/
    # perfer testing with the context manager...
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"health": "ok!"}
        assert len(caplog.records) == 2
        assert "GET /health" in caplog.records[0].msg
        assert "elapsed_ms" in caplog.records[1].msg

def test_copy_endpoint_success(caplog) -> None:
    with TestClient(app) as client:
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
            "ftp_path": "/pub/clinvar/xml/clinvar_variation/weekly_release/ClinVarVariationRelease_2023-1104.xml.gz",
            "gcs_path": "gs://tbd-not-a-real-bucket",
        }

        body["Released"] = "2022-12-05"
        response = client.post(
            "/copy",
            json=body,
        )
        assert response.status_code == 422
        assert "Input should be a valid datetime" in response.text
        assert len(caplog.records) == 4
        assert "status_code=422" in caplog.records[3].msg
