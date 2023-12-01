from fastapi.testclient import TestClient

from clinvar_ingest.api.main import app

client = TestClient(app)


def test_status_check() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"health": "ok!"}


def test_copy_endpoint() -> None:
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
