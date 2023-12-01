from fastapi.testclient import TestClient

from clinvar_ingest.api.main import app

client = TestClient(app)


def test_status_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"health": "ok!"}
