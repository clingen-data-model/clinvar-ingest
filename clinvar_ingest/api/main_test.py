from fastapi.testclient import TestClient

from clinvar_ingest.api.main import app

client = TestClient(app)


def test_status_check():
    response = client.get("/status")
    assert response.status_code == 200
    assert response.json() == {"status": "ok!"}
