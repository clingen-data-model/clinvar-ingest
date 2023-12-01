from fastapi import FastAPI, status

from clinvar_ingest.api.model import ClinvarFTPWatcherPayload, ParsePayload

app = FastAPI()


@app.get("/status", status_code=status.HTTP_200_OK)
async def status():
    return {"status": "ok!"}


@app.get("/copy", status_code=status.HTTP_201_CREATED)
async def copy(payload: ClinvarFTPWatcherPayload):
    return {"status": "ok!"}


@app.post("/parse", status_code=status.HTTP_201_CREATED)
async def parse(payload: ParsePayload):
    return {"status": "ok!"}
