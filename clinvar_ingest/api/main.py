from fastapi import FastAPI, status

from clinvar_ingest.api.model import ClinvarFTPWatcherPayload, CopyResponse, ParsePayload

app = FastAPI()


@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    return {"health": "ok!"}


@app.get("/copy", status_code=status.HTTP_201_CREATED, response_model=CopyResponse)
async def copy(payload: ClinvarFTPWatcherPayload):
    try:

    except Exception:
        


@app.post("/parse", status_code=status.HTTP_201_CREATED)
async def parse(payload: ParsePayload):
    return {"status": "ok!"}
