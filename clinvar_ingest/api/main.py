import logging

from fastapi import FastAPI, HTTPException, status

from clinvar_ingest.api.lifespan_hooks import read_log_conf
from clinvar_ingest.api.middleware import LogRequests
from clinvar_ingest.api.model import (
    ClinvarFTPWatcherPayload,
    CopyResponse,
    ParsePayload,
)

logger = logging.getLogger("api")

app = FastAPI(lifespan=read_log_conf)
app.add_middleware(LogRequests)


@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    return {"health": "ok!"}


@app.post("/copy", status_code=status.HTTP_201_CREATED, response_model=CopyResponse)
async def copy(payload: ClinvarFTPWatcherPayload):
    try:
        ftp_path = f"{payload.directory}/{payload.name}"
        gcs_path = "gs://tbd-not-a-real-bucket"
        return CopyResponse(
            ftp_path=ftp_path,
            gcs_path=gcs_path,
        )
    except Exception:
        msg = f"Failed to copy {ftp_path}."
        logger.exception(msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=msg,
        )


@app.post("/parse", status_code=status.HTTP_201_CREATED)
async def parse(payload: ParsePayload):
    return {"todo": "implement me"}


@app.post("/create_external_tables", status_code=status.HTTP_201_CREATED)
async def create_external_tables():
    return {"todo": "implement me"}


@app.post("/create_internal_tables", status_code=status.HTTP_201_CREATED)
async def create_internal_tables():
    return {"todo": "implement me"}


@app.post("/create_cleaned_tables", status_code=status.HTTP_201_CREATED)
async def create_cleaned_tables():
    return {"todo": "implement me"}


@app.post("/post_to_slack", status_code=status.HTTP_201_CREATED)
async def post_to_slack():
    return {"todo": "implement me"}
