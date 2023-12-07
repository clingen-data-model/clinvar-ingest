import logging
from contextlib import asynccontextmanager
import os

from fastapi import FastAPI, HTTPException, status
from google.cloud.storage import Client as GCSClient

from clinvar_ingest.api.middleware import LogRequests
from clinvar_ingest.api.model.requests import (
    ClinvarFTPWatcherRequest,
    CopyResponse,
    ParseRequest,
    TodoRequest,
)
import clinvar_ingest.config as config
from clinvar_ingest.cloud.gcs import http_upload_urllib, http_upload_shell

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server starting up")
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(LogRequests)

gcs_storage_client = GCSClient()


@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    return {"health": "ok!"}


@app.post("/copy", status_code=status.HTTP_201_CREATED, response_model=CopyResponse)
async def copy(payload: ClinvarFTPWatcherRequest):
    try:
        ftp_path = os.path.join(
            config.clinvar_ftp_base_url, payload.directory, payload.name
        )
        gcs_path = (
            f"gs://{config.bucket_name}/{config.bucket_staging_prefix}/{payload.name}"
        )
        logger.info(f"Copying {ftp_path} to {gcs_path}")
        http_upload_urllib(ftp_path, gcs_path, client=gcs_storage_client)
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
async def parse(payload: ParseRequest):
    return {"todo": "implement me"}


@app.post("/create_external_tables", status_code=status.HTTP_201_CREATED)
async def create_external_tables(payload: TodoRequest):
    return {"todo": "implement me"}


@app.post("/create_internal_tables", status_code=status.HTTP_201_CREATED)
async def create_internal_tables(payload: TodoRequest):
    return {"todo": "implement me"}


@app.post("/create_cleaned_tables", status_code=status.HTTP_201_CREATED)
async def create_cleaned_tables(payload: TodoRequest):
    return {"todo": "implement me"}
