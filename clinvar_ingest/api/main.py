import logging
from contextlib import asynccontextmanager
from pathlib import PurePosixPath

from fastapi import FastAPI, HTTPException, status
from google.cloud.storage import Client as GCSClient

import clinvar_ingest.config as config
from clinvar_ingest.api.middleware import LogRequests
from clinvar_ingest.api.model.requests import (
    ClinvarFTPWatcherRequest,
    CopyResponse,
    ParseRequest,
    ParseResponse,
    TodoRequest,
)
from clinvar_ingest.cloud.gcs import http_upload_urllib
from clinvar_ingest.parse import parse_and_write_files

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server starting up")
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(LogRequests)


def _get_gcs_client() -> GCSClient:
    if getattr(_get_gcs_client, "client", None) is None:
        setattr(_get_gcs_client, "client", GCSClient())
    return getattr(_get_gcs_client, "client")


@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    return {"health": "ok!"}


@app.post("/copy", status_code=status.HTTP_201_CREATED, response_model=CopyResponse)
async def copy(payload: ClinvarFTPWatcherRequest):
    ftp_base = config.clinvar_ftp_base_url.strip("/")
    ftp_dir = PurePosixPath(payload.directory)
    ftp_file = PurePosixPath(payload.name)
    ftp_path = f"{ftp_base}/{ftp_dir.relative_to(ftp_dir.anchor) / ftp_file}"

    gcs_base = f"gs://{config.bucket_name}"
    gcs_dir = PurePosixPath(config.bucket_staging_prefix)
    gcs_file = PurePosixPath(payload.name)
    gcs_path = f"{gcs_base}/{gcs_dir.relative_to(gcs_dir.anchor) / gcs_file}"

    logger.info(f"Copying {ftp_path} to {gcs_path}")

    try:
        http_upload_urllib(ftp_path, gcs_path, client=_get_gcs_client())
        return CopyResponse(
            ftp_path=ftp_path,
            gcs_path=gcs_path,
        )
    except Exception as e:
        msg = f"Failed to copy {ftp_path}."
        logger.exception(msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=msg,
        ) from e


@app.post("/parse", status_code=status.HTTP_201_CREATED, response_model=ParseResponse)
async def parse(payload: ParseRequest):
    output_files = parse_and_write_files(
        payload.input_path,
        payload.output_path,
        disassemble=not payload.no_disassemble,
        jsonify_content=not payload.no_jsonify_content,
    )
    return ParseResponse(parsed_files=output_files)


@app.post("/create_external_tables", status_code=status.HTTP_201_CREATED)
async def create_external_tables(payload: TodoRequest):
    return {"todo": "implement me"}


@app.post("/create_internal_tables", status_code=status.HTTP_201_CREATED)
async def create_internal_tables(payload: TodoRequest):
    return {"todo": "implement me"}


@app.post("/create_cleaned_tables", status_code=status.HTTP_201_CREATED)
async def create_cleaned_tables(payload: TodoRequest):
    return {"todo": "implement me"}
