import logging
from contextlib import asynccontextmanager
from pathlib import PurePosixPath

from fastapi import FastAPI, HTTPException, Request, status
from google.cloud import bigquery
from google.cloud.storage import Client as GCSClient

import clinvar_ingest.config
from clinvar_ingest.api.middleware import LogRequests
from clinvar_ingest.api.model.requests import (
    ClinvarFTPWatcherRequest,
    CopyResponse,
    CreateExternalTablesRequest,
    CreateExternalTablesResponse,
    ParseRequest,
    ParseResponse,
    TodoRequest,
)
from clinvar_ingest.cloud.bigquery.create_tables import run_create
from clinvar_ingest.cloud.gcs import http_upload_urllib
from clinvar_ingest.parse import parse_and_write_files

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.env = clinvar_ingest.config.get_env()
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
async def copy(request: Request, payload: ClinvarFTPWatcherRequest):
    env: clinvar_ingest.config.Env = request.app.env
    # TODO allow source path to be in a bucket or file (for testing)
    ftp_base = str(payload.host).strip("/")
    ftp_dir = PurePosixPath(payload.directory)
    ftp_file = PurePosixPath(payload.name)
    ftp_path = f"{ftp_base}/{ftp_dir.relative_to(ftp_dir.anchor) / ftp_file}"

    gcs_base = f"gs://{env.bucket_name}"
    gcs_dir = PurePosixPath(env.bucket_staging_prefix)
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
        msg = f"Failed to copy {ftp_path}"
        logger.exception(msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=msg,
        ) from e


@app.post("/parse", status_code=status.HTTP_201_CREATED, response_model=ParseResponse)
async def parse(payload: ParseRequest):
    try:
        output_files = parse_and_write_files(
            payload.input_path,
            payload.output_path,
            disassemble=not payload.no_disassemble,
            jsonify_content=not payload.no_jsonify_content,
        )
        return ParseResponse(parsed_files=output_files)
    except Exception as e:
        msg = f"Failed to parse {payload.input_path} and write to {payload.output_path}"
        logger.exception(msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=msg,
        ) from e


@app.post(
    "/create_external_tables",
    status_code=status.HTTP_201_CREATED,
    response_model=CreateExternalTablesResponse,
)
async def create_external_tables(payload: CreateExternalTablesRequest):
    tables_created = run_create(payload)

    for table_name, table in tables_created.items():
        table: bigquery.Table = table
        logger.info(
            "Created table %s:%s.%s",
            table.project,
            table.dataset_id,
            table.table_id,
        )
    entity_type_table_ids = {
        entity_type: table.full_table_id
        for entity_type, table in tables_created.items()
    }

    return entity_type_table_ids


@app.post("/create_internal_tables", status_code=status.HTTP_201_CREATED)
async def create_internal_tables(payload: TodoRequest):
    return {"todo": "implement me"}


@app.post("/create_cleaned_tables", status_code=status.HTTP_201_CREATED)
async def create_cleaned_tables(payload: TodoRequest):
    return {"todo": "implement me"}
