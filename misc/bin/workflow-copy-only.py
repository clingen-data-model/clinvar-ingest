#!/usr/bin/env python3
import logging
import os
from pathlib import Path, PurePosixPath

from google.cloud import bigquery
from google.cloud.storage import Client as GCSClient

from clinvar_ingest.api.model.requests import (
    ClinvarFTPWatcherRequest,
    CopyResponse,
    CreateExternalTablesRequest,
    CreateExternalTablesResponse,
    CreateInternalTablesRequest,
    DropExternalTablesRequest,
    ParseRequest,
    ParseResponse,
)
from clinvar_ingest.cloud.bigquery.create_tables import (
    create_internal_tables,
    drop_external_tables,
    run_create_external_tables,
)
from clinvar_ingest.cloud.gcs import copy_file_to_bucket, http_download_requests
from clinvar_ingest.config import get_env
from clinvar_ingest.parse import ClinVarIngestFileFormat, parse_and_write_files
from clinvar_ingest.slack import send_slack_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("clinvar-ingest-workflow")


# wf_input = {
#     "Directory": "/clingen-data-model/clinvar-ingest/main/test/data",
#     "Host": "https://raw.githubusercontent.com",
#     "Last Modified": "2023-10-07 15:47:16",
#     "Name": "OriginalTestDataSet.xml.gz",
#     "Release Date": "2023-10-07",
#     "Released": "2023-10-07 15:47:16",
#     "Size": 46719,
# }


def create_execution_id(
    seed: str,
    file_format: ClinVarIngestFileFormat,
    reprocessed: bool = False,
    suffix: str = "",
) -> str:
    if env.release_tag is None:
        raise RuntimeError("Must specify 'release_tag' in the environment")
    repro = "_reprocessed" if reprocessed else ""
    return f"clinvar_{file_format}_{seed}_{env.release_tag}{repro}{suffix}"


def _get_gcs_client() -> GCSClient:
    if getattr(_get_gcs_client, "client", None) is None:
        setattr(_get_gcs_client, "client", GCSClient())
    return getattr(_get_gcs_client, "client")


################################################################
### Initialization code

# Main env for the codebase
env = get_env()

# Workflow specific input (which also comes from the env)
wf_input = ClinvarFTPWatcherRequest(**os.environ)  # type: ignore

workflow_execution_id = create_execution_id(
    wf_input.release_date.isoformat().replace("-", "_"),
    ClinVarIngestFileFormat(wf_input.file_format or env.file_format_mode),
    wf_input.released != wf_input.last_modified,
    suffix="_copy_only",
)
workflow_id_message = f"Workflow Execution ID: {workflow_execution_id}"
send_slack_message("Starting " + workflow_id_message)
_logger.info(workflow_id_message)

################################################################
# Run copy step. Copies a source XML file from an HTTP/FTP server to GCS
# If the host is a GCS bucket, it will download from there rather than using HTTP


def copy(payload: ClinvarFTPWatcherRequest, skip_existing: bool = True) -> CopyResponse:
    _logger.info(f"copy payload: {payload.model_dump_json()}")

    gcs_base = (
        f"gs://{env.bucket_name}/{env.executions_output_prefix}/{workflow_execution_id}"
    )
    gcs_dir = PurePosixPath(env.bucket_staging_prefix)
    gcs_file = PurePosixPath(payload.name)
    gcs_path = f"{gcs_base}/{gcs_dir.relative_to(gcs_dir.anchor) / gcs_file}"

    client = _get_gcs_client()

    source_host = str(payload.host)
    source_file_size = payload.size

    source_base = str(payload.host).strip("/")
    source_dir = PurePosixPath(payload.directory)
    source_file = PurePosixPath(payload.name)
    source_path = (
        f"{source_base}/{source_dir.relative_to(source_dir.anchor) / source_file}"
    )

    if skip_existing:
        # If the blob already exists and the size matches the expected size
        # from the `payload`, return early.
        bucket = client.bucket(env.bucket_name)
        # Remove the scheme and bucket name
        gcs_blob_name = gcs_path[len(f"gs://{env.bucket_name}/") :]
        # Retrieve blob metadata
        blob = bucket.get_blob(gcs_blob_name)

        if blob is not None and blob.size == source_file_size:
            _logger.info(
                f"Skipping copy, file already exists and size matches: {gcs_path}"
            )
            return CopyResponse(ftp_path=source_path, gcs_path=gcs_path)

    if source_host.split("://", maxsplit=1)[0] in ["http", "https", "ftp"]:
        _logger.info(f"Copying {source_path} to {gcs_path}")
        local_path = http_download_requests(
            http_uri=source_path,
            local_path=source_file,  # Just the file name, relative to current working directory
            file_size=source_file_size,
        )
        _logger.info(f"Downloaded {source_path} to {local_path}")

    elif source_host.startswith("gs://"):
        _logger.info(f"Copying {source_path} to {gcs_path}")
        with open(source_file, "wb") as f:
            client.download_blob_to_file(source_path, f)
        local_path = Path(source_file)
        _logger.info(f"Downloaded {source_path} to {local_path}")

    else:
        raise ValueError(f"Unsupported host scheme: {source_host}")

    # Upload file to GCS
    copy_file_to_bucket(
        local_file_uri=str(local_path), remote_blob_uri=gcs_path, client=client
    )
    _logger.info(f"Uploaded {local_path} to {gcs_path}")
    return CopyResponse(ftp_path=source_path, gcs_path=gcs_path)


try:
    copy_response = copy(wf_input)
    _logger.info(f"Copy response: {copy_response.model_dump_json()}")
except Exception as e:
    msg = "Failed during 'copy'."
    _logger.exception(msg)
    send_slack_message(workflow_id_message + " - " + msg)
    raise e


################################################################
_logger.info("Workflow succeeded")
src_url = os.path.join(str(wf_input.host), wf_input.directory, wf_input.name)
send_slack_message(
    workflow_id_message
    + " - Copy-Only Workflow succeeded."
    + "\n"
    + f"Copied {src_url} to {copy_response.gcs_path}"
)
