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
from clinvar_ingest.cloud.bigquery import processing_history
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

# Example payload from the FTP Watcher
# wf_input = {
#     "Directory": "/pub/clinvar/xml/VCV_xml_old_format",
#     "Host": "https://ftp.ncbi.nlm.nih.gov/",
#     "Last Modified": "2024-01-07 15:47:16",
#     "Name": "ClinVarVariationRelease_2024-02.xml.gz",
#     "Release Date": "2024-02-01",
#     "Released": "2024-02-01 15:47:16",
#     "Size": 3298023159,
# }


def create_execution_id(
    seed: str, file_format: ClinVarIngestFileFormat, reprocessed: bool = False
) -> str:
    if env.release_tag is None:
        raise RuntimeError("Must specify 'release_tag' in the environment")
    repro = "_reprocessed" if reprocessed else ""
    return f"clinvar_{file_format}_{seed}_{env.release_tag}{repro}"


def _get_gcs_client() -> GCSClient:
    if getattr(_get_gcs_client, "client", None) is None:
        setattr(_get_gcs_client, "client", GCSClient())
    return getattr(_get_gcs_client, "client")


def _get_bq_client() -> bigquery.Client:
    if getattr(_get_bq_client, "client", None) is None:
        setattr(_get_bq_client, "client", bigquery.Client())
    return getattr(_get_bq_client, "client")


################################################################
### Initialization code

# Main env for the codebase
env = get_env()

# Workflow specific input (which also comes from the env)
wf_input = ClinvarFTPWatcherRequest(**os.environ)  # type: ignore
file_mode = ClinVarIngestFileFormat(wf_input.file_format or env.file_format_mode)
release_date = wf_input.release_date.isoformat()

_logger.info(f"File mode: {file_mode}, release_date: {release_date}")

workflow_execution_id = create_execution_id(
    release_date.replace("-", "_"),
    file_mode,
    wf_input.released != wf_input.last_modified,
)
workflow_id_message = f"Workflow Execution ID: {workflow_execution_id}"
send_slack_message("Starting " + workflow_id_message)
_logger.info(workflow_id_message)

################################################################
# Write record to processing_history indicating this workflow has begun
# write_start_processing_fn = {
#     ClinVarIngestFileFormat.RCV: processing_history.write_rcv_started,
#     ClinVarIngestFileFormat.VCV: processing_history.write_vcv_started,
# }[file_mode]
processing_history_table = processing_history.ensure_initialized(
    client=_get_bq_client()
)
processing_history_pairs_view = processing_history.ensure_pairs_view_exists(
    processing_history_table=processing_history_table,
    client=_get_bq_client(),
)
processing_history.write_started(
    processing_history_table=processing_history_table,
    release_date=release_date,
    release_tag=env.release_tag,
    file_type=file_mode,
    # The directory within the executions_output_prefix. See gcs_base in copy()
    bucket_dir=workflow_execution_id,
    client=_get_bq_client(),
    error_if_exists=False,
)


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
# Reads an XML file from GCS, parses it, and writes the parsed data to GCS


def parse(payload: ParseRequest, limit=None) -> ParseResponse:
    _logger.info(f"parse payload: {payload.model_dump_json()}")
    parse_format_mode = ClinVarIngestFileFormat(
        wf_input.file_format or env.file_format_mode
    )
    _logger.info(f"Parsing file using mode: {parse_format_mode}")
    execution_prefix = f"{env.executions_output_prefix}/{workflow_execution_id}"
    parse_output_path = (
        f"gs://{env.bucket_name}/{execution_prefix}/{env.parse_output_prefix}"
    )
    output_files = parse_and_write_files(
        payload.input_path,
        parse_output_path,
        disassemble=payload.disassemble,
        jsonify_content=payload.jsonify_content,
        file_format=parse_format_mode,
        limit=limit,
    )
    return ParseResponse(parsed_files=output_files)  # type: ignore


try:
    parse_response = parse(
        ParseRequest(input_path=copy_response.gcs_path),
        # limit=1000,
    )
    _logger.info(f"Parse response: {parse_response.model_dump_json}")
except Exception as e:
    msg = "Failed during 'parse'."
    _logger.exception(msg)
    send_slack_message(workflow_id_message + " - " + msg)
    raise e


################################################################
# Creates external tables in BigQuery from the parsed data in GCS


# def create_external_tables(
#     payload: CreateExternalTablesRequest,
# ) -> CreateExternalTablesResponse:
#     _logger.info(f"create_external_tables payload: {payload.model_dump_json()}")
#     external_tables_created = run_create_external_tables(payload)
#     for external_table_name, table in external_tables_created.items():
#         table: bigquery.Table = table
#         _logger.info(
#             "Created table %s as %s:%s.%s",
#             external_table_name,
#             table.project,
#             table.dataset_id,
#             table.table_id,
#         )

#     entity_type_table_ids = {
#         entity_type: f"{table.project}.{table.dataset_id}.{table.table_id}"
#         for entity_type, table in external_tables_created.items()
#     }
#     return CreateExternalTablesResponse(root=entity_type_table_ids)


# try:
#     create_external_tables_response = create_external_tables(
#         CreateExternalTablesRequest(
#             destination_dataset=workflow_execution_id,
#             source_table_paths=parse_response.parsed_files,  # type: ignore
#         )
#     )
#     _logger.info(
#         f"Create External Tables response: {create_external_tables_response.model_dump_json()}"
#     )
# except Exception as e:
#     msg = "Failed during 'create_external_tables'."
#     _logger.exception(msg)
#     send_slack_message(workflow_id_message + " - " + msg)
#     raise e


# ################################################################
# # Create internal tables

# try:
#     create_internal_tables_request = CreateInternalTablesRequest(
#         source_dest_table_map={
#             # source -> destination
#             external: external.replace("_external", "")
#             for external in create_external_tables_response.root.values()
#         }
#     )
#     _logger.info(
#         f"Create Internal Tables request: {create_internal_tables_request.model_dump_json()}"
#     )

#     create_internal_tables_response = create_internal_tables(
#         create_internal_tables_request
#     )
#     _logger.info(
#         f"Create Internal Tables response: {create_internal_tables_response.model_dump_json()}"
#     )
# except Exception as e:
#     msg = "Failed during 'create_internal_tables'."
#     _logger.exception(msg)
#     send_slack_message(workflow_id_message + " - " + msg)
#     raise e

# ################################################################
# # Drop external tables
# try:
#     drop_external_tables_request = DropExternalTablesRequest(
#         root=create_external_tables_response.root
#     )
#     _logger.info(
#         f"Drop External Tables request: {drop_external_tables_request.model_dump_json()}"
#     )

#     drop_external_tables_response = drop_external_tables(drop_external_tables_request)
# except Exception as e:
#     msg = "Failed during 'drop_external_tables'."
#     _logger.exception(msg)
#     send_slack_message(workflow_id_message + " - " + msg)
#     raise e


################################################################
# Write record to processing_history indicating this workflow has begun
# write_start_processing_fn = {
#     ClinVarIngestFileFormat.RCV: processing_history.write_rcv_started,
#     ClinVarIngestFileFormat.VCV: processing_history.write_vcv_started,
# }[file_mode]
processing_history_table = processing_history.ensure_initialized()
processing_history.write_finished(
    processing_history_table=processing_history_table,
    release_date=release_date,
    release_tag=env.release_tag,
    file_type=file_mode,
    # The directory within the executions_output_prefix. See gcs_base in copy()
    bucket_dir=workflow_execution_id,
    parsed_files=parse_response.parsed_files,
    client=_get_bq_client(),
)


################################################################
_logger.info("Workflow succeeded")
send_slack_message(workflow_id_message + " - Workflow succeeded.")
