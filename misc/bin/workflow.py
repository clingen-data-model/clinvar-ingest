#!/usr/bin/env python3
from pathlib import PurePosixPath
import os
import logging

from google.cloud.storage import Client as GCSClient
from google.cloud import bigquery

from clinvar_ingest.cloud.bigquery.create_tables import (
    run_create_external_tables,
    create_internal_tables,
    drop_external_tables,
)
from clinvar_ingest.config import get_env
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
from clinvar_ingest.cloud.gcs import copy_file_to_bucket, http_download_requests
from clinvar_ingest.parse import parse_and_write_files
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

# wf_input = {
#     "Host": "https://ftp.ncbi.nlm.nih.gov",
#     "Directory": "/pub/clinvar/xml/clinvar_variation/weekly_release",
#     "Name": "ClinVarVariationRelease_2023-1104.xml.gz",
#     "Size": 3160398711,
#     "Released": "2023-11-05 15:47:16",
#     "Last Modified": "2023-11-05 15:47:16",
#     "Release Date": "2023-11-04",
# }

# wf_input = {
#     "Directory": "/pub/clinvar/xml/VCV_xml_old_format",
#     "Host": "https://ftp.ncbi.nlm.nih.gov/",
#     "Last Modified": "2024-01-07 15:47:16",
#     "Name": "ClinVarVariationRelease_2024-02.xml.gz",
#     "Release Date": "2024-02-01",
#     "Released": "2024-02-01 15:47:16",
#     "Size": 3298023159,
# }


def create_execution_id(seed: str):
    if env.release_tag is None:
        raise RuntimeError("Must specify 'release_tag' in the environment")
    return f"{seed}_{env.release_tag}"


def _get_gcs_client() -> GCSClient:
    if getattr(_get_gcs_client, "client", None) is None:
        setattr(_get_gcs_client, "client", GCSClient())
    return getattr(_get_gcs_client, "client")


################################################################
### Initialization code

# Main env for the codebase
env = get_env()

# Workflow specific input (which also comes from the env)
wf_input = ClinvarFTPWatcherRequest(**os.environ)

reprocess_prior_release = "_reprocessed" if wf_input.released != wf_input.last_modified else ""
workflow_execution_base = create_execution_id(
    wf_input.release_date.isoformat().replace("-", "_")
)
workflow_execution_id = f"clinvar_{workflow_execution_base}{reprocess_prior_release}"
workflow_id_message = f"Workflow Execution ID: {workflow_execution_id}"
send_slack_message("Starting " + workflow_id_message)
_logger.info(workflow_id_message)

################################################################
# Run copy step. Copies a source XML file from an HTTP/FTP server to GCS


def copy(payload: ClinvarFTPWatcherRequest) -> CopyResponse:
    _logger.info(f"copy payload: {payload.model_dump_json()}")
    ftp_base = str(payload.host).strip("/")
    ftp_dir = PurePosixPath(payload.directory)
    ftp_file = PurePosixPath(payload.name)
    ftp_file_size = payload.size
    ftp_path = f"{ftp_base}/{ftp_dir.relative_to(ftp_dir.anchor) / ftp_file}"

    gcs_base = (
        f"gs://{env.bucket_name}/{env.executions_output_prefix}/{workflow_execution_id}"
    )
    gcs_dir = PurePosixPath(env.bucket_staging_prefix)
    gcs_file = PurePosixPath(payload.name)
    gcs_path = f"{gcs_base}/{gcs_dir.relative_to(gcs_dir.anchor) / gcs_file}"

    _logger.info(f"Copying {ftp_path} to {gcs_path}")

    # Download file
    local_path = http_download_requests(
        http_uri=ftp_path,
        local_path=ftp_file,  # Just the file name, relative to current working directory
        file_size=ftp_file_size,
    )
    _logger.info(f"Downloaded {ftp_path} to {local_path}")

    # Upload file to GCS
    client = _get_gcs_client()
    copy_file_to_bucket(
        local_file_uri=local_path, remote_blob_uri=gcs_path, client=client
    )
    _logger.info(f"Uploaded {local_path} to {gcs_path}")
    return CopyResponse(ftp_path=ftp_path, gcs_path=gcs_path)


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


def parse(payload: ParseRequest) -> ParseResponse:
    _logger.info(f"parse payload: {payload.model_dump_json()}")
    execution_prefix = f"{env.executions_output_prefix}/{workflow_execution_id}"
    parse_output_path = (
        f"gs://{env.bucket_name}/{execution_prefix}/{env.parse_output_prefix}"
    )
    output_files = parse_and_write_files(
        payload.input_path,
        parse_output_path,
        disassemble=payload.disassemble,
        jsonify_content=payload.jsonify_content,
    )
    return ParseResponse(parsed_files=output_files)


try:
    parse_response = parse(ParseRequest(input_path=copy_response.gcs_path))
    _logger.info(f"Parse response: {parse_response.model_dump_json}")
except Exception as e:
    msg = "Failed during 'parse'."
    _logger.exception(msg)
    send_slack_message(workflow_id_message + " - " + msg)
    raise e


################################################################
# Creates external tables in BigQuery from the parsed data in GCS


def create_external_tables(
    payload: CreateExternalTablesRequest,
) -> CreateExternalTablesResponse:
    _logger.info(f"create_external_tables payload: {payload.model_dump_json()}")
    external_tables_created = run_create_external_tables(payload)
    for external_table_name, table in external_tables_created.items():
        table: bigquery.Table = table
        _logger.info(
            "Created table %s as %s:%s.%s",
            external_table_name,
            table.project,
            table.dataset_id,
            table.table_id,
        )

    entity_type_table_ids = {
        entity_type: f"{table.project}.{table.dataset_id}.{table.table_id}"
        for entity_type, table in external_tables_created.items()
    }
    return CreateExternalTablesResponse(root=entity_type_table_ids)


try:
    create_external_tables_response = create_external_tables(
        CreateExternalTablesRequest(
            destination_dataset=workflow_execution_id,
            source_table_paths=parse_response.parsed_files,
        )
    )
    _logger.info(
        f"Create External Tables response: {create_external_tables_response.model_dump_json()}"
    )
except Exception as e:
    msg = "Failed during 'create_external_tables'."
    _logger.exception(msg)
    send_slack_message(workflow_id_message + " - " + msg)
    raise e


################################################################
# Create internal tables

try:
    create_internal_tables_request = CreateInternalTablesRequest(
        source_dest_table_map={
            # source -> destination
            external: external.replace("_external", "")
            for external in create_external_tables_response.root.values()
        }
    )
    _logger.info(
        f"Create Internal Tables request: {create_internal_tables_request.model_dump_json()}"
    )

    create_internal_tables_response = create_internal_tables(create_internal_tables_request)
    _logger.info(
        f"Create Internal Tables response: {create_internal_tables_response.model_dump_json()}"
    )
except Exception as e:
    msg = "Failed during 'create_internal_tables'."
    _logger.exception(msg)
    send_slack_message(workflow_id_message + " - " + msg)
    raise e

################################################################
# Drop external tables
try:
    drop_external_tables_request = DropExternalTablesRequest(root=create_external_tables_response.root)
    _logger.info(
        f"Drop External Tables request: {drop_external_tables_request.model_dump_json()}"
    )

    drop_external_tables_response = drop_external_tables(drop_external_tables_request)
except Exception as e:
    msg = "Failed during 'drop_external_tables'."
    _logger.exception(msg)
    send_slack_message(workflow_id_message + " - " + msg)
    raise e

################################################################
_logger.info("Workflow succeeded")
send_slack_message(workflow_id_message + " - Workflow succeeded.")
