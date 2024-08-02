#!/usr/bin/env python3
################################################################
# This script is the main entrypoint for the BigQuery ingestion workflow.
# This checks to see if there are new parsed outputs from an RCV and a VCV file
# and if so, creates a bigquery dataset from them.


import json
import logging
import os

from google.cloud import bigquery
from google.cloud.storage import Client as GCSClient

from clinvar_ingest.api.model.requests import (
    ClinvarFTPWatcherRequest,
    CreateExternalTablesRequest,
    CreateExternalTablesResponse,
    CreateInternalTablesRequest,
    DropExternalTablesRequest,
    walk_and_replace,
)
from clinvar_ingest.cloud.bigquery import processing_history
from clinvar_ingest.cloud.bigquery.create_tables import (
    create_internal_tables,
    drop_external_tables,
    ensure_dataset_exists,
    run_create_external_tables,
)
from clinvar_ingest.config import get_env
from clinvar_ingest.parse import ClinVarIngestFileFormat
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
processing_history_table = processing_history.ensure_initialized(
    client=_get_bq_client()
)
processing_history_pairs_view = processing_history.ensure_pairs_view_exists(
    processing_history_table=processing_history_table,
    client=_get_bq_client(),
)

processing_history_pairs = processing_history.processed_pairs_ready_to_be_ingested(
    processing_history_pairs_view, client=_get_bq_client()
)
for row in processing_history_pairs:
    _logger.info(row)
    release_date = row.get("release_date", None)
    vcv_file_type = row.get("vcv_file_type", None)
    vcv_pipeline_version = row.get("vcv_pipeline_version", None)
    vcv_processing_started = row.get("vcv_processing_started", None)
    vcv_processing_finished = row.get("vcv_processing_finished", None)
    vcv_release_date = row.get("vcv_release_date", None)
    vcv_xml_release_date = row.get("vcv_xml_release_date", None)
    vcv_bucket_dir = row.get("vcv_bucket_dir", None)
    vcv_parsed_files = json.loads(row.get("vcv_parsed_files", None))

    rcv_file_type = row.get("rcv_file_type", None)
    rcv_pipeline_version = row.get("rcv_pipeline_version", None)
    rcv_processing_started = row.get("rcv_processing_started", None)
    rcv_processing_finished = row.get("rcv_processing_finished", None)
    rcv_release_date = row.get("rcv_release_date", None)
    rcv_xml_release_date = row.get("rcv_xml_release_date", None)
    rcv_bucket_dir = row.get("rcv_bucket_dir", None)
    rcv_parsed_files = json.loads(row.get("rcv_parsed_files", None))

    # We will use the VCV file's release date as the "release date" for both
    # And the VCV pipeline version as the "pipeline version"
    final_release_date = vcv_xml_release_date
    final_release_date_sanitized = final_release_date.isoformat().replace("-", "_")
    # The dataset name to create is clinvar with the release date and code version
    target_dataset_name = (
        f"clinvar_{final_release_date_sanitized}_{vcv_pipeline_version}"
    )

    # Infer the project from the gcloud configuration
    bq_client = _get_bq_client()
    # Infer the dataset location from the configured storage bucket
    bucket = _get_gcs_client().get_bucket(env.bucket_name)
    bucket_location = bucket.location

    dataset = ensure_dataset_exists(
        project=bq_client.project,
        dataset_id=target_dataset_name,
        client=bq_client,
        location=bucket_location,
    )
    _logger.info(f"Created dataset: {dataset.dataset_id}")

    # Create VCV external tables
    vcv_create_tables_request = CreateExternalTablesRequest(
        destination_dataset=target_dataset_name, source_table_paths=vcv_parsed_files
    )
    _logger.info(
        f"VCV Create External Tables request: {vcv_create_tables_request.model_dump_json()}"
    )
    vcv_create_tables_response = run_create_external_tables(vcv_create_tables_request)
    vcv_ext_resp_json = json.dumps(
        walk_and_replace(vcv_create_tables_response, processing_history._dump_fn)
    )
    _logger.info(f"VCV Create External Tables response: {vcv_ext_resp_json}")

    # Create RCV external tables
    rcv_create_tables_request = CreateExternalTablesRequest(
        destination_dataset=target_dataset_name, source_table_paths=rcv_parsed_files
    )
    _logger.info(
        f"RCV Create External Tables request: {rcv_create_tables_request.model_dump_json()}"
    )
    rcv_create_tables_response = run_create_external_tables(rcv_create_tables_request)
    rcv_ext_resp_json = json.dumps(
        walk_and_replace(rcv_create_tables_response, processing_history._dump_fn)
    )
    _logger.info(f"RCV Create External Tables response: {rcv_ext_resp_json}")

    # Update the processing history table to insert the final release date into the VCV
    # and RCV rows to indicate that they have been ingested
    # Update VCV final release_date
    _logger.info("Updating VCV final release date.")
    _logger.info(
        f"processing_history_table: {processing_history_table}, "
        f"xml_release_date: {vcv_xml_release_date}, "
        f"release_tag: {vcv_pipeline_version}, "
        f"file_type: {vcv_file_type}, "
        f"bucket_dir: {vcv_bucket_dir}, "
        f"final_release_date: {final_release_date}"
    )
    processing_history.update_final_release_date(
        processing_history_table=processing_history_table,
        xml_release_date=vcv_xml_release_date,
        release_tag=vcv_pipeline_version,
        file_type=vcv_file_type,
        bucket_dir=vcv_bucket_dir,
        final_release_date=final_release_date,
    )
    # Update RCV final release_date
    _logger.info("Updating RCV final release date.")
    _logger.info(
        f"processing_history_table: {processing_history_table}, "
        f"xml_release_date: {rcv_xml_release_date}, "
        f"release_tag: {rcv_pipeline_version}, "
        f"file_type: {rcv_file_type}, "
        f"bucket_dir: {rcv_bucket_dir}, "
        f"final_release_date: {final_release_date}"
    )
    processing_history.update_final_release_date(
        processing_history_table=processing_history_table,
        xml_release_date=rcv_xml_release_date,
        release_tag=rcv_pipeline_version,
        file_type=rcv_file_type,
        bucket_dir=rcv_bucket_dir,
        final_release_date=final_release_date,
    )

    # TODO send slack message saying which VCV and RCV releases were included
    # And what the final release date is and what BigQuery dataset it was written to


import sys

sys.exit(0)

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
            source_table_paths=parse_response.parsed_files,  # type: ignore
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


# ################################################################
# # Create internal tables

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

    create_internal_tables_response = create_internal_tables(
        create_internal_tables_request
    )
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
    drop_external_tables_request = DropExternalTablesRequest(
        root=create_external_tables_response.root
    )
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
# Write record to processing_history indicating this workflow has begun
# processing_history_table = processing_history.ensure_initialized()
# processing_history.write_finished(
#     processing_history_table=processing_history_table,
#     release_date=release_date,
#     release_tag=env.release_tag,
#     file_type=file_mode,
#     # The directory within the executions_output_prefix. See gcs_base in copy()
#     bucket_dir=workflow_execution_id,
#     client=_get_bq_client(),
# )


################################################################
_logger.info("Workflow succeeded")
send_slack_message(workflow_id_message + " - Workflow succeeded.")
