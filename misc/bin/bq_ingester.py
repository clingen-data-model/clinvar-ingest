#!/usr/bin/env python3
################################################################
# This script is the main entrypoint for the BigQuery ingestion workflow.
# This checks to see if there are new parsed outputs from an RCV and a VCV file
# and if so, creates a bigquery dataset from them.


import json
import logging
import sys
import traceback

from google.cloud import bigquery
from google.cloud.storage import Client as GCSClient

from clinvar_ingest.api.model.requests import (
    CreateExternalTablesRequest,
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
from clinvar_ingest.slack import send_slack_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("clinvar-ingest-workflow")


# Environment needs to contain
# CLINVAR_INGEST_SLACK_TOKEN
# CLINVAR_INGEST_SLACK_CHANNEL
# CLINVAR_INGEST_BUCKET
# CLINVAR_INGEST_RELEASE_TAG
# CLINVAR_INGEST_BQ_META_DATASET
# BQ_DEST_PROJECT


def _get_gcs_client() -> GCSClient:
    if getattr(_get_gcs_client, "client", None) is None:
        setattr(_get_gcs_client, "client", GCSClient())
    return getattr(_get_gcs_client, "client")


def _get_bq_client() -> bigquery.Client:
    if getattr(_get_bq_client, "client", None) is None:
        setattr(_get_bq_client, "client", bigquery.Client())
    return getattr(_get_bq_client, "client")

################################################################
### rollback exception handler for deleting processing_history entries

rollback_rows = []


def bq_ingest_rollback_exception_handler(exc_type, exc_value, exc_traceback):
    """
    https://docs.python.org/3/library/sys.html#sys.excepthook
    """
    exception_details = "".join(
        traceback.format_exception(exc_type, exc_value, exc_traceback)
    )

    # Log the exception
    _logger.error("Uncaught exception:\n%s", exception_details)

    _logger.warning("Rolling back started BQ ingest rows.")
    for row in rollback_rows:
        _logger.info(f"Rolling back row: {row}")
        c = processing_history.delete(
            processing_history_table=row["processing_history_table"],
            release_tag=row["release_tag"],
            file_type=row["file_type"],
            xml_release_date=row["xml_release_date"],
            client=_get_bq_client(),
        )
        _logger.info(f"Deleted {c} rows from processing_history.")

    # Call the default exception handler
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


# Add the exception handler as the global exception handler.
# NOTE: this modifies global state and will affect all subsequent exceptions in this script or any other script which imports this script.
sys.excepthook = bq_ingest_rollback_exception_handler

################################################################
### Initialization code

# Main env for the codebase
env = get_env()
_logger.info(f"BQ Ingest environment: {env}")

if env.file_format_mode != "bq":
    msg = f"Expected file_format_mode to be 'bq' but got '{env.file_format_mode}'"
    _logger.error(msg)
    send_slack_message(msg)
    sys.exit(1)

################################################################
# Write record to processing_history indicating this workflow has begun
processing_history_table = processing_history.ensure_initialized(
    client=_get_bq_client()
)
processing_history_view = processing_history.ensure_history_view_exists(
    processing_history_table=processing_history_table,
    client=_get_bq_client(),
)
processing_history_needing_bq_ingest = (
    processing_history.processed_entries_ready_for_bq_ingest(
        processing_history_view, client=_get_bq_client()
    )
)
msg = f"Found {processing_history_needing_bq_ingest.total_rows} VCV/RCV datasets to ingest."
_logger.info(msg)

if not processing_history_needing_bq_ingest.total_rows:
    sys.exit(0)

send_slack_message(msg)


def json_parse_if_string(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


# Store the information needed to roll back a started ingest if an error occurs
# bq_ingest_rows_deleted = processing_history.delete(
#     processing_history_table=processing_history_table,
#     release_date=vcv_xml_release_date,
#     release_tag=vcv_pipeline_version,
#     file_type=env.file_format_mode,
#     xml_release_date=vcv_xml_release_date,
#     client=_get_bq_client(),
# )


# update processing_history to set the bq ingest as having started
rows_to_ingest = []
for row in processing_history_needing_bq_ingest:
    rows_to_ingest.append(row)
    vcv_pipeline_version = row.get("vcv_pipeline_version", None)
    vcv_xml_release_date = row.get("vcv_xml_release_date", None)
    vcv_bucket_dir = row.get("vcv_bucket_dir", None)
    schema_version = row.get("vcv_schema_version", None)
    bq_ingest_write_result, query_job = processing_history.write_started(
        processing_history_table=processing_history_table,
        release_date=str(vcv_xml_release_date),
        release_tag=vcv_pipeline_version,
        schema_version=schema_version,
        file_type=env.file_format_mode,
        client=_get_bq_client(),
        bucket_dir=vcv_bucket_dir,
        xml_release_date=str(vcv_xml_release_date),
        error_if_exists=False,
    )
    _logger.info(
        f"Created bq_ingest_processing record with version {vcv_pipeline_version} and "
        f"release date {vcv_xml_release_date}."
    )
    # Add the started row to the rollback list
    rollback_rows.append(
        {
            "processing_history_table": processing_history_table,
            "release_tag": vcv_pipeline_version,
            "file_type": env.file_format_mode,
            "xml_release_date": str(vcv_xml_release_date),
            "client": _get_bq_client(),
        }
    )

# Now process individual rows
for row in rows_to_ingest:
    _logger.info(row)
    release_date = row.get("release_date", None)
    vcv_file_type = row.get("vcv_file_type", None)
    vcv_pipeline_version = row.get("vcv_pipeline_version", None)
    vcv_schema_version = row.get("vcv_schema_version", None)
    vcv_processing_started = row.get("vcv_processing_started", None)
    vcv_processing_finished = row.get("vcv_processing_finished", None)
    vcv_release_date = row.get("vcv_release_date", None)
    vcv_xml_release_date = row.get("vcv_xml_release_date", None)
    vcv_bucket_dir = row.get("vcv_bucket_dir", None)
    vcv_parsed_files = json_parse_if_string(row.get("vcv_parsed_files", None))

    rcv_file_type = row.get("rcv_file_type", None)
    rcv_pipeline_version = row.get("rcv_pipeline_version", None)
    rcv_schema_version = row.get("rcv_schema_version", None)
    rcv_processing_started = row.get("rcv_processing_started", None)
    rcv_processing_finished = row.get("rcv_processing_finished", None)
    rcv_release_date = row.get("rcv_release_date", None)
    rcv_xml_release_date = row.get("rcv_xml_release_date", None)
    rcv_bucket_dir = row.get("rcv_bucket_dir", None)
    rcv_parsed_files = json_parse_if_string(row.get("rcv_parsed_files", None))

    # We will use the VCV file's release date as the "release date" for both
    # And the VCV pipeline version as the "pipeline version"
    final_release_date = vcv_xml_release_date
    final_release_date_sanitized = final_release_date.isoformat().replace("-", "_")
    # The dataset name to create is clinvar with the release date and code version
    target_dataset_name = (
        f"clinvar_{final_release_date_sanitized}_{vcv_pipeline_version}"
    )

    # Infer the dataset location from the configured storage bucket
    bq_client = _get_bq_client()
    bucket = _get_gcs_client().get_bucket(env.bucket_name)
    bucket_location = bucket.location

    dataset = ensure_dataset_exists(
        project=bq_client.project,
        dataset_id=target_dataset_name,
        client=bq_client,
        location=bucket_location,
    )
    _logger.info(f"Created dataset: {dataset.dataset_id}")

    msg = f"""
        BQ Ingest now processing {vcv_bucket_dir} and {rcv_bucket_dir} files into dataset: {dataset.dataset_id}.
        """
    _logger.info(msg)
    send_slack_message(msg)

    # Create VCV external tables
    vcv_create_tables_request = CreateExternalTablesRequest(
        destination_dataset=target_dataset_name,
        source_table_paths=vcv_parsed_files,
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
        destination_dataset=target_dataset_name,
        source_table_paths=rcv_parsed_files,
    )
    _logger.info(
        f"RCV Create External Tables request: {rcv_create_tables_request.model_dump_json()}"
    )
    rcv_create_tables_response = run_create_external_tables(rcv_create_tables_request)
    rcv_ext_resp_json = json.dumps(
        walk_and_replace(rcv_create_tables_response, processing_history._dump_fn)
    )
    _logger.info(f"RCV Create External Tables response: {rcv_ext_resp_json}")

    # VCV create internal tables
    vcv_create_internal_tables_request = CreateInternalTablesRequest(
        source_dest_table_map={
            # source -> destination
            str(table): str(table).replace("_external", "")
            for _, table in vcv_create_tables_response.items()
        }
    )
    _logger.info(
        f"VCV Create Internal Tables request: {vcv_create_internal_tables_request.model_dump_json()}"
    )
    vcv_create_internal_tables_response = create_internal_tables(
        vcv_create_internal_tables_request
    )
    _logger.info(
        f"VCV Create Internal Tables response: {vcv_create_internal_tables_response.model_dump_json()}"
    )

    # RCV create internal tables
    rcv_create_internal_tables_request = CreateInternalTablesRequest(
        source_dest_table_map={
            # source -> destination
            str(table): str(table).replace("_external", "")
            for _, table in rcv_create_tables_response.items()
        }
    )
    _logger.info(
        f"RCV Create Internal Tables request: {rcv_create_internal_tables_request.model_dump_json()}"
    )
    rcv_create_internal_tables_response = create_internal_tables(
        rcv_create_internal_tables_request
    )
    _logger.info(
        f"RCV Create Internal Tables response: {rcv_create_internal_tables_response.model_dump_json()}"
    )

    # Drop VCV external tables
    vcv_drop_external_tables_request = DropExternalTablesRequest(
        root=vcv_create_tables_response
    )
    _logger.info(
        f"VCV Drop External Tables request: {vcv_drop_external_tables_request.model_dump_json()}"
    )
    vcv_drop_external_tables_response = drop_external_tables(
        vcv_drop_external_tables_request
    )

    # Drop RCV external tables
    rcv_drop_external_tables_request = DropExternalTablesRequest(
        root=rcv_create_tables_response
    )
    _logger.info(
        f"RCV Drop External Tables request: {rcv_drop_external_tables_request.model_dump_json()}"
    )
    rcv_drop_external_tables_response = drop_external_tables(
        rcv_drop_external_tables_request
    )

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
        final_dataset_id=dataset.dataset_id,
        client=bq_client,
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
        final_dataset_id=dataset.dataset_id,
        client=bq_client,
    )

    # Update BQ final release_date
    _logger.info("Updating BQ final release date.")
    _logger.info(
        f"processing_history_table: {processing_history_table}, "
        f"xml_release_date: {vcv_xml_release_date}, "
        f"release_tag: {vcv_pipeline_version}, "
        f"file_type: {env.file_format_mode}, "
        f"bucket_dir: {vcv_bucket_dir}, "
        f"final_release_date: {final_release_date}"
    )
    processing_history.update_final_release_date(
        processing_history_table=processing_history_table,
        xml_release_date=vcv_xml_release_date,
        release_tag=vcv_pipeline_version,
        file_type=env.file_format_mode,
        bucket_dir=vcv_bucket_dir,
        final_release_date=final_release_date,
        final_dataset_id=dataset.dataset_id,
        client=bq_client,
    )

    processing_history.write_finished(
        processing_history_table=processing_history_table,
        release_date=str(final_release_date),
        release_tag=env.release_tag,
        file_type=env.file_format_mode,
        parsed_files={},
        bucket_dir=vcv_bucket_dir,
        client=_get_bq_client(),
    )

    msg = f"""
        BQ Ingest workflow succeeded.
        Processed VCV release dated {vcv_xml_release_date} from {vcv_bucket_dir} and
        RCV release dated {rcv_xml_release_date} from {rcv_bucket_dir} into dataset {dataset.dataset_id}.
        """
    _logger.info(msg)
    send_slack_message(msg)

    # Remove the started row from the rollback list, since this ingest has succeeded
    rollback_rows = [
        row
        for row in rollback_rows
        if row["xml_release_date"] != str(vcv_xml_release_date)
        or row["release_tag"] != vcv_pipeline_version
    ]
