#!/usr/bin/env python3
################################################################
# This script is the main entrypoint for the execution of BigQuery
# stored procedures against on or more datasets in the ingestion workflow.

import logging
import sys

from google.cloud import bigquery

from clinvar_ingest.cloud.bigquery import processing_history
from clinvar_ingest.cloud.bigquery.stored_procedures import execute_all
from clinvar_ingest.config import get_stored_procedures_env
from clinvar_ingest.slack import send_slack_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("stored-procedures-workflow")


def _get_bq_client() -> bigquery.Client:
    if getattr(_get_bq_client, "client", None) is None:
        _get_bq_client.client = bigquery.Client()
    return _get_bq_client.client

################################################################
### Initialization code

# Main env for tprocessing stored procedures - different from the rest!!!!
env = get_stored_procedures_env()
_logger.info(f"Stored procedures execution environment: {env}")

################################################################
#
processing_history_table = processing_history.ensure_initialized(
    client=_get_bq_client()
)

processing_history_view = processing_history.ensure_history_view_exists(
    processing_history_table=processing_history_table,
    client=_get_bq_client(),
)

processed_entries_needing_sp_run = processing_history.processed_entries_ready_for_sp_processing(
    processing_history_view, client=_get_bq_client()
)
msg = f"Found {processed_entries_needing_sp_run.total_rows} datasets to run stored procedures on."
_logger.info(msg)

if not processed_entries_needing_sp_run.total_rows:
    sys.exit(0)

send_slack_message(msg)

# update processing_history.bq_ingest_started for ALL processing_history_view
rows_to_ingest = []
for row in processed_entries_needing_sp_run:
    rows_to_ingest.append(row)
    vcv_pipeline_version = row.get("vcv_pipeline_version", None)
    vcv_xml_release_date = row.get("vcv_xml_release_date", None)
    vcv_bucket_dir = row.get("vcv_bucket_dir", None)
    schema_version = row.get("vcv_schema_version", None)
    sp_processing_write_result = processing_history.write_started(
        processing_history_table=processing_history_table,
        release_date=str(vcv_xml_release_date),
        release_tag=vcv_pipeline_version,
        schema_version=schema_version,
        file_type=env.file_format_mode,
        client=_get_bq_client(),
        bucket_dir=vcv_bucket_dir,
        xml_release_date=str(vcv_xml_release_date),
        error_if_exists=True,
    )
    _logger.info(
        f"Initiated stored procedure processing for {vcv_pipeline_version} and "
        f"{vcv_xml_release_date}."
    )

# Now process individual rows
for row in rows_to_ingest:
    _logger.info(row)
    release_date = row.get("release_date", None)
    vcv_pipeline_version = row.get("vcv_pipeline_version", None)
    vcv_release_date = row.get("vcv_release_date", None)
    vcv_xml_release_date = row.get("vcv_xml_release_date", None)
    vcv_bucket_dir = row.get("vcv_bucket_dir", None)
    schema_version = row.get("vcv_schema_version", None)

    msg = f"Executing stored procedures on dataset dated {release_date}"
    _logger.info(msg)
    # send_slack_message(msg)
    try:
        result = execute_all(client=_get_bq_client(), project_id=env.bq_dest_project, release_date=release_date)
        msg = ""
        _logger.info(msg)
        # send_slack_message(msg)

        processing_history.write_finished(
            processing_history_table=processing_history_table,
            release_date=str(release_date),
            release_tag=env.release_tag,
            file_type=env.file_format_mode,
            parsed_files=None,
            bucket_dir=vcv_bucket_dir,
            client=_get_bq_client(),
        )
    except Exception as e:
        processing_history.write_started(
            processing_history_table=processing_history_table,
            release_date=None,
            release_tag=vcv_pipeline_version,
            schema_version=schema_version,
            file_type=env.file_format_mode,
            client=_get_bq_client(),
            bucket_dir=vcv_bucket_dir,
            xml_release_date=str(vcv_xml_release_date),
            error_if_exists=True,
            )
        msg = f"""
              Reset processing_history_table VCV bq_ingest_processing dated {vcv_xml_release_date} version
              {vcv_pipeline_version}.
              """
        raise e
        # send_slack_message(msg)

