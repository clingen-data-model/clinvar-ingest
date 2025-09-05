#!/usr/bin/env python3
################################################################
# This script is the main entrypoint for the execution of BigQuery
# stored procedures against on or more datasets in the ingestion workflow.

import logging
import sys
import traceback

from google.cloud import bigquery

from clinvar_ingest.cloud.bigquery import processing_history
from clinvar_ingest.cloud.bigquery.stored_procedures import execute_all
from clinvar_ingest.config import get_stored_procedures_env
from clinvar_ingest.slack import send_slack_message
from clinvar_ingest.utils import ClinVarIngestFileFormat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("stored-procedures-workflow")


def _get_bq_client() -> bigquery.Client:
    if getattr(_get_bq_client, "client", None) is None:
        setattr(_get_bq_client, "client", bigquery.Client())
    return getattr(_get_bq_client, "client")


################################################################
### rollback exception handler for deleting processing_history entries

rollback_rows = []


def sp_rollback_exception_handler(exc_type, exc_value, exc_traceback):
    """
    https://docs.python.org/3/library/sys.html#sys.excepthook
    """
    exception_details = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

    # Log the exception
    _logger.error("Uncaught exception:\n%s", exception_details)

    _logger.warning("Rolling back started SP ingest rows.")
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
# NOTE: this modifies global state and will affect all subsequent exceptions
# in this script or any other script which imports this script.
sys.excepthook = sp_rollback_exception_handler

################################################################
### Initialization code

# Main env for tprocessing stored procedures - different from the rest!!!!
env = get_stored_procedures_env()
_logger.info(f"Stored procedures execution environment: {env}")

if env.file_format_mode != ClinVarIngestFileFormat.SP.value:
    msg = f"stored-procedure workflow got unexpected file_format_mode: {env.file_format_mode}"
    _logger.warning(msg)
    raise ValueError(msg)

################################################################
#
processing_history_table = processing_history.ensure_initialized(client=_get_bq_client())

processing_history_view = processing_history.ensure_history_view_exists(
    processing_history_table=processing_history_table,
    client=_get_bq_client(),
)

processed_entries_needing_sp_run = processing_history.processed_entries_ready_for_sp_processing(
    processing_history_view, client=_get_bq_client()
)
total_rows = processed_entries_needing_sp_run.total_rows
rows_needing_sp_run = list(processed_entries_needing_sp_run)
release_dates_str = ", ".join(r.get("release_date").isoformat() for r in rows_needing_sp_run)
msg = f"Found {total_rows} datasets to run stored procedures on. ({release_dates_str})"
_logger.info(msg)

if not total_rows:
    sys.exit(0)

send_slack_message(msg)

# update processing_history.bq_ingest_started for ALL processing_history_view
rows_to_ingest = []
for row in rows_needing_sp_run:
    rows_to_ingest.append(row)
    vcv_pipeline_version = row.get("vcv_pipeline_version", None)
    vcv_xml_release_date = row.get("vcv_xml_release_date", None)
    vcv_bucket_dir = row.get("vcv_bucket_dir", None)
    schema_version = row.get("vcv_schema_version", None)
    sp_processing_write_result = processing_history.write_started(
        processing_history_table=processing_history_table,
        release_date=str(vcv_xml_release_date),
        release_tag=env.release_tag,
        schema_version=schema_version,
        file_type=ClinVarIngestFileFormat(env.file_format_mode),
        client=_get_bq_client(),
        bucket_dir=vcv_bucket_dir,
        xml_release_date=str(vcv_xml_release_date),
        error_if_exists=False,
    )

    msg = f"""
        Initiated stored procedure processing for release dated {vcv_xml_release_date=} {vcv_pipeline_version=} release_tag={env.release_tag}.
        """
    _logger.info(msg)

    # Add the started row to the rollback list
    rollback_rows.append(
        {
            "processing_history_table": processing_history_table,
            "release_tag": env.release_tag,
            "file_type": env.file_format_mode,
            "xml_release_date": str(vcv_xml_release_date),
        }
    )

# Now process individual rows
for row in rows_to_ingest:
    _logger.info(row)
    # required
    release_date = row["release_date"]
    vcv_pipeline_version = row["vcv_pipeline_version"]
    vcv_xml_release_date = row["vcv_xml_release_date"]
    vcv_bucket_dir = row["vcv_bucket_dir"]
    dataset_id = row["final_dataset_id"]
    # optional
    schema_version = row.get("vcv_schema_version")

    msg = f"Executing stored procedures on dataset dated {release_date}"
    _logger.info(msg)
    send_slack_message(msg)
    try:
        result = execute_all(
            client=_get_bq_client(),
            project_id=env.bq_dest_project,
            release_date=release_date,
            dataset=dataset_id,
        )

        processing_history.write_finished(
            processing_history_table=processing_history_table,
            release_date=str(release_date),
            release_tag=env.release_tag,
            file_type=ClinVarIngestFileFormat(env.file_format_mode),
            parsed_files={},
            bucket_dir=vcv_bucket_dir,
            client=_get_bq_client(),
        )
        msg = f"""
                Stored procedure execution successful for release dated vcv_xml_release_date={vcv_xml_release_date.isoformat()} {vcv_pipeline_version=} release_tag={env.release_tag}.
            """
        _logger.info(msg)
        send_slack_message(msg)
    except Exception as e:
        msg = f"""
              Stored procedure execution failed for release dated vcv_xml_release_date={vcv_xml_release_date.isoformat()} {vcv_pipeline_version=} release_tag={env.release_tag}.
              """
        _logger.error(msg)
        send_slack_message(msg)
        raise e

    # Remove the started row from the rollback list, since this ingest has succeeded
    rollback_rows = [
        row
        for row in rollback_rows
        if row["xml_release_date"] != str(vcv_xml_release_date) or row["release_tag"] != env.release_tag
    ]

    vi_gs_url = f"gs://{env.clinvar_gks_bucket}/{release_date}/dev/vi.jsonl.gz"
    try:
        client = _get_bq_client()
        table_id = f"{dataset_id}.variation_identity"
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON, compression=bigquery.Compression.GZIP
        )
        extract_job = client.extract_table(table_id, vi_gs_url, job_config=job_config)
        extract_job.result(timeout=1800)  # Wait for the job to complete (30 minute timeout)
        msg = f"Successfully exported variation_identity file to {vi_gs_url}"
        _logger.info(msg)
        send_slack_message(msg)
    except Exception as e:
        error_msg = f"BigQuery extract job to {vi_gs_url} failed: {e}"
        _logger.error(error_msg)
        send_slack_message(error_msg)
        raise e
