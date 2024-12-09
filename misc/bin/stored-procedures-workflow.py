#!/usr/bin/env python3
################################################################
# This script is the main entrypoint for the execution of BigQuery
# stored procedures against on or more datasets in the ingestion workflow.

import logging
import os

from google.cloud import bigquery

from clinvar_ingest.cloud.bigquery import processing_history
from clinvar_ingest.config import get_stored_procedures_env
from clinvar_ingest.slack import send_slack_message

from clinvar_ingest.cloud.bigquery.stored_procedures import execute_all

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
### Initialization code

# Main env for tprocessing stored procedures - different from the rest!!!!
env = get_stored_procedures_env()
_logger.info(f"Stored procedures execution environment: {env}")

################################################################
# Write record to processing_history indicating this workflow has begun
processing_history_table = processing_history.ensure_initialized(
    client=_get_bq_client()
)

# TODO ===== which records to process

# TODO: consider request/response validation of request in requests.py - overkill?
client = _get_bq_client()
try:
    msg = f"Executing stored procedures on dataset dated {env.release_date}"
    _logger.info(msg)
    # send_slack_message(msg)
    result = execute_all(client=client, project_id=env.bq_dest_project, release_date=env.release_date)
    msg = f""
    _logger.info(msg)
    # send_slack_message(msg)
except Exception as e:
    msg = f""
    _logger.error(e)
    # send_slack_message(msg)
