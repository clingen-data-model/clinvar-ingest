#!/usr/bin/env python3
################################################################
# Entrypoint of the vrsify Cloud Run job: claims releases whose stored procedures
# have finished and submits a Cloud Batch job that runs `clinvar-ingest vrsify` on each.
#
# Besides the base clinvar-ingest environment (file_format=vrsify), it reads:
#   VRSIFY_IMAGE             image for the Batch job (required)
#   VRSIFY_MIN_RELEASE_DATE  earliest release to claim, YYYY-MM-DD (required)
#   VRSIFY_MAX_RELEASES      releases claimed per run (default: 1)
#   VRSIFY_SLACK             "true" to post to Slack, here and in the Batch job (default: false)
#   VRSIFY_DRY_RUN           "true" to log what would be claimed without writing anything
#   VRSIFY_SERVICE_ACCOUNT   service account of the Batch job
#                            (default: clinvar-ingest-pipeline@<project>.iam.gserviceaccount.com)
#   VRSIFY_MACHINE_TYPE, VRSIFY_BOOT_DISK_TYPE  Batch VM overrides

import datetime
import logging

from google.cloud import bigquery

from clinvar_ingest.config import env_or_dotenv_or, get_vrsify_env
from clinvar_ingest.slack import send_slack_message
from clinvar_ingest.utils import ClinVarIngestFileFormat
from clinvar_ingest.vrsify import batch, dispatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("vrsify-workflow")


def _flag(name: str) -> bool:
    return env_or_dotenv_or(name, default="false").lower() == "true"


env = get_vrsify_env()
if env.file_format_mode != ClinVarIngestFileFormat.VRSIFY.value:
    raise ValueError(f"vrsify workflow got unexpected file_format_mode: {env.file_format_mode}")

settings = dispatch.DispatchSettings(
    min_release_date=datetime.date.fromisoformat(env_or_dotenv_or("VRSIFY_MIN_RELEASE_DATE", throw=True)),
    max_releases=int(env_or_dotenv_or("VRSIFY_MAX_RELEASES", default="1")),
    slack=_flag("VRSIFY_SLACK"),
    dry_run=_flag("VRSIFY_DRY_RUN"),
)
project = env.bq_dest_project
batch_settings = batch.BatchSettings(
    project=project,
    region=env.location,
    service_account=env_or_dotenv_or(
        "VRSIFY_SERVICE_ACCOUNT", default=f"clinvar-ingest-pipeline@{project}.iam.gserviceaccount.com"
    ),
    image=env_or_dotenv_or("VRSIFY_IMAGE", throw=True),
    environment=dispatch.task_environment(env, {k: env_or_dotenv_or(k) for k in dispatch.TASK_ENVIRONMENT_KEYS}),
)
if machine_type := env_or_dotenv_or("VRSIFY_MACHINE_TYPE"):
    batch_settings.machine_type = machine_type
if boot_disk_type := env_or_dotenv_or("VRSIFY_BOOT_DISK_TYPE"):
    batch_settings.boot_disk_type = boot_disk_type
# Not the whole env: it carries the Slack token
_logger.info(f"vrsify dispatch settings: {settings}, release_tag={env.release_tag}, image={batch_settings.image}")


def notify(message: str) -> None:
    _logger.info(message)
    if settings.slack:
        send_slack_message(message)


client = bigquery.Client(project=project)
# Read directly: this job never creates or replaces processing_history or its view
processing_history_table = client.get_table(f"{project}.{env.bq_meta_dataset}.processing_history")
dispatch.run(env, settings, batch_settings, processing_history_table, client, notify)
