"""
Claim releases that are ready for vrsify and submit a Cloud Batch job for each.

Run on a schedule (misc/bin/vrsify-workflow.py). Each run claims at most `max_releases`
releases on or after `min_release_date`, so that a new deployment cannot start a backfill
of every earlier release.
"""

import datetime
import logging
from collections.abc import Callable
from dataclasses import dataclass

from google.cloud import bigquery

from clinvar_ingest.config import VrsifyEnv
from clinvar_ingest.vrsify import batch, history
from clinvar_ingest.vrsify.runner import default_uris

_logger = logging.getLogger("clinvar_ingest")

# Environment variables the Batch job's vrsify run reads, passed through from the dispatcher
TASK_ENVIRONMENT_KEYS = ("CLINVAR_INGEST_REFGET_STORE",)


@dataclass
class DispatchSettings:
    min_release_date: datetime.date
    max_releases: int = 1
    input_wait_seconds: int = 3600
    slack: bool = False
    dry_run: bool = False


def task_environment(env: VrsifyEnv, extra: dict[str, str]) -> dict[str, str]:
    """Settings the Batch job needs to write processing_history and post to Slack."""
    variables = {
        "BQ_DEST_PROJECT": env.bq_dest_project,
        "CLINVAR_INGEST_BQ_META_DATASET": env.bq_meta_dataset,
        "CLINVAR_INGEST_RELEASE_TAG": env.release_tag,
        "CLINVAR_INGEST_SCHEMA_VERSION": env.schema_version,
        "CLINVAR_INGEST_LOCATION": env.location,
        "CLINVAR_INGEST_SLACK_CHANNEL": env.slack_channel,
        "CLINVAR_GKS_BUCKET": env.clinvar_gks_bucket,
        "file_format": env.file_format_mode,
    }
    return variables | {k: v for k, v in extra.items() if v}


def task_args(release: history.ReadyRelease, settings: DispatchSettings) -> list[str]:
    args = [
        "--release-date",
        str(release.release_date),
        "--history-bucket-dir",
        release.bucket_dir,
        "--input-newer-than",
        release.sp_processing_finished.isoformat(),
        "--input-wait",
        str(settings.input_wait_seconds),
    ]
    if settings.slack:
        args.append("--slack")
    return args


def run(
    env: VrsifyEnv,
    settings: DispatchSettings,
    batch_settings: batch.BatchSettings,
    table: bigquery.Table,
    client: bigquery.Client,
    notify: Callable[[str], None],
    submit: Callable[[batch.BatchSettings, str, dict], str] = batch.submit,
) -> list[str]:
    """Claim and submit ready releases, returning the submitted Batch job names."""
    for row in history.unfinished(table, client):
        notify(
            f"vrsify row for {row.release_date} started at {row.processing_started.isoformat()} has not finished "
            f"(pipeline_version={row.pipeline_version}, bucket_dir={row.bucket_dir}). "
            "If its Batch job failed, delete the row to let the release be claimed again."
        )

    ready = history.ready_releases(table, settings.min_release_date, client)
    _logger.info(f"{len(ready)} releases ready for vrsify: {[str(r.release_date) for r in ready]}")
    submitted = []
    for release in ready[: settings.max_releases]:
        args = task_args(release, settings)
        definition = batch.job_definition(batch_settings, args, slack=settings.slack)
        name = batch.job_id(str(release.release_date))
        if settings.dry_run:
            _logger.info(f"Dry run: would claim {release} and submit {name} with arguments {args}")
            continue

        history.claim(table, release, env.release_tag, env.schema_version, client)
        try:
            job_name = submit(batch_settings, name, definition)
        except Exception as e:
            # The claim was written moments ago by this run; release it so the next run retries
            history.release_claim(table, release.release_date, env.release_tag, client)
            notify(f"vrsify could not submit a Batch job for {release.release_date}: {type(e).__name__}: {e}")
            raise
        _, output_uri = default_uris(str(release.release_date))
        notify(f"vrsify submitted {job_name} for {release.release_date} -> {output_uri}")
        submitted.append(job_name)
    return submitted
