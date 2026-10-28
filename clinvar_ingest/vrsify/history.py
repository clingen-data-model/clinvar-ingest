"""
processing_history bookkeeping for the vrsify step.

A release is ready for vrsify once its stored-procedures (`sp`) row is finished and it has
no `vrsify` row. Claiming a release writes its `vrsify` started row, so later dispatcher
runs skip it; the Batch job writes the finished row once its output is uploaded. A failed
job leaves the started row in place, which keeps the release from being claimed again
until that row is deleted.

Only rows with file_type `vrsify` are ever written or deleted here. The queries read the
processing_history table directly rather than processing_history_view, whose definition
the other workflow jobs own.
"""

import datetime
import logging
from dataclasses import dataclass

from google.cloud import bigquery

from clinvar_ingest.cloud.bigquery import processing_history
from clinvar_ingest.utils import ClinVarIngestFileFormat

_logger = logging.getLogger("clinvar_ingest")

VRSIFY = ClinVarIngestFileFormat.VRSIFY


@dataclass(frozen=True)
class ReadyRelease:
    release_date: datetime.date
    bucket_dir: str
    sp_processing_finished: datetime.datetime


def ready_releases_sql(processing_history_table: str) -> str:
    return f"""
    SELECT sp.release_date, sp.bucket_dir, sp.processing_finished AS sp_processing_finished
    FROM `{processing_history_table}` sp
    WHERE sp.file_type = '{ClinVarIngestFileFormat.SP}'
    AND sp.processing_finished IS NOT NULL
    AND sp.release_date >= @min_release_date
    AND NOT EXISTS (
        SELECT 1
        FROM `{processing_history_table}` v
        WHERE v.file_type = '{VRSIFY}'
        AND v.release_date = sp.release_date
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.release_date ORDER BY sp.processing_finished DESC) = 1
    ORDER BY sp.release_date
    """  # noqa: S608


def ready_releases(
    processing_history_table: bigquery.Table,
    min_release_date: datetime.date,
    client: bigquery.Client,
) -> list[ReadyRelease]:
    """
    Releases on or after `min_release_date` whose sp row is finished and that have no
    vrsify row, oldest first.
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("min_release_date", "DATE", min_release_date)]
    )
    rows = client.query(ready_releases_sql(str(processing_history_table)), job_config=job_config).result()
    return [ReadyRelease(r.release_date, r.bucket_dir, r.sp_processing_finished) for r in rows]


def unfinished_sql(processing_history_table: str) -> str:
    return f"""
    SELECT release_date, pipeline_version, bucket_dir, processing_started
    FROM `{processing_history_table}`
    WHERE file_type = '{VRSIFY}'
    AND processing_finished IS NULL
    ORDER BY release_date
    """  # noqa: S608


def unfinished(processing_history_table: bigquery.Table, client: bigquery.Client) -> list:
    """vrsify rows that were started and never finished: running, failed, or lost jobs."""
    return list(client.query(unfinished_sql(str(processing_history_table))).result())


def claim(
    processing_history_table: bigquery.Table,
    release: ReadyRelease,
    release_tag: str,
    schema_version: str,
    client: bigquery.Client,
) -> None:
    """Write the vrsify started row for `release`; raises if a matching row already exists."""
    processing_history.write_started(
        processing_history_table=processing_history_table,
        release_date=str(release.release_date),
        release_tag=release_tag,
        schema_version=schema_version,
        file_type=VRSIFY,
        bucket_dir=release.bucket_dir,
        client=client,
        xml_release_date=str(release.release_date),
        # Never replace an existing row: with error_if_exists=False, write_started
        # deletes the match first
        error_if_exists=True,
    )


def release_claim(
    processing_history_table: bigquery.Table,
    release_date: datetime.date,
    release_tag: str,
    client: bigquery.Client,
) -> int:
    """Delete the vrsify row of one release, e.g. a claim whose Batch job was never submitted."""
    deleted = processing_history.delete(
        processing_history_table=processing_history_table,
        release_tag=release_tag,
        file_type=VRSIFY,
        xml_release_date=str(release_date),
        client=client,
    )
    if deleted != 1:
        _logger.warning(f"Expected to delete 1 vrsify row for {release_date}, deleted {deleted}")
    return deleted


def finish(
    processing_history_table: bigquery.Table,
    release_date: str,
    release_tag: str,
    bucket_dir: str,
    client: bigquery.Client,
) -> None:
    """Mark the vrsify row of `release_date` finished; raises unless exactly one row matches."""
    processing_history.write_finished(
        processing_history_table=processing_history_table,
        release_date=release_date,
        release_tag=release_tag,
        file_type=VRSIFY,
        bucket_dir=bucket_dir,
        parsed_files={},
        client=client,
    )
