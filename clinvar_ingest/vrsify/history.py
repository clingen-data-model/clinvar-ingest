"""
processing_history bookkeeping for the vrsify step, which the dispatcher owns.

A release is ready for vrsify once its stored-procedures (`sp`) row is finished and it has
no `vrsify` row. Claiming a release writes its `vrsify` started row; the dispatcher
execution that claimed it writes the finished row, recording the Batch job in
`parsed_files`, once the job's output checks out. A failed release keeps its started row,
which keeps it from being claimed again until a person deletes it.

Only rows with file_type `vrsify` are ever written here. The queries read the
processing_history table directly rather than processing_history_view, whose definition
the other workflow jobs own.
"""

import datetime
from dataclasses import dataclass, field

from google.cloud import bigquery

from clinvar_ingest.cloud.bigquery import processing_history
from clinvar_ingest.utils import ClinVarIngestFileFormat

VRSIFY = ClinVarIngestFileFormat.VRSIFY


@dataclass(frozen=True)
class ReadyRelease:
    release_date: datetime.date
    bucket_dir: str
    sp_processing_finished: datetime.datetime


@dataclass(frozen=True)
class VrsifyRow:
    """A claimed release's vrsify row: its key, and what to record when it finishes."""

    release_date: datetime.date
    pipeline_version: str
    bucket_dir: str
    sp_processing_finished: datetime.datetime
    parsed_files: dict = field(default_factory=dict)


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


def claim(
    processing_history_table: bigquery.Table,
    release: ReadyRelease,
    release_tag: str,
    schema_version: str,
    client: bigquery.Client,
) -> VrsifyRow:
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
    return VrsifyRow(release.release_date, release_tag, release.bucket_dir, release.sp_processing_finished)


def finish(processing_history_table: bigquery.Table, row: VrsifyRow, client: bigquery.Client) -> None:
    """Mark the vrsify row finished; raises unless exactly one row matches."""
    processing_history.write_finished(
        processing_history_table=processing_history_table,
        release_date=str(row.release_date),
        release_tag=row.pipeline_version,
        file_type=VRSIFY,
        bucket_dir=row.bucket_dir,
        parsed_files=row.parsed_files,
        client=client,
    )


def delete_sql(processing_history_table: str, row: VrsifyRow) -> str:
    """The statement a person runs to let a failed release be claimed again. Not executed here."""
    return (
        f"DELETE FROM `{processing_history_table}` "  # noqa: S608
        f"WHERE file_type = '{VRSIFY}' "
        f"AND release_date = '{row.release_date}' "
        f"AND pipeline_version = '{row.pipeline_version}' "
        f"AND processing_finished IS NULL;"
    )
