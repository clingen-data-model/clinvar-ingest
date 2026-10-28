"""
The vrsify dispatcher, run on a schedule (misc/bin/vrsify-workflow.py). It claims releases
whose stored procedures have finished, and takes each one through to the end in the same
execution:

1. claim the release: write its vrsify started row;
2. wait for its vi.jsonl.gz export to have been written after its stored procedures
   finished. The stored-procedures job marks its row finished before it exports, and an
   export from an earlier run of the release may still be in place;
3. submit a Batch job that runs `clinvar-ingest vrsify --release-date ...`, and wait for it;
4. check the job's provenance and write the finished row.

A failure after the claim is reported once, with the statement that deletes the started
row, and the row is left in place: later executions only claim ready releases, so the
release waits for a person, as a failed stored-procedures release does. A failure before
any claim is only logged and fails the execution, and the next execution retries.

The Batch job reports its own start, output and errors; the dispatcher reports a release
scheduled, completed, or failed.
"""

import datetime
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace

from google.cloud import bigquery

from clinvar_ingest.cloud.gcs import parse_blob_uri
from clinvar_ingest.config import VrsifyEnv
from clinvar_ingest.vrsify import batch, history
from clinvar_ingest.vrsify.runner import default_uris, provenance_uri

_logger = logging.getLogger("clinvar_ingest")

# Environment variables the Batch job's vrsify run reads, passed through from the dispatcher
TASK_ENVIRONMENT_KEYS = ("CLINVAR_INGEST_REFGET_STORE",)


@dataclass
class DispatchSettings:
    min_release_date: datetime.date
    max_releases: int = 1
    export_wait_seconds: int = 3600
    export_poll_seconds: int = 60
    job_poll_seconds: int = 30
    slack: bool = False
    dry_run: bool = False


def written_at(uri: str) -> datetime.datetime | None:
    """When the gs:// object `uri` was last written, or None if it does not exist."""
    blob = parse_blob_uri(uri)
    if not blob.exists():
        return None
    blob.reload()
    return blob.updated


def read_provenance(uri: str) -> dict | None:
    blob = parse_blob_uri(uri)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


@dataclass
class Cloud:
    """The Batch and GCS calls the dispatcher makes, and its sleep, replaceable in tests."""

    submit: Callable[[batch.BatchSettings, str, dict], str] = batch.submit
    job_state: Callable[[str], str | None] = batch.job_state
    written_at: Callable[[str], datetime.datetime | None] = written_at
    read_provenance: Callable[[str], dict | None] = read_provenance
    sleep: Callable[[float], None] = time.sleep


class ReleaseFailedError(RuntimeError):
    """A claimed release that cannot be completed; the message says why."""


def task_environment(env: VrsifyEnv, extra: dict[str, str]) -> dict[str, str]:
    """Settings the Batch job needs: its bucket, and the Slack channel of the environment."""
    variables = {
        "BQ_DEST_PROJECT": env.bq_dest_project,
        "CLINVAR_INGEST_RELEASE_TAG": env.release_tag,
        "CLINVAR_INGEST_SLACK_CHANNEL": env.slack_channel,
        "CLINVAR_GKS_BUCKET": env.clinvar_gks_bucket,
        "file_format": env.file_format_mode,
    }
    return variables | {k: v for k, v in extra.items() if v}


@dataclass
class Dispatcher:
    env: VrsifyEnv
    settings: DispatchSettings
    batch_settings: batch.BatchSettings
    table: bigquery.Table
    client: bigquery.Client
    notify: Callable[[str], None]
    cloud: Cloud = field(default_factory=Cloud)

    def run(self) -> None:
        """Claim ready releases, at most `max_releases`, and take each through to the end."""
        ready = history.ready_releases(self.table, self.settings.min_release_date, self.client)
        _logger.info(f"{len(ready)} releases ready for vrsify: {[str(r.release_date) for r in ready]}")
        for release in ready[: self.settings.max_releases]:
            if self.settings.dry_run:
                _logger.info(f"Dry run: would claim {release}")
                continue
            row = history.claim(self.table, release, self.env.release_tag, self.env.schema_version, self.client)
            try:
                self.complete(row)
            except Exception as e:
                reason = str(e) if isinstance(e, ReleaseFailedError) else f"{type(e).__name__}: {e}"
                self._report_failure(row, reason)
                raise

    def complete(self, row: history.VrsifyRow) -> None:
        """Run the claimed release's Batch job and finish its row; raises if it cannot."""
        release_date = str(row.release_date)
        input_uri, output_uri = default_uris(release_date)
        self._wait_for_export(input_uri, row.sp_processing_finished)

        name = batch.job_id(release_date)
        args = ["--release-date", release_date] + (["--slack"] if self.settings.slack else [])
        definition = batch.job_definition(self.batch_settings, args, slack=self.settings.slack)
        job = self.cloud.submit(self.batch_settings, name, definition)
        self.notify(f"vrsify scheduled for release dated {release_date}: Batch job {name}")

        state = self._wait_for_job(job)
        if state is None:
            raise ReleaseFailedError(f"its Batch job {job} no longer exists")
        if state != "SUCCEEDED":
            raise ReleaseFailedError(f"its Batch job {job} ended {state}; the job's own message and log have the error")

        provenance = self._check_provenance(job, input_uri, output_uri)
        history.finish(self.table, replace(row, parsed_files={"batch_job": job}), self.client)
        self.notify(
            f"vrsify completed for release dated {release_date}: {provenance.get('records')} records -> {output_uri}"
        )

    def _wait_for_export(self, input_uri: str, newer_than: datetime.datetime) -> None:
        polls = self.settings.export_wait_seconds // self.settings.export_poll_seconds
        for poll in range(polls + 1):
            exported = self.cloud.written_at(input_uri)
            if exported is not None and exported > newer_than:
                _logger.info(f"{input_uri} was written at {exported.isoformat()}")
                return
            if poll < polls:
                _logger.info(f"Waiting for {input_uri} written after {newer_than.isoformat()}")
                self.cloud.sleep(self.settings.export_poll_seconds)
        raise ReleaseFailedError(
            f"no {input_uri} was written after its stored procedures finished at {newer_than.isoformat()} "
            f"within {self.settings.export_wait_seconds // 60} minutes"
        )

    def _wait_for_job(self, job: str) -> str | None:
        """Wait until Batch job `job` is no longer queued or running; its final state, or None if it is gone."""
        while (state := self.cloud.job_state(job)) in batch.ACTIVE_STATES:
            _logger.info(f"Batch job {job} is {state}")
            self.cloud.sleep(self.settings.job_poll_seconds)
        return state

    def _check_provenance(self, job: str, input_uri: str, output_uri: str) -> dict:
        prov_uri = provenance_uri(output_uri)
        provenance = self.cloud.read_provenance(prov_uri)
        if provenance is None:
            problem = f"{prov_uri} does not exist"
        elif provenance.get("host", {}).get("batch", {}).get("BATCH_JOB_ID") != job.rsplit("/", 1)[-1]:
            problem = f"{prov_uri} was not written by it"
        elif [i.get("uri") for i in provenance.get("inputs", [])] != [input_uri]:
            problem = f"{prov_uri} names inputs other than {input_uri}"
        else:
            return provenance
        raise ReleaseFailedError(f"its Batch job {job} succeeded, but {problem}")

    def _report_failure(self, row: history.VrsifyRow, reason: str) -> None:
        _, output_uri = default_uris(str(row.release_date))
        leftover = ""
        try:
            if self.cloud.written_at(output_uri) is not None:
                leftover = (
                    f" Also move or delete {output_uri} and {provenance_uri(output_uri)}, "
                    "since vrsify does not replace existing output."
                )
        except Exception:
            _logger.exception(f"Could not check for {output_uri}")
        message = (
            f"vrsify failed for release dated {row.release_date} (pipeline_version={row.pipeline_version}): "
            f"{reason}. It will NOT be retried automatically: the release's vrsify processing_history row "
            f"stays started, which keeps it from being claimed again. Resolve the cause, then delete the "
            f"row so that the next run claims the release again: {history.delete_sql(str(self.table), row)}"
            f"{leftover}"
        )
        _logger.error(message)
        self.notify(message)
