#!/usr/bin/env python3
################################################################
# This script is the main entrypoint for the execution of BigQuery
# stored procedures against on or more datasets in the ingestion workflow.

import logging
import sys

from google.api_core.exceptions import NotFound
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

# variation_identity export failures are recorded and the loop continues
# so later releases still get their stored procedures run. The job exits
# non-zero at the end if any occurred so Cloud Run marks it Failed.
vi_export_failures: list[str] = []

# Now process individual rows
for idx, row in enumerate(rows_to_ingest):
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
        # Resolve the current and baseline schemas the way the procedures
        # resolve them for themselves, via clinvar_ingest.schema_on. Every
        # procedure below (dataset_diff_on, variation_identity_incremental,
        # variation_vrs_changed) takes only a date and writes into the schema it
        # resolves, while we thread final_dataset_id separately - two sources of
        # truth for one dataset. schema_on is ORDER BY release_date DESC LIMIT
        # 1, so same-date duplicate datasets resolve arbitrarily.
        #
        # This runs BEFORE execute_all deliberately: once the procedures have
        # run there is nothing left to protect, and a mismatch here means the
        # procedures would write to a dataset we do not read. Raising in this
        # block is fatal by design - write_finished is never reached, so this
        # release's sp processing_history row stays unfinished and blocks all
        # later runs until an operator resolves it out of band.
        schema_row = next(
            iter(
                _get_bq_client()
                .query(
                    f"""
                    SELECT
                      (SELECT schema_name FROM `clinvar_ingest.schema_on`(DATE '{release_date}')) AS cur_schema,
                      (SELECT schema_name FROM `clinvar_ingest.schema_on`(
                         (SELECT prev_release_date FROM `clinvar_ingest.schema_on`(DATE '{release_date}')))
                      ) AS base_schema
                    """,
                    project=env.bq_dest_project,
                )
                .result()
            )
        )
        cur_schema = schema_row["cur_schema"]
        base_schema = schema_row["base_schema"]
        if cur_schema != dataset_id:
            raise ValueError(
                f"clinvar_ingest.schema_on(DATE '{release_date}') resolved schema "
                f"'{cur_schema}' but processing_history final_dataset_id is "
                f"'{dataset_id}'. The stored procedures resolve their target "
                f"schema themselves, so they would write to a dataset this "
                f"workflow does not read. Refusing to run them."
            )
        # A release cannot be its own baseline. Currently unreachable -
        # all_releases derives prev_release_date with LAG over DISTINCT dates,
        # so same-date duplicate datasets collapse to one row - but it UNION
        # ALLs the schema scan with historic_release_dates without deduping, so
        # a historic date coinciding with a live release would produce
        # prev_release_date == release_date. That would have dataset_diff_on
        # diff a schema against itself into empty-but-present diff_* tables,
        # which is exactly the state variation_identity_build's existence-only
        # guard reads as "incremental is safe": it would then rebuild
        # variation_identity from itself and leave the release never updated,
        # with a 0-row delta exported and every step reporting success.
        if base_schema == cur_schema:
            raise ValueError(
                f"clinvar_ingest.schema_on resolved the same schema "
                f"'{cur_schema}' as both the current and the baseline release "
                f"for {release_date}, so the incremental build would use this "
                f"release as its own baseline and silently never update it. "
                f"Check all_releases/historic_release_dates for a duplicate "
                f"release date. Refusing to run the stored procedures."
            )

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
        # This run claimed every eligible release via write_started up front,
        # so any release that did not reach write_finished is now stuck: the
        # failing release itself plus any later releases in this batch that
        # were pre-claimed but never attempted. All of them need their sp
        # processing_history rows cleared before the job will resume.
        stuck_releases = [vcv_xml_release_date.isoformat()] + [
            r["vcv_xml_release_date"].isoformat() for r in rows_to_ingest[idx + 1 :]
        ]
        batch_note = (
            f" This run claimed {len(rows_to_ingest)} releases as a batch "
            f"at startup ({', '.join(r['vcv_xml_release_date'].isoformat() for r in rows_to_ingest)}) "
            f"but did not complete all of them."
            if len(rows_to_ingest) > 1
            else ""
        )
        msg = (
            f"Stored procedure execution failed for release dated "
            f"vcv_xml_release_date={vcv_xml_release_date.isoformat()} "
            f"{vcv_pipeline_version=} release_tag={env.release_tag}.{batch_note} "
            f"The job will NOT retry automatically and is now paused until "
            f"this is resolved. The following release(s) have stuck sp "
            f"processing_history rows that must be deleted before the job "
            f"resumes (this includes the failing release and any later "
            f"releases in this batch that were pre-claimed but never "
            f"attempted): {', '.join(stuck_releases)}. Investigate the "
            f"failure, then run: "
            f"DELETE FROM `{processing_history_table}` "
            f"WHERE pipeline_version = '{env.release_tag}' "
            f"AND file_type = '{ClinVarIngestFileFormat.SP.value}' "
            f"AND processing_finished IS NULL;  "
            f"Error: {e}"
        )
        _logger.error(msg)
        send_slack_message(msg)
        raise e

    # Incremental variation_identity export, replicating clinvar-gkm's
    # src/scripts/export-vi-table-to-gcs.sh (its default, incremental mode) so
    # both producers of this object agree on its shape: export only the
    # variations whose variation_identity row changed since the prior release
    # (~0.3% of a weekly release) rather than the whole ~4.5M row snapshot.
    # vrs-python normalizes just the delta and the unchanged variations' VRS
    # results are carried forward when gkm_vrs is loaded.
    #
    # These calls live here rather than in the stored_procedures list on
    # purpose: they are part of the export deliverable, so a failure is handled
    # by the non-fatal export path below instead of pausing SP processing for
    # every later release.
    vi_gs_url = f"gs://{env.clinvar_gks_bucket}/{release_date}/dev/vi.jsonl.gz"
    try:
        client = _get_bq_client()
        # Fully qualify every table reference: client.query() resolves
        # unqualified names against BQ_DEST_PROJECT while get_table() and
        # extract_table() resolve against the client's own default project, so
        # an unqualified id would silently split across two projects if those
        # ever differ. For the same reason extract_table() is passed project=.
        qualified_dataset = f"{env.bq_dest_project}.{dataset_id}"
        vi_extract_table_id = f"{qualified_dataset}.vi_extract"

        # 1. Decide the export mode. variation_vrs_changed reports no status and
        # marks EVERY variation as changed when it finds no usable baseline, so
        # the only honest signal is whether the baseline schema resolved above
        # actually has a variation_identity table to diff against.
        has_baseline = base_schema is not None
        if has_baseline:
            try:
                client.get_table(f"{env.bq_dest_project}.{base_schema}.variation_identity")
            except NotFound:
                has_baseline = False
        export_mode = "incremental" if has_baseline else "FULL - no usable baseline"

        # 2. Compute the changed / removed variation sets vs the prior release.
        client.query(
            f"CALL `clinvar_ingest.variation_vrs_changed`(DATE '{release_date}');",
            project=env.bq_dest_project,
        ).result()

        # 3. Materialize only the changed variations' variation_identity rows.
        client.query(
            f"""
            CREATE OR REPLACE TABLE `{vi_extract_table_id}` AS
            SELECT vi.*
            FROM `{qualified_dataset}.variation_identity` vi
            JOIN `{qualified_dataset}.variation_vrs_changed` c USING(variation_id)
            """,
            project=env.bq_dest_project,
        ).result()

        # Count with COUNT(*) rather than Table.num_rows, which is Optional and
        # can be None before table metadata settles - that would skip the
        # zero-change alert below and print "None of None" in Slack.
        count_row = next(
            iter(
                client.query(
                    f"""
                    SELECT
                      (SELECT COUNT(*) FROM `{vi_extract_table_id}`) AS changed_count,
                      (SELECT COUNT(*) FROM `{qualified_dataset}.variation_identity`) AS total_count
                    """,
                    project=env.bq_dest_project,
                ).result()
            )
        )
        changed_count = count_row["changed_count"]
        total_count = count_row["total_count"]
        _logger.info(
            f"variation_identity export for {release_date}: {changed_count} of {total_count} "
            f"rows ({export_mode}, baseline schema={base_schema})"
        )

        if changed_count == 0:
            zero_msg = (
                f"NOTE: ZERO variations changed for release dated {release_date} "
                f"(0 of {total_count}). The export is still being written, since a "
                f"missing object would break clinvar-gkm's vrsify.sh outright, and "
                f"vrs-to-bq-table.sh's count check treats an empty changed set as "
                f"'clone the baseline unchanged'. Whether that script can actually "
                f"read an empty gzip export has NOT been verified - check "
                f"{vi_gs_url} before relying on gkm_vrs for {release_date}. This "
                f"normally means a re-ingest of a release identical to its "
                f"predecessor; investigate if that is not expected."
            )
            _logger.warning(zero_msg)
            send_slack_message(zero_msg)

        # 4. Extract the changed set as a single file. Deliberately not a
        # wildcard URI: clinvar-gkm's vrsify.sh reads exactly this one object.
        job_config = bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON, compression=bigquery.Compression.GZIP
        )
        extract_job = client.extract_table(
            vi_extract_table_id, vi_gs_url, job_config=job_config, project=env.bq_dest_project
        )
        extract_job.result(timeout=1800)  # Wait for the job to complete (30 minute timeout)
        msg = (
            f"Successfully exported {changed_count} of {total_count} variation_identity "
            f"rows ({export_mode}) to {vi_gs_url}"
        )
        _logger.info(msg)
        send_slack_message(msg)
    except Exception as e:
        # The tables this export writes (variation_vrs_changed,
        # variation_vrs_removed, vi_extract) all live in this release's own
        # dataset and are rebuilt with CREATE OR REPLACE, so a partial failure
        # is safe to retry and does not touch the cross-release temporal tables
        # that make a failed stored procedure dangerous. It is not inert either:
        # clinvar-gkm's vrs-to-bq-table.sh reads variation_vrs_changed and
        # variation_vrs_removed to decide how to build gkm_vrs from the previous
        # release's copy, so a stale set here can misdirect that step. Record
        # and continue so one bad export does not block SP execution for the
        # rest of the batch; we exit non-zero after the loop so Cloud Run still
        # marks the execution as Failed.
        vi_export_failures.append(vcv_xml_release_date.isoformat())
        error_msg = (
            f"variation_identity export failed for release dated "
            f"vcv_xml_release_date={vcv_xml_release_date.isoformat()} "
            f"{vcv_pipeline_version=} release_tag={env.release_tag}. "
            f"Stored procedures already completed for this release, so its "
            f"sp processing_history row is finished and does NOT need to be "
            f"deleted. The SP job is NOT paused and will continue processing "
            f"the remaining releases in this run. After investigating, re-run "
            f"the export manually from the clinvar-gkm repo with "
            f"./src/scripts/export-vi-table-to-gcs.sh {release_date}. That "
            f"script hardcodes its bucket and project, so it writes "
            f"gs://clinvar-gkm/{release_date}/dev/vi.jsonl.gz - the same object "
            f"as this export ({vi_gs_url}) only while CLINVAR_GKS_BUCKET="
            f"clinvar-gkm and BQ_DEST_PROJECT=clingen-dev; if either is "
            f"overridden for this deployment, copy the result to the URL above. "
            f"Do NOT use its --full flag as a "
            f"substitute: that writes sharded vi-*.jsonl.gz objects instead, "
            f"and clinvar-gkm's vrsify.sh reads only vi.jsonl.gz. "
            f"Error: {e}"
        )
        _logger.error(error_msg)
        send_slack_message(error_msg)

if vi_export_failures:
    sys.exit(
        f"variation_identity export failed for {len(vi_export_failures)} "
        f"release(s): {', '.join(vi_export_failures)}. See earlier Slack "
        f"messages for details."
    )
