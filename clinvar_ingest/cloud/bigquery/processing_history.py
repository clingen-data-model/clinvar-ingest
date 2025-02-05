import json
import logging
from pathlib import PurePath

import google.cloud.bigquery.table
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pydantic import BaseModel

from clinvar_ingest.api.model.requests import walk_and_replace
from clinvar_ingest.cloud.bigquery.create_tables import (
    ensure_dataset_exists,
    schema_file_path_for_table,
)
from clinvar_ingest.config import get_env
from clinvar_ingest.utils import ClinVarIngestFileFormat

_logger = logging.getLogger("clinvar_ingest")


def _dump_fn(val):
    if isinstance(val, PurePath):
        return str(val)
    if isinstance(val, BaseModel):
        return val.model_dump()
    if isinstance(val, bigquery.Table):
        return str(val)
    # if its a date-like
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def _internal_model_dump(obj):
    return walk_and_replace(obj, _dump_fn)


def create_processing_history_table(
    client: bigquery.Client,
    table_reference: bigquery.TableReference,
) -> bigquery.Table:
    """
    Similar to create_tables.create_table, but without the external file importing.
    `processing_history` is purely an internal table.
    """
    if table_reference.table_id != "processing_history":
        raise ValueError("Table name must be 'processing_history'")
    schema_path = schema_file_path_for_table("processing_history")
    schema = client.schema_from_json(schema_path)

    table = bigquery.Table(table_reference, schema=schema)
    return client.create_table(table)  # error if exists


def ensure_history_view_exists(
    processing_history_table: bigquery.Table,
    client: bigquery.Client | None = None,
):
    """
    Creates the view of the processing history table linking
    VCV, RCV processing events within a day of each other,
    along with subsequent BQ Ingest and Stored procedures
    processing steps (the latter two may not have run yet).

    Used downstream by the BQ Ingest and Stored procedures
    cloud scheduler invoked processing steps, to determine
    when to run and track start and end times of each step.
    """
    if client is None:
        client = bigquery.Client()
    # Get the project from the client
    project = client.project
    env = get_env()
    dataset_name = env.bq_meta_dataset  # The last part of <project>.<dataset_name>
    table_name = "processing_history_view"

    # saved in bigquery as a saved query in clingen-dev called "processing_history_view"
    # creates a view called clingen-dev.clinvar_ingest.processing_history_view
    query = f"""
    CREATE OR REPLACE VIEW `{project}.{dataset_name}.{table_name}` AS
    SELECT
        -- Use the release date from the VCV file as the final release date
        vcv.release_date as release_date,
        -- Use the final dataset id from the VCV file as the final dataset id
        vcv.final_dataset_id as final_dataset_id,
        -- VCV fields
        vcv.file_type as vcv_file_type,
        vcv.pipeline_version as vcv_pipeline_version,
        vcv.schema_version as vcv_schema_version,
        vcv.processing_started as vcv_processing_started,
        vcv.processing_finished as vcv_processing_finished,
        vcv.release_date as vcv_release_date,
        vcv.xml_release_date as vcv_xml_release_date,
        vcv.bucket_dir as vcv_bucket_dir,
        vcv.parsed_files as vcv_parsed_files,
        -- RCV fields
        rcv.file_type as rcv_file_type,
        rcv.pipeline_version as rcv_pipeline_version,
        rcv.schema_version as rcv_schema_version,
        rcv.processing_started as rcv_processing_started,
        rcv.processing_finished as rcv_processing_finished,
        rcv.release_date as rcv_release_date,
        rcv.xml_release_date as rcv_xml_release_date,
        rcv.bucket_dir as rcv_bucket_dir,
        rcv.parsed_files as rcv_parsed_files,
        -- BQ Ingest fields
        bq.file_type as bq_file_type,
        bq.release_date as bq_release_date,
        bq.pipeline_version as bq_pipeline_version,
        bq.schema_version as bq_schema_version,
        bq.processing_started as bq_processing_started,
        bq.processing_finished as bq_processing_finished,
        -- Stored procedure processing fields
        sp.file_type as sp_file_type,
        sp.release_date as sp_release_date,
        sp.pipeline_version as sp_pipeline_version,
        sp.schema_version as sp_schema_version,
        sp.processing_started as sp_processing_started,
        sp.processing_finished as sp_processing_finished,
        --
    FROM
    (SELECT * FROM `{processing_history_table}` WHERE file_type = "vcv") vcv
    INNER JOIN
    (SELECT * FROM `{processing_history_table}` WHERE file_type = "rcv") rcv
    ON (
        vcv.xml_release_date >= DATE_SUB(rcv.xml_release_date, INTERVAL 1 DAY)
        AND
        vcv.xml_release_date <= DATE_ADD(rcv.xml_release_date, INTERVAL 1 DAY)
    )
    AND vcv.pipeline_version = rcv.pipeline_version
    LEFT JOIN
    (SELECT * FROM `{processing_history_table}` WHERE file_type = "bq") bq
    ON (
        bq.release_date >= DATE_SUB(vcv.xml_release_date, INTERVAL 1 DAY)
        AND
        bq.release_date <= DATE_ADD(vcv.xml_release_date, INTERVAL 1 DAY)
        AND
        bq.release_date >= DATE_SUB(rcv.xml_release_date, INTERVAL 1 DAY)
        AND
        bq.release_date <= DATE_ADD(rcv.xml_release_date, INTERVAL 1 DAY)
    )
    LEFT JOIN
    (SELECT * FROM `{processing_history_table}` WHERE file_type = "sp") sp
    ON sp.release_date = bq.release_date
    """  # noqa: S608
    query_job = client.query(query)
    _ = query_job.result()
    return client.get_table(f"{project}.{dataset_name}.{table_name}")


def ensure_initialized(
    client: bigquery.Client | None = None,
    # storage_client: storage.Client | None = None,
) -> bigquery.Table:
    """
    Ensures that the bigquery clinvar-ingest metadata dataset and processing_history
    table is initialized. If not, initializes it.

    Ensures the dataset exists in the same region as the configured storage bucket, in the
    project configured through the dotenv or environment variables.

    Returns the processing_history `bigquery.Table` object.

    Example:
    client = bigquery.Client()
    storage_client = storage.Client()
    table = ensure_initialized(client=client, storage_client=storage_client)
    print(str(table))
    """
    env = get_env()
    dataset_name = env.bq_meta_dataset  # The last part of <project>.<dataset_name>

    if client is None:
        client = bigquery.Client()

    dataset = ensure_dataset_exists(
        client,
        project=env.bq_dest_project,
        dataset_id=dataset_name,
        location=env.location,
    )

    # Check to see if a table named "processing_history" exists in that dataset
    table_id = "processing_history"
    table_reference = bigquery.TableReference(dataset.reference, table_id)
    try:
        table = client.get_table(table_reference)
    except NotFound:
        table = create_processing_history_table(client, table_reference)
    return table


def check_started_exists(
    processing_history_table: bigquery.Table,
    release_date: str,
    release_tag: str,
    file_type: ClinVarIngestFileFormat,
    bucket_dir: str,
    client: bigquery.Client | None = None,
):
    sql = f"""
    SELECT COUNT(*) as c
    FROM {processing_history_table}
    WHERE file_type = @file_type
    AND pipeline_version = @pipeline_version
    AND xml_release_date = @xml_release_date
    AND bucket_dir = @bucket_dir;
    """  # noqa: S608
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_type", "STRING", file_type),
            bigquery.ScalarQueryParameter("pipeline_version", "STRING", release_tag),
            bigquery.ScalarQueryParameter("xml_release_date", "STRING", release_date),
            bigquery.ScalarQueryParameter("bucket_dir", "STRING", bucket_dir),
        ]
    )
    if client is None:
        client = bigquery.Client()

    query_job = client.query(sql, job_config=job_config)
    results = query_job.result()
    row = next(results)
    return row.c > 0
    # for row in results:
    #     return row.c > 0


def delete(
    processing_history_table: bigquery.Table,
    # release_date: str | None,
    release_tag: str,
    file_type: ClinVarIngestFileFormat,
    xml_release_date: str | None = None,
    client: bigquery.Client | None = None,
) -> int:
    """
    Deletes processing_history rows which match every parameter.
    """
    stmt = f"""
    DELETE FROM {processing_history_table}
    WHERE pipeline_version = @release_tag
    AND file_type = @file_type
    AND xml_release_date = @xml_release_date
    """  # noqa: S608
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # bigquery.ScalarQueryParameter("release_date", "STRING", release_date),
            bigquery.ScalarQueryParameter("release_tag", "STRING", release_tag),
            bigquery.ScalarQueryParameter("file_type", "STRING", file_type),
            bigquery.ScalarQueryParameter(
                "xml_release_date", "STRING", xml_release_date
            ),
        ]
    )
    if client is None:
        client = bigquery.Client()

    _logger.info(
        f"Deleting rows from processing_history: {stmt} (release_tag={release_tag}, file_type={file_type}, xml_release_date={xml_release_date})"
    )
    query_job = client.query(stmt, job_config=job_config)
    _ = query_job.result()
    deleted_count = query_job.dml_stats.deleted_row_count
    _logger.info(f"Deleted {deleted_count} rows from processing_history.")
    return deleted_count


def write_started(  # noqa: PLR0913
    processing_history_table: bigquery.Table,
    release_date: str | None,
    release_tag: str,
    schema_version: str,
    file_type: ClinVarIngestFileFormat,
    bucket_dir: str | None = None,  # TODO - Causes problems due to SQl Lookup?
    client: bigquery.Client | None = None,
    ftp_released: str | None = None,
    ftp_last_modified: str | None = None,
    xml_release_date: str | None = None,
    error_if_exists=True,
):
    """
    Writes the status of processing to the processing_history table.

    Example:
    write_vcv_started(
        processing_history_table=table,
        release_date="2024-07-23",
        release_tag="kf_dev",
        vcv_bucket_dir="clinvar_vcv_2024_07_23_kf_dev",
        client=client,
    )

    TODO might consider using the client.insert_rows method instead of a query
    since it returns the rows inserted.
    """
    if client is None:
        client = bigquery.Client()
    fully_qualified_table_id = str(processing_history_table)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("release_date", "STRING", release_date),
            bigquery.ScalarQueryParameter("release_tag", "STRING", release_tag),
            bigquery.ScalarQueryParameter("schema_version", "STRING", schema_version),
            bigquery.ScalarQueryParameter("file_type", "STRING", file_type),
            bigquery.ScalarQueryParameter("bucket_dir", "STRING", bucket_dir),
            bigquery.ScalarQueryParameter(
                "xml_release_date", "STRING", xml_release_date
            ),
            bigquery.ScalarQueryParameter("ftp_released", "STRING", ftp_released),
            bigquery.ScalarQueryParameter(
                "ftp_last_modified", "STRING", ftp_last_modified
            ),
            bigquery.ScalarQueryParameter("pipeline_version", "STRING", release_tag),
        ]
    )

    # Check to see if there is a matching existing row
    select_query = f"""
    SELECT COUNT(*) as c
    FROM `{fully_qualified_table_id}`
    WHERE file_type = @file_type
    AND pipeline_version = @release_tag
    AND xml_release_date = @release_date
    AND bucket_dir = @bucket_dir
    """  # TODO prepared statement  # noqa: S608
    _logger.info(
        f"Checking if matching row exists for job started event. "
        f"file_type={file_type}, release_date={release_date}, "
        f"release_tag={release_tag}, bucket_dir={bucket_dir}"
    )
    query_job = client.query(select_query, job_config=job_config)
    results = query_job.result()
    for row in results:
        if row.c != 0:
            if error_if_exists:
                raise RuntimeError(
                    f"Expected 0 row to exist for the finished event, but found {row.c}. "
                    f"file_type={file_type}, release_date={release_date}, "
                    f"release_tag={release_tag}, bucket_dir={bucket_dir}"
                )
            _logger.warning(
                f"Expected 0 rows to exist for the started event, but found {row.c}. "
                f"file_type={file_type}, release_date={release_date}, "
                f"release_tag={release_tag}, bucket_dir={bucket_dir}"
            )
            _logger.warning("Deleting existing row.")
            delete_query = f"""
                DELETE FROM {fully_qualified_table_id}
                WHERE file_type = @file_type
                AND pipeline_version = @release_tag
                AND xml_release_date = @release_date
                AND bucket_dir = @bucket_dir
                """  # noqa: S608
            query_job = client.query(delete_query, job_config=job_config)
            _ = query_job.result()
            _logger.info(f"Deleted {query_job.dml_stats.deleted_row_count} rows.")

    sql = f"""
    INSERT INTO {fully_qualified_table_id}
    (release_date, file_type, pipeline_version, schema_version, processing_started, xml_release_date, bucket_dir, ftp_released, ftp_last_modified)
    VALUES
    (@release_date, @file_type, @pipeline_version, @schema_version, CURRENT_TIMESTAMP(), @xml_release_date, @bucket_dir, @ftp_released, @ftp_last_modified);
    """

    # Run a synchronous query job and get the results
    query_job = client.query(sql, job_config=job_config)
    result = query_job.result()
    _logger.info(
        (
            "processing_history record written for job started event."
            "release_date=%s, file_type=%s"
        ),
        release_date,
        file_type,
    )
    return result, query_job


def write_finished(
    processing_history_table: bigquery.Table,
    release_date: str,
    release_tag: str,
    file_type: ClinVarIngestFileFormat,
    bucket_dir: str,
    parsed_files: dict | None = None,  # ParseResponse.parsed_files
    client: bigquery.Client | None = None,
):
    """
    Writes the status of the VCV processing to the processing_history table.

    TODO might consider using the client.insert_rows method instead of a query
    since it returns the rows inserted.
    """
    if client is None:
        client = bigquery.Client()
    fully_qualified_table_id = str(processing_history_table)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("release_date", "STRING", release_date),
            bigquery.ScalarQueryParameter("release_tag", "STRING", release_tag),
            bigquery.ScalarQueryParameter("file_type", "STRING", file_type),
            bigquery.ScalarQueryParameter("bucket_dir", "STRING", bucket_dir),
        ]
    )

    # Check to make sure there is exactly 1 started row before doing the update
    select_query = f"""
    SELECT COUNT(*) as c
    FROM {fully_qualified_table_id}
    WHERE file_type = @file_type
    AND pipeline_version = @release_tag
    AND xml_release_date = @release_date
    AND bucket_dir = @bucket_dir
    """  # noqa: S608
    _logger.info(
        f"Ensuring 1 started row exists before writing finished event. "
        f"file_type={file_type}, release_date={release_date}, "
        f"release_tag={release_tag}, bucket_dir={bucket_dir}"
    )
    query_job = client.query(select_query, job_config=job_config)
    results = query_job.result()
    for row in results:
        if row.c != 1:
            raise RuntimeError(
                f"Expected 1 row to exist for the finished event, but found {row.c}."
                f"file_type={file_type}, release_date={release_date}, "
                f"release_tag={release_tag}, bucket_dir={bucket_dir}"
            )

    # Do the update. Technically between the above select and this update, another
    # matching row could have been created, but this is unlikely.
    query = f"""
    UPDATE {fully_qualified_table_id}
    SET processing_finished = CURRENT_TIMESTAMP(),
        parsed_files = @parsed_files
    WHERE file_type = @file_type
    AND pipeline_version = @release_tag
    AND xml_release_date = @release_date
    AND bucket_dir = @bucket_dir
    """  # noqa: S608
    # print(f"Query: {query}")
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "parsed_files", "JSON", json.dumps(_internal_model_dump(parsed_files))
            ),
            bigquery.ScalarQueryParameter("release_date", "STRING", release_date),
            bigquery.ScalarQueryParameter("release_tag", "STRING", release_tag),
            bigquery.ScalarQueryParameter("file_type", "STRING", file_type),
            bigquery.ScalarQueryParameter("bucket_dir", "STRING", bucket_dir),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        result = query_job.result()

        if query_job.errors:
            # Print any errors if they occurred
            _logger.error("Errors occurred during the update operation:")
            for error in query_job.errors:
                _logger.error(error)
            raise RuntimeError(
                f"Error occurred during update operation: {query_job.errors}"
            )
        if (
            query_job.dml_stats.updated_row_count > 1
            or query_job.dml_stats.inserted_row_count > 1
        ):
            msg = (
                "More than one row was updated while updating processing_history "
                f"for the finished event: dml_stats={query_job.dml_stats}, "
                f"file_type={file_type}, release_date={release_date}, "
                f"release_tag={release_tag}, bucket_dir={bucket_dir}"
            )
            _logger.error(msg)
            raise RuntimeError(msg)
        if (
            query_job.dml_stats.updated_row_count == 0
            and query_job.dml_stats.inserted_row_count == 0
        ):
            msg = (
                "No rows were updated during the write_finished. "
                f"file_type={file_type}, release_date={release_date}, "
                f"release_tag={release_tag}, bucket_dir={bucket_dir}"
            )
            _logger.error(msg)
            raise RuntimeError(msg)
        _logger.info(
            (
                "processing_history record written for job finished event."
                "release_date=%s, file_type=%s"
            ),
            release_date,
            file_type,
        )
        return result, query_job

    except RuntimeError as e:
        _logger.error(f"Error occurred during update query:{query}\n{e}")
        raise e


def update_final_release_date(  # noqa: PLR0913
    processing_history_table: bigquery.Table,
    xml_release_date: str,
    release_tag: str,
    file_type: ClinVarIngestFileFormat,
    bucket_dir: str,
    final_release_date: str,
    final_dataset_id: str,
    client: bigquery.Client | None = None,
):
    """
    Updates the final release date and final dataset id field
    of a run in the processing_history table.
    """
    if client is None:
        client = bigquery.Client()

    fully_qualified_table_id = str(processing_history_table)
    query = f"""
    UPDATE {fully_qualified_table_id}
    SET release_date = '{final_release_date}',
        final_dataset_id = '{final_dataset_id}'
    WHERE file_type = '{file_type}'
    AND pipeline_version = '{release_tag}'
    AND xml_release_date = '{xml_release_date}'
    AND bucket_dir = '{bucket_dir}'
    """  # TODO prepared statement  # noqa: S608
    # job_config = bigquery.QueryJobConfig(
    #     query_parameters=[
    #         bigquery.ScalarQueryParameter("release_date", "STRING", final_release_date)
    #     ]
    # )
    _logger.info(f"query={query}")

    query_job = client.query(
        query,
        # job_config=job_config,
    )
    result = query_job.result()
    if query_job.errors:
        # Print any errors if they occurred
        _logger.error("Errors occurred during the update operation:")
        for error in query_job.errors:
            _logger.error(error)
        raise RuntimeError(
            f"Error occurred during update operation: {query_job.errors}"
        )
    if (
        query_job.dml_stats.updated_row_count > 1
        or query_job.dml_stats.inserted_row_count > 1
    ):
        msg = (
            "More than one row was updated while updating processing_history "
            f"for the final release date: dml_stats={query_job.dml_stats}, "
            f"file_type={file_type}, xml_release_date={xml_release_date}, "
            f"release_tag={release_tag}, bucket_dir={bucket_dir}"
        )
        _logger.error(msg)
        raise RuntimeError(msg)
    if (
        query_job.dml_stats.updated_row_count == 0
        and query_job.dml_stats.inserted_row_count == 0
    ):
        msg = (
            "No rows were updated during the update_final_release_date. "
            f"file_type={file_type}, xml_release_date={xml_release_date}, "
            f"release_tag={release_tag}, bucket_dir={bucket_dir}"
        )
        _logger.error(msg)
        raise RuntimeError(msg)
    _logger.info(
        (
            "processing_history record updated for final release date."
            "xml_release_date=%s, file_type=%s"
        ),
        xml_release_date,
        file_type,
    )
    return result, query_job


def read_processing_history_entries(
    processing_history_view_table: bigquery.Table,
    client: bigquery.Client | None = None,
) -> google.cloud.bigquery.table.RowIterator:
    """
    Reads the pairwise view of the processing history table linking
    VCV and RCV processing events within a day of each other.
    Used downstream during the ingest to bigquery step.
    """
    if client is None:
        client = bigquery.Client()
    query = f"""
    SELECT
        release_date
        vcv_file_type,
        vcv_pipeline_version,
        vcv_processing_started,
        vcv_processing_finished,
        vcv_xml_release_date,
        vcv_bucket_dir,
        vcv_parsed_files,
        rcv_file_type,
        rcv_pipeline_version,
        rcv_processing_started,
        rcv_processing_finished,
        rcv_xml_release_date,
        rcv_bucket_dir,
        rcv_parsed_files
    FROM {processing_history_view_table}
    """
    query_job = client.query(query)
    return query_job.result()


def processed_entries_ready_for_bq_ingest(
    processing_history_view_table: bigquery.Table,
    client: bigquery.Client | None = None,
) -> google.cloud.bigquery.table.RowIterator:
    """
    Reads the pairwise view of the processing history table linking
    VCV and RCV processing events within a day of each other, and
    returns those which have finished vcv/rcv ingest processing but have not
    yet run bq ingest against them.
    """
    if client is None:
        client = bigquery.Client()
    query = f"""
    SELECT
        release_date,
        vcv_file_type,
        vcv_pipeline_version,
        vcv_schema_version,
        vcv_processing_started,
        vcv_processing_finished,
        vcv_xml_release_date,
        vcv_bucket_dir,
        vcv_parsed_files,
        rcv_file_type,
        rcv_pipeline_version,
        rcv_schema_version,
        rcv_processing_started,
        rcv_processing_finished,
        rcv_xml_release_date,
        rcv_bucket_dir,
        rcv_parsed_files
    FROM {processing_history_view_table}
    WHERE vcv_processing_finished IS NOT NULL
    AND rcv_processing_finished IS NOT NULL
    AND bq_release_date IS NULL
    AND bq_processing_started IS NULL
    ORDER BY vcv_xml_release_date
    """
    query_job = client.query(query)
    return query_job.result()


def processed_entries_ready_for_sp_processing(
    processing_history_view_table: bigquery.Table,
    client: bigquery.Client | None = None,
) -> google.cloud.bigquery.table.RowIterator:
    """
    Reads the pairwise view of the processing history table linking
    VCV and RCV processing events within a day of each other, and
    returns those which have finished bq ingest processing but have not
    yet had stored procedures (sp) executed against them.
    """
    if client is None:
        client = bigquery.Client()
    query = f"""
    SELECT
        release_date,
        vcv_file_type,
        vcv_pipeline_version,
        vcv_schema_version,
        vcv_processing_started,
        vcv_processing_finished,
        vcv_xml_release_date,
        vcv_bucket_dir,
        vcv_parsed_files,
        rcv_file_type,
        rcv_pipeline_version,
        rcv_schema_version,
        rcv_processing_started,
        rcv_processing_finished,
        rcv_xml_release_date,
        rcv_bucket_dir,
        rcv_parsed_files,
        bq_file_type,
        bq_release_date,
        bq_pipeline_version,
        bq_schema_version,
        bq_processing_started,
        bq_processing_finished,
    FROM {processing_history_view_table}
    WHERE vcv_processing_finished IS NOT NULL
    AND rcv_processing_finished IS NOT NULL
    AND bq_processing_finished IS NOT NULL
    AND release_date IS NOT NULL
    AND sp_release_date IS NULL
    ORDER BY release_date
    """
    query_job = client.query(query)
    return query_job.result()


# def ingested_entries_ready_to_be_processed(
#     processing_history_view_table: bigquery.Table,
#     client: bigquery.Client | None = None,
# ) -> google.cloud.bigquery.table.RowIterator:
#     pass


# def update_bq_ingest_processing(
#         processing_history_table: bigquery.Table,
#         pipeline_version: str,
#         xml_release_date: str,
#         bq_ingest_processing: bool | None = True,
#         client: bigquery.Client | None = None,
# ):
#     if client is None:
#         client = bigquery.Client()
#     fully_qualified_table_id = str(processing_history_table)
#     query = f"""
#     UPDATE {fully_qualified_table_id}
#     SET bq_ingest_processing = {bq_ingest_processing}
#     WHERE file_type = '{ClinVarIngestFileFormat.VCV}'
#     AND pipeline_version = '{pipeline_version}'
#     AND xml_release_date = '{xml_release_date}'
#     """  # TODO prepared statement
#     query_job = client.query(query)
#     return query_job.result()

# TODO - Insert a BQ record type entry in this method above vs updating the flag

# def write_vcv_started(
#     processing_history_table: bigquery.Table,
#     release_date: str,
#     release_tag: str,
#     vcv_bucket_dir: str,
#     client: bigquery.Client | None = None,
# ):
#     return write_started(
#         processing_history_table=processing_history_table,
#         release_date=release_date,
#         release_tag=release_tag,
#         file_type=ClinVarIngestFileFormat("vcv"),
#         bucket_dir=vcv_bucket_dir,
#         client=client,
#     )


# def write_vcv_finished(
#     processing_history_table: bigquery.Table,
#     release_date: str,
#     release_tag: str,
#     vcv_bucket_dir: str,
#     client: bigquery.Client | None = None,
# ):
#     return write_finished(
#         processing_history_table=processing_history_table,
#         release_date=release_date,
#         release_tag=release_tag,
#         file_type=ClinVarIngestFileFormat("vcv"),
#         bucket_dir=vcv_bucket_dir,
#         client=client,
#     )


# def write_rcv_started(
#     processing_history_table: bigquery.Table,
#     release_date: str,
#     release_tag: str,
#     bucket_dir: str,
#     client: bigquery.Client | None = None,
# ):
#     return write_started(
#         processing_history_table=processing_history_table,
#         release_date=release_date,
#         release_tag=release_tag,
#         file_type=ClinVarIngestFileFormat("rcv"),
#         bucket_dir=bucket_dir,
#         client=client,
#     )


# def write_rcv_finished(
#     processing_history_table: bigquery.Table,
#     release_date: str,
#     release_tag: str,
#     bucket_dir: str,
#     client: bigquery.Client | None = None,
# ):
#     return write_finished(
#         processing_history_table=processing_history_table,
#         release_date=release_date,
#         release_tag=release_tag,
#         file_type=ClinVarIngestFileFormat("rcv"),
#         bucket_dir=bucket_dir,
#         client=client,
#     )
