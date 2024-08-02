import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from clinvar_ingest.cloud.bigquery.create_tables import (
    ensure_dataset_exists,
    schema_file_path_for_table,
)
from clinvar_ingest.config import Env, get_env
from clinvar_ingest.utils import ClinVarIngestFileFormat

_logger = logging.getLogger("clinvar_ingest")


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


def ensure_pairs_view_exists(
    processing_history_table: bigquery.Table,
    client: bigquery.Client | None = None,
):
    """
    Creates the pairwise view of the processing history table linking
    VCV and RCV processing events within a day of each other.
    Used downstream during the ingest to bigquery step.
    """
    if client is None:
        client = bigquery.Client()
    # Get the project from the client
    project = client.project
    env: Env = get_env()
    dataset_name = env.bq_meta_dataset  # The last part of <project>.<dataset_name>
    table_name = "processing_history_pairs"

    # saved in bigquery as a saved query in clingen-dev called "processing_history_pairs"
    # creates a view called clingen-dev.clinvar_ingest.processing_history_pairs
    query = f"""
    CREATE OR REPLACE VIEW `{project}.{dataset_name}.{table_name}` AS
    SELECT
        -- Use the release date from the VCV file as the final release date
        vcv.xml_release_date as release_date,
        -- VCV fields
        vcv.file_type as vcv_file_type,
        vcv.pipeline_version as vcv_pipeline_version,
        vcv.processing_started as vcv_processing_started,
        vcv.processing_finished as vcv_processing_finished,
        vcv.xml_release_date as vcv_xml_release_date,
        vcv.bucket_dir as vcv_bucket_dir,
        -- RCV fields
        rcv.file_type as rcv_file_type,
        rcv.pipeline_version as rcv_pipeline_version,
        rcv.processing_started as rcv_processing_started,
        rcv.processing_finished as rcv_processing_finished,
        rcv.xml_release_date as rcv_xml_release_date,
        rcv.bucket_dir as rcv_bucket_dir,
    FROM
    (SELECT * FROM `{processing_history_table}` WHERE file_type = "vcv") vcv
    INNER JOIN
    (SELECT * FROM `{processing_history_table}` WHERE file_type = "rcv") rcv
    ON (
        vcv.xml_release_date >= DATE_SUB(rcv.xml_release_date, INTERVAL 1 DAY)
        AND
        vcv.xml_release_date <= DATE_ADD(rcv.xml_release_date, INTERVAL 1 DAY)
    )
    """
    query_job = client.query(query)
    _ = query_job.result()
    return client.get_table(f"{project}.{dataset_name}.{table_name}")


def ensure_initialized(
    client: bigquery.Client | None = None, storage_client: storage.Client | None = None
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
    env: Env = get_env()
    dataset_name = env.bq_meta_dataset  # The last part of <project>.<dataset_name>

    if client is None:
        client = bigquery.Client()

    if storage_client is None:
        storage_client = storage.Client()

    # Look up the bucket from the env and get its location
    # Throws error if bucket not found
    bucket = storage_client.get_bucket(env.bucket_name)
    bucket_location = bucket.location

    dataset = ensure_dataset_exists(
        client,
        project=env.bq_dest_project,
        dataset_id=dataset_name,
        location=bucket_location,
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
    """
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
    for row in results:
        return row.c > 0


def write_started(
    processing_history_table: bigquery.Table,
    release_date: str,
    release_tag: str,
    file_type: ClinVarIngestFileFormat,
    bucket_dir: str,
    client: bigquery.Client | None = None,
    error_if_exists=True,
):
    """
    Writes the status of the VCV processing to the processing_history table.

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

    # Check to see if there is a matching existing row
    select_query = f"""
    SELECT COUNT(*) as c
    FROM {fully_qualified_table_id}
    WHERE file_type = '{file_type}'
    AND pipeline_version = '{release_tag}'
    AND xml_release_date = '{release_date}'
    AND bucket_dir = '{bucket_dir}'
    """  # TODO prepared statement
    _logger.info(
        f"Checking if matching row exists for job started event. "
        f"file_type={file_type}, release_date={release_date}, "
        f"release_tag={release_tag}, bucket_dir={bucket_dir}"
    )
    query_job = client.query(select_query)
    results = query_job.result()
    for row in results:
        if row.c != 0:
            if error_if_exists:
                raise RuntimeError(
                    f"Expected 1 row to exist for the finished event, but found {row.c}. "
                    f"file_type={file_type}, release_date={release_date}, "
                    f"release_tag={release_tag}, bucket_dir={bucket_dir}"
                )
            else:
                _logger.warning(
                    f"Expected 0 rows to exist for the started event, but found {row.c}."
                    f"file_type={file_type}, release_date={release_date}, "
                    f"release_tag={release_tag}, bucket_dir={bucket_dir}"
                )
                _logger.warning("Deleting existing row.")
                delete_query = f"""
                DELETE FROM {fully_qualified_table_id}
                WHERE file_type = '{file_type}'
                AND pipeline_version = '{release_tag}'
                AND xml_release_date = '{release_date}'
                AND bucket_dir = '{bucket_dir}'
                """
                query_job = client.query(delete_query)
                _ = query_job.result()
                _logger.info(f"Deleted {query_job.dml_stats.deleted_row_count} rows.")

    sql = f"""
    INSERT INTO {fully_qualified_table_id}
    (release_date, file_type, pipeline_version, processing_started, xml_release_date, bucket_dir)
    VALUES
    (NULL, @file_type, @pipeline_version, CURRENT_TIMESTAMP(), @xml_release_date, @bucket_dir);
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # Omitting release_date until VCV and RCV are merged
            # bigquery.ScalarQueryParameter("release_date", "STRING", None),
            bigquery.ScalarQueryParameter("file_type", "STRING", file_type),
            bigquery.ScalarQueryParameter("pipeline_version", "STRING", release_tag),
            bigquery.ScalarQueryParameter("xml_release_date", "STRING", release_date),
            bigquery.ScalarQueryParameter("bucket_dir", "STRING", bucket_dir),
        ]
    )

    # Run a synchronous query job and get the results
    query_job = client.query(sql, job_config=job_config)
    _ = query_job.result()
    _logger.info(
        (
            "processing_history record written for job started event."
            "release_date=%s, file_type=%s"
        ),
        release_date,
        file_type,
    )


def write_finished(
    processing_history_table: bigquery.Table,
    release_date: str,
    release_tag: str,
    file_type: ClinVarIngestFileFormat,
    bucket_dir: str,
    client: bigquery.Client | None = None,
):
    """
    Writes the status of the VCV processing to the processing_history table.

    Example:
    write_finished(
        processing_history_table=table,
        release_date="2024-07-23",
        release_tag="local_dev",
        file_type=ClinVarIngestFileFormat("vcv"),
        vcv_bucket_dir="clinvar_vcv_2024_07_23_local_dev",
        client=client,
    )

    TODO might consider using the client.insert_rows method instead of a query
    since it returns the rows inserted.
    """
    if client is None:
        client = bigquery.Client()
    fully_qualified_table_id = str(processing_history_table)

    # Check to make sure there is exactly 1 started row before doing the update
    select_query = f"""
    SELECT COUNT(*) as c
    FROM {fully_qualified_table_id}
    WHERE file_type = '{file_type}'
    AND pipeline_version = '{release_tag}'
    AND xml_release_date = '{release_date}'
    AND bucket_dir = '{bucket_dir}'
    """
    _logger.info(
        f"Ensuring 1 started row exists before writing finished event. "
        f"file_type={file_type}, release_date={release_date}, "
        f"release_tag={release_tag}, bucket_dir={bucket_dir}"
    )
    query_job = client.query(select_query)
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
    SET processing_finished = CURRENT_TIMESTAMP()
    WHERE file_type = '{file_type}'
    AND pipeline_version = '{release_tag}'
    AND xml_release_date = '{release_date}'
    AND bucket_dir = '{bucket_dir}'
    """
    # print(f"Query: {query}")

    try:
        query_job = client.query(query)
        result = query_job.result()

        if query_job.errors:
            # Print any errors if they occurred
            _logger.error("Errors occurred during the update operation:")
            for error in query_job.errors:
                _logger.error(error)
            raise RuntimeError(
                f"Error occurred during update operation: {query_job.errors}"
            )
        elif (
            query_job.dml_stats.updated_row_count > 1  # type: ignore
            or query_job.dml_stats.inserted_row_count > 1  # type: ignore
        ):
            msg = (
                "More than one row was updated while updating processing_history "
                f"for the finished event: dml_stats={query_job.dml_stats}, "
                f"file_type={file_type}, release_date={release_date}, "
                f"release_tag={release_tag}, bucket_dir={bucket_dir}"
            )
            _logger.error(msg)
            raise RuntimeError(msg)
        elif (
            query_job.dml_stats.updated_row_count == 0  # type: ignore
            and query_job.dml_stats.inserted_row_count == 0  # type: ignore
        ):
            msg = (
                "No rows were updated during the write_finished. "
                f"file_type={file_type}, release_date={release_date}, "
                f"release_tag={release_tag}, bucket_dir={bucket_dir}"
            )
            _logger.error(msg)
            raise RuntimeError(msg)
        else:
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
