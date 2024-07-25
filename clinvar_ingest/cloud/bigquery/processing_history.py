import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from clinvar_ingest.cloud.bigquery.create_tables import (
    ensure_dataset_exists,
    schema_file_path_for_table,
)
from clinvar_ingest.config import Env, get_env

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


def ensure_initialized(
    client: bigquery.Client | None = None, storage_client: storage.Client | None = None
) -> bigquery.Table:
    """
    Ensures that the bigquery clinvar-ingest metadata dataset and processing_history
    table is initialized. If not, initializes it.

    Ensures the dataset exists in the same region as the configured storage bucket, in the
    project configured through the dotenv or environment variables.

    Returns the processing_history `bigquery.Table` object.
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


def write_vcv_started(
    processing_history_table: bigquery.Table,
    release_date: str,
    release_tag: str,
    vcv_bucket_dir: str,
    client: bigquery.Client | None = None,
):
    """
    Writes the status of the VCV processing to the processing_history table.

    -- ADD VCV JOB LOG
    INSERT INTO `clingen-dev.clinvar_ingest.processing_history`
    (release_date, vcv_pipeline_version, vcv_processing_started, vcv_release_date, vcv_bucket_dir)
    VALUES
    (NULL, "kf_dev_tag", CURRENT_TIMESTAMP(), "2024-07-23", "clinvar_vcv_2024_07_23_kf_dev_tag");

    TODO might consider using the client.insert_rows method instead of a query
    since it returns the rows inserted.
    """
    fully_qualified_table_id = str(processing_history_table)
    sql = f"""
    -- ADD VCV JOB LOG
    INSERT INTO {fully_qualified_table_id}
    (release_date, vcv_pipeline_version, vcv_processing_started, vcv_release_date, vcv_bucket_dir)
    VALUES
    (@release_date, @vcv_pipeline_version, CURRENT_TIMESTAMP(), @vcv_release_date, @vcv_bucket_dir);
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # Omitting release_date until VCV and RCV are merged
            bigquery.ScalarQueryParameter("release_date", "STRING", None),
            bigquery.ScalarQueryParameter(
                "vcv_pipeline_version", "STRING", release_tag
            ),
            bigquery.ScalarQueryParameter("vcv_release_date", "STRING", release_date),
            bigquery.ScalarQueryParameter("vcv_bucket_dir", "STRING", vcv_bucket_dir),
        ]
    )

    if client is None:
        client = bigquery.Client()

    # Run a synchronous query job and get the results
    query_job = client.query(sql, job_config=job_config)
    _ = query_job.result()
    _logger.info(
        "processing_history record written for VCV started event release_date=%s",
        release_date,
    )


def write_vcv_finished(
    processing_history_table: bigquery.Table,
    release_date: str,
    release_tag: str,
    client: bigquery.Client | None = None,
):
    """
    UPDATE `clingen-dev.clinvar_ingest.processing_history`
    SET vcv_processing_finished = CURRENT_TIMESTAMP()
    WHERE vcv_pipeline_version = "kf_dev_tag"
    AND vcv_release_date = "2024-07-23";
    """
    sql = f"""
    UPDATE {processing_history_table}
    SET vcv_processing_finished = CURRENT_TIMESTAMP()
    WHERE vcv_pipeline_version = @vcv_pipeline_version
    AND vcv_release_date = @vcv_release_date;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "vcv_pipeline_version", "STRING", release_tag
            ),
            bigquery.ScalarQueryParameter("vcv_release_date", "STRING", release_date),
        ]
    )

    if client is None:
        client = bigquery.Client()

    # Run a synchronous query job and get the results
    query_job = client.query(sql, job_config=job_config)
    _ = query_job.result()


"""
-- SELECT * FROM `clingen-dev.clinvar_ingest.processing_history`
-- Updating table at begininng of RCV ingest for
-- release date 2024-07-23
-- release_tag kf_dev_tag
-- bucket dir clinvar_rcv_2024_07_23_kf_dev_tag

DELETE FROM `clingen-dev.clinvar_ingest.processing_history` WHERE 1=1;

-- ADD RCV JOB LOG
INSERT INTO `clingen-dev.clinvar_ingest.processing_history`
  (release_date, rcv_pipeline_version, rcv_processing_started, rcv_release_date, rcv_bucket_dir)
VALUES
  (NULL, "kf_dev_tag", CURRENT_TIMESTAMP(), "2024-07-23", "clinvar_rcv_2024_07_23_kf_dev_tag");

-- RCV FINISHED LOG
UPDATE `clingen-dev.clinvar_ingest.processing_history`
SET rcv_processing_finished = CURRENT_TIMESTAMP()
WHERE rcv_pipeline_version = "kf_dev_tag"
AND rcv_release_date = "2024-07-23";

-- ADD VCV JOB LOG
INSERT INTO `clingen-dev.clinvar_ingest.processing_history`
  (release_date, vcv_pipeline_version, vcv_processing_started, vcv_release_date, vcv_bucket_dir)
VALUES
  (NULL, "kf_dev_tag", CURRENT_TIMESTAMP(), "2024-07-23", "clinvar_vcv_2024_07_23_kf_dev_tag");

-- VCV FINISHED LOG
UPDATE `clingen-dev.clinvar_ingest.processing_history`
SET vcv_processing_finished = CURRENT_TIMESTAMP()
WHERE vcv_pipeline_version = "kf_dev_tag"
AND vcv_release_date = "2024-07-23";


-- BQ-INGEST STEP: UNMATCHED VCV RUNS
SELECT * FROM `clingen-dev.clinvar_ingest.processing_history` a
WHERE vcv_processing_finished is not NULL
AND rcv_processing_finished is NULL
-- Merged record hasn't been written yet
AND NOT EXISTS (
  SELECT * FROM `clingen-dev.clinvar_ingest.processing_history` b
  WHERE a.vcv_release_date = b.vcv_release_date
  AND a.vcv_pipeline_version = b.vcv_pipeline_version
  AND rcv_processing_finished IS NOT NULL
);


-- BQ-INGEST STEP: UNMATCHED RCV RUNS
SELECT * FROM `clingen-dev.clinvar_ingest.processing_history` a
WHERE rcv_processing_finished is not NULL
AND vcv_processing_finished is NULL
-- Merged record hasn't been written yet
AND NOT EXISTS (
  SELECT * FROM `clingen-dev.clinvar_ingest.processing_history` b
  WHERE a.vcv_release_date = b.vcv_release_date
  AND a.vcv_pipeline_version = b.vcv_pipeline_version
  AND rcv_processing_finished IS NOT NULL
);
"""
