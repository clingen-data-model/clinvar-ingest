"""
Functions for setting up tables in bigquery.
Tables are defined as external, to load NDJSON/JSONL
files from Google Cloud Storage.

Usable as a script or programmatic module.
"""

import logging
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery.dataset import Dataset, DatasetReference

from clinvar_ingest.api.model.requests import (
    CreateExternalTablesRequest,
    CreateInternalTablesRequest,
)
from clinvar_ingest.cloud.gcs import parse_blob_uri
from clinvar_ingest.config import get_env

_logger = logging.getLogger("clinvar_ingest")

file_dir = Path(__file__).parent.resolve()
bq_schemas_dir = file_dir / "bq_json_schemas"


def ensure_dataset_exists(
    client: bigquery.Client, project: str, dataset_id: str, location: str
) -> bigquery.Dataset:
    """
    Check if dataset exists. If not, create it. Returns the Dataset object.
    """
    dataset_ref = DatasetReference(project=project, dataset_id=dataset_id)
    ds_inp = Dataset(dataset_ref=dataset_ref)
    ds_inp.location = location
    try:
        dataset = client.get_dataset(dataset_ref=dataset_ref)
    except NotFound as _:
        dataset = client.create_dataset(dataset=ds_inp)
    except Exception as e:
        _logger.error("Other exception received: %s", str(e))
        raise e
    return dataset


def schema_file_path_for_table(table_name: str) -> str:
    """
    Returns the path to the BigQuery schema file for the given table name.
    """
    schema_path = bq_schemas_dir / f"{table_name}.bq.json"
    return schema_path


def create_table(
    table_name: str,
    dataset: bigquery.Dataset,
    blob_uri: str,
    client: bigquery.Client,
) -> bigquery.Table:
    """
    Creates a table in the given dataset, using the given bucket and path.
    """
    table_ref = dataset.table(table_name)
    external_config = bigquery.ExternalConfig(
        bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    external_config.source_uris = [blob_uri]
    external_config.autodetect = True
    external_config.schema = client.schema_from_json(
        schema_file_path_for_table(table_name)
    )

    table = bigquery.Table(table_ref, schema=None)
    table.external_data_configuration = external_config
    table = client.create_table(table, exists_ok=True)
    return table


def run_create_external_tables(
    args: CreateExternalTablesRequest,
) -> dict[str, bigquery.Table]:
    """
    Creates tables using args parsed by `cli.parse_args`
    """
    bq_client = bigquery.Client()
    gcs_client = storage.Client()
    env = get_env()

    destination_project = env.bq_dest_project
    if env.bq_dest_project is None:
        if bq_client.project is None:
            raise ValueError(
                "gcloud client project is None and --project arg not provided"
            )
        destination_project = bq_client.project
        _logger.info("Using default project from gcloud environment: %s", args.project)

    source_buckets = set()
    for table_name, gcs_blob_path in args.source_table_paths.items():
        parsed_blob = parse_blob_uri(gcs_blob_path.root, gcs_client)
        _logger.info(
            "Parsed blob bucket: %s, path: %s", parsed_blob.bucket, parsed_blob.name
        )
        bucket_obj = gcs_client.get_bucket(parsed_blob.bucket.name)
        bucket_location = bucket_obj.location
        source_buckets.add(parsed_blob.bucket.name)

    # sanity check that all source paths are in the same bucket
    if len(source_buckets) > 1:
        raise ValueError(
            f"All source paths must be in the same bucket. Got: [{source_buckets}]. "
            f"source_table_paths: {args.source_table_paths}"
        )

    # create dataset if not exists
    dataset_obj = ensure_dataset_exists(
        bq_client,
        project=destination_project,
        dataset_id=args.destination_dataset,
        location=bucket_location,
    )
    if not dataset_obj:
        raise RuntimeError(f"Didn't get a dataset object back. run_create args: {args}")

    outputs = {}
    # TODO maybe do something more clever if it fails part way through
    # but maybe not, it could be useful to see the partial results
    for table_name, gcs_blob_path in args.source_table_paths.items():
        table = create_table(
            table_name,
            dataset=dataset_obj,
            blob_uri=gcs_blob_path.root,
            client=bq_client,
        )
        outputs[table_name] = table
    return outputs


def create_internal_tables(
    args: CreateInternalTablesRequest,
) -> CreateInternalTablesRequest:
    """
    -- clingen-dev.2023_10_07_2024_02_19T172113657352.variation_archive
    CREATE TABLE `clingen-dev.2023_10_07_2024_02_19T172113657352.variation_archive_internal`
    AS SELECT * from `clingen-dev.2023_10_07_2024_02_19T172113657352.variation_archive`

    """
    env = get_env()
    table_map = args.source_dest_table_map
    # Initial validation
    for source_table, dest_table in table_map.items():
        dest_table = bigquery.TableReference.from_string(dest_table)
        if dest_table.project != env.bq_dest_project:
            raise ValueError(
                f"Cross-project table copies not currently supported. "
                f"Destination table project must be {env.bq_dest_project}. Got: {dest_table.project}"
            )

    def ctas_copy(
        source_table_ref: bigquery.TableReference,
        dest_table_ref: bigquery.TableReference,
        bq_client: bigquery.Client,
    ) -> bigquery.QueryJob:
        query = (
            f"CREATE TABLE `{dest_table_ref}` " f"AS SELECT * from `{source_table_ref}`"
        )
        _logger.info(
            f"Creating table {dest_table_ref} as select * from {source_table_ref}"
        )
        _logger.info(f"Query:\n{query}")
        query_job = bq_client.query(query)
        return query_job

    bq_client = bigquery.Client()
    # Copy each
    for source_table, dest_table in table_map.items():
        source_table_ref = bigquery.TableReference.from_string(source_table)
        dest_table_ref = bigquery.TableReference.from_string(dest_table)

        # Verify source table exists
        try:
            _ = bq_client.get_table(source_table_ref)
        except NotFound as e:
            raise ValueError(f"Source table {source_table_ref} not found") from e

        _logger.info(
            "Copying table %s to %s",
            source_table_ref,
            dest_table_ref,
        )
        create_job = ctas_copy(
            source_table_ref=source_table_ref,
            dest_table_ref=dest_table_ref,
            bq_client=bq_client,
        )

        # Wait for job to complete
        job_result = create_job.result()
        _logger.info("Job %s completed: %s", create_job.job_id, job_result)

    return args
