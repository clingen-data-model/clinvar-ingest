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

from clinvar_ingest.api.model.requests import CreateExternalTablesRequest
from clinvar_ingest.cloud.gcs import parse_blob_uri

_logger = logging.getLogger(__name__)


def create_sql(
    project,
    dataset,
    bucket,
    path,
    filename="clinvar_ingest/cloud/bigquery/table_definitions/create_external_tables.sql",
):
    with open(filename, encoding="utf-8") as f:
        sql = f.read()

    sql = sql.format(project=project, dataset=dataset, bucket=bucket, path=path)

    return sql


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


def run_create_old(args: CreateExternalTablesRequest):
    """
    Creates tables using args parsed by `cli.parse_args`
    """
    client = bigquery.Client()
    if args.project is None:
        if client.project is None:
            raise ValueError(
                "gcloud client project is None and --project arg not provided"
            )
        args.project = client.project
        _logger.info("Using default project from gcloud environment: %s", args.project)

    if args.path:
        if not args.path.startswith("/"):
            args.path = "/" + args.path

    bucket_obj = storage.Client().get_bucket(args.bucket)
    bucket_location = bucket_obj.location

    # create dataset if not exists
    dataset_obj = ensure_dataset_exists(
        client, args.project, dataset_id=args.dataset, location=bucket_location
    )
    if not dataset_obj:
        raise RuntimeError(f"Didn't get a dataset object back. run_create args: {args}")

    sql = create_sql(args.project, args.dataset, args.bucket, args.path)

    create_job = client.query(sql)
    results = create_job.result()
    for row in results:
        print(row)


def schema_file_path_for_table(table_name: str) -> dict:
    """
    Loads the schema for the given table name.
    """
    base_path = Path("clinvar_ingest/cloud/bigquery/bq_json_schemas")
    schema_path = base_path / f"{table_name}.bq.json"
    return schema_path
    # with open(schema_path, encoding="utf-8") as f:
    #     schema = json.load(f)
    # return schema


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


def run_create(args: CreateExternalTablesRequest):
    """
    Creates tables using args parsed by `cli.parse_args`
    """
    bq_client = bigquery.Client()
    gcs_client = storage.Client()

    if args.destination_project is None:
        if bq_client.project is None:
            raise ValueError(
                "gcloud client project is None and --project arg not provided"
            )
        args.destination_project = bq_client.project
        _logger.info("Using default project from gcloud environment: %s", args.project)

    source_buckets = set()
    for table_name, gcs_blob_path in args.source_table_paths.items():
        parsed_blob = parse_blob_uri(gcs_blob_path.root, gcs_client)
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
        project=args.destination_project,
        dataset_id=args.destination_dataset,
        location=bucket_location,
    )
    if not dataset_obj:
        raise RuntimeError(f"Didn't get a dataset object back. run_create args: {args}")

    # TODO run for reach table_name, path in args.source_paths
    # sql = create_sql(args.project, args.dataset, args.bucket, args.path)
    outputs = {}
    for table_name, gcs_blob_path in args.source_table_paths.items():
        table = create_table(
            table_name,
            dataset=dataset_obj,
            blob_uri=gcs_blob_path.root,
            client=bq_client,
        )
        outputs[table_name] = table
    return outputs
