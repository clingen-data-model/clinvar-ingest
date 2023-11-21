"""
Functions for setting up tables in bigquery.
Tables are defined as external, to load NDJSON/JSONL
files from Google Cloud Storage.

Usable as a script or programmatic module.
"""
import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery.dataset import Dataset, DatasetReference

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


def run_create(args):
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
