"""
Functions for setting up tables in bigquery.
Tables are defined as external, to load NDJSON/JSONL
files from Google Cloud Storage.

Usable as a script or programmatic module.
"""
import argparse
import sys
import logging

from google.cloud import bigquery

_logger = logging.getLogger(__name__)


def create_sql(
    project,
    dataset,
    bucket,
    path,
    filename="clinvar_ingest/bigquery/table_definitions/create_external_tables.sql",
):
    with open(filename, encoding="utf-8") as f:
        sql = f.read()

    sql = sql.format(project=project, dataset=dataset, bucket=bucket, path=path)

    return sql


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=str, required=False)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--path", type=str, default="")

    return parser.parse_args(argv)


def do_create(argv=sys.argv[1:]):
    args = parse_args(argv)
    client = bigquery.Client()
    if args.project is None:
        args.project = client.project
        _logger.info("Using default project from gcloud environment: %s", args.project)

    sql = create_sql(args.project, args.dataset, args.bucket, args.path)

    create_job = client.query(sql)
    results = create_job.result()
    for row in results:
        print(row)


if __name__ == "__main__":
    do_create()
