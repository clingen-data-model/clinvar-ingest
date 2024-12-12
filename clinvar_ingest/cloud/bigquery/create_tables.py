"""
Functions for setting up tables in bigquery.
Tables are defined as external, to load NDJSON/JSONL
files from Google Cloud Storage.

Usable as a script or programmatic module.
"""

import logging
from collections import OrderedDict
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from google.cloud.bigquery.dataset import Dataset, DatasetReference

from clinvar_ingest.api.model.requests import (
    CreateExternalTablesRequest,
    CreateInternalTablesRequest,
    DropExternalTablesRequest,
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


def check_dataset_exists(
    client: bigquery.Client, project: str, dataset_id: str
) -> bigquery.Dataset | None:
    """Check if dataset exists. Returns the Dataset object or None."""
    dataset_ref = DatasetReference(project=project, dataset_id=dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref=dataset_ref)
    except NotFound:
        dataset = None
    return dataset


def schema_file_path_for_table(table_name: str) -> Path:
    """
    Returns the path to the BigQuery schema file for the given table name.
    """
    raw_table_name = table_name.replace("_external", "")
    return bq_schemas_dir / f"{raw_table_name}.bq.json"


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
    return client.create_table(table, exists_ok=True)


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
        _logger.info(
            "Using default project from gcloud environment: %s", bq_client.project
        )

    source_buckets = set()
    bucket_location = None
    for table_name, gcs_blob_path in args.source_table_paths.items():
        parsed_blob = parse_blob_uri(gcs_blob_path.root, gcs_client)
        _logger.info(
            "Parsed blob bucket: %s, path: %s for table %s",
            parsed_blob.bucket,
            parsed_blob.name,
            table_name,
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
        external_table_name = table_name + "_external"
        table = create_table(
            external_table_name,
            dataset=dataset_obj,
            blob_uri=gcs_blob_path.root,
            client=bq_client,
        )
        outputs[external_table_name] = table
    return outputs


def create_internal_tables(
    args: CreateInternalTablesRequest,
) -> CreateInternalTablesRequest:
    """
    -- clingen-dev.2023_10_07_2024_02_19T172113657352.variation_archive
    CREATE TABLE `clingen-dev.2023_10_07_2024_02_19T172113657352.variation_archive_internal`
    AS SELECT * from `clingen-dev.2023_10_07_2024_02_19T172113657352.variation_archive`

    """

    def get_query_for_copy(
        source_table_ref: bigquery.TableReference,
        dest_table_ref: bigquery.TableReference,
    ) -> tuple[str, bool]:
        dedupe_queries = {
            "gene": f"CREATE OR REPLACE TABLE `{dest_table_ref}` AS "  # noqa: S608
            f"SELECT * EXCEPT (vcv_id, row_num) from "
            f"(SELECT ge.*, ROW_NUMBER() OVER (PARTITION BY ge.id "
            f"ORDER BY vcv.date_last_updated DESC, vcv.id DESC) row_num "
            f"FROM `{source_table_ref}` AS ge "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.variation_archive` AS vcv "
            f"ON ge.vcv_id = vcv.id) where row_num = 1",
            "submission": f"CREATE OR REPLACE TABLE `{dest_table_ref}` AS "  # noqa: S608
            f"SELECT * EXCEPT (scv_id, row_num) from "
            f"(SELECT se.*, ROW_NUMBER() OVER (PARTITION BY se.id "
            f"ORDER BY vcv.date_last_updated DESC, vcv.id DESC) row_num "
            f"FROM `{source_table_ref}` AS se "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.clinical_assertion` AS scv "
            f"ON se.scv_id = scv.id "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.variation_archive` AS vcv "
            f"ON scv.variation_archive_id = vcv.id) "
            f"where row_num = 1",
            "submitter": f"CREATE OR REPLACE TABLE `{dest_table_ref}` AS "  # noqa: S608
            f"SELECT * EXCEPT (scv_id, row_num) from "
            f"(SELECT se.*, ROW_NUMBER() OVER (PARTITION BY se.id "
            f"ORDER BY vcv.date_last_updated DESC, vcv.id DESC) row_num "
            f"FROM `{source_table_ref}` AS se "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.clinical_assertion` AS scv "
            f"ON se.scv_id = scv.id "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.variation_archive` AS vcv "
            f"ON scv.variation_archive_id = vcv.id) "
            f"where row_num = 1",
            "trait": f"CREATE OR REPLACE TABLE `{dest_table_ref}` AS "  # noqa: S608
            f"SELECT * EXCEPT (rcv_id, row_num) from "
            f"(SELECT te.*, ROW_NUMBER() OVER (PARTITION BY te.id "
            f"ORDER BY vcv.date_last_updated DESC, vcv.id DESC) row_num "
            f"FROM `{source_table_ref}` AS te "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.rcv_accession` AS rcv "
            f"ON te.rcv_id = rcv.id "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.variation_archive` AS vcv "
            f"ON rcv.variation_archive_id = vcv.id) "
            f"where row_num = 1",
            "trait_set": f"CREATE OR REPLACE TABLE `{dest_table_ref}` AS "  # noqa: S608
            f"SELECT * EXCEPT (rcv_id, row_num) from "
            f"(SELECT tse.*, ROW_NUMBER() OVER (PARTITION BY tse.id "
            f"ORDER BY vcv.date_last_updated DESC, vcv.id DESC) row_num "
            f"FROM `{source_table_ref}` AS tse "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.rcv_accession` AS rcv "
            f"ON tse.rcv_id = rcv.id "
            f"JOIN `{dest_table_ref.project}.{dest_table_ref.dataset_id}.variation_archive` AS vcv "
            f"ON rcv.variation_archive_id = vcv.id) "
            f"where row_num = 1",
        }
        default_query = f"CREATE OR REPLACE TABLE `{dest_table_ref}` AS SELECT * from `{source_table_ref}`"  # noqa: S608
        query = dedupe_queries.get(dest_table_ref.table_id, default_query)
        return query, query == default_query

    env = get_env()
    table_map = args.source_dest_table_map
    # Initial validation
    non_dedupe_tables = {}
    dedupe_tables = {}
    for source_table, dest_table in table_map.items():
        source_table_ref = bigquery.TableReference.from_string(source_table)
        dest_table_ref = bigquery.TableReference.from_string(dest_table)
        if dest_table_ref.project != env.bq_dest_project:
            raise ValueError(
                f"Cross-project table copies not currently supported. "
                f"Destination table project must be {env.bq_dest_project}. Got: {dest_table_ref.project}"
            )
        _, is_default_query = get_query_for_copy(source_table_ref, dest_table_ref)
        if is_default_query:
            non_dedupe_tables[source_table_ref] = dest_table_ref
        else:
            dedupe_tables[source_table_ref] = dest_table_ref
    # since dedupe queries depend on other tables being in existence,
    # we need to run them after the non-dedupe tables
    ordered_tables = OrderedDict()
    ordered_tables.update(non_dedupe_tables)
    ordered_tables.update(dedupe_tables)

    def ctas_copy(
        source_table_ref: bigquery.TableReference,
        dest_table_ref: bigquery.TableReference,
        bq_client: bigquery.Client,
    ) -> bigquery.QueryJob:
        query, _ = get_query_for_copy(source_table_ref, dest_table_ref)
        _logger.info(f"Creating table {dest_table_ref} from {source_table_ref}")
        _logger.info(f"Query:\n{query}")
        return bq_client.query(query)

    bq_client = bigquery.Client()
    # Copy each
    for source_table_ref, dest_table_ref in ordered_tables.items():
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


def drop_external_tables(
    args: DropExternalTablesRequest,
) -> DropExternalTablesRequest:
    drop_tables = [
        f"DROP TABLE {bq_table};" for table_name, bq_table in args.root.items()
    ]
    drop_tables_query = " ".join(drop_tables)
    _logger.info(f"Drop external tables query: {drop_tables_query}")

    bq_client = bigquery.Client()
    bq_client.query(drop_tables_query)

    return args
