"""
Functions for executing stored procedures in bigquery.
"""

import logging

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

# Note that in order to get these procedures to access the google cloud drive
# containing the spreadsheet updated by Danielle et al, I had to issue the
# following command that I cannot find an equivalent role in the 'architecture'
# project for the clinvar-ingest-pipeline user:
#
# gcloud auth login --enable-gdrive-access --update-adc
#

# These stored procedures are taken directly from the 'clingen-dev' BigQuery
# shared queries:
#   - post-ingest-step-1
#   - post-ingest-step-2
#   - post-ingest-step-3
# Unfortunately, BQ does not appear to provide a way to call those shared
# procedures directly.
#
#

_logger = logging.getLogger("clinvar_ingest")

stored_procedures = [
    "CALL `clinvar_ingest.dataset_preparation_v2`({0});",
]


def execute_each(
    client: bigquery.Client, project_id: str, release_date: str | None
) -> RowIterator:
    """Execute each procedure in the list of stored procedures individualy,
       substituting the release_date date if provided.

    :param client: bigquery client
    :param project_id: project id
    :param release_date: yyyy_mm_dd, the yyyy_mm_dd formatted date or None to use the BQ `CURRENT_DATE()`
    """
    as_of_date = "CURRENT_DATE()" if release_date is None else f"'{release_date}'"
    results = []
    for query in stored_procedures:
        query_with_args = query.format(as_of_date)
        try:
            logging.info(f"Executing stored procedure: {query_with_args}")
            job = client.query(query_with_args, project=project_id)
            result = job.result()
            logging.info(
                f"Successfully ran stored procedure: {query_with_args}\nresult={result}"
            )
            results.append(result)
        except Exception as e:
            msg = f"Failed to execute stored procedure: {query_with_args} {e}"
            logging.error(msg)
            raise e
    return results


def execute_all(
    client: bigquery.Client, project_id: str, release_date: str | None
) -> RowIterator:
    """Execute the list of stored procedures as one single script,
       substituting the release_date date if provided.

    :param client: bigquery client
    :param project_id: project id
    :param release_date: yyyy_mm_dd, the yyyy_mm_dd formatted date or None to use the BQ `CURRENT_DATE()`
    """
    as_of_date = "CURRENT_DATE()" if release_date is None else f"'{release_date}'"
    query_with_args = [query.format(as_of_date) for query in stored_procedures]
    query = "\n".join(query_with_args)
    _logger.info(f"Executing stored procedures via query: {query}")
    try:
        job = client.query(query, project=project_id)
        result = job.result()
        _logger.info(f"Successfully ran stored procedures, result: {result}")
        return result
    except Exception as e:
        msg = f"Failed to execute stored procedure: {e}"
        logging.error(msg)
        raise e


# TODO Consider external file vs inline as above - might make maintenance easier?
# TODO def execute_all_from_file(file=...,):


if __name__ == "__main__":
    bq_client = bigquery.Client()
    # execute_stored_procedures(client=bq_client, project_id="clingen-dev", yyyy_mm_dd="2024-10-20")
    execute_all(client=bq_client, project_id="clingen-dev", release_date="2024-10-20")
