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
stored_procedures = [
    # post-ingest-step-1
    # -- 01 scv_summary
    "CALL `clinvar_ingest.scv_summary_proc`({0});",
    # -- 01.1 gc_scvs
    "CALL `clinvar_ingest.gc_scv_proc`({0});",
    # -- 02 single_gene_variation
    "CALL `clinvar_ingest.single_gene_variation_proc`({0});",

    # post-ingest-step-2
    # -- clinvar_genes
    "CALL `clinvar_ingest.clinvar_genes_proc`({0});",
    # -- clinvar_submitters
    "CALL `clinvar_ingest.clinvar_submitters_proc`({0});",
    # -- clinvar_variations
    "CALL `clinvar_ingest.clinvar_variations_proc`({0});",
    # -- clinvar_vcvs
    "CALL `clinvar_ingest.clinvar_vcvs_proc`({0});",
    # -- clinvar_scvs
    "CALL `clinvar_ingest.clinvar_scvs_proc`({0});",
    # -- clinvar_gc_scvs
    "CALL `clinvar_ingest.clinvar_gc_scvs_proc`({0});",
    # -- clinvar_var_scv_change
    "CALL `clinvar_ingest.clinvar_var_scv_change_proc`();",
    # -- voi_vcv_scv
    "CALL `clinvar_ingest.voi_vcv_scv_proc`();",
    # -- voi_and_voi_scv_group
    "CALL `clinvar_ingest.voi_and_voi_scv_group_proc`();",
    # -- voi_group_change
    "CALL `clinvar_ingest.voi_group_change_proc`();",
    # -- voi_top_group_change
    "CALL `clinvar_ingest.voi_top_group_change_proc`();",
    # -- voi_summary_change
    "CALL `clinvar_ingest.voi_summary_change_proc`();",

    # post-ingest-step-3
    # -- gather variations for tracker reports
    "CALL `variation_tracker.report_variation_proc`();",
    # -- build all VCEP tracker report tables
    "CALL `variation_tracker.variation_track_proc`();",
    # -- build genomeconnect tracker report tables
    "CALL `variation_tracker.gc_report_proc`({0});",
]


def execute_each(client: bigquery.Client, project_id: str, yyyy_mm_dd: str|None) -> RowIterator:
    """Execute the list of stored procedures, substituting the yyyy_mm_dd date if provided.

    :param client: bigquery client
    :param project_id: project id
    :param yyyy_mm_dd: yyyy_mm_dd, the yyyy_mm_dd formatted date or None to use the BQ `CURRENT_DATE()`
    """
    as_of_date = 'CURRENT_DATE()' if yyyy_mm_dd is None else f"'{yyyy_mm_dd}'"
    for lb_query in stored_procedures:
        lb_query_with_args = lb_query.format(as_of_date)
        try:
            logging.info(f"Executing stored procedure: {lb_query_with_args}")
            job = client.query(lb_query_with_args, project=project_id)
            result = job.result()
            logging.info(f"Successfully ran stored procedure: {lb_query_with_args}\nresult={result}")
            return result
        except Exception as e:
            msg = f"Failed to execute stored procedure: {lb_query_with_args} {e}"
            logging.error(msg)
            raise e

def execute_all(client: bigquery.Client, project_id: str, yyyy_mm_dd: str|None) -> RowIterator:
    as_of_date = 'CURRENT_DATE()' if yyyy_mm_dd is None else f"'{yyyy_mm_dd}'"
    lb_query_with_args = [lb_query.format(as_of_date) for lb_query in stored_procedures]
    query = " ".join(lb_query_with_args)
    try:
        job = client.query(query, project=project_id)
        result = job.result()
        return result
    except Exception as e:
        msg = f"Failed to execute stored procedure: {query} {e}"
        logging.error(msg)
        raise e

# TODO Consider external file vs inline as above - might make maintenace easier?
# TODO def execute_all_from_file(file=...,):


if __name__ == "__main__":
    bq_client = bigquery.Client()
    # execute_stored_procedures(client=bq_client, project_id="clingen-dev", yyyy_mm_dd="2024-10-20")
    execute_all(client=bq_client, project_id="clingen-dev", yyyy_mm_dd="2024-10-20")