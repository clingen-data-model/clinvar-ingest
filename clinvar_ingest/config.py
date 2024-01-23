import os
import pathlib

from dotenv import dotenv_values
from pydantic import BaseModel

_bucket_name = os.environ.get("CLINVAR_INGEST_BUCKET", "")
_bucket_staging_prefix = os.environ.get("CLINVAR_INGEST_STAGING_PREFIX", "clinvar_xml")
_bucket_parsed_prefix = os.environ.get("CLINVAR_INGEST_PARSED_PREFIX", "clinvar_parsed")
_bucket_job_status_prefix = os.environ.get(
    "CLINVAR_INGEST_JOB_STATUS_PREFIX", "job_statuses"
)
_clinvar_ftp_base_url = os.environ.get(
    "CLINVAR_FTP_BASE_URL", "https://ftp.ncbi.nlm.nih.gov"
)
_dotenv_env = os.environ.get("DOTENV_ENV", "dev")
_dotenv_values = dotenv_values(pathlib.Path(__file__).parent / f".{_dotenv_env}.env")


class Env(BaseModel):
    bq_dest_dataset: str
    bq_dest_project: str
    bucket_name: str
    bucket_staging_prefix: str
    bucket_parsed_prefix: str
    clinvar_ftp_base_url: str
    parse_output_prefix: str
    job_status_prefix: str


def get_env() -> Env:
    """
    Returns an Env object using the default environment
    variables and any default values.
    """
    return Env(
        bq_dest_dataset=_dotenv_values["BQ_DEST_DATASET"],
        bq_dest_project=_dotenv_values["BQ_DEST_PROJECT"],
        bucket_name=_bucket_name,
        bucket_staging_prefix=_bucket_staging_prefix,
        bucket_parsed_prefix=_bucket_parsed_prefix,
        clinvar_ftp_base_url=_clinvar_ftp_base_url,
        parse_output_prefix=_dotenv_values["PARSE_OUTPUT_PREFIX"],
        job_status_prefix=_bucket_job_status_prefix,
    )
