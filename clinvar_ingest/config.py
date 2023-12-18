import os

from pydantic import BaseModel

_bucket_name = os.environ.get("CLINVAR_INGEST_BUCKET", None)
_bucket_staging_prefix = os.environ.get("CLINVAR_INGEST_STAGING_PREFIX", "clinvar_xml")
_bucket_parsed_prefix = os.environ.get("CLINVAR_INGEST_PARSED_PREFIX", "clinvar_parsed")
_clinvar_ftp_base_url = os.environ.get(
    "CLINVAR_FTP_BASE_URL", "https://ftp.ncbi.nlm.nih.gov"
)


class Env(BaseModel):
    bucket_name: str
    bucket_staging_prefix: str
    bucket_parsed_prefix: str
    clinvar_ftp_base_url: str


def get_env() -> Env:
    """
    Returns an Env object using the default environment
    variables and any default values.
    """
    return Env(
        bucket_name=_bucket_name,
        bucket_staging_prefix=_bucket_staging_prefix,
        bucket_parsed_prefix=_bucket_parsed_prefix,
        clinvar_ftp_base_url=_clinvar_ftp_base_url,
    )
