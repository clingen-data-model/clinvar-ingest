import os

from pydantic import BaseModel

bucket_name = os.environ.get("CLINVAR_INGEST_BUCKET", None)
bucket_staging_prefix = os.environ.get("CLINVAR_INGEST_STAGING_PREFIX", "clinvar_xml")
bucket_parsed_prefix = os.environ.get("CLINVAR_INGEST_PARSED_PREFIX", "clinvar_parsed")
clinvar_ftp_base_url = os.environ.get(
    "CLINVAR_FTP_BASE_URL", "https://ftp.ncbi.nlm.nih.gov"
)


class Env(BaseModel):
    bucket_name: str
    bucket_staging_prefix: str = bucket_staging_prefix
    bucket_parsed_prefix: str = bucket_parsed_prefix
    clinvar_ftp_base_url: str = clinvar_ftp_base_url


env = Env()
