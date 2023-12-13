import os

bucket_name = os.environ.get("CLINVAR_INGEST_BUCKET", None)
bucket_staging_prefix = os.environ.get("CLINVAR_INGEST_STAGING_PREFIX", "clinvar_xml")
bucket_parsed_prefix = os.environ.get("CLINVAR_INGEST_PARSED_PREFIX", "clinvar_parsed")
clinvar_ftp_base_url = os.environ.get(
    "CLINVAR_FTP_BASE_URL", "https://ftp.ncbi.nlm.nih.gov"
)
