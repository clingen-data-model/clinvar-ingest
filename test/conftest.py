from types import SimpleNamespace

import pytest


@pytest.fixture(scope="session", autouse=True)
def env_config():
    c = SimpleNamespace()
    c.bucket_name = "clinvar-ingest-not-a-real-bucket"
    c.bucket_staging_prefix = "clinvar_xml"
    c.bucket_parsed_prefix = "clinvar_parsed"
    c.clinvar_ftp_base_url = "https://ftp.ncbi.nlm.nih.gov"
    return c
