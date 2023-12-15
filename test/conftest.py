from types import SimpleNamespace

import pytest

from clinvar_ingest import config


@pytest.fixture(scope="session", autouse=True)
def env_config():
    """
    Overrides clinvar_ingest.config values.
    Also returns a SimpleNamespace with the same values for use in tests.

    The main reason this is in conftest.py is that it gets run by pytest first,
    which enables setting environment values used in the FastAPI app before
    clinvar.api.main is imported, necessary since the app is constructed upon import
    and checks whether the required environment variables are set.
    """
    c = SimpleNamespace()
    c.bucket_name = "clinvar-ingest-not-a-real-bucket"
    c.bucket_staging_prefix = "clinvar_xml"
    c.bucket_parsed_prefix = "clinvar_parsed"
    c.clinvar_ftp_base_url = "https://ftp.ncbi.nlm.nih.gov"

    for k in vars(c):
        setattr(config, k, getattr(c, k))

    return c
