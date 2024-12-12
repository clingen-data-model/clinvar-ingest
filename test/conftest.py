import json
import logging.config

import pytest

from clinvar_ingest import config


@pytest.fixture
def log_conf():
    with open("log_conf.json") as f:
        conf = json.load(f)
        logging.config.dictConfig(conf)


@pytest.fixture(scope="session", autouse=True)
def env_config() -> config.Env:
    """
    Overrides clinvar_ingest.config values.
    Also returns a SimpleNamespace with the same values for use in tests.

    The main reason this is in conftest.py is that it gets run by pytest first,
    which enables setting environment values used in the FastAPI app before
    clinvar.api.main is imported, necessary since the app is constructed upon import
    and checks whether the required environment variables are set.
    """
    config._dotenv_values = {
        "file_format": "vcv",
        "CLINVAR_INGEST_BUCKET": "clinvar-ingest-not-a-real-bucket",
        "BQ_DEST_PROJECT": "not a real project",
        "CLINVAR_INGEST_RELEASE_TAG": "not a real tag",
    }

    return config.get_env()
