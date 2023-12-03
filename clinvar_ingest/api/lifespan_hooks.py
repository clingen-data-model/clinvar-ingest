import logging.config
from contextlib import asynccontextmanager

from fastapi import FastAPI

from clinvar_ingest.log_conf import log_conf

@asynccontextmanager
async def read_log_conf(app: FastAPI):
    # https://fastapi.tiangolo.com/advanced/events/
    logging.config.dictConfig(log_conf)
    yield
