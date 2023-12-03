import logging.config
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def read_log_conf(app: FastAPI):
    # https://fastapi.tiangolo.com/advanced/events/
    logging.config.dictConfig(config)
    yield
