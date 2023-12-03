import logging.config
from contextlib import asynccontextmanager

import yaml
from fastapi import FastAPI


@asynccontextmanager
async def read_log_conf(app: FastAPI):
    # https://fastapi.tiangolo.com/advanced/events/
    with open("log_conf.yaml", "rt") as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    yield
