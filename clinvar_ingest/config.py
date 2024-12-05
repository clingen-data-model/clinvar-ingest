import os
import pathlib
from email.policy import default
from typing import Literal

from dotenv import dotenv_values
from pydantic import BaseModel, field_validator

_dotenv_env = os.environ.get("DOTENV_ENV", "dev")
_dotenv_values = dotenv_values(pathlib.Path(__file__).parent / f".{_dotenv_env}.env")


def env_or_dotenv_or(
        key_name: str, default: str | None = None, throw: bool = False
) -> str:
    """
    Retrieves a value from the environment.
    If not set, retrieve it from the dotenv file.
    If not set in the dotenv file, return the default value.

    If throw is True, and the value and default is falsy, raise a ValueError.
    """
    val = os.environ.get(key_name, _dotenv_values.get(key_name, default))
    if throw and not val:
        raise ValueError(f"{key_name} must be set")
    return val

class _BaseEnv(BaseModel):
    bq_dest_project: str
    bq_meta_dataset: str
    slack_token: str | None
    slack_channel: str


def _get_base_env() -> _BaseEnv:
    return _BaseEnv(
        bq_dest_project=env_or_dotenv_or("BQ_DEST_PROJECT", throw=True),
        bq_meta_dataset=env_or_dotenv_or(
            "CLINVAR_INGEST_BQ_META_DATASET", default="clinvar_ingest"
        ),
        slack_token=env_or_dotenv_or("CLINVAR_INGEST_SLACK_TOKEN"),
        # defaults to test "clinvar-message-test"
        slack_channel=env_or_dotenv_or(
            "CLINVAR_INGEST_SLACK_CHANNEL", default="C06QFR0278D"
        ))


class Env(_BaseEnv):
    bucket_name: str
    bucket_staging_prefix: str
    parse_output_prefix: str
    executions_output_prefix: str
    release_tag: str
    schema_version: str
    file_format_mode: Literal["vcv", "rcv"] = "vcv"

    @field_validator("bucket_name")
    @classmethod
    def _validate_bucket_name(cls, v, _info):
        if not v:
            raise ValueError("CLINVAR_INGEST_BUCKET must be set")
        return v


def get_env() -> Env:
    _base_env = _get_base_env()
    return Env(
        **_base_env.model_dump(),
        bucket_name=env_or_dotenv_or("CLINVAR_INGEST_BUCKET", throw=True),
        bucket_staging_prefix=env_or_dotenv_or(
            "CLINVAR_INGEST_STAGING_PREFIX", default="clinvar_xml"
        ),
        parse_output_prefix=env_or_dotenv_or(
            "CLINVAR_INGEST_PARSED_PREFIX", default="clinvar_parsed"
        ),
        executions_output_prefix=env_or_dotenv_or(
            "CLINVAR_INGEST_EXECUTIONS_PREFIX", default="executions"
        ),
        release_tag=env_or_dotenv_or("CLINVAR_INGEST_RELEASE_TAG", throw=True),
        schema_version=env_or_dotenv_or("CLINVAR_INGEST_SCHEMA_VERSION", default="v2"),
    )


class StoredProceduresEnv(_BaseEnv):
    release_date: str


def get_stored_procedures_env() -> StoredProceduresEnv:
    _base_env = _get_base_env()
    return StoredProceduresEnv(
        **_base_env.model_dump(),
        release_date=env_or_dotenv_or("CLINVAR_INGEST_RELEASE_DATE"),
    )
