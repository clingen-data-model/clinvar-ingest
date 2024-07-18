import os
import pathlib
from typing import Literal

from dotenv import dotenv_values
from pydantic import BaseModel, field_validator

_bucket_name = os.environ.get("CLINVAR_INGEST_BUCKET", "")
_bucket_staging_prefix = os.environ.get("CLINVAR_INGEST_STAGING_PREFIX", "clinvar_xml")
_bucket_parsed_prefix = os.environ.get("CLINVAR_INGEST_PARSED_PREFIX", "clinvar_parsed")
_bucket_executions_prefix = os.environ.get(
    "CLINVAR_INGEST_EXECUTIONS_PREFIX", "executions"
)
_slack_token = os.environ.get("CLINVAR_INGEST_SLACK_TOKEN", "")
# defaults to test "clinvar-message-test"
_slack_channel = os.environ.get("CLINVAR_INGEST_SLACK_CHANNEL", "C06QFR0278D")

_release_tag = os.environ.get("CLINVAR_INGEST_RELEASE_TAG", "")

_dotenv_env = os.environ.get("DOTENV_ENV", "dev")
_dotenv_values = dotenv_values(pathlib.Path(__file__).parent / f".{_dotenv_env}.env")


class Env(BaseModel):
    bq_dest_project: str
    bucket_name: str
    bucket_staging_prefix: str
    bucket_parsed_prefix: str
    parse_output_prefix: str
    executions_output_prefix: str
    slack_token: str
    slack_channel: str
    release_tag: str
    file_format_mode: Literal["vcv", "rcv"] = "vcv"

    @field_validator("bucket_name")
    @classmethod
    def _validate_bucket_name(cls, v, info):
        if not v:
            raise ValueError("CLINVAR_INGEST_BUCKET must be set")
        return v


def get_env() -> Env:
    """
    Returns an Env object using the default environment
    variables and any default values.
    """
    return Env(
        bq_dest_project=_dotenv_values["BQ_DEST_PROJECT"],
        bucket_name=_bucket_name or _dotenv_values["CLINVAR_INGEST_BUCKET"],
        bucket_staging_prefix=_bucket_staging_prefix,
        bucket_parsed_prefix=_bucket_parsed_prefix,
        parse_output_prefix=_bucket_parsed_prefix,
        executions_output_prefix=_bucket_executions_prefix,
        slack_token=_slack_token
        or _dotenv_values.get("CLINVAR_INGEST_SLACK_TOKEN", ""),
        slack_channel=_slack_channel
        or _dotenv_values.get("CLINVAR_INGEST_SLACK_CHANNEL", ""),
        release_tag=_release_tag
        or _dotenv_values.get("CLINVAR_INGEST_RELEASE_TAG", ""),
    )
