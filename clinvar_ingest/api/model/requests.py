import re
from datetime import date, datetime
from pathlib import PurePath
from typing import Annotated, Any, Callable, Literal, Optional, Union

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StringConstraints,
    ValidationInfo,
    field_serializer,
    field_validator,
    validator,
)

from clinvar_ingest.status import StepName, StepStatus

# Helper functions


def to_title_case(string: str) -> str:
    return " ".join(word.capitalize() for word in string.split("_"))


def walk_and_replace(d: dict, dump_fn: Callable[[Any], Any]):
    if isinstance(d, dict):
        for k, v in d.items():
            d[k] = walk_and_replace(v, dump_fn)
    elif isinstance(d, list):
        for i in range(len(d)):
            d[i] = walk_and_replace(d[i], dump_fn)
    else:
        d = dump_fn(d)
    return d


def _dump_fn(val):
    if isinstance(val, PurePath):
        return str(val)
    if isinstance(val, BaseModel):
        return val.model_dump()
    return val


# Constrained raw string types. For when a value is a string, but has constraints.

BigqueryDatasetId = Annotated[str, StringConstraints(pattern=r"^[a-zA-Z0-9_]+$")]

BigqueryFullTableId = Annotated[
    str, StringConstraints(pattern=r"^[a-zA-Z0-9-_]+:[a-zA-Z0-9_]+.[a-zA-Z0-9-_]+$")
]


# Request and response models


def strict_datetime_field_validator(cls, v, info: ValidationInfo) -> datetime:
    # print(f"Validating {info.field_name} with value {v}")
    if not v:
        raise ValueError(f"{info.field_name} was empty")
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        pattern = re.compile(r"(\d{4})-?(\d{2})-?(\d{2})[\sT](\d{2}):(\d{2}):(\d{2})")
        if not pattern.match(v):
            raise ValueError(
                f"Input should be a valid datetime, str did not match regex format: {v}"
            )
        return datetime.fromisoformat(v)
    raise ValueError(f"Input should be a valid datetime: {v}")


class ClinvarFTPWatcherRequest(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_title_case,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "Host": "https://ftp.ncbi.nlm.nih.gov",
                    "Directory": "/pub/clinvar/xml/clinvar_variation/weekly_release",
                    "Name": "ClinVarVariationRelease_2023-1104.xml.gz",
                    "Size": 3160398711,
                    "Released": "2023-11-05 15:47:16",
                    "Last Modified": "2023-11-05 15:47:16",
                    "Release Date": "2023-11-04",
                }
            ]
        },
    )

    # Host to connect to and download the file from
    # e.g. https://ftp.ncbi.nlm.nih.gov or https://raw.githubusercontent.com
    host: AnyUrl
    directory: str
    name: str
    size: int
    released: datetime
    last_modified: datetime
    release_date: date

    @field_validator("released", "last_modified", mode="before")
    @classmethod
    def _validate_datetime(cls, v, info: ValidationInfo) -> datetime:
        return strict_datetime_field_validator(cls, v, info)


class CopyResponse(BaseModel):
    ftp_path: str
    gcs_path: str


class ParseRequest(BaseModel):
    input_path: str
    disassemble: bool = Field(default=True)
    jsonify_content: bool = Field(default=True)


class GcsBlobPath(RootModel):
    """
    A GCS blob path, such as gs://my-bucket/my-file.txt
    Validates path structure, does not check if the file exists.
    """

    root: str

    @validator("root")
    def _validate(cls, v):  # pylint: disable=E0213
        if not v.startswith("gs://"):
            raise ValueError(f"Must be a gs:// URL: {v}")
        AnyUrl(v)
        return v


class PurePathStr(RootModel):
    """
    A PurePath, such as /my/file.txt
    Validates path structure, does not check if the file exists.
    Keeps the value as a str, so it is JSON serializable without a custom serializer.
    """

    root: str

    @validator("root")
    def _validate(cls, v):  # pylint: disable=E0213
        PurePath(v)
        return v


class ParseResponse(BaseModel):
    """
    Map of entity type to either GCS path (gs:// URLs) or path to local file
    """

    parsed_files: dict[str, Union[GcsBlobPath, PurePathStr]]

    @field_serializer("parsed_files", when_used="always")
    def _serialize(self, v):
        return walk_and_replace(v, _dump_fn)


class CreateExternalTablesRequest(BaseModel):
    """
    Defines the arguments to the create_external_tables endpoint.
    Values are used by create_tables.run_create_external_tables.
    """

    destination_dataset: str
    source_table_paths: dict[str, GcsBlobPath]

    @field_validator("destination_dataset")
    @classmethod
    def _validate_destination_dataset(cls, v):
        if not v:
            raise ValueError("destination_dataset must not be empty")
        pattern = r"^[a-zA-Z0-9_]+$"
        if not re.match(pattern, v):
            raise ValueError(
                f"destination_dataset must match pattern {pattern}. Got: {v}"
            )
        return v


class CreateExternalTablesResponse(RootModel):
    """
    Map of entity type to full table id (project:dataset.table)
    """

    root: dict[str, BigqueryFullTableId]


class InitializeWorkflowResponse(BaseModel):
    """
    Defines the response from create_workflow_id endpoint.
    """

    workflow_execution_id: Annotated[
        str,
        Field(
            description=(
                "The base value to be used to initialize a workflow_execution_id. "
                "The workflow_execution_id will be a concatenation of this value and a timestamp, "
                "representing a single instance of a workflow run, on a particular seed value."
                "For example a seed value of '2024-01-24' will result in a workflow ID of '2024-01-24_<timestamp>'."
            )
        ),
    ]


class InitializeStepRequest(BaseModel):
    """
    Defines the request to the initialize_step endpoint.
    """

    workflow_execution_id: str
    step_name: StepName
    message: Optional[str] = None


class InitializeStepResponse(BaseModel):
    """
    Defines the response from the initialize_step endpoint.
    """

    workflow_execution_id: str
    step_name: StepName
    step_status: StepStatus
    timestamp: datetime

    @field_serializer("timestamp", when_used="always")
    def _timestamp_serializer(self, v: datetime):
        return v.isoformat()


class GetStepStatusRequest(BaseModel):
    """
    Defines the request to the get_step_status endpoint.
    """

    workflow_execution_id: str
    step_name: StepName


class GetStepStatusResponse(BaseModel):
    """
    Defines the response from the get_step_status endpoint.
    """

    workflow_execution_id: str
    step_name: StepName
    step_status: StepStatus
    timestamp: datetime
    message: Optional[str] = None

    @field_serializer("timestamp", when_used="always")
    def _timestamp_serializer(self, v: datetime):
        return v.isoformat()


class StepStartedResponse(BaseModel):
    """
    Defines the response from the step_started endpoint.
    """

    workflow_execution_id: str
    step_name: StepName

    timestamp: datetime
    step_status: Literal[StepStatus.STARTED] = StepStatus.STARTED
    message: Optional[str] = None

    @field_serializer("timestamp", when_used="always")
    def _timestamp_serializer(self, v: datetime):
        return v.isoformat()


class TodoRequest(BaseModel):  # A shim to get the workflow pieced together
    todo: str
