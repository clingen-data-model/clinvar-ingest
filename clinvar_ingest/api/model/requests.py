from datetime import date, datetime
from pathlib import PurePath
from typing import Any, Callable, Union

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_serializer,
    validator,
)


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

    host: AnyUrl
    directory: str
    name: str
    size: int
    released: datetime
    last_modified: datetime
    release_date: date


class CopyResponse(BaseModel):
    ftp_path: str
    gcs_path: str


class ParseRequest(BaseModel):
    input_path: str
    output_path: str
    no_disassemble: bool = Field(default=True)
    no_jsonify_content: bool = Field(default=True)


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


class GcsBucketName(RootModel):
    root: str

    @validator("root")
    def _validate(cls, v):  # pylint: disable=E0213
        alpha_lower = "abcdefghijklmnopqrstuvwxyz"
        alpha_upper = alpha_lower.upper()
        nums = "0123456789"
        symbols = "-_."
        allowed_chars = set(alpha_lower + alpha_upper + nums + symbols)

        for c in v:
            if c not in allowed_chars:
                raise ValueError(f"Invalid character in bucket name: {c}")
        return v


class ParseResponse(BaseModel):
    # Either GCS path (gs:// URLs) or paths to local files
    parsed_files: dict[str, Union[GcsBlobPath, PurePathStr]]

    @field_serializer("parsed_files", when_used="always")
    def _serialize(self, v):
        return walk_and_replace(v, _dump_fn)


class CreateExternalTablesRequest(BaseModel):
    """
    Defines the arguments to the create_external_tables endpoint.
    Values are used by create_tables.run_create.
    """

    destination_project: str
    destination_dataset: str

    source_path: PurePathStr
    source_bucket: str


class TodoRequest(BaseModel):  # A shim to get the workflow pieced together
    todo: str
