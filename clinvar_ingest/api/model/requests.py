from datetime import date, datetime
from pathlib import PurePath
from typing import Union

from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    RootModel,
    validator,
)


def to_title_case(string: str) -> str:
    return " ".join(word.capitalize() for word in string.split("_"))


class ClinvarFTPWatcherRequest(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_title_case,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "Name": "ClinVarVariationRelease_2023-1104.xml.gz",
                    "Size": 3160398711,
                    "Released": "2023-11-05 15:47:16",
                    "Last Modified": "2023-11-05 15:47:16",
                    "Directory": "/pub/clinvar/xml/clinvar_variation/weekly_release",
                    "Release Date": "2023-11-04",
                }
            ]
        },
    )

    name: str
    size: int
    released: datetime
    last_modified: datetime
    directory: str
    release_date: date


class CopyResponse(BaseModel):
    ftp_path: str
    gcs_path: str


class ParseRequest(BaseModel):
    input_path: str
    output_path: str
    no_disassemble: bool = Field(default=True)
    no_jsonify_content: bool = Field(default=True)


class GCSBlobPath(RootModel):
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


class PurePathModel(RootModel):
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
    # Either URLs (such as gs:// URLs) or paths to local files which exist
    parsed_files: dict[str, Union[GCSBlobPath, PurePathModel]]


class TodoRequest(BaseModel):  # A shim to get the workflow pieced together
    todo: str
