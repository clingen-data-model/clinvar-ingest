from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field


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


class ParseRequest(BaseModel):
    ftp_path: str
    gcs_path: str
    no_disassemble: bool = Field(default=True)
    no_jsonify_content: bool = Field(default=True)


class CopyResponse(BaseModel):
    ftp_path: str
    gcs_path: str


class TodoRequest(BaseModel):  # A shim to get the workflow pieced together
    todo: str
