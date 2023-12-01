from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_pascal


def to_title_case(string: str) -> str:
    return " ".join(word.capitalize() for word in string.split("_"))


class ClinvarFTPWatcherPayload(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_pascal, 
        populate_by_name=True,
        json_schema_extra: {
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
        }
    )

    name: str
    size: int
    released: datetime
    last_modified: datetime
    directory: str
    release_date: date

class ParsePayload(BaseModel):
    input_filename: str
    output_directory: str
    no_disassemble: bool = Field(default=True)
    no_jsonify_content: bool = Field(default=True)


class CopyResponse(BaseModel):
    ftp_path: str
    gcs_path: str
