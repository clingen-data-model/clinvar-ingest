from pydantic import BaseModel


class CopyResponse(BaseModel):
    ftp_path: str
    gcs_path: str
