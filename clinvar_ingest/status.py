from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class StepName(StrEnum):
    COPY = "copy"
    PARSE = "parse"
    CREATE_EXTERNAL_TABLES = "create_external_tables"


class StepStatus(StrEnum):
    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass
class StatusValue(dict):
    status: StepStatus
    step: StepName
    timestamp: datetime
    message: str | dict | None = None
