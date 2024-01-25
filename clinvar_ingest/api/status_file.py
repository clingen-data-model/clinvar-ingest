"""
This module is for creating and writing messages to status files for workflow jobs so that external
services can monitor the status of the workflow jobs. The status files are written to a GCS bucket.
"""
import json
from datetime import datetime

from clinvar_ingest.cloud.gcs import blob_writer
from clinvar_ingest.status import StatusValue, StepName, StepStatus


def write_status_file(
    bucket: str,
    file_prefix: str,
    step: StepName,
    status: StepStatus,
    message: str = None,
    timestamp: str = datetime.utcnow().isoformat(),
) -> StatusValue:
    """
    This function writes a status file to a GCS bucket. The status file is a JSON file with the following format:
    {
        "status": "succeeded",
        "step": "COPY",
        "message": "All files copied successfully",
        "timestamp": "2021-06-24T16:12:00.000000"
    }

    TODO maybe make a way to run this locally for testing, not writing to a bucket.
    """
    status_value = StatusValue(
        status=status, step=step, timestamp=timestamp, message=message
    )

    gcs_uri = f"gs://{bucket}/{file_prefix}/{step}-{status}.json"
    with blob_writer(gcs_uri) as writer:
        writer.write(json.dumps(vars(status_value)).encode("utf-8"))
    return status_value
