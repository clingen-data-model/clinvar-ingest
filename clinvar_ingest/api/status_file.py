"""
This module is for creating and writing messages to status files for workflow jobs so that external
services can monitor the status of the workflow jobs. The status files are written to a GCS bucket.
"""
import json
from datetime import datetime

from clinvar_ingest.cloud.gcs import blob_writer
from clinvar_ingest.config import Env, get_env
from clinvar_ingest.status import StatusValue, StepName, StepStatus


def write_status_file(
    file_prefix: str,
    step: StepName,
    status: StepStatus,
    message: str = None,
    timestamp: datetime = datetime.now(),
) -> StatusValue:
    """
    This function writes a status file to a GCS bucket. The status file is a JSON file with the following format:
    {
        "status": "succeeded",
        "step": "COPY",
        "message": "All files copied successfully",
        "timestamp": "2021-06-24T16:12:00.000000"
    }
    """
    status_value = StatusValue(
        status=status, step=step, timestamp=timestamp, message=message
    )
    env: Env = get_env()
    gcs_uri = f"gs://{env.bucket_name}/{env.job_status_prefix}/{step}-{status}.json"
    with blob_writer(gcs_uri) as writer:
        writer.write(json.dumps(vars(status_value)).encode("utf-8"))
    return status_value


"""
job_id = "2024-01-24_<timestamp>"

# TODO make the output directory paths nest under the job_id



"""
