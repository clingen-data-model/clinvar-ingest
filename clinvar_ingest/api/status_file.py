"""
This module is for creating and writing messages to status files for workflow jobs so that external
services can monitor the status of the workflow jobs. The status files are written to a GCS bucket.
"""

import datetime
import json
import logging

from google.cloud.storage import Blob
from google.cloud.storage import Client as GCSClient

from clinvar_ingest.cloud.gcs import blob_writer
from clinvar_ingest.status import StatusValue, StepName, StepStatus

_logger = logging.getLogger("clinvar_ingest")


def write_status_file(
    bucket: str,
    file_prefix: str,
    step: StepName,
    status: StepStatus,
    message: str | None = None,
    timestamp: str = datetime.datetime.now(datetime.UTC).isoformat(),
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
    _logger.debug(f"Writing status file to {gcs_uri} with content: {status_value}")
    with blob_writer(gcs_uri) as writer:
        writer.write(json.dumps(vars(status_value)).encode("utf-8"))
    return status_value


# TODO an improvement here for performance reasons in the status check endpoint
# would be to make blob existence knowable without necessarily downloading the blob
# a finished workflow step would not need to download the started file to know it's finished
def get_status_file(
    bucket: str,
    file_prefix: str,
    step: StepName,
    status: StepStatus,
) -> StatusValue:
    """
    Retrieve the contents of a status file from a GCS Bucket, as a StatusValue object.
    Example:
    execution_id = "2024-01-25_2024-01-25T19:40:49.363883"
    status_value = get_status_file(
        "clinvar-ingest",
        f"executions/{execution_id}",
        StepName.COPY,
        StepStatus.STARTED)
    """
    # https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.bucket.Bucket#google_cloud_storage_bucket_Bucket_list_blobs
    client = GCSClient()
    bucket = client.bucket(bucket)
    blob: Blob = bucket.get_blob(f"{file_prefix}/{step}-{status}.json")
    if blob is None:
        raise ValueError(
            f"Could not find status file for step {step} with status {status} "
            f"in bucket {bucket} and file prefix {file_prefix}"
        )
    content = blob.download_as_string()
    return StatusValue(**json.loads(content))
