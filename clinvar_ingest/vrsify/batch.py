"""
Cloud Batch job definitions for running `clinvar-ingest vrsify` on one release.
"""

import datetime
import json
import logging
from dataclasses import dataclass, field

_logger = logging.getLogger("clinvar_ingest")

# The Secret Manager secret the other clinvar-ingest jobs read the Slack token from
SLACK_TOKEN_SECRET = "clinvar-ingest-slack-token"  # noqa: S105


@dataclass
class BatchSettings:
    project: str
    region: str
    service_account: str
    image: str
    # A full release (~4.5M records) translates in ~3 minutes on c4-standard-8, against
    # ~12.5 on e2-standard-4. c4 requires Hyperdisk; e2 and older families take pd-ssd.
    machine_type: str = "c4-standard-8"
    boot_disk_type: str = "hyperdisk-balanced"
    boot_disk_size_gb: int = 50
    max_run_duration_seconds: int = 14400
    environment: dict[str, str] = field(default_factory=dict)


def job_id(release_date: str, now: datetime.datetime | None = None) -> str:
    now = now or datetime.datetime.now(datetime.UTC)
    return f"vrsify-{release_date}-{now:%Y%m%d%H%M%S}"


def job_definition(settings: BatchSettings, vrsify_args: list[str], slack: bool = False) -> dict:
    """
    The Batch job for `clinvar-ingest vrsify VRSIFY_ARGS...`. With `slack`, the job reads
    the Slack token from Secret Manager at run time, so its value never appears in the
    job definition.
    """
    environment: dict = {"variables": dict(settings.environment)}
    if slack:
        environment["secretVariables"] = {
            "CLINVAR_INGEST_SLACK_TOKEN": f"projects/{settings.project}/secrets/{SLACK_TOKEN_SECRET}/versions/latest"
        }
    return {
        "taskGroups": [
            {
                "taskCount": 1,
                "taskSpec": {
                    "runnables": [
                        {
                            "container": {
                                "imageUri": settings.image,
                                "entrypoint": "clinvar-ingest",
                                "commands": ["vrsify", *vrsify_args],
                            }
                        }
                    ],
                    "environment": environment,
                    "maxRetryCount": 0,
                    "maxRunDuration": f"{settings.max_run_duration_seconds}s",
                },
            }
        ],
        "allocationPolicy": {
            "location": {"allowedLocations": [f"regions/{settings.region}"]},
            "instances": [
                {
                    "policy": {
                        "machineType": settings.machine_type,
                        "provisioningModel": "STANDARD",
                        "bootDisk": {"type": settings.boot_disk_type, "sizeGb": settings.boot_disk_size_gb},
                    }
                }
            ],
            "serviceAccount": {"email": settings.service_account},
        },
        "labels": {"service": "clinvar-ingest", "component": "vrsify"},
        "logsPolicy": {"destination": "CLOUD_LOGGING"},
    }


def submit(settings: BatchSettings, name: str, definition: dict) -> str:
    """Submit `definition` as Batch job `name`, returning the job's full resource name."""
    from google.cloud import batch_v1

    client = batch_v1.BatchServiceClient()
    job = client.create_job(
        parent=f"projects/{settings.project}/locations/{settings.region}",
        job_id=name,
        job=batch_v1.Job.from_json(json.dumps(definition)),
    )
    _logger.info(f"Submitted Batch job {job.name}")
    return job.name
