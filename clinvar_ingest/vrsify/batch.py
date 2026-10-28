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


# Memory per vCPU of each predefined machine type class, the least across the e2, n2 and
# c4 families. The task requests 90% of the VM's memory, leaving room for the Batch agent.
_MIB_PER_VCPU = {"standard": 3840, "highmem": 7680, "highcpu": 1024}
_MEMORY_REQUEST_FRACTION = 0.9


def compute_resource(machine_type: str) -> dict | None:
    """
    The task's resource request for a predefined machine type, e.g. c4-standard-8: all of
    its vCPUs and most of its memory, so the task has the VM and the job shows what it
    uses. Without a request, Batch assumes 2 vCPUs and 2000 MiB per task. None for machine
    types not named <family>-<class>-<vCPUs>.
    """
    parts = machine_type.split("-")
    if len(parts) != 3 or parts[1] not in _MIB_PER_VCPU or not parts[2].isdigit():  # noqa: PLR2004
        return None
    vcpus = int(parts[2])
    return {
        "cpuMilli": vcpus * 1000,
        "memoryMib": int(vcpus * _MIB_PER_VCPU[parts[1]] * _MEMORY_REQUEST_FRACTION),
    }


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
    task_spec = {
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
    }
    if resource := compute_resource(settings.machine_type):
        task_spec["computeResource"] = resource
    return {
        "taskGroups": [{"taskCount": 1, "taskSpec": task_spec}],
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
