#!/usr/bin/env bash
#
# Submit a Cloud Batch job that runs `clinvar-ingest vrsify` for one release.
#
# Usage:
#   misc/bin/submit-vrsify-batch.sh RELEASE_DATE --image IMAGE [options] [-- VRSIFY_ARGS...]
#
# Options:
#   --image IMAGE         Image with the vrsify dependencies (from build-vrsify-image.sh)
#   --machine-type TYPE   Compute Engine machine type (default: e2-standard-4)
#   --boot-disk-type TYPE Boot disk type, where the store is extracted (default: pd-ssd;
#                         machine families such as c4 and n4 need hyperdisk-balanced)
#   --boot-disk-size GB   Boot disk size in GB (default: 50)
#   --max-run-duration S  Job time limit in seconds (default: 14400)
#   --dry-run             Print the job definition instead of submitting it
#
# Anything after `--` is passed to `clinvar-ingest vrsify`, e.g. -- --overwrite
# The output defaults to gs://$CLINVAR_GKS_BUCKET/RELEASE_DATE/dev/vi-vrs-python.jsonl.gz
#
# Environment overrides (defaults): project (clingen-dev), region (us-east1),
# service_account (clinvar-ingest-pipeline@clingen-dev.iam.gserviceaccount.com),
# CLINVAR_GKS_BUCKET (clinvar-gkm).

set -euo pipefail

usage() { sed -n '3,23p' "$0" | sed 's/^# \{0,1\}//'; }

if [ $# -lt 1 ] || [[ "$1" == -* ]]; then
    usage >&2
    exit 2
fi
release_date="$1"
shift
if [[ ! "$release_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "RELEASE_DATE must be YYYY-MM-DD: $release_date" >&2
    exit 2
fi

project="${project:-clingen-dev}"
region="${region:-us-east1}"
service_account="${service_account:-clinvar-ingest-pipeline@${project}.iam.gserviceaccount.com}"
bucket="${CLINVAR_GKS_BUCKET:-clinvar-gkm}"
image=""
machine_type="e2-standard-4"
boot_disk_type="pd-ssd"
boot_disk_size=50
max_run_duration=14400
dry_run=0
vrsify_args=()

while [ $# -gt 0 ]; do
    case "$1" in
        --image) image="$2"; shift 2 ;;
        --machine-type) machine_type="$2"; shift 2 ;;
        --boot-disk-type) boot_disk_type="$2"; shift 2 ;;
        --boot-disk-size) boot_disk_size="$2"; shift 2 ;;
        --max-run-duration) max_run_duration="$2"; shift 2 ;;
        --dry-run) dry_run=1; shift ;;
        --) shift; vrsify_args=("$@"); break ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done
if [ -z "$image" ]; then
    echo "--image is required" >&2
    exit 2
fi

job_name="vrsify-${release_date}-$(date -u +%Y%m%d%H%M%S)"
job_json="$(mktemp)"
trap 'rm -f "$job_json"' EXIT

# ${arr[@]+...}: an empty array is "unbound" under set -u in macOS bash 3.2
python3 - "$job_json" ${vrsify_args[@]+"${vrsify_args[@]}"} <<PY
import json, sys
commands = ["vrsify", "--release-date", "${release_date}"] + sys.argv[2:]
job = {
    "taskGroups": [{
        "taskCount": 1,
        "taskSpec": {
            "runnables": [{"container": {"imageUri": "${image}", "entrypoint": "clinvar-ingest", "commands": commands}}],
            "environment": {"variables": {"CLINVAR_GKS_BUCKET": "${bucket}"}},
            "maxRetryCount": 0,
            "maxRunDuration": "${max_run_duration}s",
        },
    }],
    "allocationPolicy": {
        "location": {"allowedLocations": ["regions/${region}"]},
        "instances": [{"policy": {
            "machineType": "${machine_type}",
            "provisioningModel": "STANDARD",
            "bootDisk": {"type": "${boot_disk_type}", "sizeGb": ${boot_disk_size}},
        }}],
        "serviceAccount": {"email": "${service_account}"},
    },
    "labels": {"service": "clinvar-ingest", "component": "vrsify"},
    "logsPolicy": {"destination": "CLOUD_LOGGING"},
}
with open(sys.argv[1], "w") as f:
    json.dump(job, f, indent=2)
PY

if [ "$dry_run" = 1 ]; then
    echo "# job ${job_name} in ${project}/${region}"
    cat "$job_json"
    exit 0
fi

gcloud batch jobs submit "$job_name" \
    --project "$project" \
    --location "$region" \
    --config "$job_json"
echo "Submitted ${job_name}. Follow it with:"
echo "  gcloud batch jobs describe ${job_name} --project ${project} --location ${region}"
