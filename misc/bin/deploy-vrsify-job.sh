#!/usr/bin/env bash
#
# Create or update the clinvar-ingest-vrsify Cloud Run job, which claims releases whose
# stored procedures have finished and submits a Cloud Batch job running vrsify on each
# (misc/bin/vrsify-workflow.py). Creates no Cloud Scheduler job: executions are manual.
#
# Usage: misc/bin/deploy-vrsify-job.sh IMAGE
#   IMAGE  an image from build-vrsify-image.sh; the Batch jobs use the same image
#
# Required environment:
#   CLINVAR_INGEST_RELEASE_TAG  pipeline_version of the vrsify rows, e.g. v2_6_1
#   VRSIFY_MIN_RELEASE_DATE     earliest release the job claims, YYYY-MM-DD. Earlier
#                               releases were translated outside this pipeline.
# Optional environment (defaults):
#   VRSIFY_SLACK (false), CLINVAR_INGEST_SLACK_CHANNEL (the config default, a test
#   channel), VRSIFY_DRY_RUN (false), project (clingen-dev), region (us-east1),
#   instance_name (clinvar-ingest-vrsify)

set -euo pipefail

if [ $# -ne 1 ]; then
    sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
fi
image="$1"
: "${CLINVAR_INGEST_RELEASE_TAG:?must be set}"
: "${VRSIFY_MIN_RELEASE_DATE:?must be set}"
if [[ ! "$VRSIFY_MIN_RELEASE_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "VRSIFY_MIN_RELEASE_DATE must be YYYY-MM-DD: $VRSIFY_MIN_RELEASE_DATE" >&2
    exit 2
fi

project="${project:-clingen-dev}"
region="${region:-us-east1}"
instance_name="${instance_name:-clinvar-ingest-vrsify}"
service_account="clinvar-ingest-pipeline@${project}.iam.gserviceaccount.com"

env_vars="file_format=vrsify"
env_vars="$env_vars,BQ_DEST_PROJECT=${project}"
env_vars="$env_vars,CLINVAR_INGEST_BQ_META_DATASET=clinvar_ingest"
env_vars="$env_vars,CLINVAR_INGEST_RELEASE_TAG=${CLINVAR_INGEST_RELEASE_TAG}"
env_vars="$env_vars,CLINVAR_INGEST_LOCATION=${region}"
env_vars="$env_vars,CLINVAR_GKS_BUCKET=clinvar-gkm"
env_vars="$env_vars,VRSIFY_IMAGE=${image}"
env_vars="$env_vars,VRSIFY_MIN_RELEASE_DATE=${VRSIFY_MIN_RELEASE_DATE}"
env_vars="$env_vars,VRSIFY_SLACK=${VRSIFY_SLACK:-false}"
env_vars="$env_vars,VRSIFY_DRY_RUN=${VRSIFY_DRY_RUN:-false}"
if [ -n "${CLINVAR_INGEST_SLACK_CHANNEL:-}" ]; then
    env_vars="$env_vars,CLINVAR_INGEST_SLACK_CHANNEL=${CLINVAR_INGEST_SLACK_CHANNEL}"
fi

# No retries: a retried execution would find its own claim and submit nothing, and a
# failure here needs a look at processing_history rather than another attempt
gcloud run jobs deploy "$instance_name" \
    --project="$project" \
    --region="$region" \
    --image="$image" \
    --command=python3 \
    --args=/app/vrsify-workflow.py \
    --cpu=1 \
    --memory=1Gi \
    --task-timeout=10m \
    --max-retries=0 \
    --service-account="$service_account" \
    --set-env-vars="$env_vars" \
    --set-secrets=CLINVAR_INGEST_SLACK_TOKEN=clinvar-ingest-slack-token:latest \
    --labels=service=clinvar-ingest,component=vrsify
