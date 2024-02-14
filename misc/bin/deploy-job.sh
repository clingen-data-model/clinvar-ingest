#!/usr/bin/env bash

set -xeuo pipefail

branch=$(git rev-parse --abbrev-ref HEAD)
commit=$(git rev-parse HEAD)
echo "Branch: $branch"
echo "Commit: $commit"

instance_name="clinvar-ingest-${branch}"
clinvar_ingest_bucket="clinvar-ingest"

region="us-central1"
# project=$(gcloud config get project)
image_tag=workflow-py-$commit
image=gcr.io/clingen-dev/clinvar-ingest:$image_tag
pipeline_service_account=clinvar-ingest-pipeline@clingen-dev.iam.gserviceaccount.com
deployment_service_account=clinvar-ingest-deployment@clingen-dev.iam.gserviceaccount.com
clinvar_ftp_url="https://raw.githubusercontent.com"

if gcloud run jobs list --region us-central1 | awk '{print $2}' | grep "^$instance_name$"  ; then
    echo "Cloud Run Job $instance_name already exists"
    echo "Deleting Cloud Run Service"
    gcloud run jobs delete $instance_name --region $region
fi


# Build the image
cloudbuild=.cloudbuild/workflow-py.docker.cloudbuild.yaml

tar --no-xattrs -c \
    clinvar_ingest \
    test \
    misc \
    Dockerfile \
    Dockerfile-workflow-py \
    pyproject.toml \
    log_conf.json \
    .cloudbuild \
    | gzip --fast > archive.tar.gz

gcloud builds submit \
    --substitutions="COMMIT_SHA=${image_tag}" \
    --config .cloudbuild/workflow-py.docker.cloudbuild.yaml \
    --gcs-log-dir=gs://${clinvar_ingest_bucket}/build/logs \
    archive.tar.gz

# gcloud run jobs create [JOB] --image=IMAGE [--args=[ARG,...]]
#     [--binary-authorization=POLICY] [--breakglass=JUSTIFICATION]
#     [--command=[COMMAND,...]] [--cpu=CPU] [--key=KEY]
#     [--labels=[KEY=VALUE,...]] [--max-retries=MAX_RETRIES]
#     [--memory=MEMORY] [--parallelism=PARALLELISM] [--region=REGION]
#     [--service-account=SERVICE_ACCOUNT]
#     [--set-cloudsql-instances=[CLOUDSQL-INSTANCES,...]]
#     [--set-secrets=[KEY=SECRET_NAME:SECRET_VERSION,...]]
#     [--task-timeout=TASK_TIMEOUT] [--tasks=TASKS; default=1]
#     [--vpc-connector=VPC_CONNECTOR] [--vpc-egress=VPC_EGRESS]
#     [--async | --execute-now --wait]
#     [--env-vars-file=FILE_PATH | --set-env-vars=[KEY=VALUE,...]]
#     [GCLOUD_WIDE_FLAG ...]

gcloud run jobs create $instance_name \
    --cpu=2 \
    --memory=2Gi \
    --task-timeout=10h \
    --image=$image \
    --region=$region \
    --service-account=$pipeline_service_account \
    --set-env-vars=CLINVAR_INGEST_BUCKET=$clinvar_ingest_bucket
