#!/usr/bin/env bash

set -xeo pipefail

if [ -z "$branch" ]; then
    branch=$(git rev-parse --abbrev-ref HEAD)
else
    echo "branch set in environment"
fi
if [ -z "$commit" ]; then
    commit=$(git rev-parse HEAD)
else
    echo "commit set in environment"
fi

echo "Branch: $branch"
echo "Commit: $commit"

set -u

instance_name="clinvar-ingest-${branch}"
clinvar_ingest_bucket="clinvar-ingest"

region="us-central1"
# project=$(gcloud config get project)
image_tag=workflow-py-$commit
image=gcr.io/clingen-dev/clinvar-ingest:$image_tag
pipeline_service_account=clinvar-ingest-pipeline@clingen-dev.iam.gserviceaccount.com
deployment_service_account=clinvar-ingest-deployment@clingen-dev.iam.gserviceaccount.com


if gcloud run jobs list --region us-central1 | awk '{print $2}' | grep "^$instance_name$"  ; then
    echo "Cloud Run Job $instance_name already exists"
    echo "Deleting Cloud Run Job"
    gcloud run jobs delete $instance_name --region $region --quiet
fi

################################################################
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

################################################################
# Deploy job

gcloud run jobs create $instance_name \
    --cpu=2 \
    --memory=8Gi \
    --task-timeout=10h \
    --image=$image \
    --region=$region \
    --service-account=$pipeline_service_account \
    --set-env-vars=CLINVAR_INGEST_BUCKET=$clinvar_ingest_bucket
