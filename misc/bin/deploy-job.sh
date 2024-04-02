#!/usr/bin/env bash

set -xeo pipefail

# if [ -z "$branch" ]; then
#     branch=$(git rev-parse --abbrev-ref HEAD)
# else
#     echo "branch set in environment"
# fi
# if [ -z "$commit" ]; then
#     commit=$(git rev-parse HEAD)
# else
#     echo "commit set in environment"
# fi
if [ -z "$release_tag" ]; then
    release_tag="missing_release_tag" # ensure underscore separators for BQ naming
else
    echo "release_tag set in environment"
fi

if [ -z "$instance_name" ]; then
    instance_name="clinvar-ingest-${release_tag}"
else
    echo "instance_name set in environment"
fi

echo "Release tag: $release_tag"
echo "Instance name: $instance_name"

set -u

clinvar_ingest_bucket="clinvar-ingest"

region="us-central1"
# project=$(gcloud config get project)
image_tag=workflow-py-${release_tag}
image=gcr.io/clingen-dev/clinvar-ingest:$image_tag
pipeline_service_account=clinvar-ingest-pipeline@clingen-dev.iam.gserviceaccount.com
deployment_service_account=clinvar-ingest-deployment@clingen-dev.iam.gserviceaccount.com

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
if gcloud run jobs list --region us-central1 | awk '{print $2}' | grep "^$instance_name$"  ; then
    echo "Cloud Run Job $instance_name already exists - updating it"
    command="update"
else
    echo "Cloud Run Job $instance_name doesn't exist - creating it"
    command="create"
fi
gcloud run jobs $command $instance_name \
    --cpu=2 \
    --memory=8Gi \
    --task-timeout=10h \
    --image=$image \
    --region=$region \
    --service-account=$pipeline_service_account \
    --set-env-vars=CLINVAR_INGEST_BUCKET=$clinvar_ingest_bucket,CLINVAR_INGEST_RELEASE_TAG=${release_tag} \
    --set-secrets=CLINVAR_INGEST_SLACK_TOKEN=clinvar-ingest-slack-token:latest
