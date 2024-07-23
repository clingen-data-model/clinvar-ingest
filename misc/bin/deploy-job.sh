#!/usr/bin/env bash

####
# This script is used to deploy a cloud run job from a local codebase, with
# default global configurations like the service account, the bucket, the region, etc.

# These variables must be set in the environment:
# - release_tag:
#       The name to call this release. By default the docker image and name of the
#       cloud run job will be tagged with this name. This literal string is used in the
#       bigquery dataset name, and must be alphanumeric and underscores.
# - instance_name:
#       The name of the cloud run job. By default, it will be set to 'clinvar-ingest-${release_tag}'.
#       NOTE: The instance name must be alphanumeric and hyphens only. So if release_tag contains
#       underscores, instance_name must be set explicitly, to remove the underscores.
#
# To avoid publishing slack status messages, set CLINVAR_INGEST_SLACK_CHANNEL to an empty string
# Being set but empty will stop the default value from being used.
#
# Example:
# This will build the local codebase and deploy it with an image tagged with 'local-dev'
# and a cloud run job named 'clinvar-ingest-local-dev':
#
# release_tag=local_dev instance_name=clinvar-ingest-local-dev CLINVAR_INGEST_SLACK_CHANNEL='' bash deploy-job.sh
#
####

set -xeo pipefail

if [ -z "$release_tag" ]; then
    release_tag="missing_release_tag" # ensure underscore separators for BQ naming
else
    echo "release_tag set in environment"
fi

# Check if release_tag is only alphanumeric and underscores
if [[ "$release_tag" =~ [^a-zA-Z0-9_] ]]; then
    echo "The release_tag contains characters other than alphanumeric and underscores."
    exit 1
fi

if [ -z "$instance_name" ]; then
    instance_name="clinvar-ingest-${release_tag}"
    # Check if the variable contains only alphanumeric characters and hyphens
    if [[ "$instance_name" =~ [^a-zA-Z0-9-] ]]; then
        echo "The instance_name contains characters other than alphanumeric and hyphens."
        exit 1
    fi
else
    echo "instance_name set in environment"
fi

echo "Release tag: $release_tag"
echo "Instance name: $instance_name"

if [ -n "$clinvar_ingest_cmd" ]; then
    echo "clinvar_ingest_cmd set in environment"
else
    clinvar_ingest_cmd="" # Resets it to the default in the docker image
fi

if [ -n "$file_format" ]; then
    echo "file_format set in environment"
else
    file_format=""
fi


# Disallow unset variables after they've been validated
set -u

clinvar_ingest_bucket="clinvar-ingest-dev"

region="us-east1"
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
if gcloud run jobs list --region $region | awk '{print $2}' | grep "^$instance_name$"  ; then
    echo "Cloud Run Job $instance_name already exists - updating it"
    command="update"
else
    echo "Cloud Run Job $instance_name doesn't exist - creating it"
    command="create"
fi

env_vars="CLINVAR_INGEST_BUCKET=$clinvar_ingest_bucket,CLINVAR_INGEST_RELEASE_TAG=${release_tag}"
if [ -n "$file_format" ]; then
    env_vars="$env_vars,file_format=${file_format}"
fi

gcloud run jobs $command $instance_name \
    --cpu=2 \
    --memory=8Gi \
    --task-timeout=10h \
    --image=$image \
    --region=$region \
    --command="$clinvar_ingest_cmd" \
    --service-account=$pipeline_service_account \
    --set-env-vars=CLINVAR_INGEST_BUCKET=$clinvar_ingest_bucket,CLINVAR_INGEST_RELEASE_TAG=${release_tag} \
    --set-secrets=CLINVAR_INGEST_SLACK_TOKEN=clinvar-ingest-slack-token:latest
