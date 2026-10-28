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
# PROJECT is the Google Cloud project every gcloud command here acts on (default:
# clingen-dev), whatever project the gcloud configuration names.
#
# Example:
# This will build the local codebase and deploy it with an image tagged with 'local-dev'
# and a cloud run job named 'clinvar-ingest-local-dev':
#
# release_tag=local_dev instance_name=clinvar-ingest-local-dev CLINVAR_INGEST_SLACK_CHANNEL='' bash deploy-job.sh
#
# An instance_name containing 'vrsify' deploys the vrsify dispatcher
# (misc/bin/vrsify-workflow.py) with an image that has the vrsify dependencies, tagged
# vrsify-${CLINVAR_INGEST_RELEASE_TAG}, which its Batch jobs also run. It also needs
# VRSIFY_MIN_RELEASE_DATE (YYYY-MM-DD), the earliest release it claims, and takes
# VRSIFY_SLACK (default false). Its Cloud Scheduler job is created paused:
#
# CLINVAR_INGEST_RELEASE_TAG=v2_6_1 instance_name=clinvar-ingest-vrsify VRSIFY_MIN_RELEASE_DATE=2026-10-24 bash deploy-job.sh
#
####

set -xeo pipefail

if [ -z "$CLINVAR_INGEST_RELEASE_TAG" ]; then
    echo "Must set CLINVAR_INGEST_RELEASE_TAG"
    exit 1
fi

# Check if CLINVAR_INGEST_RELEASE_TAG is only alphanumeric and underscores
if [[ "$CLINVAR_INGEST_RELEASE_TAG" =~ [^a-zA-Z0-9_] ]]; then
    echo "The CLINVAR_INGEST_RELEASE_TAG contains characters other than alphanumeric and underscores."
    exit 1
fi

if [ -z "$instance_name" ]; then
    echo "Must set instance_name"
    exit 1
fi
# Check if instance_name contains only alphanumeric characters and hyphens
if [[ "$instance_name" =~ [^a-zA-Z0-9-] ]]; then
    echo "The instance_name contains characters other than alphanumeric and hyphens."
    exit 1
fi


echo "Release tag: $CLINVAR_INGEST_RELEASE_TAG"
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

vrsify=false
if [[ "$instance_name" == *vrsify* ]]; then
    vrsify=true
    if [[ ! "${VRSIFY_MIN_RELEASE_DATE:-}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "A vrsify instance needs VRSIFY_MIN_RELEASE_DATE=YYYY-MM-DD, the earliest release it claims"
        exit 1
    fi
    # Set rather than defaulted: the release GitHub Action carries both over from the
    # jobs it deploys before this one
    file_format=vrsify
    clinvar_ingest_cmd=python,/app/vrsify-workflow.py
fi


# Disallow unset variables after they've been validated
set -u

clinvar_ingest_bucket="clinvar-ingest-dev"

region="us-east1"
project="${PROJECT:-clingen-dev}"
image_tag=workflow-py-${CLINVAR_INGEST_RELEASE_TAG}
cloudbuild=.cloudbuild/workflow-py.docker.cloudbuild.yaml
if $vrsify; then
    image_tag=vrsify-${CLINVAR_INGEST_RELEASE_TAG}
    cloudbuild=.cloudbuild/vrsify.docker.cloudbuild.yaml
fi
image=gcr.io/clingen-dev/clinvar-ingest:$image_tag
pipeline_service_account=clinvar-ingest-pipeline@clingen-dev.iam.gserviceaccount.com
# deployment_service_account=clinvar-ingest-deployment@clingen-dev.iam.gserviceaccount.com

################################################################
# Build the image

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
    --project="$project" \
    --substitutions="COMMIT_SHA=${image_tag}" \
    --config "$cloudbuild" \
    --gcs-log-dir=gs://${clinvar_ingest_bucket}/build/logs \
    archive.tar.gz

################################################################
# Deploy job
if gcloud run jobs list --project="$project" --region $region | awk '{print $2}' | grep "^$instance_name$"  ; then
    echo "Cloud Run Job $instance_name already exists - updating it"
    command="update"
else
    echo "Cloud Run Job $instance_name doesn't exist - creating it"
    command="create"
fi

env_vars="CLINVAR_INGEST_BUCKET=$clinvar_ingest_bucket"
env_vars="$env_vars,CLINVAR_INGEST_RELEASE_TAG=${CLINVAR_INGEST_RELEASE_TAG}"
if [ -n "$file_format" ]; then
    env_vars="$env_vars,file_format=${file_format}"
fi

if [ ! -v CLINVAR_INGEST_BQ_META_DATASET ]; then
    CLINVAR_INGEST_BQ_META_DATASET=clinvar_ingest
fi
env_vars="$env_vars,CLINVAR_INGEST_BQ_META_DATASET=${CLINVAR_INGEST_BQ_META_DATASET}"

if [ ! -v BQ_DEST_PROJECT ]; then
  BQ_DEST_PROJECT=clingen-dev
fi
env_vars="$env_vars,BQ_DEST_PROJECT=${BQ_DEST_PROJECT}"

# if instance_name contains stored-procedures make env vars
if [[ $instance_name =~ ^.*stored-procedures.*$ ]]; then
    # Resetting env_vars here - not inheriting previous
    env_vars="file_format=${file_format}"
    env_vars="$env_vars,BQ_DEST_PROJECT=${BQ_DEST_PROJECT}"
    env_vars="$env_vars,CLINVAR_INGEST_BQ_META_DATASET=${CLINVAR_INGEST_BQ_META_DATASET}"
    env_vars="$env_vars,CLINVAR_INGEST_RELEASE_TAG=${CLINVAR_INGEST_RELEASE_TAG}"
    # Destination bucket for the variation_identity export.
    # This overrides the .dev.env default baked into the container.
    if [ ! -v CLINVAR_GKS_BUCKET ]; then
      CLINVAR_GKS_BUCKET=clinvar-gkm
    fi
    env_vars="$env_vars,CLINVAR_GKS_BUCKET=${CLINVAR_GKS_BUCKET}"
fi

if $vrsify; then
    # Resetting env_vars here - not inheriting previous
    env_vars="file_format=${file_format}"
    env_vars="$env_vars,BQ_DEST_PROJECT=${BQ_DEST_PROJECT}"
    env_vars="$env_vars,CLINVAR_INGEST_BQ_META_DATASET=${CLINVAR_INGEST_BQ_META_DATASET}"
    env_vars="$env_vars,CLINVAR_INGEST_RELEASE_TAG=${CLINVAR_INGEST_RELEASE_TAG}"
    env_vars="$env_vars,CLINVAR_INGEST_LOCATION=${region}"
    env_vars="$env_vars,CLINVAR_GKS_BUCKET=${CLINVAR_GKS_BUCKET:-clinvar-gkm}"
    # The Batch jobs run the image the dispatcher runs
    env_vars="$env_vars,VRSIFY_IMAGE=${image}"
    env_vars="$env_vars,VRSIFY_MIN_RELEASE_DATE=${VRSIFY_MIN_RELEASE_DATE}"
    env_vars="$env_vars,VRSIFY_SLACK=${VRSIFY_SLACK:-false}"
fi

####
# gcloud project level labels must exist before use
####
metadata=`gcloud projects describe $project --format=json`
service=`echo $metadata | jq '.labels["service"]'`
component=`echo $metadata | jq '.labels["component"]'`
if [ $service == "null" || $component == "null" ]; then
  # set default labels on the project
  gcloud projects update $project --update-labels=service="service",component="component"
fi
#####

# stored-procedures must not retry on failure: a failure there almost always
# indicates a data issue that needs manual intervention, and retrying would
# just produce an avalanche of Slack failure messages and repeated partial
# mutations to cross-release temporal tables.
extra_args=()
if [[ "$instance_name" == *stored-procedures* ]]; then
    extra_args+=(--max-retries=0)
fi
# Nor the vrsify dispatcher: a release it failed keeps its started row for a person to
# resolve, and a failure before any claim is retried by the next scheduled run
if $vrsify; then
    extra_args+=(--max-retries=0)
fi

gcloud run jobs $command $instance_name \
      --project="$project" \
      --cpu=2 \
      --memory=8Gi \
      --task-timeout=24h \
      --image=$image \
      --region=$region \
      --command="$clinvar_ingest_cmd" \
      --service-account=$pipeline_service_account \
      --set-env-vars=$env_vars \
      --set-secrets=CLINVAR_INGEST_SLACK_TOKEN=clinvar-ingest-slack-token:latest \
      --labels=service="clinvar-ingest",component=$instance_name \
      "${extra_args[@]}"

if [[ $instance_name =~ ^.*bq-ingest.*$|^.*stored-procedures.*$ ]]; then
    # turn off file globbing
    set -f
    gcloud scheduler jobs ${command} http ${instance_name} \
      --project="$project" \
      --location ${region} \
      --uri=https://${region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${project}/jobs/${instance_name}:run \
      --http-method POST \
      --oauth-service-account-email=$pipeline_service_account \
      --schedule='*/15 * * * *'
fi

if $vrsify; then
    # Created paused, and left paused or running on later deploys; resume it with
    # gcloud scheduler jobs resume $instance_name --project $project --location $region
    set -f
    if gcloud scheduler jobs describe "$instance_name" --project="$project" --location "$region" > /dev/null 2>&1; then
        scheduler_command="update"
    else
        scheduler_command="create"
    fi
    gcloud scheduler jobs ${scheduler_command} http ${instance_name} \
      --project="$project" \
      --location ${region} \
      --uri=https://${region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${project}/jobs/${instance_name}:run \
      --http-method POST \
      --oauth-service-account-email=$pipeline_service_account \
      --schedule='*/15 * * * *'
    if [ "$scheduler_command" = "create" ]; then
        gcloud scheduler jobs pause "$instance_name" --project="$project" --location "$region"
    fi
fi
