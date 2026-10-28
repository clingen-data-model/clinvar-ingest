#!/usr/bin/env bash
#
# Build the workflow image with the vrsify dependencies, tagged
#   gcr.io/clingen-dev/clinvar-ingest:vrsify-<git commit>[-dirty]
# Deploys nothing: the Cloud Run jobs keep the images deploy-job.sh builds.
#
# Usage: misc/bin/build-vrsify-image.sh
#
# Environment overrides: project (default clingen-dev), clinvar_ingest_bucket (default
# clinvar-ingest-dev, where build logs go).

set -euo pipefail

project="${project:-clingen-dev}"
clinvar_ingest_bucket="${clinvar_ingest_bucket:-clinvar-ingest-dev}"

tag="$(git rev-parse --short=12 HEAD)"
if [ -n "$(git status --porcelain)" ]; then
    tag="${tag}-dirty"
fi

workdir="$(mktemp -d)"
trap 'rm -r "$workdir"' EXIT
tar --no-xattrs -c \
    clinvar_ingest \
    misc \
    Dockerfile-workflow-py \
    pyproject.toml \
    log_conf.json \
    .cloudbuild \
    | gzip --fast > "$workdir/archive.tar.gz"

gcloud builds submit \
    --project "$project" \
    --substitutions="COMMIT_SHA=${tag}" \
    --config .cloudbuild/vrsify.docker.cloudbuild.yaml \
    --gcs-log-dir="gs://${clinvar_ingest_bucket}/build/logs" \
    "$workdir/archive.tar.gz"

echo "Built gcr.io/${project}/clinvar-ingest:vrsify-${tag}"
