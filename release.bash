#!/usr/bin/env bash
LATEST_COMMIT=$(git rev-parse --short HEAD)
gcloud builds submit --substitutions="COMMIT_SHA=$LATEST_COMMIT,CUSTOM_BRANCH_TAG=latest" --config .cloudbuild/docker.cloudbuild.yaml .

# LATEST_COMMIT=$(git rev-parse --short HEAD)
# docker build . --platform=linux/amd64 -t gcr.io/clingen-dev/clinvar-ingest:latest -t gcr.io/clingen-dev/clinvar-ingest:${LATEST_COMMIT}
# docker push gcr.io/clingen-dev/clinvar-ingest:latest
# docker push gcr.io/clingen-dev/clinvar-ingest:${LATEST_COMMIT}
#