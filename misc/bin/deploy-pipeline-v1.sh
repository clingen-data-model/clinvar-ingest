#!/bin/bash

####
# This script deploys the copy-only job we are using
# for the old XML files as long as they continue to be
# produced. This ensures we have a copy of the files
# even if we are no longer parsing and ingesting into BQ.
####

set -xeuo pipefail

deploy_script=$(pwd)/misc/bin/deploy-job-main.sh

git clone git@github.com:clingen-data-model/clinvar-ingest.git clinvar-ingest-repo-tmp

cd clinvar-ingest-repo-tmp

# Switch to code revision v1_0_0
git switch -C branch-v1_0_0 v1_0_0


# Deploy the copy-only job for v1
export CLINVAR_INGEST_RELEASE_TAG=v1_0_0
export CLINVAR_INGEST_BQ_META_DATASET=clinvar_ingest_v1
export instance_name=clinvar-ingest-copy-only-v1
export file_format=vcv
export clinvar_ingest_cmd=python,/app/workflow-copy-only.py

bash "$deploy_script"

cd ..
rm -rf clinvar-ingest-repo-tmp
