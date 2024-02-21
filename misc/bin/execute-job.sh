#!/bin/bash

set -xeo pipefail

if [ -z "$branch" ]; then
    branch=$(git rev-parse --abbrev-ref HEAD)
else
    echo "branch set in environment"
fi

if [ "$JOB_WAIT" == "1" ]; then
    wait_opt="--wait"
else
    wait_opt="--async" # the default
fi

set -u

branch=$(git rev-parse --abbrev-ref HEAD)
commit=$(git rev-parse HEAD)
echo "Branch: $branch"
echo "Commit: $commit"

job_name="clinvar-ingest-${branch}"
region="us-central1"

# Global Variables
CLINVAR_INGEST_BUCKET="clinvar-ingest"


# Larger input
host="https://ftp.ncbi.nlm.nih.gov"
directory="/pub/clinvar/xml/VCV_xml_old_format"
name="ClinVarVariationRelease_2024-02.xml.gz"
size=3298023159
last_modified="2024-01-07T15:47:16"
released="2024-02-01T15:47:16"
release_date="2024-02-01"


# Smaller input
host=https://raw.githubusercontent.com
directory=/clingen-data-model/clinvar-ingest/main/test/data
name=OriginalTestDataSet.xml.gz
size=46719
last_modified=2023-10-07T15:47:16
released=2023-10-07T15:47:16
release_date=2023-10-07


env_vars="CLINVAR_INGEST_BUCKET=$CLINVAR_INGEST_BUCKET"
env_vars="$env_vars,host=$host"
env_vars="$env_vars,directory=$directory"
env_vars="$env_vars,name=$name"
env_vars="$env_vars,size=$size"
env_vars="$env_vars,last_modified=$last_modified"
env_vars="$env_vars,released=$released"
env_vars="$env_vars,release_date=$release_date"

gcloud run jobs execute $job_name \
    --region $region \
    $wait_opt \
    --update-env-vars=$env_vars
