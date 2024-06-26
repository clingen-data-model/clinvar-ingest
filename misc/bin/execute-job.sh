#!/bin/bash

########

# Example:
# release_tag=local_dev instance_name=clinvar-ingest-local-dev CLINVAR_INGEST_SLACK_CHANNEL='' bash misc/bin/execute-job.shz
########


set -xeo pipefail
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

if [ "$JOB_WAIT" == "1" ]; then
    wait_opt="--wait"
else
    wait_opt="--async" # the default
fi

set -u

job_name=$instance_name
region="us-east1"

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

# Larger input 2024-06-03
host="https://ftp.ncbi.nlm.nih.gov"
directory="/pub/clinvar/xml/VCV_xml_old_format"
name=ClinVarVariationRelease_2024-0603.xml.gz
size=3910466672
last_modified='2024-06-04T09:09:59'
released='2024-06-04T09:09:59'
release_date='2024-06-03'

# Smaller input
host=https://raw.githubusercontent.com
directory=/clingen-data-model/clinvar-ingest/main/test/data
name=OriginalTestDataSet.xml.gz
size=46719
last_modified=2023-10-07T15:47:16
released=2023-10-07T15:47:16
release_date=2023-10-07


# Small test input in GCS
# host=gs://clinvar-ingest
# directory=test-data
# name=OriginalTestDataSet.xml.gz
# size=46719
# last_modified=2023-10-07T15:47:16
# released=2023-10-07T15:47:16
# release_date=2023-10-07

env_vars="CLINVAR_INGEST_BUCKET=$CLINVAR_INGEST_BUCKET"
env_vars="$env_vars,CLINVAR_INGEST_SLACK_CHANNEL=$CLINVAR_INGEST_SLACK_CHANNEL"
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
