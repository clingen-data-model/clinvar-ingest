#!/bin/sh
# Local env for running workflow.py
# Example:
# source local-env.sh && python misc/bin/workflow.py
# source local-env.sh && python misc/bin/bq-ingester.py

export CLINVAR_INGEST_SLACK_TOKEN=''
export CLINVAR_INGEST_BUCKET=clinvar-ingest-dev
export CLINVAR_INGEST_RELEASE_TAG=local_dev
export BQ_DEST_PROJECT=clingen-dev
export CLINVAR_INGEST_SLACK_CHANNEL=''

# Smaller input
host=https://raw.githubusercontent.com
directory=/clingen-data-model/clinvar-ingest/main/test/data
name=OriginalTestDataSet.xml.gz
size=46719
last_modified=2023-10-07T15:47:16
released=2023-10-07T15:47:16
release_date=2023-10-07
file_format=vcv

# gs://clinvar-ingest/executions/clinvar_2024_06_03_v1_0_0_alpha/clinvar_xml/ClinVarVariationRelease_2024-0603.xml.gz
host=gs://clinvar-ingest
directory=/executions/clinvar_2024_06_03_v1_0_0_alpha/clinvar_xml/
name=ClinVarVariationRelease_2024-0603.xml.gz
size=46719
last_modified=2023-10-07T15:47:16
released=2023-10-07T15:47:16
release_date=2023-10-07
file_format=vcv

# Small test input in GCS
host=gs://clinvar-ingest
directory=/test-data
name=OriginalTestDataSet.xml.gz
size=46719
last_modified=2023-10-07T15:47:16
released=2023-10-07T15:47:16
release_date=2023-10-07
file_format=vcv

# VCV 2024-06-11 input in GCS
# gs://clinvar-ingest/executions/clinvar_2024_06_11_v1_0_0_alpha/clinvar_xml/ClinVarVariationRelease_2024-0611.xml.gz
host=gs://clinvar-ingest
directory=/executions/clinvar_2024_06_11_v1_0_0_alpha/clinvar_xml/
name=ClinVarVariationRelease_2024-0611.xml.gz
size=3907976797
last_modified=2024-06-11T12:00:00
released=2024-06-11T12:00:00
release_date=2024-06-11
file_format=vcv

# RCV 2024-06-10 input in GCS
# gs://clinvar-ingest/test-data/ClinVarRCVRelease_2024-0610.xml.gz
host=gs://clinvar-ingest
directory=/test-data
name=ClinVarRCVRelease_2024-0610.xml.gz
size=4342574098
last_modified=2024-06-10T12:00:00
released=2024-06-10T12:00:00
release_date=2024-06-10
file_format=rcv

export host
export directory
export name
export size
export last_modified
export released
export release_date
export file_format
