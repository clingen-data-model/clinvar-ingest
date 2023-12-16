#!/usr/bin/env bash

set -xeuo pipefail

out_dir=bq_json_schemas

dataset=clingen-dev:asdf
dataset=clingen-stage:clinvar_2023_12_09_v1_6_61

mkdir -p $out_dir
#rm -rf $out_dir/*

# verify dataset exists
echo "Verifying dataset $dataset exists and listing tables..."
bq ls $dataset

for table_name in `bq ls $dataset | tail +3 | awk '{print $1}'`; do
    bq show --schema --format=prettyjson "${dataset}.${table_name}" > "${out_dir}/${table_name}.bq.json"
done
