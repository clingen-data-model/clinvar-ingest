# clinvar-ingest

Successor to https://github.com/DataBiosphere/clinvar-ingest

This project is for loading, processing, outputting, uploading relational-normalized ClinVar Variation Release XML files.


# Processing XML Files

The primary use case is processing ClinVar Variation Release XML files into relationally normalized, newline delimited JSON. These by default are outputted (as a file per model type) to a directory specified with `-o`.

```
$ clinvar-ingest -i test/data/OriginalTestDataSet.xml.gz -o outputs
```

# Uploading outputs to external databases

BigQuery is currently supported. There are 2 steps involved in this.

1) Upload table files to Google Cloud Storage.
2) Create External Tables that point to the uploaded GCS files.

Step (2) is provided via the clinvar_ingest installed CLIs.

```
$ clinvar-ingest-create-tables \
    --project clingen-dev \
    --dataset clinvar_ingest_new \
    --bucket clinvar-ingest
```
