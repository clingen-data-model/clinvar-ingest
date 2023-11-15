# clinvar-ingest

Successor to https://github.com/DataBiosphere/clinvar-ingest

This project is for loading, processing, outputting, uploading relational-normalized ClinVar Variation Release XML files.


# Parse input files

The primary use case is processing ClinVar Variation Release XML files into relationally normalized, newline delimited JSON. By default a file per model type is written to a directory specified with `-o`, within a directory with the `ReleaseDate` attribute from the top level of the XML file.

```
$ clinvar-ingest parse \
    -i test/data/OriginalTestDataSet.xml.gz \
    -o outputs
```

The above command creates an `outputs` directory, and in this case also a `2023-10-07` directory in that, because that is the release date in this XML file.

# Uploading outputs to Google Cloud Storage (GCS) bucket

While these files are useful on their own, it is useful to have them in a cloud storage bucket, which also enables creating BigQuery external tables.

The source directory `outputs/2023-10-07` in this example is the same one written above in the parsing step.

The destination bucket must be some already-existing bucket. The destination prefix can be anything, and refers to the 'directory' the file tree under local source directory `./outputs/2023-10-07` will be written into in the bucket. It may make sense to include the source directory as the suffix if the destination prefix. As a minimum, the release date directory should be included to anchor all the files to a specific release.

```
clinvar-ingest upload \
    --source-directory outputs/2023-10-07 \
    --destination-bucket clinvar-ingest \
    --destination-prefix outputs/2023-10-07
```

# Creating external database tables

BigQuery is currently supported.

The tables should be created to point to the same bucket and destination prefix as uploaded above. The directory tree under `--path` is expected to match the `<entity_type>/*` structure where each entity_type has a directory that contains some number of newline-delimited JSON files.

```
$ clinvar-ingest create-tables \
    --project clingen-dev \
    --dataset clinvar_ingest_new \
    --bucket clinvar-ingest \
    --path outputs/2023-10-07
```
