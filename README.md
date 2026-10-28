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

# Translating variation_identity expressions to VRS (vrsify)

`vrsify` reads a `variation_identity` export (`gs://clinvar-gkm/YYYY-MM-DD/dev/vi.jsonl.gz`) and writes one line per input record, `{"in": <record>, "out": <VRS object>}`, translating genomic expressions (SPDI, gnomAD, and HGVS `g.`/`m.`) with [vrs-python](https://github.com/ga4gh/vrs-python) against a [RefgetStore](https://static.ferriter.dev/refgetstore/) of genomic sequences. Records that are not translated get `{"errors": ...}` saying why, as failed translations do: transcript and protein expressions, and Allele or copy number records with no source expression. Records of classes vrsify does not translate (`Unknown`, `Not Available`, `Haplotype`) get a null `out`. A `.provenance.json` file is written beside the output.

It needs the `vrsify` optional dependencies, which build [gtars](https://github.com/databio/gtars) from source and so need a Rust toolchain: `pip install '.[vrsify]'`.

```
clinvar-ingest vrsify --release-date 2026-10-24
```

writes `gs://clinvar-gkm/2026-10-24/dev/vi-vrs-python.jsonl.gz`. By default the RefgetStore is the published genomic store archive, downloaded and checked against its SHA-256; `--refget-store` takes a local store directory instead. Existing outputs are not replaced without `--overwrite`.

To run it on Cloud Batch, build an image with the `vrsify` dependencies, then submit a job with it:

```
misc/bin/build-vrsify-image.sh
misc/bin/submit-vrsify-batch.sh 2026-10-24 --image gcr.io/clingen-dev/clinvar-ingest:vrsify-<commit>
```
