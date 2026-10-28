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

`vrsify` reads a `variation_identity` export (`gs://clinvar-gkm/YYYY-MM-DD/dev/vi.jsonl.gz`) and writes one line per input record, `{"in": <record>, "out": <VRS object>}`, translating genomic expressions (SPDI, gnomAD, and HGVS `g.`/`m.`) with [vrs-python](https://github.com/ga4gh/vrs-python) against a [RefgetStore](https://static.ferriter.dev/refgetstore/) of genomic sequences. Records that are not translated get `{"errors": ...}` saying why, as failed translations do: transcript and protein expressions, and Allele or copy number records with no source expression. Records of classes vrsify does not translate (`Unknown`, `Not Available`, `Haplotype`) get a null `out`. Translation uses only the RefgetStore: UTA and NCBI sequence fetching are disabled in the translation workers, so a record whose translation would need either, or a sequence missing from the store, gets an `errors` row. A `.provenance.json` file is written beside the output.

It needs the `vrsify` optional dependencies, which build [gtars](https://github.com/databio/gtars) from source and so need a Rust toolchain: `pip install '.[vrsify]'`.

```
clinvar-ingest vrsify --release-date 2026-10-24
```

writes `gs://clinvar-gkm/2026-10-24/dev/vi-vrs-python.jsonl.gz`. By default the RefgetStore is the published genomic store archive, downloaded and checked against its SHA-256; `--refget-store` takes a local store directory instead. Existing outputs are not replaced without `--overwrite`.

`clinvar-ingest vrsify-submit 2026-10-24 --image gcr.io/clingen-dev/clinvar-ingest:vrsify-<release tag>` runs it on Cloud Batch, outside the pipeline and without touching `processing_history` (`--dry-run` prints the job definition). Jobs run on a c4-standard-8 VM with a `hyperdisk-balanced` boot disk, where a full release translates in about 3 minutes.

## In the pipeline

The `vrsify` step follows the stored procedures step. Its Cloud Run job, the dispatcher (`misc/bin/vrsify-workflow.py`), runs every 15 minutes. It claims the next release whose `sp` row in `processing_history` is finished and that has no `vrsify` row, on or after `VRSIFY_MIN_RELEASE_DATE`, and takes it through to the end in the same execution:

1. writes the release's `vrsify` started row;
2. waits up to an hour for the release's `vi.jsonl.gz` to have been written after its `sp` row finished. The stored procedures job exports `vi.jsonl.gz` after marking its row finished, and an export from an earlier run of the release may still be in place;
3. submits a Batch job that runs `clinvar-ingest vrsify --release-date ...`, and waits for it;
4. once the job has succeeded and its provenance names that job and the release's export, writes the finished row, with the Batch job's name in `parsed_files`.

A failure after the claim is reported once, with the statement that deletes the started row, and the row stays started: later executions only claim ready releases, so the release waits for a person, as a failed stored procedures release does. Once the row is deleted, the next execution claims the release again. A failure before any claim fails that execution without a Slack message: no row is left started, and the next execution retries. The dispatcher only writes `processing_history` rows whose `file_type` is `vrsify`, and reads the table directly rather than through `processing_history_view`.

`deploy-job.sh` deploys it like the other jobs, with an `instance_name` containing `vrsify` (see the script). It builds the image with the `vrsify` dependencies as `vrsify-<release tag>`, next to the other jobs' `workflow-py-<release tag>`, and the Batch jobs run that same image. `VRSIFY_MIN_RELEASE_DATE` is a fixed cutoff chosen at deployment: releases before it were translated outside this pipeline. Its Cloud Scheduler job is created paused.

With `VRSIFY_SLACK=true`, posts go to the `CLINVAR_INGEST_SLACK_CHANNEL` channel: the dispatcher reports a release scheduled, completed, or not retried, and the Batch job reports starting, writing its output, and its own errors. The Batch job reads the token from the `clinvar-ingest-slack-token` secret at run time, so its value is not part of the job definition.
