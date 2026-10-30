import logging
import os
import sys
from argparse import Namespace

import coloredlogs

from clinvar_ingest.api.model.requests import CreateExternalTablesRequest
from clinvar_ingest.cli import parse_args
from clinvar_ingest.cloud.bigquery.create_tables import run_create_external_tables
from clinvar_ingest.cloud.gcs import copy_file_to_bucket
from clinvar_ingest.fs import find_files
from clinvar_ingest.parse import parse_and_write_files

_logger = logging.getLogger("clinvar_ingest")


def run_parse(args: Namespace):
    """
    Primary entrypoint function. Takes CLI arg vector excluding program name.
    """
    output_files = parse_and_write_files(
        args.input_filename,
        args.output_directory,
        gzip_output=args.gzip_output,
        disassemble=args.disassemble,
        jsonify_content=args.jsonify_content == "true",
        file_format=args.file_format,
    )
    print(output_files)


def run_upload(args: Namespace):
    print(f"Uploading files to bucket: {args.destination_bucket}")

    file_paths = find_files(args.source_directory)

    dest_uri_prefix = f"gs://{args.destination_bucket}"
    if args.destination_prefix:
        dest_uri_prefix += "/" + args.destination_prefix

    for file_path in file_paths:
        s = args.source_directory + "/" + file_path
        d = dest_uri_prefix + "/" + file_path
        copy_file_to_bucket(s, d)


def run_vrsify(args: Namespace):
    # Imported here: vrsify needs the optional `vrsify` dependencies
    from clinvar_ingest.config import env_or_dotenv_or
    from clinvar_ingest.vrsify.runner import DEFAULT_REFGET_STORE, default_uris, vrsify

    input_uri, output_uri = args.input_uri, args.output_uri
    if args.release_date:
        default_input, default_output = default_uris(args.release_date)
        input_uri = input_uri or default_input
        output_uri = output_uri or default_output
    if not input_uri or not output_uri:
        raise ValueError("vrsify needs --release-date, or both --input-uri and --output-uri")

    if args.seqrepo and args.refget_store:
        raise ValueError("Pass --seqrepo or --refget-store, not both")

    if args.slack:
        # Slack messages take their token and channel from the environment
        from clinvar_ingest.config import get_vrsify_env

        get_vrsify_env()

    return vrsify(
        input_uri=input_uri,
        output_uri=output_uri,
        refget_store=args.refget_store or env_or_dotenv_or("CLINVAR_INGEST_REFGET_STORE", default=DEFAULT_REFGET_STORE),
        workers=args.workers or os.cpu_count(),
        batch_size=args.batch_size,
        record_timeout=args.record_timeout,
        overwrite=args.overwrite,
        work_dir=args.work_dir,
        slack=args.slack,
        release_date=args.release_date,
        seqrepo=args.seqrepo,
        refget_reads=args.refget_reads,
    )


def run_vrsify_submit(args: Namespace):
    import json

    from clinvar_ingest.config import env_or_dotenv_or
    from clinvar_ingest.vrsify import batch

    settings = batch.BatchSettings(
        project=args.project,
        region=args.region,
        service_account=args.service_account,
        image=args.image,
        environment={"CLINVAR_GKS_BUCKET": env_or_dotenv_or("CLINVAR_GKS_BUCKET", default="clinvar-gkm")},
    )
    if args.machine_type:
        settings.machine_type = args.machine_type
    if args.boot_disk_type:
        settings.boot_disk_type = args.boot_disk_type
    if args.boot_disk_size:
        settings.boot_disk_size_gb = args.boot_disk_size
    labels = {"release-date": args.release_date, "trigger": "manual"}
    for label in args.label:
        key, sep, value = label.partition("=")
        if not sep:
            raise ValueError(f"--label takes KEY=VALUE: {label}")
        labels[key] = value
    definition = batch.job_definition(settings, ["--release-date", args.release_date, *args.vrsify_args], labels=labels)
    name = batch.job_id(args.release_date)
    if args.dry_run:
        print(f"# job {name} in {settings.project}/{settings.region}")
        print(json.dumps(definition, indent=2))
        return
    batch.submit(settings, name, definition)
    print(
        f"Follow it with: gcloud batch jobs describe {name} --project {settings.project} --location {settings.region}"
    )


def run_cli(argv: list[str]):
    """
    Primary entrypoint function for CLI args. Takes argv vector excluding program name.
    """
    args = parse_args(argv)
    if args.subcommand == "parse":
        return run_parse(args)
    if args.subcommand == "upload":
        return run_upload(args)
    if args.subcommand == "create-tables":
        req = CreateExternalTablesRequest(**vars(args))
        resp = run_create_external_tables(req)
        return {entity_type: table.full_table_id for entity_type, table in resp.items()}
    if args.subcommand == "vrsify":
        run_vrsify(args)
        return None
    if args.subcommand == "vrsify-submit":
        run_vrsify_submit(args)
        return None
    raise ValueError(f"Unknown subcommand: {args.subcommand}")


def main(argv=sys.argv[1:]):
    """
    Used when executing main as a script.
    Initializes default configs.
    """
    coloredlogs.install(level="INFO")
    return run_cli(argv)


if __name__ == "__main__":
    main()
