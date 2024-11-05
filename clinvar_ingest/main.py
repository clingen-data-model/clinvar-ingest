import logging
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
