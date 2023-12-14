import logging
import sys

import coloredlogs

from clinvar_ingest.cli import parse_args
from clinvar_ingest.cloud.bigquery.create_tables import run_create
from clinvar_ingest.cloud.gcs import copy_file_to_bucket
from clinvar_ingest.fs import find_files
from clinvar_ingest.parse import parse_and_write_files

_logger = logging.getLogger("clinvar-ingest")


def run_parse(args):
    """
    Primary entrypoint function. Takes CLI arg vector excluding program name.
    """
    output_files = parse_and_write_files(
        args.input_filename,
        args.output_directory,
        disassemble=not args.no_disassemble,
        jsonify_content=not args.no_jsonify_content,
    )
    print(output_files)


def run_upload(args):
    print(f"Uploading files to bucket: {args.destination_bucket}")

    file_paths = find_files(args.source_directory)

    dest_uri_prefix = f"gs://{args.destination_bucket}"
    if args.destination_prefix:
        dest_uri_prefix += "/" + args.destination_prefix

    for file_path in file_paths:
        s = args.source_directory + "/" + file_path
        d = dest_uri_prefix + "/" + file_path
        copy_file_to_bucket(s, d)


def run_cli(argv):
    """
    Primary entrypoint function for CLI args. Takes argv vector excluding program name.
    """
    args = parse_args(argv)
    if args.subcommand == "parse":
        return run_parse(args)
    elif args.subcommand == "upload":
        return run_upload(args)
    elif args.subcommand == "create-tables":
        return run_create(args)
    else:
        raise ValueError(f"Unknown subcommand: {args.subcommand}")


def main(argv=sys.argv[1:]):
    """
    Used when executing main as a script.
    Initializes default configs.
    """
    coloredlogs.install(level="INFO")
    return run_cli(argv)


if __name__ == "__main__":
    sys.exit(main())
