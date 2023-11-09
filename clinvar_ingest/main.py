import logging
import argparse
import sys
import coloredlogs
import json
import gzip

from clinvar_ingest.reader import read_clinvar_xml, get_clinvar_xml_releaseinfo
from clinvar_ingest.model import dictify
from clinvar_ingest.fs import assert_mkdir, find_files
from clinvar_ingest.cloud.gcs import copy_file_to_bucket

_logger = logging.getLogger(__name__)


def get_open_file(d: dict, root_dir: str, label: str, suffix=".ndjson", mode="w"):
    """
    Takes a dictionary of labels to file handles. Opens a new file handle using
    label and suffix in root_dir if not already in the dictionary.
    """
    if label not in d:
        label_dir = f"{root_dir}/{label}"
        assert_mkdir(root_dir)
        assert_mkdir(label_dir)
        filepath = f"{label_dir}/{label}{suffix}"
        _logger.info("Opening file for writing: %s", filepath)
        d[label] = open(filepath, mode, encoding="utf-8")
    return d[label]


def _open(filename: str):
    """
    Opens a file with path `filename`. If `filename` ends in .gz, opens as gzip.
    """
    if filename.endswith(".gz"):
        return gzip.open(filename)
    else:
        return open(filename, "rb")


def parse_and_write_files(
    input_filename: str, output_directory: str, disassemble=True
) -> list:
    """
    Parses input file, writes outputs to output directory.

    Returns the dict of types to their output files.
    """
    open_output_files = {}
    with _open(input_filename) as f_in:
        releaseinfo = get_clinvar_xml_releaseinfo(f_in)
        release_date = releaseinfo["release_date"]
        print(f"Parsing release date: {release_date}")

    # Release directory is within the output directory
    output_release_directory = f"{output_directory}/{release_date}"

    try:
        with _open(input_filename) as f_in:
            for obj in read_clinvar_xml(f_in, disassemble=disassemble):
                entity_type = obj.entity_type
                f_out = get_open_file(
                    open_output_files,
                    root_dir=output_release_directory,
                    label=entity_type,
                )
                f_out.write(json.dumps(dictify(obj)))
                f_out.write("\n")
    except Exception as e:
        print("Exception caught in parse_and_write_files")
        raise e
    finally:
        print("Closing output files")
        for f in open_output_files.values():
            f.close()

    return {k: v.name for k, v in open_output_files.items()}


def run_parse(args):
    """
    Primary entrypoint function. Takes CLI arg vector excluding program name.
    """
    assert_mkdir(args.output_directory)
    output_files = parse_and_write_files(
        args.input_filename, args.output_directory, disassemble=not args.no_disassemble
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


def parse_args(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand", help="Subcommands")

    # PARSE
    parse_sp = subparsers.add_parser("parse")
    parse_sp.add_argument("--input-filename", "-i", required=True, type=str)
    parse_sp.add_argument("--output-directory", "-o", required=True, type=str)
    parse_sp.add_argument(
        "--no-disassemble",
        action="store_true",
        help="Disable splitting nested Model objects into separate outputs",
    )

    # UPLOAD
    upload_sp = subparsers.add_parser("upload")
    upload_sp.add_argument(
        "--destination-bucket",
        "-d",
        required=True,
        type=str,
        help="Bucket to upload directory to",
    )
    upload_sp.add_argument(
        "--destination-prefix",
        "-p",
        type=str,
        default=None,
        help="Prefix in bucket to place uploaded directory under",
    )
    upload_sp.add_argument(
        "--source-directory",
        "-s",
        type=str,
        required=True,
        help="Local directory to upload",
    )

    return parser.parse_args(argv)


def run_cli(argv):
    """
    Primary entrypoint function for CLI args. Takes argv vector excluding program name.
    """
    args = parse_args(argv)
    if args.subcommand == "parse":
        return run_parse(args)
    elif args.subcommand == "upload":
        return run_upload(args)


def main(argv=sys.argv[1:]):
    """
    Used when executing main as a script.
    Initializes default configs.
    """
    coloredlogs.install(level="INFO")
    return run_cli(argv)


if __name__ == "__main__":
    sys.exit(main())
