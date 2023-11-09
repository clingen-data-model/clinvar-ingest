import logging
import argparse
import sys
import coloredlogs
import json
import gzip

from clinvar_ingest.reader import read_clinvar_xml, get_clinvar_xml_releaseinfo
from clinvar_ingest.model import dictify
from clinvar_ingest.fs import assert_mkdir
from clinvar_ingest.cloud.gcs import copy_file_to_bucket

_logger = logging.getLogger(__name__)


def get_open_file(d: dict, root_dir: str, label: str, suffix=".ndjson", mode="w"):
    """
    Takes a dictionary of labels to file handles. Opens a new file handle using
    label and suffix in root_dir if not already in the dictionary.
    """
    if label not in d:
        filepath = f"{root_dir}/{label}{suffix}"
        _logger.info("Opening file for writing: %s", filepath)
        d[label] = open(filepath, mode, encoding="utf-8")
    return d[label]


def _open(filename: str):
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
    assert_mkdir(output_release_directory)

    try:
        with _open(input_filename) as f_in:
            for obj in read_clinvar_xml(f_in, disassemble=disassemble):
                # print(f"output: {str(obj)}")
                entity_type = obj.entity_type
                f_out = get_open_file(
                    open_output_files,
                    root_dir=output_release_directory,
                    label=entity_type,
                )
                f_out.write(json.dumps(dictify(obj)))
                f_out.write("\n")
    except Exception as e:
        print("Exception caught in main function")
        raise e
    finally:
        print("Closing output files")
        for f in open_output_files.values():
            f.close()

    return {k: v.name for k, v in open_output_files.items()}


def run(argv=sys.argv[1:]):
    """
    Primary entrypoint function. Takes CLI arg vector excluding program name.
    """
    args = parse_args(argv)
    assert_mkdir(args.output_directory)
    output_files = parse_and_write_files(
        args.input_filename, args.output_directory, disassemble=not args.no_disassemble
    )
    print(output_files)

    if args.upload_to_bucket:
        print(f"Uploading files to bucket: {args.upload_to_bucket}")
        for obj_type, output_file in output_files.items():
            copy_file_to_bucket(
                output_file, f"gs://{args.upload_to_bucket}/{output_file}"
            )


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-filename", "-i", required=True, type=str)
    parser.add_argument("--output-directory", "-o", required=True, type=str)
    parser.add_argument(
        "--no-disassemble",
        action="store_true",
        help="Disable splitting nested Model objects into separate outputs",
    )
    parser.add_argument(
        "--upload-to-bucket",
        type=str,
        help=(
            "If set, after files are written to output-directory, they "
            "will also be uploaded to this bucket"
        ),
    )
    return parser.parse_args(argv)


def main(argv=sys.argv[1:]):
    """
    Used when executing main as a script.
    Initializes default configs.
    """
    coloredlogs.install(level="INFO")
    return run(argv)


if __name__ == "__main__":
    sys.exit(main())
