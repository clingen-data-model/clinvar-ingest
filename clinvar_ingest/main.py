import argparse
import sys
import coloredlogs
import json
import gzip

from clinvar_ingest.reader import read_clinvar_xml
from clinvar_ingest.model import dictify
from clinvar_ingest.fs import assert_mkdir


def get_open_file(d: dict, root_dir: str, label: str, suffix=".ndjson", mode="w"):
    """
    Takes a dictionary of labels to file handles. Opens a new file handle using
    label and suffix in root_dir if not already in the dictionary.
    """
    if label not in d:
        d[label] = open(f"{root_dir}/{label}{suffix}", mode, encoding="utf-8")
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
    assert_mkdir(output_directory)
    keep_going = {"value": True}
    try:
        with _open(input_filename) as f_in:
            for obj in read_clinvar_xml(
                f_in, keep_going=keep_going, disassemble=disassemble
            ):
                print(f"output: {str(obj)}")
                entity_type = obj.entity_type
                f_out = get_open_file(
                    open_output_files, root_dir=output_directory, label=entity_type
                )
                f_out.write(json.dumps(dictify(obj)))
                f_out.write("\n")
    except Exception as e:
        print("Exception caught in main function")
        raise e
    finally:
        print("Shutting down reader gracefully")
        keep_going["value"] = False
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


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-filename", "-i", required=True, type=str)
    parser.add_argument("--output-directory", "-o", required=True, type=str)
    parser.add_argument(
        "--no-disassemble",
        action="store_true",
        help="Disable splitting nested Model objects into separate outputs",
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
