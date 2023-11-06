import argparse
import sys
import coloredlogs
import json

from clinvar_ingest.reader import read_clinvar_xml
from clinvar_ingest.model import dictify
from clinvar_ingest.fs import assert_mkdir


def get_open_file(d: dict, root_dir: str, label: str, suffix=".ndjson", mode="w"):
    """ """
    if label not in d:
        d[label] = open(f"{root_dir}/{label}{suffix}", mode, encoding="utf-8")
    return d[label]


def main(argv=sys.argv[1:]):
    """
    Primary entrypoint function. Takes CLI arg vector excluding program name.
    """
    args = parse_args(argv)
    open_output_files = {}
    assert_mkdir(args.output_directory)
    try:
        with open(args.input_filename, "rb") as f_in:
            for obj in read_clinvar_xml(f_in, disassemble=not args.no_disassemble):
                print(f"output: {str(obj)}")

                entity_type = obj.entity_type
                f_out = get_open_file(
                    open_output_files, root_dir=args.output_directory, label=entity_type
                )

                f_out.write(json.dumps(dictify(obj)))
                f_out.write("\n")
    except Exception as e:
        print("Exception caught in main function")
        raise e
    finally:
        for f in open_output_files.values():
            f.close()


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


def _main(argv=sys.argv[1:]):
    """
    Used when executing main as a script.
    Initializes default configs.
    """
    coloredlogs.install(level="INFO")
    return main(argv)


if __name__ == "__main__":
    sys.exit(_main())
