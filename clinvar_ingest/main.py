import argparse
import sys
import coloredlogs
import json

from clinvar_ingest.reader import read_clinvar_xml
from clinvar_ingest.model import dictify


def main(argv=sys.argv[1:]):
    args = parse_args(argv)
    with open(args.input_filename, "rb") as f_in:
        with open(args.output_filename, "wb") as f_out:
            for obj in read_clinvar_xml(f_in):
                print(f"output: {str(obj)}")
                f_out.write(json.dumps(dictify(obj)).encode("utf-8"))
                f_out.write("\n".encode("utf-8"))


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-filename", "-i", required=True, type=str)
    parser.add_argument("--output-filename", "-o", required=True, type=str)
    return parser.parse_args(argv)


def _main(argv=sys.argv[1:]):
    """
    Used when executing main as a script
    """
    coloredlogs.install(level="INFO")
    return main(argv)


if __name__ == "__main__":
    sys.exit(_main())
