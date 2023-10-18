import click
import xmltodict
import argparse
import sys
import coloredlogs

from clinvar_ingest.reader import read_clinvar_xml

# @click.command()
# @click.option("--input-filename", "-i", required=True, type=click.Path())
# @click.option("--output-filename", "-o", required=True, type=click.Path())
# def parse(input_filename: click.Path, output_filename: click.Path):


def main(argv):
    args = parse_args(argv)
    with open(args.input_filename, "rb") as f_in:
        with open(args.output_filename, "wb") as f_out:
            for obj in read_clinvar_xml(f_in):
                print(f"output: " + obj)


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-filename", "-i", required=True, type=str)
    parser.add_argument("--output-filename", "-o", required=True, type=str)
    return parser.parse_args(argv)


if __name__ == "__main__":
    coloredlogs.install(level="INFO")
    main(sys.argv[1:])
