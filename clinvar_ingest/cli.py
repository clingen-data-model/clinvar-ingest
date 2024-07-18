import argparse
import json


def parse_args(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        dest="subcommand", help="Subcommands", required=True
    )

    # PARSE
    parse_sp = subparsers.add_parser("parse")
    parse_sp.add_argument("--input-filename", "-i", required=True, type=str)
    parse_sp.add_argument("--output-directory", "-o", required=True, type=str)
    parse_sp.add_argument(
        "--gzip-output",
        action="store_true",
        help=(
            "Compress output files with GZIP. "
            "Set environment variable GZIP_COMPRESSLEVEL to set compression level (default: 9)"
        ),
    )
    parse_sp.add_argument(
        "--disassemble",
        type=bool,
        default=True,
        help="Split nested Model objects into separate output files (default: true)",
    )
    parse_sp.add_argument(
        "--jsonify-content",
        choices=["true", "false"],
        default="true",
        help="JSON encode content fields (default: true)",
    )
    parse_sp.add_argument(
        "--file-format",
        choices=["vcv", "rcv"],
        default="vcv",
        help="Format of input file (default: vcv)",
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

    # CREATE TABLES
    create_table_sp = subparsers.add_parser("create-tables")
    create_table_sp.add_argument("--destination-dataset", type=str, required=True)
    create_table_sp.add_argument("--source-table-paths", type=json.loads, required=True)

    return parser.parse_args(argv)
