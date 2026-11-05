import argparse
import json

from clinvar_ingest.vrsify import defaults as vrsify_defaults


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

    # VRSIFY
    vrsify_sp = subparsers.add_parser(
        "vrsify", help="Translate a variation_identity export's genomic expressions to VRS objects"
    )
    vrsify_sp.add_argument(
        "--release-date",
        type=str,
        help="YYYY-MM-DD; sets the default --input-uri and --output-uri under the CLINVAR_GKS_BUCKET bucket",
    )
    vrsify_sp.add_argument(
        "--input-uri", type=str, help="gs:// URI or local path of vi.jsonl.gz input, glob characters allowed"
    )
    vrsify_sp.add_argument("--output-uri", type=str, help="gs:// URI or local path of the .jsonl.gz output")
    vrsify_sp.add_argument(
        "--refget-store",
        type=str,
        default=None,
        help=(
            "RefgetStore directory, .tar.gz archive URL, or remote store URL, from which the sequences the input "
            "reads are fetched into a local cache before translating "
            "(default: CLINVAR_INGEST_REFGET_STORE; one of the two, or --seqrepo, is required)"
        ),
    )
    vrsify_sp.add_argument(
        "--refget-reads",
        choices=vrsify_defaults.REFGET_READS,
        default=vrsify_defaults.REFGET_READS_DEFAULT,
        help=(
            "whole: each worker loads every sequence it reads into memory. ranges: sequences stay on local disk "
            "and each read takes just the bases it covers, which the workers' shared file cache serves "
            "(default: %(default)s)"
        ),
    )
    vrsify_sp.add_argument(
        "--refget-prefetch-threads",
        type=int,
        default=vrsify_defaults.PREFETCH_THREADS,
        help=(
            "For a remote store: how many sequence files to fetch at a time before translating "
            "(default: %(default)s)"
        ),
    )
    vrsify_sp.add_argument(
        "--seqrepo",
        type=str,
        default=None,
        help="Read sequences from this SeqRepo instance directory or .tar.gz archive URL instead of a RefgetStore",
    )
    vrsify_sp.add_argument("--workers", type=int, default=None, help="Worker processes (default: CPU count)")
    vrsify_sp.add_argument(
        "--batch-size",
        type=int,
        default=vrsify_defaults.BATCH_SIZE,
        help="Records per worker task (default: %(default)s)",
    )
    vrsify_sp.add_argument(
        "--record-timeout",
        type=int,
        default=vrsify_defaults.RECORD_TIMEOUT_SECONDS,
        help="Seconds before a record becomes an error (default: %(default)s)",
    )
    vrsify_sp.add_argument("--overwrite", action="store_true", help="Replace existing output objects")
    vrsify_sp.add_argument("--work-dir", type=str, default=None, help="Local working directory (default: a temp dir)")
    vrsify_sp.add_argument(
        "--slack", action="store_true", help="Post start, finish and failure messages to Slack (off by default)"
    )

    # VRSIFY-SUBMIT
    submit_sp = subparsers.add_parser("vrsify-submit", help="Submit a Cloud Batch job that runs vrsify for a release")
    submit_sp.add_argument("release_date", type=str, help="YYYY-MM-DD")
    submit_sp.add_argument("--image", type=str, required=True, help="Image with the vrsify dependencies")
    submit_sp.add_argument("--project", type=str, default="clingen-dev")
    submit_sp.add_argument("--region", type=str, default="us-east1")
    submit_sp.add_argument(
        "--service-account", type=str, default="clinvar-ingest-pipeline@clingen-dev.iam.gserviceaccount.com"
    )
    submit_sp.add_argument(
        "--machine-type", type=str, default=None, help=f"(default: {vrsify_defaults.BATCH_MACHINE_TYPE})"
    )
    submit_sp.add_argument(
        "--boot-disk-type",
        type=str,
        default=None,
        help="Where the store is extracted (default: hyperdisk-balanced, which c4 requires)",
    )
    submit_sp.add_argument(
        "--boot-disk-size",
        type=int,
        default=None,
        help=f"Boot disk size in GB (default: {vrsify_defaults.BATCH_BOOT_DISK_SIZE_GB})",
    )
    submit_sp.add_argument(
        "--label",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="An extra GCP label for the job and its VM and disk, e.g. experiment=store-comparison; repeatable",
    )
    submit_sp.add_argument("--dry-run", action="store_true", help="Print the job definition instead of submitting")
    submit_sp.epilog = "Arguments after -- are passed to `clinvar-ingest vrsify`, e.g. -- --overwrite"

    # Split off vrsify-submit's pass-through arguments: argparse.REMAINDER after a
    # positional would also take the options that follow it
    vrsify_args = []
    if argv[:1] == ["vrsify-submit"] and "--" in argv:
        split = argv.index("--")
        argv, vrsify_args = argv[:split], argv[split + 1 :]
    args = parser.parse_args(argv)
    if args.subcommand == "vrsify-submit":
        args.vrsify_args = vrsify_args
    return args
