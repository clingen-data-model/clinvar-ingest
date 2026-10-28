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
        help="RefgetStore directory or .tar.gz archive URL (default: CLINVAR_INGEST_REFGET_STORE, else the published genomic store)",
    )
    vrsify_sp.add_argument("--workers", type=int, default=None, help="Worker processes (default: CPU count)")
    vrsify_sp.add_argument("--batch-size", type=int, default=50, help="Records per worker task (default: 50)")
    vrsify_sp.add_argument(
        "--record-timeout", type=int, default=300, help="Seconds before a record becomes an error (default: 300)"
    )
    vrsify_sp.add_argument("--overwrite", action="store_true", help="Replace existing output objects")
    vrsify_sp.add_argument("--work-dir", type=str, default=None, help="Local working directory (default: a temp dir)")
    vrsify_sp.add_argument(
        "--slack", action="store_true", help="Post start, finish and failure messages to Slack (off by default)"
    )
    vrsify_sp.add_argument(
        "--input-newer-than",
        type=str,
        default=None,
        help="ISO 8601 timestamp; wait for a gs:// input written after it before starting",
    )
    vrsify_sp.add_argument(
        "--input-wait",
        type=int,
        default=3600,
        help="Seconds to wait for --input-newer-than before failing (default: 3600)",
    )
    vrsify_sp.add_argument(
        "--history-bucket-dir",
        type=str,
        default=None,
        help=(
            "Mark this release's vrsify row in processing_history finished once the output is written. "
            "The bucket_dir of the row, which the dispatcher wrote when it claimed the release."
        ),
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
    submit_sp.add_argument("--machine-type", type=str, default=None, help="(default: c4-standard-8)")
    submit_sp.add_argument(
        "--boot-disk-type",
        type=str,
        default=None,
        help="Where the store is extracted (default: hyperdisk-balanced, which c4 requires)",
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
