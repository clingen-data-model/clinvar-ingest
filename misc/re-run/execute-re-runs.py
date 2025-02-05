"""
Execute a cloud run job across a series of ClinVar FTP Watcher messages.
(messages can also point to GCS files)

NOTE: Must update global variables at the top of the script to match the job, file, and file format.

Usage:
    python execute-re-runs.py
"""

import json
import os
import subprocess

region = "us-east1"
bq_meta_dataset = "clinvar_ingest"


instance_name = "clinvar-rcv-ingest"
# ftp_watcher_file = "misc/re-run/ftp-watcher-rcv-2024-01-26.txt"
# ftp_watcher_file = "misc/re-run/prior_xml_archives-ftp-watcher-rcv-no1-26.txt"
ftp_watcher_file = "misc/re-run/prior_xml_archives-ftp-watcher-rcv.txt"
file_format = "rcv"


# instance_name = "clinvar-vcv-ingest"
# ftp_watcher_file = "misc/re-run/ftp-watcher-vcv-2024-01-26.txt"
# ftp_watcher_file = "misc/re-run/prior_xml_archives-ftp-watcher-vcv.txt"
# file_format = "vcv"


def run_command(
    cmd_array,
    # env_dict=None
):
    """
    Run a command with optional environment variables, capture output, and handle errors.

    Args:
        cmd_array (list): Command and arguments as a list
        env_dict (dict, optional): Environment variables to set

    Raises:
        subprocess.CalledProcessError: If the command returns non-zero exit status
    """
    # Run process with pipe for output
    with subprocess.Popen(  # noqa: S603
        cmd_array,
        # env=env_dict,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,  # Use text mode instead of universal_newlines
        bufsize=1,  # Line buffered
    ) as process:
        # Print output in real-time
        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break
            if line:
                print(line.rstrip())

        # Check return code and raise error if non-zero
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd_array)

        return process.returncode


global_env = {
    "instance_name": instance_name,  # Cloud run job name
    # TODO can't override this because the one in the job isn't a string it's a secret manager ref
    # this is okay because the channel also needs to be set in the job to send messages and we
    # set that to empty string here
    # "CLINVAR_INGEST_SLACK_TOKEN": "",  # Override job variable to disable messaging
    "CLINVAR_INGEST_SLACK_CHANNEL": "",  # Override job variable to disable messaging
    "CLINVAR_INGEST_BUCKET": "clinvar-ingest-dev",  # Already set on job, no need to override
    # "CLINVAR_INGEST_RELEASE_TAG": "v2_0_4_alpha",  # Already set on job, no need to override
    "CLINVAR_INGEST_BQ_META_DATASET": bq_meta_dataset,  # Already set on job, no need to override
    "BQ_DEST_PROJECT": "clingen-dev",
    "file_format": file_format,  # Set already on job but provide again for explicitness
}

with open(ftp_watcher_file) as f:
    for line in f:
        # Each FTP Watcher message is an array of records
        watcher_records = json.loads(line)
        if not isinstance(watcher_records, list):
            raise ValueError("Expected a list of records: " + line)
        for record in watcher_records:
            print(f"Executing job for {record}")
            env = {}
            env.update(global_env)
            env.update(record)

            env_str = ",".join([f"{k}={v}" for k, v in env.items()])
            print(f"Environment variable update string: {env_str}")

            # Same command as in execute-job.sh
            cmd_args = [
                "gcloud",
                "run",
                "jobs",
                "execute",
                global_env["instance_name"],
                "--region",
                region,
                "--async",
                "--update-env-vars",
                env_str,
            ]

            # Execute the job with subprocess.popen
            try:
                run_command(cmd_args)
            except subprocess.CalledProcessError as e:
                print(f"Job failed for {record}")
                print(e)
                break

            print(f"Job executed successfully for {json.dumps(record)}")
