import json
import os
import subprocess

region = "us-east1"
# execute_job_script = "misc/bin/execute-job.sh"
# env_file = "./local-env.sh"
# ftp_watcher_file = "confluent-prod_clinvar-somatic-ftp-watcher_20241203.txt"
ftp_watcher_file = "misc/re-run/ftp-watcher-vcv-2024-10-27.txt"


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
    "instance_name": "clinvar-vcv-ingest",
    "CLINVAR_INGEST_SLACK_TOKEN": "",
    "CLINVAR_INGEST_SLACK_CHANNEL": "",
    "CLINVAR_INGEST_BUCKET": "clinvar-ingest-dev",
    "CLINVAR_INGEST_RELEASE_TAG": "v2_0_4_alpha",
    "CLINVAR_INGEST_BQ_META_DATASET": "clinvar_ingest",
    "BQ_DEST_PROJECT": "clingen-dev",
}

with open(ftp_watcher_file) as f:
    for line in f:
        # Each FTP Watcher message is an array of records
        watcher_records = json.loads(line)
        if not isinstance(watcher_records, list):
            raise ValueError("Expected a list of records: " + line)
        for record in watcher_records:
            print(f"Executing job for {record}")
            # cmd_args = ["bash", execute_job_script]
            env = {}
            env.update(global_env)
            env.update(record)

            env_str = ",".join([f"{k}='{v}'" for k, v in env.items()])

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
                print(f"Job failed for {record['file_name']}")
                print(e)
                break

            print(f"Job executed successfully for {json.dumps(record)}")
