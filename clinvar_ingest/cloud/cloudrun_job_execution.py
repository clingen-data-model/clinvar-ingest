import logging
from typing import Any

from google.cloud import run_v2

_logger = logging.getLogger("clinvar_ingest")


def invoke_job(project_id: str, location: str, job_name: str, **env_vars: dict) -> Any:
    client = run_v2.JobsClient()
    execution_job_name = None
    try:
        job_path = client.job_path(project=project_id, location=location, job=job_name)
        env_var_list = [
            run_v2.EnvVar({"name": key, "value": value}) for key, value in env_vars.items()
        ]
        overrides = run_v2.RunJobRequest.Overrides({"container_overrides": [
            run_v2.RunJobRequest.Overrides.ContainerOverride({"env": env_var_list})]})

        execution_request = run_v2.RunJobRequest({
            "name": job_path,
            "overrides": overrides})

        response = client.run_job(request=execution_request)

        # Monitor the execution response
        execution_job_name = response.metadata.name
        _logger.info(f"Job execution started: {execution_job_name}")
        _logger.info("Waiting for job to complete...")
        # TODO consider  not waiting for the result here so caller doesn't wait
        result = response.result()  # Wait for the job to complete
        _logger.info(f"Job completed successfully with result: {result}.")
        return result
    except Exception as e:
        _logger.error(f"Exception executing job '{execution_job_name}': {e}.")
        raise e


if __name__ == "__main__":
    # invoke_job("clingen-dev", "us-east1", "clinvar-ingest-copy-only-v1",
    #            env_vars={"arg1": "Arg1", "arg2": "Arg2"})
    invoke_job("clingen-dev", "us-east1", "clinvar-ingest-copy-only-v1")
# test with and without args
