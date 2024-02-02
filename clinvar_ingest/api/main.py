import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import PurePosixPath

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, status
from google.cloud import bigquery
from google.cloud.storage import Client as GCSClient

import clinvar_ingest.config
from clinvar_ingest.api.middleware import LogRequests
from clinvar_ingest.api.model.requests import (
    ClinvarFTPWatcherRequest,
    CopyResponse,
    CreateExternalTablesRequest,
    CreateExternalTablesResponse,
    GetStepStatusResponse,
    InitializeStepRequest,
    InitializeStepResponse,
    InitializeWorkflowResponse,
    ParseRequest,
    ParseResponse,
    StepStartedResponse,
    TodoRequest,
)
from clinvar_ingest.api.status_file import (
    StepStatus,
    get_status_file,
    write_status_file,
)
from clinvar_ingest.cloud.bigquery.create_tables import run_create_external_tables
from clinvar_ingest.cloud.gcs import http_upload_urllib
from clinvar_ingest.parse import parse_and_write_files
from clinvar_ingest.status import StepName

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.env = clinvar_ingest.config.get_env()
    logger.info("Server starting up")
    yield


app = FastAPI(lifespan=lifespan, openapi_url="/openapi.json", docs_url="/api")
app.add_middleware(LogRequests)


def _get_gcs_client() -> GCSClient:
    if getattr(_get_gcs_client, "client", None) is None:
        setattr(_get_gcs_client, "client", GCSClient())
    return getattr(_get_gcs_client, "client")


@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    return {"health": "ok!"}


@app.post(
    "/create_workflow_execution_id/{initial_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=InitializeWorkflowResponse,
)
async def create_workflow_execution_id(initial_id: str):
    assert initial_id is not None and len(initial_id) > 0
    timestamp = datetime.utcnow().isoformat().replace(":", "").replace(".", "")
    execution_id = f"{initial_id}_{timestamp}"
    return InitializeWorkflowResponse(workflow_execution_id=execution_id)


@app.post(
    "/initialize_step",
    status_code=status.HTTP_201_CREATED,
    response_model=InitializeStepResponse,
)
async def initialize_step(request: Request, payload: InitializeStepRequest):
    env: clinvar_ingest.config.Env = request.app.env
    workflow_execution_id = payload.workflow_execution_id
    step_name = payload.step_name
    step_status = StepStatus.STARTED
    message = payload.message

    status_value = write_status_file(
        bucket=env.bucket_name,
        file_prefix=f"{env.executions_output_prefix}/{workflow_execution_id}",
        step=step_name,
        status=step_status,
        message=message,
    )

    return InitializeStepResponse(
        workflow_execution_id=workflow_execution_id,
        step_name=step_name,
        step_status=step_status,
        timestamp=status_value.timestamp,
    )


@app.get(
    "/step_status/{workflow_execution_id}/{step_name}",
    status_code=status.HTTP_200_OK,
    response_model=GetStepStatusResponse,
)
async def get_step_status(
    request: Request,
    workflow_execution_id: str,
    step_name: StepName,
):
    env: clinvar_ingest.config.Env = request.app.env
    file_prefix = f"{env.executions_output_prefix}/{workflow_execution_id}"
    logger.debug("Reading %s status from %s", step_name, file_prefix)

    # Cannot get status of step that has not started. Raise 404.
    try:
        status_value = get_status_file(
            bucket=env.bucket_name,
            file_prefix=file_prefix,
            step=step_name,
            status=StepStatus.STARTED,
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e

    try:
        status_value = get_status_file(
            bucket=env.bucket_name,
            file_prefix=file_prefix,
            step=step_name,
            status=StepStatus.SUCCEEDED,
        )
    except ValueError as _:
        logger.info(
            "Step %s in execution %s started and has not succeeded",
            step_name,
            workflow_execution_id,
        )

    try:
        status_value = get_status_file(
            bucket=env.bucket_name,
            file_prefix=file_prefix,
            step=step_name,
            status=StepStatus.FAILED,
        )
    except ValueError as _:
        logger.info(
            "Step %s in execution %s started and still running",
            step_name,
            workflow_execution_id,
        )

    return GetStepStatusResponse(
        workflow_execution_id=workflow_execution_id,
        step_name=step_name,
        step_status=status_value.status,
        timestamp=status_value.timestamp,
        message=status_value.message,
    )


@app.post(
    "/fake/{workflow_execution_id}/{step_name}",
    status_code=status.HTTP_201_CREATED,
    response_model=StepStartedResponse,
)
async def fake_step(
    request: Request,
    background_tasks: BackgroundTasks,
    workflow_execution_id: str,
    step_name: str,
):
    env: clinvar_ingest.config.Env = request.app.env
    start_status = write_status_file(
        env.bucket_name,
        f"{env.executions_output_prefix}/{workflow_execution_id}",
        step_name,
        StepStatus.STARTED,
    )
    logger.info("Fake %s started", step_name)

    def task():
        # Here's where we do the actual step work, in a callable sent to BackgroundTasks.
        write_status_file(
            env.bucket_name,
            f"{env.executions_output_prefix}/{workflow_execution_id}",
            step_name,
            StepStatus.SUCCEEDED,
        )
        logger.info(
            "Fake %s for workflow %s succeeded", step_name, workflow_execution_id
        )

    background_tasks.add_task(task)
    logger.info("Fake %s background task added", step_name)

    logger.info("Fake %s returning", step_name)
    return StepStartedResponse(
        workflow_execution_id=workflow_execution_id,
        step_name=step_name,
        timestamp=start_status.timestamp,
    )


@app.post(
    "/copy/{workflow_execution_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=StepStartedResponse,
)
async def copy(
    request: Request,
    workflow_execution_id: str,
    payload: ClinvarFTPWatcherRequest,
    background_tasks: BackgroundTasks,
):
    env: clinvar_ingest.config.Env = request.app.env
    step_name = StepName.COPY
    # TODO allow source path to be in a bucket or file (for testing)
    ftp_base = str(payload.host).strip("/")
    ftp_dir = PurePosixPath(payload.directory)
    ftp_file = PurePosixPath(payload.name)
    ftp_path = f"{ftp_base}/{ftp_dir.relative_to(ftp_dir.anchor) / ftp_file}"

    gcs_base = (
        f"gs://{env.bucket_name}/{env.executions_output_prefix}/{workflow_execution_id}"
    )
    gcs_dir = PurePosixPath(env.bucket_staging_prefix)
    gcs_file = PurePosixPath(payload.name)
    gcs_path = f"{gcs_base}/{gcs_dir.relative_to(gcs_dir.anchor) / gcs_file}"

    logger.info(f"Copying {ftp_path} to {gcs_path}")

    start_status = write_status_file(
        env.bucket_name,
        f"{env.executions_output_prefix}/{workflow_execution_id}",
        step_name,
        StepStatus.STARTED,
    )
    logger.info("%s step for workflow %s started", step_name, workflow_execution_id)

    def task():
        try:
            http_upload_urllib(ftp_path, gcs_path, client=_get_gcs_client())
            write_status_file(
                env.bucket_name,
                f"{env.executions_output_prefix}/{workflow_execution_id}",
                step_name,
                StepStatus.SUCCEEDED,
                message=CopyResponse(
                    ftp_path=ftp_path,
                    gcs_path=gcs_path,
                ).model_dump_json(),
            )

        except Exception as e:  # pylint: disable=W0718
            msg = f"Failed to copy {ftp_path}"
            logger.exception(msg)
            write_status_file(
                env.bucket_name,
                f"{env.executions_output_prefix}/{workflow_execution_id}",
                step_name,
                StepStatus.FAILED,
                message=f"{msg}: {e}",
            )

    background_tasks.add_task(task)
    logger.info("%s step task for workflow %s added", step_name, workflow_execution_id)

    logger.info(
        "%s step task for workflow %s returning", step_name, workflow_execution_id
    )
    return StepStartedResponse(
        workflow_execution_id=workflow_execution_id,
        step_name=step_name,
        timestamp=start_status.timestamp,
    )


@app.post(
    "/parse/{workflow_execution_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=StepStartedResponse,
)
async def parse(
    request: Request,
    workflow_execution_id: str,
    payload: ParseRequest,
    background_tasks: BackgroundTasks,
):
    env: clinvar_ingest.config.Env = request.app.env
    step_name = StepName.PARSE
    start_status = write_status_file(
        env.bucket_name,
        f"{env.executions_output_prefix}/{workflow_execution_id}",
        step_name,
        StepStatus.STARTED,
    )
    logger.info("%s step for workflow %s started", step_name, workflow_execution_id)

    def task():
        try:
            output_files = parse_and_write_files(
                payload.input_path,
                env.parse_output_prefix,
                disassemble=payload.disassemble,
                jsonify_content=payload.jsonify_content,
            )
            write_status_file(
                env.bucket_name,
                f"{env.executions_output_prefix}/{workflow_execution_id}",
                step_name,
                StepStatus.SUCCEEDED,
                message=ParseResponse(parsed_files=output_files).model_dump_json(),
            )
        except Exception as e:
            msg = f"Failed to parse {payload.input_path} and write to {env.parse_output_prefix}"
            logger.exception(msg)
            write_status_file(
                env.bucket_name,
                f"{env.executions_output_prefix}/{workflow_execution_id}",
                step_name,
                StepStatus.FAILED,
                message=f"{msg}: {e}",
            )

    background_tasks.add_task(task)
    logger.info("%s step task for workflow %s added", step_name, workflow_execution_id)

    logger.info(
        "%s step task for workflow %s returning", step_name, workflow_execution_id
    )
    return StepStartedResponse(
        workflow_execution_id=workflow_execution_id,
        step_name=step_name,
        timestamp=start_status.timestamp,
    )


@app.post(
    "/create_external_tables/{workflow_execution_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=StepStartedResponse,
)
async def create_external_tables(
    request: Request,
    workflow_execution_id: str,
    payload: CreateExternalTablesRequest,
    background_tasks: BackgroundTasks,
):
    env: clinvar_ingest.config.Env = request.app.env
    step_name = StepName.CREATE_EXTERNAL_TABLES
    logger.info(f"Creating external tables {payload.source_table_paths}")

    start_status = write_status_file(
        env.bucket_name,
        f"{env.executions_output_prefix}/{workflow_execution_id}",
        step_name,
        StepStatus.STARTED,
    )
    logger.info("%s step for workflow %s started", step_name, workflow_execution_id)

    def task():
        try:
            tables_created = run_create_external_tables(payload)

            for table_name, table in tables_created.items():
                table: bigquery.Table = table
                logger.info(
                    "Created table %s as %s:%s.%s",
                    table_name,
                    table.project,
                    table.dataset_id,
                    table.table_id,
                )
            entity_type_table_ids = {
                entity_type: table.full_table_id
                for entity_type, table in tables_created.items()
            }

            write_status_file(
                env.bucket_name,
                f"{env.executions_output_prefix}/{workflow_execution_id}",
                step_name,
                StepStatus.SUCCEEDED,
                message=CreateExternalTablesResponse(
                    root=entity_type_table_ids
                ).model_dump_json(),
            )
        except Exception as e:
            msg = f"Failed to create external tables for {payload.model_dump()}: {e}"
            logger.exception(msg)
            write_status_file(
                env.bucket_name,
                f"{env.executions_output_prefix}/{workflow_execution_id}",
                step_name,
                StepStatus.FAILED,
                message=f"{msg}: {e}",
            )

    background_tasks.add_task(task)
    logger.info("%s step task for workflow %s added", step_name, workflow_execution_id)

    logger.info(
        "%s step task for workflow %s returning", step_name, workflow_execution_id
    )
    return StepStartedResponse(
        workflow_execution_id=workflow_execution_id,
        step_name=step_name,
        timestamp=start_status.timestamp,
    )


@app.post("/create_internal_tables", status_code=status.HTTP_201_CREATED)
async def create_internal_tables(payload: TodoRequest):
    return {"todo": "implement me"}


@app.post("/create_cleaned_tables", status_code=status.HTTP_201_CREATED)
async def create_cleaned_tables(payload: TodoRequest):
    return {"todo": "implement me"}
