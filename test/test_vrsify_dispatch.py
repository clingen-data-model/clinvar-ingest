import datetime
import json

import pytest

from clinvar_ingest.config import VrsifyEnv
from clinvar_ingest.utils import ClinVarIngestFileFormat
from clinvar_ingest.vrsify import batch, dispatch, history

NOW = datetime.datetime.now(datetime.UTC)
SP_FINISHED = NOW - datetime.timedelta(minutes=5)
EXPORTED = SP_FINISHED + datetime.timedelta(seconds=25)
TABLE = "proj.clinvar_ingest.processing_history"
DAY = "2026-10-24"
INPUT = f"gs://clinvar-gkm/{DAY}/dev/vi.jsonl.gz"
OUTPUT = f"gs://clinvar-gkm/{DAY}/dev/vi-vrs-python.jsonl.gz"
PROVENANCE = f"gs://clinvar-gkm/{DAY}/dev/vi-vrs-python.provenance.json"
DELETE = (
    f"DELETE FROM `{TABLE}` WHERE file_type = 'vrsify' AND release_date = '{DAY}' "  # noqa: S608
    "AND pipeline_version = 'v2_6_1' AND processing_finished IS NULL;"
)


def release(day: str) -> history.ReadyRelease:
    return history.ReadyRelease(
        datetime.date.fromisoformat(day), f"clinvar_vcv_{day.replace('-', '_')}_v2_6_1", SP_FINISHED
    )


class FakeHistory:
    """processing_history's vrsify rows, in memory."""

    def __init__(self, ready=()):
        self.ready = list(ready)
        self.started: list[history.VrsifyRow] = []
        self.finished: list[history.VrsifyRow] = []

    def ready_releases(self, _table, _min_date, _client):
        claimed = {r.release_date for r in self.started}
        return [r for r in self.ready if r.release_date not in claimed]

    def claim(self, _table, ready, release_tag, _schema_version, _client):
        row = history.VrsifyRow(ready.release_date, release_tag, ready.bucket_dir, ready.sp_processing_finished)
        self.started.append(row)
        return row

    def finish(self, _table, row, _client):
        self.finished.append(row)


class FakeCloud(dispatch.Cloud):
    """GCS objects, and a Batch job that moves through `job_states`, one state per poll."""

    def __init__(self):
        self.objects: dict[str, datetime.datetime] = {}
        self.provenance: dict[str, dict] = {}
        self.job_states: list[str | None] = ["QUEUED", "RUNNING", "SUCCEEDED"]
        self.submitted: list[tuple[str, dict]] = []
        self.submit_error: Exception | None = None
        self.sleeps: list[float] = []
        self.on_sleep = lambda: None
        super().__init__(
            submit=self._submit,
            job_state=self._job_state,
            written_at=self.objects.get,
            read_provenance=self.provenance.get,
            sleep=self._sleep,
        )

    def _submit(self, settings, name, definition):
        if self.submit_error:
            raise self.submit_error
        self.submitted.append((name, definition))
        return f"projects/{settings.project}/locations/{settings.region}/jobs/{name}"

    def _job_state(self, job):
        state = self.job_states.pop(0) if len(self.job_states) > 1 else self.job_states[0]
        if state == "SUCCEEDED" and PROVENANCE not in self.provenance:
            self.write_output(job)
        return state

    def _sleep(self, seconds):
        self.sleeps.append(seconds)
        self.on_sleep()

    def write_output(self, job, records=1634):
        self.objects[OUTPUT] = NOW
        self.provenance[PROVENANCE] = {
            "inputs": [{"uri": INPUT}],
            "records": records,
            "host": {"batch": {"BATCH_JOB_ID": job.rsplit("/", 1)[-1]}},
        }


@pytest.fixture
def env():
    return VrsifyEnv(
        bq_dest_project="proj",
        bq_meta_dataset="clinvar_ingest",
        slack_token="xoxb-secret",  # noqa: S106
        slack_channel="C123",
        location="us-east1",
        release_tag="v2_6_1",
        schema_version="v2",
        file_format_mode="vrsify",
        clinvar_gks_bucket="clinvar-gkm",
    )


@pytest.fixture
def batch_settings(env):
    return batch.BatchSettings(
        project="proj",
        region="us-east1",
        service_account="sa@proj.iam.gserviceaccount.com",
        image="gcr.io/proj/clinvar-ingest:vrsify-v2_6_1",
        environment=dispatch.task_environment(env, {}),
    )


@pytest.fixture
def fake_history(monkeypatch):
    fake = FakeHistory([release(DAY)])
    for name in ("ready_releases", "claim", "finish"):
        monkeypatch.setattr(history, name, getattr(fake, name))
    return fake


@pytest.fixture
def cloud():
    fake = FakeCloud()
    fake.objects[INPUT] = EXPORTED
    return fake


@pytest.fixture
def messages():
    return []


@pytest.fixture
def dispatcher(env, batch_settings, fake_history, cloud, messages):
    settings = dispatch.DispatchSettings(min_release_date=datetime.date(2026, 10, 24))
    return dispatch.Dispatcher(env, settings, batch_settings, TABLE, None, messages.append, cloud)


def test_takes_a_release_from_claim_to_finished(dispatcher, fake_history, cloud, messages):
    dispatcher.run()

    ((name, definition),) = cloud.submitted
    job = f"projects/proj/locations/us-east1/jobs/{name}"
    assert name.startswith(f"vrsify-{DAY}-")
    task = definition["taskGroups"][0]["taskSpec"]
    assert task["runnables"][0]["container"]["commands"] == ["vrsify", "--release-date", DAY]
    assert "secretVariables" not in task["environment"]
    assert [r.release_date for r in fake_history.started] == [datetime.date(2026, 10, 24)]
    (finished,) = fake_history.finished
    assert finished.parsed_files == {"batch_job": job}
    assert (finished.pipeline_version, finished.bucket_dir) == ("v2_6_1", "clinvar_vcv_2026_10_24_v2_6_1")
    assert messages == [
        f"vrsify scheduled for release dated {DAY}: Batch job {name}",
        f"vrsify completed for release dated {DAY}: 1634 records -> {OUTPUT}",
    ]
    # The job was polled while it was queued and running
    assert cloud.sleeps == [30, 30]


def test_a_finished_release_is_not_claimed_again(dispatcher, cloud):
    dispatcher.run()
    dispatcher.run()
    assert len(cloud.submitted) == 1


@pytest.mark.parametrize("earlier", [None, SP_FINISHED - datetime.timedelta(days=7)])
def test_waits_for_an_export_newer_than_the_stored_procedures(dispatcher, fake_history, cloud, earlier):
    del cloud.objects[INPUT]
    if earlier:
        cloud.objects[INPUT] = earlier

    def export_on_third_poll():
        if len(cloud.sleeps) == 3:
            cloud.objects[INPUT] = EXPORTED

    cloud.on_sleep = export_on_third_poll
    dispatcher.run()
    assert cloud.sleeps[:3] == [60, 60, 60]
    assert len(fake_history.finished) == 1


def test_no_export_within_the_wait_is_reported(dispatcher, fake_history, cloud, messages):
    del cloud.objects[INPUT]
    with pytest.raises(dispatch.ReleaseFailedError, match=f"no {INPUT} was written"):
        dispatcher.run()
    assert len(cloud.sleeps) == 60
    assert cloud.submitted == []
    (message,) = messages
    assert "will NOT be retried automatically" in message
    assert DELETE in message
    assert "Also move or delete" not in message
    assert fake_history.finished == []


@pytest.mark.parametrize(
    ("states", "reason"),
    [
        (["RUNNING", "FAILED"], "ended FAILED; the job's own message and log have the error"),
        (["RUNNING", None], "no longer exists"),
    ],
)
def test_failed_or_missing_job_is_reported(dispatcher, fake_history, cloud, messages, states, reason):
    cloud.job_states = states
    cloud.objects[OUTPUT] = NOW  # written before the job failed
    with pytest.raises(dispatch.ReleaseFailedError):
        dispatcher.run()
    scheduled, failure = messages
    assert scheduled.startswith("vrsify scheduled")
    assert reason in failure
    assert DELETE in failure
    assert f"Also move or delete {OUTPUT} and {PROVENANCE}" in failure
    assert fake_history.finished == []
    assert len(fake_history.started) == 1


@pytest.mark.parametrize(
    ("provenance", "problem"),
    [
        (None, "vi-vrs-python.provenance.json does not exist"),
        ({"host": {"batch": {"BATCH_JOB_ID": "another-job"}}, "inputs": [{"uri": INPUT}]}, "was not written by it"),
        ("other input", "names inputs other than"),
    ],
)
def test_succeeded_job_with_unexpected_provenance_is_reported(
    dispatcher, fake_history, cloud, messages, provenance, problem
):
    cloud.job_states = ["SUCCEEDED"]

    def submit(settings, name, _definition):
        if provenance == "other input":
            cloud.provenance[PROVENANCE] = {"host": {"batch": {"BATCH_JOB_ID": name}}, "inputs": [{"uri": "gs://b/x"}]}
        elif provenance:
            cloud.provenance[PROVENANCE] = provenance
        else:
            cloud.provenance[PROVENANCE] = None  # keeps the fake job from writing one
        return f"projects/{settings.project}/locations/{settings.region}/jobs/{name}"

    cloud.submit = submit
    cloud.read_provenance = lambda uri: cloud.provenance.get(uri)
    with pytest.raises(dispatch.ReleaseFailedError):
        dispatcher.run()
    assert problem in messages[-1]
    assert fake_history.finished == []


def test_failed_submission_is_reported(dispatcher, fake_history, cloud, messages):
    cloud.submit_error = RuntimeError("quota")
    with pytest.raises(RuntimeError, match="quota"):
        dispatcher.run()
    (message,) = messages
    assert "RuntimeError: quota" in message
    assert DELETE in message
    assert len(fake_history.started) == 1


def test_failure_before_a_claim_is_not_reported(dispatcher, fake_history, monkeypatch, messages):
    def unreadable(*_):
        raise RuntimeError("BigQuery unavailable")

    monkeypatch.setattr(history, "ready_releases", unreadable)
    with pytest.raises(RuntimeError, match="BigQuery unavailable"):
        dispatcher.run()
    assert messages == []
    assert fake_history.started == []


def test_failed_release_is_left_for_a_person(dispatcher, cloud, messages):
    cloud.job_states = ["FAILED"]
    with pytest.raises(dispatch.ReleaseFailedError):
        dispatcher.run()
    # Later executions find nothing to claim and post nothing
    dispatcher.run()
    dispatcher.run()
    assert len(cloud.submitted) == 1
    assert len(messages) == 2


def test_dry_run_claims_and_submits_nothing(dispatcher, fake_history, cloud, messages):
    dispatcher.settings.dry_run = True
    dispatcher.run()
    assert fake_history.started == []
    assert cloud.submitted == []
    assert messages == []


def test_claims_at_most_max_releases_oldest_first(dispatcher, fake_history, monkeypatch):
    completed = []
    monkeypatch.setattr(dispatcher, "complete", completed.append)
    fake_history.ready = [release("2026-10-18"), release(DAY)]
    dispatcher.run()
    assert [str(r.release_date) for r in fake_history.started] == ["2026-10-18"]
    assert completed == fake_history.started


def test_slack_token_comes_from_secret_manager_never_the_definition(dispatcher, cloud):
    dispatcher.settings.slack = True
    dispatcher.run()
    ((_, definition),) = cloud.submitted
    task = definition["taskGroups"][0]["taskSpec"]
    assert task["runnables"][0]["container"]["commands"][-1] == "--slack"
    assert task["environment"]["secretVariables"] == {
        "CLINVAR_INGEST_SLACK_TOKEN": "projects/proj/secrets/clinvar-ingest-slack-token/versions/latest"
    }
    assert "xoxb-secret" not in repr(definition)


def test_task_environment_has_no_processing_history_settings(batch_settings):
    variables = batch.job_definition(batch_settings, [])["taskGroups"][0]["taskSpec"]["environment"]["variables"]
    assert "CLINVAR_INGEST_BQ_META_DATASET" not in variables
    assert variables["CLINVAR_GKS_BUCKET"] == "clinvar-gkm"
    assert variables["CLINVAR_INGEST_SLACK_CHANNEL"] == "C123"


def test_job_definition_uses_c4_with_hyperdisk(batch_settings):
    policy = batch.job_definition(batch_settings, [])["allocationPolicy"]["instances"][0]["policy"]
    assert policy["machineType"] == "c4-standard-8"
    assert policy["bootDisk"]["type"] == "hyperdisk-balanced"


@pytest.mark.parametrize(
    ("machine_type", "expected"),
    [
        ("c4-standard-8", {"cpuMilli": 8000, "memoryMib": 27648}),  # of 30 GB
        ("e2-standard-4", {"cpuMilli": 4000, "memoryMib": 13824}),  # of 16 GB
        ("c4-highmem-4", {"cpuMilli": 4000, "memoryMib": 27648}),  # of 31 GB
        ("e2-custom-4-8192", None),
        ("e2-medium", None),
    ],
)
def test_task_requests_the_vm(batch_settings, machine_type, expected):
    batch_settings.machine_type = machine_type
    task = batch.job_definition(batch_settings, [])["taskGroups"][0]["taskSpec"]
    assert task.get("computeResource") == expected


def test_job_definition_parses_as_a_batch_job(batch_settings):
    batch_v1 = pytest.importorskip("google.cloud.batch_v1")
    job = batch_v1.Job.from_json(json.dumps(batch.job_definition(batch_settings, ["--release-date", "x"], slack=True)))
    assert job.task_groups[0].task_spec.environment.secret_variables["CLINVAR_INGEST_SLACK_TOKEN"].endswith("latest")
    assert job.allocation_policy.instances[0].policy.boot_disk.type_ == "hyperdisk-balanced"


def test_ready_releases_reads_only_the_table():
    sql = history.ready_releases_sql(TABLE)
    assert "processing_history_view" not in sql
    assert "file_type = 'sp'" in sql
    assert "file_type = 'vrsify'" in sql
    for statement in ("UPDATE", "DELETE", "INSERT"):
        assert statement not in sql.upper()


def test_claim_never_replaces_an_existing_row(monkeypatch):
    seen = {}
    monkeypatch.setattr(history.processing_history, "write_started", lambda **kw: seen.update(kw))
    claimed = history.claim("table", release(DAY), "v2_6_1", "v2", client=None)
    assert seen["file_type"] == ClinVarIngestFileFormat.VRSIFY
    assert seen["error_if_exists"] is True
    assert seen["release_date"] == seen["xml_release_date"] == DAY
    assert seen["bucket_dir"] == "clinvar_vcv_2026_10_24_v2_6_1"
    assert claimed.parsed_files == {}


def test_finish_uses_the_rows_own_key(monkeypatch):
    seen = {}
    monkeypatch.setattr(history.processing_history, "write_finished", lambda **kw: seen.update(kw))
    row = history.VrsifyRow(
        datetime.date(2026, 10, 24), "v2_6_0", "clinvar_vcv_2026_10_24_v2_6_0", SP_FINISHED, {"batch_job": "j"}
    )
    history.finish("table", row, client=None)
    assert seen["file_type"] == ClinVarIngestFileFormat.VRSIFY
    assert seen["release_tag"] == "v2_6_0"
    assert seen["bucket_dir"] == "clinvar_vcv_2026_10_24_v2_6_0"
    assert seen["parsed_files"] == {"batch_job": "j"}
