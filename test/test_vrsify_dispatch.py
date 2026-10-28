import datetime

import pytest

from clinvar_ingest.config import VrsifyEnv
from clinvar_ingest.utils import ClinVarIngestFileFormat
from clinvar_ingest.vrsify import batch, dispatch, history, runner

SP_FINISHED = datetime.datetime(2026, 10, 27, 1, 19, 34, tzinfo=datetime.UTC)
RELEASES = [
    history.ReadyRelease(datetime.date(2026, 10, 18), "clinvar_vcv_2026_10_18_v2_6_0", SP_FINISHED),
    history.ReadyRelease(datetime.date(2026, 10, 24), "clinvar_vcv_2026_10_24_v2_6_1", SP_FINISHED),
]


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
        image="gcr.io/proj/clinvar-ingest:vrsify-abc",
        environment=dispatch.task_environment(env, {}),
    )


@pytest.fixture
def calls(monkeypatch):
    """Record the processing_history operations dispatch.run makes, in order."""
    log = []
    monkeypatch.setattr(history, "unfinished", lambda *_: [])
    monkeypatch.setattr(history, "ready_releases", lambda *_: list(RELEASES))
    monkeypatch.setattr(history, "claim", lambda _table, release, *_: log.append(("claim", release.release_date)))
    monkeypatch.setattr(
        history, "release_claim", lambda _table, release_date, *_: log.append(("release_claim", release_date))
    )
    return log


def run(env, batch_settings, calls, submit=None, **settings):
    def fake_submit(_settings, name, definition):
        calls.append(("submit", name, definition))
        return f"projects/proj/locations/us-east1/jobs/{name}"

    messages = []
    names = dispatch.run(
        env,
        dispatch.DispatchSettings(min_release_date=datetime.date(2026, 10, 18), **settings),
        batch_settings,
        table="proj.clinvar_ingest.processing_history",
        client=None,
        notify=messages.append,
        submit=submit or fake_submit,
    )
    return names, messages


def test_claims_oldest_release_first_and_at_most_max_releases(env, batch_settings, calls):
    names, messages = run(env, batch_settings, calls)
    assert [c[:2] for c in calls] == [("claim", datetime.date(2026, 10, 18)), ("submit", calls[1][1])]
    assert calls[1][1].startswith("vrsify-2026-10-18-")
    assert len(names) == 1
    assert "2026-10-18" in messages[0]

    calls.clear()
    names, _ = run(env, batch_settings, calls, max_releases=5)
    assert [c[0] for c in calls] == ["claim", "submit", "claim", "submit"]


def test_dry_run_writes_nothing(env, batch_settings, calls):
    names, _ = run(env, batch_settings, calls, dry_run=True)
    assert names == []
    assert calls == []


def test_failed_submission_releases_the_claim(env, batch_settings, calls):
    def failing_submit(*_):
        raise RuntimeError("quota")

    with pytest.raises(RuntimeError, match="quota"):
        run(env, batch_settings, calls, submit=failing_submit)
    assert calls == [("claim", datetime.date(2026, 10, 18)), ("release_claim", datetime.date(2026, 10, 18))]


def test_unfinished_rows_are_reported(env, batch_settings, calls, monkeypatch):
    class Row:
        release_date = datetime.date(2026, 10, 11)
        processing_started = SP_FINISHED
        pipeline_version = "v2_6_1"
        bucket_dir = "clinvar_vcv_2026_10_11_v2_6_0"

    monkeypatch.setattr(history, "unfinished", lambda *_: [Row()])
    _, messages = run(env, batch_settings, calls, dry_run=True)
    assert "2026-10-11" in messages[0]
    assert "has not finished" in messages[0]


def test_task_runs_vrsify_with_history_and_input_wait(env, batch_settings, calls):
    run(env, batch_settings, calls)
    definition = calls[1][2]
    task = definition["taskGroups"][0]["taskSpec"]
    assert task["runnables"][0]["container"]["commands"] == [
        "vrsify",
        "--release-date",
        "2026-10-18",
        "--history-bucket-dir",
        "clinvar_vcv_2026_10_18_v2_6_0",
        "--input-newer-than",
        "2026-10-27T01:19:34+00:00",
        "--input-wait",
        "3600",
    ]
    variables = task["environment"]["variables"]
    assert variables["CLINVAR_INGEST_RELEASE_TAG"] == "v2_6_1"
    assert variables["file_format"] == "vrsify"
    assert "secretVariables" not in task["environment"]


def test_slack_token_comes_from_secret_manager_never_the_definition(env, batch_settings, calls):
    run(env, batch_settings, calls, slack=True)
    definition = calls[1][2]
    task = definition["taskGroups"][0]["taskSpec"]
    assert task["runnables"][0]["container"]["commands"][-1] == "--slack"
    assert task["environment"]["secretVariables"] == {
        "CLINVAR_INGEST_SLACK_TOKEN": "projects/proj/secrets/clinvar-ingest-slack-token/versions/latest"
    }
    assert "xoxb-secret" not in repr(definition)


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
    import json

    batch_v1 = pytest.importorskip("google.cloud.batch_v1")
    job = batch_v1.Job.from_json(json.dumps(batch.job_definition(batch_settings, ["--release-date", "x"], slack=True)))
    assert job.task_groups[0].task_spec.environment.secret_variables["CLINVAR_INGEST_SLACK_TOKEN"].endswith("latest")
    assert job.allocation_policy.instances[0].policy.boot_disk.type_ == "hyperdisk-balanced"


def test_history_queries_touch_only_vrsify_rows_and_never_the_view():
    ready = history.ready_releases_sql("p.d.processing_history")
    unfinished = history.unfinished_sql("p.d.processing_history")
    for sql in (ready, unfinished):
        assert "processing_history_view" not in sql
        assert "DELETE" not in sql.upper()
        assert "UPDATE" not in sql.upper()
    assert "file_type = 'sp'" in ready
    assert "file_type = 'vrsify'" in ready
    assert "file_type = 'vrsify'" in unfinished


def test_claim_never_replaces_an_existing_row(monkeypatch):
    seen = {}
    monkeypatch.setattr(history.processing_history, "write_started", lambda **kw: seen.update(kw))
    history.claim("table", RELEASES[1], "v2_6_1", "v2", client=None)
    assert seen["file_type"] == ClinVarIngestFileFormat.VRSIFY
    assert seen["error_if_exists"] is True
    assert seen["release_date"] == seen["xml_release_date"] == "2026-10-24"
    assert seen["bucket_dir"] == "clinvar_vcv_2026_10_24_v2_6_1"


def test_release_claim_and_finish_name_only_vrsify(monkeypatch):
    seen = []
    monkeypatch.setattr(history.processing_history, "delete", lambda **kw: seen.append(kw) or 1)
    monkeypatch.setattr(history.processing_history, "write_finished", lambda **kw: seen.append(kw))
    history.release_claim("table", datetime.date(2026, 10, 24), "v2_6_1", client=None)
    history.finish("table", "2026-10-24", "v2_6_1", "clinvar_vcv_2026_10_24_v2_6_1", client=None)
    assert [kw["file_type"] for kw in seen] == [ClinVarIngestFileFormat.VRSIFY] * 2


class FakeBlob:
    def __init__(self, states):
        self.states = states

    def exists(self):
        return self.states[0] is not None

    def reload(self):
        pass

    @property
    def updated(self):
        return self.states[0]


def test_wait_for_input_returns_once_a_newer_export_appears(monkeypatch):
    states = [None, SP_FINISHED - datetime.timedelta(days=7), SP_FINISHED + datetime.timedelta(seconds=25)]
    monkeypatch.setattr(runner, "parse_blob_uri", lambda _uri: FakeBlob(states))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: states.pop(0))
    runner.wait_for_input("gs://b/2026-10-24/dev/vi.jsonl.gz", SP_FINISHED, timeout=3600, poll_seconds=0)
    assert len(states) == 1


def test_wait_for_input_times_out_on_a_stale_export(monkeypatch):
    stale = [SP_FINISHED - datetime.timedelta(days=7)]
    monkeypatch.setattr(runner, "parse_blob_uri", lambda _uri: FakeBlob(stale))
    with pytest.raises(TimeoutError, match="last written at"):
        runner.wait_for_input("gs://b/2026-10-24/dev/vi.jsonl.gz", SP_FINISHED, timeout=0)
