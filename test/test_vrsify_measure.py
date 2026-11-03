import datetime

import pytest

from clinvar_ingest.vrsify import measure


def test_phases_are_recorded_in_order_even_when_one_fails():
    phases = measure.Phases()
    with phases.phase("setup"):
        pass
    with pytest.raises(RuntimeError), phases.phase("translate"):
        raise RuntimeError("boom")
    assert list(phases.record) == ["setup", "translate"]
    for entry in phases.record.values():
        assert entry["seconds"] >= 0
        datetime.datetime.fromisoformat(entry["started_at"])


def test_container_peak_is_read_from_the_cgroup(tmp_path):
    peak = tmp_path / "memory.peak"
    peak.write_text(f"{3 * 2**30}\n")
    assert measure.container_peak_mib(peak) == 3 * 1024


@pytest.mark.parametrize("content", [None, "max\n", ""])
def test_a_missing_or_unreadable_cgroup_peak_is_none(tmp_path, content):
    peak = tmp_path / "memory.peak"
    if content is not None:
        peak.write_text(content)
    assert measure.container_peak_mib(peak) is None


def test_parent_peak_rss_is_positive():
    assert measure.parent_peak_rss_mib() > 0
