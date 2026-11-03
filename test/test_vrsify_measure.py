import datetime

import pytest

from clinvar_ingest.vrsify import measure

MIB = 2**20


def write_cgroup(path, anon_mib, file_mib, peak_mib=None):
    path.mkdir(exist_ok=True)
    (path / "memory.stat").write_text(f"anon {anon_mib * MIB}\nfile {file_mib * MIB}\nkernel 4096\n")
    (path / "memory.current").write_text(f"{(anon_mib + file_mib) * MIB}\n")
    if peak_mib is not None:
        (path / "memory.peak").write_text(f"{peak_mib * MIB}\n")


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
    write_cgroup(tmp_path, 1, 1, peak_mib=3 * 1024)
    assert measure.container_peak_mib(tmp_path) == 3 * 1024


@pytest.mark.parametrize("content", [None, "max\n", ""])
def test_a_missing_or_unreadable_cgroup_peak_is_none(tmp_path, content):
    if content is not None:
        (tmp_path / "memory.peak").write_text(content)
    assert measure.container_peak_mib(tmp_path) is None


def test_parent_peak_rss_is_positive():
    assert measure.parent_peak_rss_mib() > 0


def test_sampled_peaks_are_kept_per_phase(tmp_path):
    cgroup = tmp_path / "cg"
    write_cgroup(cgroup, anon_mib=100, file_mib=10)
    sampler = measure.MemorySampler(cgroup, interval=3600)  # sampled only at phase boundaries here
    sampler.start()
    phases = measure.Phases(sampler)
    with phases.phase("setup"):
        write_cgroup(cgroup, anon_mib=200, file_mib=3000)  # extracting: page cache grows
    with phases.phase("translate"):
        write_cgroup(cgroup, anon_mib=900, file_mib=2000)  # workers: process memory grows
    write_cgroup(cgroup, anon_mib=50, file_mib=50)
    result = sampler.stop()
    peaks = result["peaks"]
    assert peaks["setup"] == {"anon_mib": 200, "file_mib": 3000, "current_mib": 3200}
    # A phase's peak includes what is still resident as it starts: setup's page cache
    assert peaks["translate"] == {"anon_mib": 900, "file_mib": 3000, "current_mib": 3200}
    assert peaks["all"] == {"anon_mib": 900, "file_mib": 3000, "current_mib": 3200}
    assert result["samples"] >= 4


def test_without_a_cgroup_the_sampler_records_nothing(tmp_path):
    sampler = measure.MemorySampler(tmp_path / "absent")
    sampler.start()
    with measure.Phases(sampler).phase("setup"):
        pass
    assert sampler.stop() is None


def test_an_unreadable_sample_is_skipped(tmp_path):
    cgroup = tmp_path / "cg"
    write_cgroup(cgroup, anon_mib=100, file_mib=10)
    sampler = measure.MemorySampler(cgroup, interval=3600)
    (cgroup / "memory.stat").write_text("garbage\n")
    sampler.sample()
    assert sampler.samples == 0
