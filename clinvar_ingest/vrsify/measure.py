"""
Where a vrsify run's time and memory go, for its provenance record.

Every measurement is best-effort: one that cannot be taken is recorded as None, and
never fails the run.
"""

import datetime
import resource
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

# cgroup v2's high-water mark of the container's memory, page cache included
CGROUP_MEMORY_PEAK = Path("/sys/fs/cgroup/memory.peak")


class Phases:
    """When each phase of a run started and how long it took, in the order they ran."""

    def __init__(self) -> None:
        self.record: dict[str, dict] = {}

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started_at = datetime.datetime.now(datetime.UTC)
        started = time.perf_counter()
        try:
            yield
        finally:
            self.record[name] = {
                "started_at": started_at.isoformat(),
                "seconds": round(time.perf_counter() - started, 1),
            }


def parent_peak_rss_mib() -> int:
    """This process's peak resident memory; the workers are separate processes."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is in bytes on macOS and KiB on Linux
    return round((peak if sys.platform == "darwin" else peak * 1024) / 2**20)


def container_peak_mib(path: Path = CGROUP_MEMORY_PEAK) -> int | None:
    """The container's peak memory, page cache included; None outside a cgroup v2 container."""
    try:
        return round(int(path.read_text().strip()) / 2**20)
    except (OSError, ValueError):
        return None
