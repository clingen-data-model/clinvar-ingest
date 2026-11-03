"""
Where a vrsify run's time and memory go, for its provenance record.

Every measurement is best-effort: one that cannot be taken is recorded as None, and
never fails the run. The container memory figures read cgroup v2 files, so they are
None outside a Linux container.
"""

import datetime
import resource
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

CGROUP = Path("/sys/fs/cgroup")
SAMPLE_SECONDS = 1.0


class MemorySampler:
    """
    Sample the container's memory in a background thread: `anon` (process memory, which
    cannot be reclaimed), `file` (page cache, which can), and their total as the kernel
    charges it (memory.current). Keeps the peak of each, overall and per phase.

    The thread only reads three small files and updates the peaks under its own lock,
    which nothing else takes, so the worker processes forked while it runs are unaffected.
    """

    def __init__(self, cgroup: Path = CGROUP, interval: float = SAMPLE_SECONDS) -> None:
        self.cgroup = cgroup
        self.interval = interval
        self.available = (cgroup / "memory.stat").is_file() and (cgroup / "memory.current").is_file()
        self.samples = 0
        self._phase: str | None = None
        self._peaks: dict[str, dict[str, int]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="memory-sampler", daemon=True)

    def _read(self) -> dict[str, int] | None:
        try:
            stat = dict(line.split() for line in (self.cgroup / "memory.stat").read_text().splitlines())
            current = int((self.cgroup / "memory.current").read_text())
            return {"anon": int(stat["anon"]), "file": int(stat["file"]), "current": current}
        except (OSError, ValueError, KeyError):
            return None

    def sample(self) -> None:
        reading = self._read()
        if reading is None:
            return
        with self._lock:
            self.samples += 1
            for key in ("all", self._phase):
                if key is None:
                    continue
                peaks = self._peaks.setdefault(key, {"anon": 0, "file": 0, "current": 0})
                for name, value in reading.items():
                    peaks[name] = max(peaks[name], value)

    def set_phase(self, name: str | None) -> None:
        """Attribute samples to phase `name` from now on, sampling at the boundary."""
        if self.available:
            self.sample()
            with self._lock:
                self._phase = name
            self.sample()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self.sample()

    def start(self) -> None:
        if self.available:
            self._thread.start()

    def stop(self) -> dict | None:
        """Stop sampling; return the peaks in MiB, or None outside a cgroup v2 container."""
        if not self.available:
            return None
        self._stop.set()
        self._thread.join(timeout=5)
        self.sample()
        with self._lock:
            peaks = {
                phase: {f"{name}_mib": round(value / 2**20) for name, value in values.items()}
                for phase, values in self._peaks.items()
            }
        return {"interval_seconds": self.interval, "samples": self.samples, "peaks": peaks}


class Phases:
    """When each phase of a run started and how long it took, in the order they ran."""

    def __init__(self, sampler: MemorySampler | None = None) -> None:
        self.record: dict[str, dict] = {}
        self.sampler = sampler

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started_at = datetime.datetime.now(datetime.UTC)
        started = time.perf_counter()
        if self.sampler is not None:
            self.sampler.set_phase(name)
        try:
            yield
        finally:
            if self.sampler is not None:
                self.sampler.set_phase(None)
            self.record[name] = {
                "started_at": started_at.isoformat(),
                "seconds": round(time.perf_counter() - started, 1),
            }


def parent_peak_rss_mib() -> int:
    """This process's peak resident memory; the workers are separate processes."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is in bytes on macOS and KiB on Linux
    return round((peak if sys.platform == "darwin" else peak * 1024) / 2**20)


def container_peak_mib(cgroup: Path = CGROUP) -> int | None:
    """The container's peak memory, page cache included; None outside a cgroup v2 container."""
    try:
        return round(int((cgroup / "memory.peak").read_text().strip()) / 2**20)
    except (OSError, ValueError):
        return None
