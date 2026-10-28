"""
Run vrsify over variation_identity export files: read records, translate them in a
process pool, and write one `{"in": record, "out": ...}` line per input record.

Output lines are written as batches complete, so their order is not the input order.
"""

import datetime
import fnmatch
import glob
import gzip
import heapq
import importlib.metadata
import json
import logging
import os
import resource
import signal
import socket
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from clinvar_ingest.cloud.gcs import _get_gcs_client, parse_blob_uri
from clinvar_ingest.config import env_or_dotenv_or
from clinvar_ingest.vrsify.store import is_remote_store, prepare_seqrepo, prepare_store
from clinvar_ingest.vrsify.translate import ASSEMBLY_NAMES, translate_record

_logger = logging.getLogger("clinvar_ingest")

DEFAULT_REFGET_STORE = "https://static.ferriter.dev/refgetstore/genomic-2026-10-27.tar.gz"
OUTPUT_NAME = "vi-vrs-python.jsonl.gz"
SLOWEST_RECORDS_KEPT = 20
PROGRESS_LOG_SECONDS = 30

# Per-worker translators, set by _init_worker
_WORKER: dict[str, Any] = {}


def notify(message: str, slack: bool) -> None:
    """
    Log `message` and, when `slack` is set, post it to Slack with the token and channel of
    the configured environment (config.get_vrsify_env).
    """
    _logger.info(message)
    if slack:
        from clinvar_ingest.slack import send_slack_message

        send_slack_message(message)


def default_uris(release_date: str) -> tuple[str, str]:
    bucket = env_or_dotenv_or("CLINVAR_GKS_BUCKET", default="clinvar-gkm")
    prefix = f"gs://{bucket}/{release_date}/dev"
    return f"{prefix}/vi.jsonl.gz", f"{prefix}/{OUTPUT_NAME}"


def provenance_uri(output_uri: str) -> str:
    return output_uri.removesuffix(".jsonl.gz") + ".provenance.json"


def _on_alarm(_signum, _frame):
    raise TimeoutError(f"record took longer than {_WORKER['record_timeout']} s")


REFGET_READS = ("whole", "ranges")


@dataclass(frozen=True)
class SequenceSource:
    """
    Where translation workers read sequences.

    `kind` is "refget" or "seqrepo"; `location` is a local store directory or, for a
    RefgetStore, a remote store's URL. A worker caches the sequences it fetches from a
    remote store in a directory of its own under `cache_root`: gtars writes a fetched file
    in place rather than renaming it into place, so a worker sharing the cache could read
    it half-written.

    `reads` sets how a RefgetStore serves reads: "whole" loads each sequence a worker
    reads into that worker's memory (vrs-python's behavior); "ranges" leaves sequences
    on disk, where gtars reads just the bases each read covers, so workers share the
    operating system's file cache instead of each holding the sequences.
    """

    kind: str
    location: str
    cache_root: str | None = None
    reads: str = "whole"


def _data_proxy(source: SequenceSource) -> Any:
    from ga4gh.vrs.dataproxy import create_dataproxy

    if source.kind == "seqrepo":
        return create_dataproxy(f"seqrepo+file://{source.location}")
    if source.reads == "ranges":
        from ga4gh.vrs.dataproxy_refget import RefgetSequenceDataProxy

        # vrs-python loads each sequence before its first read; skipped, gtars serves
        # get_substring from the local .seq file
        RefgetSequenceDataProxy._ensure_sequence_loaded = lambda _self, _digest: None
    if is_remote_store(source.location):
        os.environ["GA4GH_VRS_REFGET_CACHE_DIR"] = str(Path(source.cache_root) / f"worker-{os.getpid()}")
        return create_dataproxy(f"refget+{source.location}")
    return create_dataproxy(f"refget+file://{source.location}")


def _init_worker(source: SequenceSource, record_timeout: int) -> None:
    """Create this worker's translators over `source`."""
    # The interpreter's start and imports came before this; the worker's lifetime starts here
    _WORKER["started"], _WORKER["cpu_at_start"] = time.time(), _usage()["cpu_seconds"]
    from clinvar_ingest.vrsify.offline import disable_remote_services

    # Before any translator exists: hgvs binds its sequence fetcher at construction
    disable_remote_services()

    from ga4gh.vrs.extras.translator import AlleleTranslator, CnvTranslator

    data_proxy = _data_proxy(source)
    _WORKER["allele_translators"] = {
        version: AlleleTranslator(data_proxy=data_proxy, default_assembly_name=name)
        for version, name in ASSEMBLY_NAMES.items()
    }
    _WORKER["cnv_translator"] = CnvTranslator(data_proxy=data_proxy)
    _WORKER["record_timeout"] = record_timeout
    signal.signal(signal.SIGALRM, _on_alarm)


def _status(out: dict | None) -> str:
    if out is None:
        return "null"
    return "error" if "errors" in out else out.get("type", "unknown")


def _usage() -> dict:
    """This process's resource counters since it started."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "cpu_seconds": usage.ru_utime + usage.ru_stime,
        "user_seconds": usage.ru_utime,
        "system_seconds": usage.ru_stime,
        # Blocked waiting (I/O, a lock, an empty queue), and preempted by the scheduler
        "voluntary_context_switches": usage.ru_nvcsw,
        "involuntary_context_switches": usage.ru_nivcsw,
        # Reads the page cache did not serve
        "major_page_faults": usage.ru_majflt,
        "block_reads": usage.ru_inblock,
        # ru_maxrss is in bytes on macOS and KiB on Linux
        "peak_rss_bytes": usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024,
    }


def _translate_batch(lines: list[str]) -> tuple[list[tuple[str, str, str, str, float]], dict]:
    """
    Return (output line, vrs_class, status, source, seconds) per input line, and the
    worker's resource counters: its process id, when it started, when this batch ran,
    the CPU time the batch took, and the worker's totals so far (_usage).
    """
    batch_start, cpu_start = time.time(), _usage()["cpu_seconds"]
    results = []
    for line in lines:
        record = json.loads(line)
        started = time.perf_counter()
        # A long call into gtars is only interrupted once it returns to Python
        signal.alarm(_WORKER["record_timeout"])
        try:
            out = translate_record(record, _WORKER["allele_translators"], _WORKER["cnv_translator"])
        finally:
            signal.alarm(0)
        elapsed = time.perf_counter() - started
        results.append(
            (
                json.dumps({"in": record, "out": out}),
                str(record.get("vrs_class")),
                _status(out),
                str(record.get("source")),
                elapsed,
            )
        )
    usage = _usage()
    worker = {
        "pid": os.getpid(),
        "started": _WORKER["started"],
        "cpu_at_start": _WORKER["cpu_at_start"],
        "batch_start": batch_start,
        "batch_end": time.time(),
        "batch_cpu_seconds": usage["cpu_seconds"] - cpu_start,
        **usage,
    }
    return results, worker


def _batches(paths: list[Path], batch_size: int) -> Iterator[list[str]]:
    batch = []
    for path in paths:
        with gzip.open(path, "rt") as f:
            for line in f:
                if line.strip():
                    batch.append(line)
                    if len(batch) == batch_size:
                        yield batch
                        batch = []
    if batch:
        yield batch


def _directory_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


TIMELINE_SECONDS = 10


class _WorkerStats:
    """Each worker's resource use over its lifetime, from the counters its batches report."""

    def __init__(self, run_start: float):
        self.run_start = run_start
        self.workers: dict[int, dict] = {}

    def add(self, report: dict) -> None:
        stats = self.workers.setdefault(
            report["pid"], {"busy_seconds": 0.0, "busy_cpu_seconds": 0.0, "timeline": Counter(), "latest": report}
        )
        wall = report["batch_end"] - report["batch_start"]
        stats["busy_seconds"] += wall
        stats["busy_cpu_seconds"] += report["batch_cpu_seconds"]
        if report["batch_end"] >= stats["latest"]["batch_end"]:
            stats["latest"] = report
        # Spread the batch's CPU time over the windows it ran in
        start, end = report["batch_start"] - self.run_start, report["batch_end"] - self.run_start
        for window in range(int(start // TIMELINE_SECONDS), int(end // TIMELINE_SECONDS) + 1):
            if wall <= 0:
                stats["timeline"][window] += report["batch_cpu_seconds"]
                break
            overlap = min(end, (window + 1) * TIMELINE_SECONDS) - max(start, window * TIMELINE_SECONDS)
            stats["timeline"][window] += report["batch_cpu_seconds"] * max(overlap, 0) / wall

    def summary(self) -> list[dict]:
        """
        Per worker: its lifetime (initialization to last batch), the time it spent in
        batches, CPU time over its lifetime and before it (interpreter start and imports),
        CPU utilization while in batches and over its lifetime, its counters, and its CPU
        seconds per TIMELINE_SECONDS window of the run.
        """
        summaries = []
        for stats in sorted(self.workers.values(), key=lambda s: s["latest"]["started"]):
            latest = stats["latest"]
            lifetime = latest["batch_end"] - latest["started"]
            lifetime_cpu = latest["cpu_seconds"] - latest["cpu_at_start"]
            windows = max(stats["timeline"], default=-1) + 1
            summaries.append(
                {
                    "lifetime_seconds": round(lifetime, 1),
                    "busy_seconds": round(stats["busy_seconds"], 1),
                    "cpu_seconds": round(lifetime_cpu, 1),
                    "startup_cpu_seconds": round(latest["cpu_at_start"], 1),
                    "user_seconds": round(latest["user_seconds"], 1),
                    "system_seconds": round(latest["system_seconds"], 1),
                    "cpu_utilization_busy": round(stats["busy_cpu_seconds"] / stats["busy_seconds"], 3)
                    if stats["busy_seconds"]
                    else None,
                    "cpu_utilization_lifetime": round(lifetime_cpu / lifetime, 3) if lifetime else None,
                    "voluntary_context_switches": latest["voluntary_context_switches"],
                    "involuntary_context_switches": latest["involuntary_context_switches"],
                    "major_page_faults": latest["major_page_faults"],
                    "block_reads": latest["block_reads"],
                    "peak_rss_mib": round(latest["peak_rss_bytes"] / 2**20),
                    "cpu_seconds_per_window": [round(stats["timeline"][w], 2) for w in range(windows)],
                }
            )
        return summaries


def translate_files(
    input_paths: list[Path],
    output_path: Path,
    source: SequenceSource,
    workers: int,
    batch_size: int,
    record_timeout: int,
) -> dict:
    """Translate every record in `input_paths` into `output_path`, returning summary statistics."""
    counts: Counter = Counter()
    slowest: list[tuple[float, str]] = []
    records = 0
    started = time.perf_counter()
    worker_stats = _WorkerStats(time.time())
    parent_cpu_start = _usage()["cpu_seconds"]
    last_log = started
    max_pending = workers * 4

    def write(done: set[Future], out) -> None:
        nonlocal records, last_log
        for future in done:
            results, report = future.result()
            worker_stats.add(report)
            for line, vrs_class, status, expression, elapsed in results:
                out.write(line + "\n")
                counts[f"{vrs_class}:{status}"] += 1
                heapq.heappush(slowest, (elapsed, expression))
                if len(slowest) > SLOWEST_RECORDS_KEPT:
                    heapq.heappop(slowest)
                records += 1
        now = time.perf_counter()
        if now - last_log > PROGRESS_LOG_SECONDS:
            _logger.info(f"Translated {records} records in {now - started:.0f} s")
            last_log = now

    with (
        gzip.open(output_path, "wt", compresslevel=6) as out,
        ProcessPoolExecutor(workers, initializer=_init_worker, initargs=(source, record_timeout)) as pool,
    ):
        pending: set[Future] = set()
        for batch in _batches(input_paths, batch_size):
            pending.add(pool.submit(_translate_batch, batch))
            if len(pending) >= max_pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                write(done, out)
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            write(done, out)

    seconds = time.perf_counter() - started
    workers_summary = worker_stats.summary()
    stats = {
        "records": records,
        "counts": dict(sorted(counts.items())),
        "seconds": round(seconds, 1),
        "slowest_records": [{"seconds": round(s, 2), "source": src} for s, src in sorted(slowest, reverse=True)],
        "worker_peak_rss_mib": sorted((w["peak_rss_mib"] for w in workers_summary), reverse=True),
        "worker_resources": workers_summary,
        "timeline_window_seconds": TIMELINE_SECONDS,
        # The parent reads input and compresses output; a saturated parent starves the workers
        "parent_cpu_utilization": round((_usage()["cpu_seconds"] - parent_cpu_start) / seconds, 3),
    }
    if source.cache_root is not None and Path(source.cache_root).is_dir():
        # What each worker fetched from the remote store and kept in its cache
        cached = sorted((_directory_bytes(d) for d in Path(source.cache_root).iterdir() if d.is_dir()), reverse=True)
        stats["refget_cache"] = {"worker_bytes": cached, "total_bytes": sum(cached)}
    busy = [w["cpu_utilization_busy"] for w in workers_summary if w["cpu_utilization_busy"] is not None]
    if busy:
        _logger.info(
            f"Worker CPU utilization while translating: {min(busy):.0%}-{max(busy):.0%}; "
            f"peak memory {sum(stats['worker_peak_rss_mib'])} MiB in total; "
            f"parent CPU utilization {stats['parent_cpu_utilization']:.0%}"
        )
    return stats


def _count_lines(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        with gzip.open(path, "rt") as f:
            total += sum(1 for line in f if line.strip())
    return total


def _refuse_vrsify_outputs(input_uri: str, names: list[str]) -> None:
    """A pattern such as vi-*.jsonl.gz also matches a vrsify output written beside the export."""
    outputs = [n for n in names if n.rsplit("/", 1)[-1].startswith(OUTPUT_NAME.removesuffix(".jsonl.gz"))]
    if outputs:
        raise ValueError(f"{input_uri} matches vrsify output, not a variation_identity export: {outputs}")


def _fetch_inputs(input_uri: str, work_dir: Path) -> tuple[list[Path], list[dict]]:
    """Resolve `input_uri` (a gs:// URI or local path, optionally with glob characters) to local files."""
    if not input_uri.startswith("gs://"):
        if any(c in input_uri for c in "*?["):
            paths = sorted(Path(p) for p in glob.glob(input_uri))
        else:
            paths = [Path(input_uri)]
        if not paths or not all(p.is_file() for p in paths):
            raise FileNotFoundError(f"No input files match {input_uri}")
        _refuse_vrsify_outputs(input_uri, [p.name for p in paths])
        return paths, [{"uri": str(p.resolve()), "size": p.stat().st_size} for p in paths]

    bucket_name, _, pattern = input_uri.removeprefix("gs://").partition("/")
    if any(c in pattern for c in "*?["):
        prefix = pattern[: min(pattern.find(c) for c in "*?[" if c in pattern)]
        blobs = [
            b for b in _get_gcs_client().list_blobs(bucket_name, prefix=prefix) if fnmatch.fnmatchcase(b.name, pattern)
        ]
    else:
        blob = parse_blob_uri(input_uri)
        blob.reload()
        blobs = [blob]
    if not blobs:
        raise FileNotFoundError(f"No input objects match {input_uri}")
    _refuse_vrsify_outputs(input_uri, [b.name for b in blobs])

    paths, records = [], []
    for index, blob in enumerate(sorted(blobs, key=lambda b: b.name)):
        path = work_dir / f"input-{index:05d}.jsonl.gz"
        _logger.info(f"Downloading gs://{bucket_name}/{blob.name}")
        blob.download_to_filename(str(path))
        paths.append(path)
        records.append(
            {
                "uri": f"gs://{bucket_name}/{blob.name}",
                "generation": blob.generation,
                "md5": blob.md5_hash,
                "size": blob.size,
            }
        )
    return paths, records


def _put(local_path: Path, uri: str, overwrite: bool) -> None:
    if not uri.startswith("gs://"):
        Path(uri).write_bytes(local_path.read_bytes())
        return
    # Without --overwrite, refuse to replace an existing object even if it appeared mid-run
    parse_blob_uri(uri).upload_from_filename(str(local_path), if_generation_match=None if overwrite else 0)
    _logger.info(f"Wrote {uri}")


def _exists(uri: str) -> bool:
    return parse_blob_uri(uri).exists() if uri.startswith("gs://") else Path(uri).exists()


def _library_versions() -> dict:
    versions = {}
    for dist in ("clinvar_ingest", "ga4gh.vrs", "gtars", "hgvs", "biocommons.seqrepo", "pysam"):
        try:
            meta = importlib.metadata.distribution(dist)
        except importlib.metadata.PackageNotFoundError:
            continue
        entry = {"version": meta.version}
        direct_url = meta.read_text("direct_url.json")
        if direct_url:
            vcs = json.loads(direct_url)
            entry["url"] = vcs.get("url")
            entry["commit"] = vcs.get("vcs_info", {}).get("commit_id")
        versions[dist] = entry
    return versions


def vrsify(  # noqa: PLR0913
    input_uri: str,
    output_uri: str,
    refget_store: str,
    workers: int,
    batch_size: int = 50,
    record_timeout: int = 300,
    overwrite: bool = False,
    work_dir: str | None = None,
    slack: bool = False,
    release_date: str | None = None,
    seqrepo: str | None = None,
    refget_reads: str = "whole",
) -> dict:
    """
    Translate the records at `input_uri` and write the results and a provenance record.

    Sequences come from `refget_store`, or from the SeqRepo `seqrepo` (a directory or a
    .tar.gz archive) when it is given. `refget_reads` is SequenceSource.reads. Messages
    name the release by `release_date`, or by the output when it is not given.
    """
    if refget_reads not in REFGET_READS:
        raise ValueError(f"refget_reads must be one of {REFGET_READS}: {refget_reads}")
    if seqrepo and refget_reads != "whole":
        raise ValueError("refget_reads applies to a RefgetStore, not a SeqRepo")
    prov_uri = provenance_uri(output_uri)
    started_at = datetime.datetime.now(datetime.UTC)
    subject = f"for release dated {release_date}" if release_date else output_uri
    notify(f"vrsify starting {subject}: {input_uri}", slack)
    try:
        if not overwrite:
            for uri in (output_uri, prov_uri):
                if _exists(uri):
                    raise FileExistsError(f"{uri} exists; pass --overwrite to replace it")
        work = Path(work_dir) if work_dir else Path(tempfile.mkdtemp(prefix="vrsify-"))
        work.mkdir(parents=True, exist_ok=True)
        setup_started = time.perf_counter()
        if seqrepo:
            location, store_provenance = prepare_seqrepo(seqrepo, work)
            source = SequenceSource("seqrepo", location)
        else:
            location, store_provenance = prepare_store(refget_store, work)
            cache_root = str(work / "refget-cache") if is_remote_store(location) else None
            source = SequenceSource("refget", location, cache_root, refget_reads)
        store_provenance["setup_seconds"] = round(time.perf_counter() - setup_started, 1)
        input_paths, input_records = _fetch_inputs(input_uri, work)
        expected = _count_lines(input_paths)

        output_path = work / OUTPUT_NAME
        stats = translate_files(input_paths, output_path, source, workers, batch_size, record_timeout)
        # The gkm_vrs load requires exactly one output row per input record
        if stats["records"] != expected:
            raise RuntimeError(f"Read {expected} records but wrote {stats['records']}")

        provenance = {
            "started_at": started_at.isoformat(),
            "finished_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "inputs": input_records,
            "output": output_uri,
            "seqrepo" if seqrepo else "refget_store": store_provenance,
            "sequence_reads": None if seqrepo else refget_reads,
            "libraries": _library_versions(),
            "workers": workers,
            "record_timeout_seconds": record_timeout,
            "host": {
                "hostname": socket.gethostname(),
                "cpu_count": os.cpu_count(),
                "batch": {k: v for k, v in os.environ.items() if k.startswith("BATCH_")},
            },
            **stats,
        }
        provenance_path = work / "provenance.json"
        provenance_path.write_text(json.dumps(provenance, indent=2) + "\n")
        _put(output_path, output_uri, overwrite)
        _put(provenance_path, prov_uri, overwrite)
    except Exception as e:
        notify(f"vrsify failed {subject}: {type(e).__name__}: {e}", slack)
        raise
    notify(f"vrsify output written {subject}: {stats['records']} records in {stats['seconds']} s", slack)
    return provenance
