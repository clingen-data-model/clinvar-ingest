"""
Run vrsify over variation_identity export files: read records, translate them in a
process pool, and write one `{"in": record, "out": ...}` line per input record.

Output lines are written as batches complete, so their order is not the input order.
"""

import datetime
import fnmatch
import gzip
import heapq
import importlib.metadata
import json
import logging
import os
import signal
import socket
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any

from clinvar_ingest.cloud.gcs import _get_gcs_client, parse_blob_uri
from clinvar_ingest.config import env_or_dotenv_or
from clinvar_ingest.vrsify.store import prepare_store
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


def _init_worker(store_dir: str, record_timeout: int) -> None:
    from ga4gh.vrs.dataproxy import create_dataproxy
    from ga4gh.vrs.extras.translator import AlleleTranslator, CnvTranslator

    data_proxy = create_dataproxy(f"refget+file://{store_dir}")
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


def _translate_batch(lines: list[str]) -> list[tuple[str, str, str, str, float]]:
    """Return (output line, vrs_class, status, source, seconds) per input line."""
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
    return results


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


def translate_files(
    input_paths: list[Path],
    output_path: Path,
    store_dir: Path,
    workers: int,
    batch_size: int,
    record_timeout: int,
) -> dict:
    """Translate every record in `input_paths` into `output_path`, returning summary statistics."""
    counts: Counter = Counter()
    slowest: list[tuple[float, str]] = []
    records = 0
    started = time.perf_counter()
    last_log = started
    max_pending = workers * 4

    def write(done: set[Future], out) -> None:
        nonlocal records, last_log
        for future in done:
            for line, vrs_class, status, source, elapsed in future.result():
                out.write(line + "\n")
                counts[f"{vrs_class}:{status}"] += 1
                heapq.heappush(slowest, (elapsed, source))
                if len(slowest) > SLOWEST_RECORDS_KEPT:
                    heapq.heappop(slowest)
                records += 1
        now = time.perf_counter()
        if now - last_log > PROGRESS_LOG_SECONDS:
            _logger.info(f"Translated {records} records in {now - started:.0f} s")
            last_log = now

    with (
        gzip.open(output_path, "wt", compresslevel=6) as out,
        ProcessPoolExecutor(workers, initializer=_init_worker, initargs=(str(store_dir), record_timeout)) as pool,
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

    return {
        "records": records,
        "counts": dict(sorted(counts.items())),
        "seconds": round(time.perf_counter() - started, 1),
        "slowest_records": [{"seconds": round(s, 2), "source": src} for s, src in sorted(slowest, reverse=True)],
    }


def _count_lines(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        with gzip.open(path, "rt") as f:
            total += sum(1 for line in f if line.strip())
    return total


def _fetch_inputs(input_uri: str, work_dir: Path) -> tuple[list[Path], list[dict]]:
    """Resolve `input_uri` (a gs:// URI or local path, optionally with glob characters) to local files."""
    if not input_uri.startswith("gs://"):
        paths = sorted(Path().glob(input_uri)) if any(c in input_uri for c in "*?[") else [Path(input_uri)]
        if not paths or not all(p.is_file() for p in paths):
            raise FileNotFoundError(f"No input files match {input_uri}")
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


def wait_for_input(uri: str, newer_than: datetime.datetime, timeout: int, poll_seconds: int = 60) -> None:
    """
    Wait up to `timeout` seconds for the gs:// object `uri` to have been written after
    `newer_than`. The stored-procedures job marks its row finished before it exports
    vi.jsonl.gz, so a release can be claimed before its export exists, and a re-run whose
    export failed leaves the previous run's export in place.
    """
    deadline = time.monotonic() + timeout
    while True:
        blob = parse_blob_uri(uri)
        if blob.exists():
            blob.reload()
            if blob.updated > newer_than:
                _logger.info(f"{uri} was written at {blob.updated.isoformat()}")
                return
            state = f"last written at {blob.updated.isoformat()}"
        else:
            state = "does not exist"
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{uri} {state}; waited {timeout} s for one written after {newer_than.isoformat()}")
        _logger.info(f"{uri} {state}; waiting for one written after {newer_than.isoformat()}")
        time.sleep(poll_seconds)


def _library_versions() -> dict:
    versions = {}
    for dist in ("clinvar_ingest", "ga4gh.vrs", "gtars", "hgvs"):
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
    input_newer_than: datetime.datetime | None = None,
    input_wait_seconds: int = 0,
    after_upload: Callable[[], None] | None = None,
) -> dict:
    """
    Translate the records at `input_uri` and write the results and a provenance record.

    With `input_newer_than`, first wait up to `input_wait_seconds` for `input_uri` (a single
    gs:// object) to have been written after that time. `after_upload` runs once both
    objects are written, before the finished message; if it raises, the run is reported
    as failed.
    """
    prov_uri = provenance_uri(output_uri)
    if not overwrite:
        for uri in (output_uri, prov_uri):
            if _exists(uri):
                raise FileExistsError(f"{uri} exists; pass --overwrite to replace it")

    started_at = datetime.datetime.now(datetime.UTC)
    notify(f"vrsify started: {input_uri} -> {output_uri}", slack)
    try:
        if input_newer_than is not None:
            wait_for_input(input_uri, input_newer_than, input_wait_seconds)
        work = Path(work_dir) if work_dir else Path(tempfile.mkdtemp(prefix="vrsify-"))
        work.mkdir(parents=True, exist_ok=True)
        store_dir, store_provenance = prepare_store(refget_store, work)
        input_paths, input_records = _fetch_inputs(input_uri, work)
        expected = _count_lines(input_paths)

        output_path = work / OUTPUT_NAME
        stats = translate_files(input_paths, output_path, store_dir, workers, batch_size, record_timeout)
        # The gkm_vrs load requires exactly one output row per input record
        if stats["records"] != expected:
            raise RuntimeError(f"Read {expected} records but wrote {stats['records']}")

        provenance = {
            "started_at": started_at.isoformat(),
            "finished_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "inputs": input_records,
            "output": output_uri,
            "refget_store": store_provenance,
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
        if after_upload is not None:
            after_upload()
    except Exception as e:
        notify(f"vrsify failed: {input_uri} -> {output_uri}: {type(e).__name__}: {e}", slack)
        raise
    notify(f"vrsify finished: {stats['records']} records in {stats['seconds']} s -> {output_uri}", slack)
    return provenance
