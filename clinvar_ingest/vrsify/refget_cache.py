"""
Fetch a remote RefgetStore's sequence files into gtars's local cache of that store.

gtars caches a remote store under one directory laid out as the store is: its manifest
(rgstore.json), indexes and aliases, and each fetched sequence file at the path the
manifest's `seqdata_path_template` gives it. A file placed at that path is read from
disk as if gtars had fetched it, so the files a run will read can be downloaded here
before any worker starts: streamed to disk rather than held in memory, with bounded
waits and retries, and published by rename so no reader sees a partial file.
"""

import json
import logging
import os
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

_logger = logging.getLogger("clinvar_ingest")

DOWNLOAD_CHUNK = 1 << 20
CONNECT_TIMEOUT_SECONDS = 30
# Longest wait for the response, and for each read of its body
READ_TIMEOUT_SECONDS = 60
DOWNLOAD_ATTEMPTS = 4
# Pause before the second attempt, doubling after
RETRY_PAUSE_SECONDS = 0.5
# Named as gtars names its own temporary files, so one count of unfinished downloads
# (runner.vrsify's provenance) covers both
TEMP_PREFIX = ".rgstore.tmp."
FAILED_KEPT = 20


class TransientDownloadError(Exception):
    """A failure the remote or the network may not repeat, so the download is retried."""


def sequence_relpath(template: str, digest: str) -> str:
    """A sequence file's path within a store, as gtars expands `seqdata_path_template`."""
    return template.replace("%s2", digest[:2]).replace("%s4", digest[:4]).replace("%s", digest)


def _bare_digest(digest: str) -> str:
    return digest.removeprefix("ga4gh:")


def _download_once(session: requests.Session, url: str, dest: Path) -> int:
    """GET `url` into `dest` through a temporary file; return the bytes written."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    temp = dest.with_name(f"{TEMP_PREFIX}{dest.name}.{os.getpid()}.{threading.get_ident()}")
    try:
        with session.get(url, stream=True, timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS)) as response:
            if response.status_code == 429 or response.status_code >= 500:  # noqa: PLR2004
                raise TransientDownloadError(f"HTTP {response.status_code} from {url}")
            response.raise_for_status()
            written = 0
            with open(temp, "wb") as fh:
                for chunk in response.iter_content(DOWNLOAD_CHUNK):
                    fh.write(chunk)
                    written += len(chunk)
            expected = response.headers.get("Content-Length")
            # A decoded body's length differs from Content-Length, which counts encoded bytes
            if expected is not None and "Content-Encoding" not in response.headers and written != int(expected):
                raise TransientDownloadError(f"{url} ended after {written} of {expected} bytes")
        os.replace(temp, dest)
        return written
    except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
        raise TransientDownloadError(f"{type(e).__name__} fetching {url}: {e}") from e
    finally:
        temp.unlink(missing_ok=True)


def download_file(get_session: Callable[[], requests.Session], url: str, dest: Path) -> int:
    """
    Download `url` to `dest`, retrying a transient failure (a failed or reset connection,
    a timeout, HTTP 429 or 5xx, a body cut short) up to DOWNLOAD_ATTEMPTS attempts with
    doubling pauses. Any other failure, such as a 404, is raised at once.
    """
    pause = RETRY_PAUSE_SECONDS
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            return _download_once(get_session(), url, dest)
        except TransientDownloadError as e:
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
            _logger.warning(f"Retrying in {pause:g} s after attempt {attempt}: {e}")
            time.sleep(pause)
            pause *= 2
    raise AssertionError("unreachable")


def cache_sequence_files(store: Any, store_url: str, cache_dir: Path, digests: Iterable[str], threads: int) -> dict:
    """
    Download the sequence files of `digests` from the remote store at `store_url` into
    `cache_dir`, gtars's cache of it, `threads` at a time and largest first, skipping
    files already there. `store` is that remote store opened over `cache_dir` with its
    sequence index loaded, which resolves each digest (SHA-512/24u, `SQ.`-prefixed or
    MD5) to its sequence. A digest that does not resolve or a file that cannot be fetched
    is reported in "failed", up to FAILED_KEPT of them, and counted in "failed_count".
    """
    digests = list(digests)
    template = json.loads((cache_dir / "rgstore.json").read_text())["seqdata_path_template"]
    failed: list[tuple[str, str]] = []
    lengths: dict[str, int] = {}
    for digest in digests:
        metadata = store.get_sequence_metadata(_bare_digest(digest))
        if metadata is None:
            failed.append((digest, "not in the store's sequence index"))
            continue
        lengths[metadata.sha512t24u] = metadata.length
    pending = []
    for digest, length in lengths.items():
        relpath = sequence_relpath(template, digest)
        if not (cache_dir / relpath).is_file():
            pending.append((length, digest, relpath))
    pending.sort(reverse=True)

    # A session per thread: each keeps its connections open between that thread's files
    local = threading.local()
    sessions: list[requests.Session] = []
    sessions_lock = threading.Lock()

    def get_session() -> requests.Session:
        if not hasattr(local, "session"):
            local.session = requests.Session()
            with sessions_lock:
                sessions.append(local.session)
        return local.session

    base = store_url.rstrip("/")
    cached = cached_bytes = 0
    try:
        with ThreadPoolExecutor(max_workers=max(1, min(threads, len(pending)))) as pool:
            futures = {
                pool.submit(download_file, get_session, f"{base}/{relpath}", cache_dir / relpath): digest
                for _, digest, relpath in pending
            }
            for future in as_completed(futures):
                try:
                    cached_bytes += future.result()
                    cached += 1
                except Exception as e:  # noqa: BLE001 - reported, and the caller decides
                    failed.append((futures[future], f"{e}"))
    finally:
        for session in sessions:
            session.close()
    return {
        "requested": len(digests),
        "distinct": len(lengths),
        "already_cached": len(lengths) - len(pending),
        "cached": cached,
        "cached_bytes": cached_bytes,
        "failed_count": len(failed),
        "failed": failed[:FAILED_KEPT],
    }
