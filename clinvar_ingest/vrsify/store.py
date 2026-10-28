"""
Obtain a RefgetStore on local disk.

A store is either a local directory containing rgstore.json, or a .tar.gz archive of one
(https:// or gs://) published next to a .sha256 file. Archives are extracted as they
download, without writing the archive itself to disk, and the extracted store is
discarded unless the archive matches that SHA-256.
"""

import hashlib
import json
import logging
import shutil
import tarfile
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

import requests

from clinvar_ingest.cloud.gcs import parse_blob_uri

_logger = logging.getLogger("clinvar_ingest")

_CHUNK_SIZE = 8 * 1024 * 1024


class _HashingReader:
    """Read-through wrapper that computes the SHA-256 of everything read."""

    def __init__(self, raw: BinaryIO):
        self.raw = raw
        self.sha256 = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        data = self.raw.read(size)
        self.sha256.update(data)
        return data

    def drain(self) -> None:
        """Read to the end, so the digest covers bytes the tar reader left unread."""
        while self.read(_CHUNK_SIZE):
            pass


def _read_text(uri: str) -> str:
    if uri.startswith("gs://"):
        return parse_blob_uri(uri).download_as_text()
    resp = requests.get(uri, timeout=60)
    resp.raise_for_status()
    return resp.text


@contextmanager
def _open_stream(uri: str) -> Iterator[BinaryIO]:
    if uri.startswith("gs://"):
        with parse_blob_uri(uri).open("rb", chunk_size=_CHUNK_SIZE) as f:
            yield f
        return
    with requests.get(uri, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        # The archive's own bytes, which the SHA-256 is of; it is not transfer-encoded
        resp.raw.decode_content = False
        yield resp.raw


def _single_top_dir(tar: tarfile.TarFile, top_dirs: set[str]) -> Iterator[tarfile.TarInfo]:
    """Yield the archive's members, requiring them all to be under one top-level directory."""
    for member in tar:
        top_dirs.add(member.name.split("/", 1)[0])
        if len(top_dirs) != 1:
            raise ValueError(f"archive must hold a single top-level directory, found {sorted(top_dirs)}")
        yield member


def _extract_archive(uri: str, work_dir: Path) -> tuple[Path, str]:
    """Extract the archive at `uri` under `work_dir` as it downloads, verifying its SHA-256."""
    # "<sha256>  <file name>", as written by shasum
    expected = _read_text(uri + ".sha256").split()[0]
    extract_dir = Path(tempfile.mkdtemp(prefix="refgetstore-", dir=work_dir))
    top_dirs: set[str] = set()
    try:
        with _open_stream(uri) as raw:
            reader = _HashingReader(raw)
            # "r|gz": a forward-only stream, so no part of the archive is kept
            with tarfile.open(fileobj=reader, mode="r|gz") as tar:
                tar.extractall(extract_dir, members=_single_top_dir(tar, top_dirs), filter="data")
            reader.drain()
        actual = reader.sha256.hexdigest()
        if actual != expected:
            raise ValueError(f"SHA-256 of {uri} is {actual}, expected {expected}")
        if not top_dirs:
            raise ValueError(f"{uri} is an empty archive")
    except BaseException:
        shutil.rmtree(extract_dir, ignore_errors=True)
        raise
    return extract_dir / top_dirs.pop(), actual


def prepare_store(store: str, work_dir: Path) -> tuple[Path, dict]:
    """
    Return the local store directory for `store` and a provenance record describing it.
    """
    local = Path(store)
    if local.is_dir():
        store_dir = local
        provenance = {"source": str(local.resolve())}
    elif store.endswith(".tar.gz") and store.startswith(("https://", "http://", "gs://")):
        _logger.info(f"Downloading and extracting RefgetStore archive {store}")
        store_dir, sha256 = _extract_archive(store, work_dir)
        provenance = {"source": store, "sha256": sha256}
    else:
        raise ValueError(f"Not a store directory or a .tar.gz archive URL: {store}")

    metadata = json.loads((store_dir / "rgstore.json").read_text())
    provenance["rgstore"] = {
        key: metadata.get(key) for key in ("sequences_digest", "collections_digest", "aliases_digest", "modified")
    }
    return store_dir, provenance
