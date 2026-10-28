"""
Obtain a RefgetStore, or a SeqRepo.

A store is a local directory containing rgstore.json, a .tar.gz archive of one (https://
or gs://) published next to a .sha256 file, or the URL of a remote store directory
(https://), whose sequences are fetched as they are first read. A SeqRepo is a local
instance directory or a .tar.gz archive of one, likewise published next to a .sha256
file. Archives are extracted as they download, without writing the archive itself to
disk, and the extracted directory is discarded unless the archive matches that SHA-256.
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
    extract_dir = Path(tempfile.mkdtemp(prefix="store-", dir=work_dir))
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


def _seqrepo_instance(root: Path) -> Path:
    """The SeqRepo instance directory (holding aliases.sqlite3) at or just under `root`."""
    for candidate in (root, *sorted(p for p in root.iterdir() if p.is_dir())):
        if (candidate / "aliases.sqlite3").is_file():
            return candidate
    raise ValueError(f"No SeqRepo instance (aliases.sqlite3) at or under {root}")


def prepare_seqrepo(seqrepo: str, work_dir: Path) -> tuple[str, dict]:
    """
    Return the SeqRepo instance directory for `seqrepo`, a local instance directory or a
    .tar.gz archive of one, and a provenance record describing it.
    """
    local = Path(seqrepo)
    if local.is_dir():
        provenance = {"source": str(local.resolve())}
        root = local
    elif seqrepo.endswith(".tar.gz") and seqrepo.startswith(("https://", "http://", "gs://")):
        _logger.info(f"Downloading and extracting SeqRepo archive {seqrepo}")
        root, sha256 = _extract_archive(seqrepo, work_dir)
        provenance = {"source": seqrepo, "sha256": sha256}
    else:
        raise ValueError(f"Not a SeqRepo directory or a .tar.gz archive URL: {seqrepo}")
    instance = _seqrepo_instance(root)
    provenance["instance"] = instance.name
    return str(instance), provenance


def is_remote_store(store: str) -> bool:
    """Whether `store` names a remote store directory, as opposed to a local one or an archive."""
    return store.startswith(("https://", "http://")) and not store.endswith(".tar.gz")


def prepare_store(store: str, work_dir: Path) -> tuple[str, dict]:
    """
    Return where translation workers open `store` (a local directory, or the remote store's
    URL) and a provenance record describing it.
    """
    local = Path(store)
    if is_remote_store(store):
        response = requests.get(f"{store.rstrip('/')}/rgstore.json", timeout=60)
        response.raise_for_status()
        location, metadata = store, response.json()
        provenance = {"source": store, "remote": True}
    else:
        if local.is_dir():
            store_dir = local
            provenance = {"source": str(local.resolve())}
        elif store.endswith(".tar.gz") and store.startswith(("https://", "http://", "gs://")):
            _logger.info(f"Downloading and extracting RefgetStore archive {store}")
            store_dir, sha256 = _extract_archive(store, work_dir)
            provenance = {"source": store, "sha256": sha256}
        else:
            raise ValueError(f"Not a store directory, a .tar.gz archive URL or a remote store URL: {store}")
        location, metadata = str(store_dir), json.loads((store_dir / "rgstore.json").read_text())

    provenance["rgstore"] = {
        key: metadata.get(key) for key in ("sequences_digest", "collections_digest", "aliases_digest", "modified")
    }
    return location, provenance
