"""
Obtain a RefgetStore on local disk.

A store is either a local directory containing rgstore.json, or a .tar.gz archive of one
(https:// or gs://) published next to a .sha256 file. Archives are downloaded, checked
against that SHA-256, and extracted under a working directory.
"""

import hashlib
import json
import logging
import tarfile
from pathlib import Path

import requests

from clinvar_ingest.cloud.gcs import parse_blob_uri

_logger = logging.getLogger("clinvar_ingest")

_CHUNK_SIZE = 8 * 1024 * 1024


def _read_text(uri: str) -> str:
    if uri.startswith("gs://"):
        return parse_blob_uri(uri).download_as_text()
    resp = requests.get(uri, timeout=60)
    resp.raise_for_status()
    return resp.text


def _download(uri: str, dest: Path) -> str:
    """Download `uri` to `dest`, returning its SHA-256."""
    digest = hashlib.sha256()
    if uri.startswith("gs://"):
        parse_blob_uri(uri).download_to_filename(str(dest))
        with dest.open("rb") as f:
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                digest.update(chunk)
        return digest.hexdigest()
    with requests.get(uri, stream=True, timeout=60) as resp, dest.open("wb") as f:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=_CHUNK_SIZE):
            digest.update(chunk)
            f.write(chunk)
    return digest.hexdigest()


def prepare_store(store: str, work_dir: Path) -> tuple[Path, dict]:
    """
    Return the local store directory for `store` and a provenance record describing it.
    """
    local = Path(store)
    if local.is_dir():
        store_dir = local
        provenance = {"source": str(local.resolve())}
    elif store.endswith(".tar.gz") and store.startswith(("https://", "http://", "gs://")):
        # "<sha256>  <file name>", as written by shasum
        expected = _read_text(store + ".sha256").split()[0]
        archive = work_dir / store.rsplit("/", 1)[-1]
        _logger.info(f"Downloading RefgetStore archive {store}")
        actual = _download(store, archive)
        if actual != expected:
            raise ValueError(f"SHA-256 of {store} is {actual}, expected {expected}")
        _logger.info(f"Extracting {archive}")
        with tarfile.open(archive) as tar:
            top_dirs = {name.split("/", 1)[0] for name in tar.getnames()}
            if len(top_dirs) != 1:
                raise ValueError(f"{store} must hold a single top-level directory, found {sorted(top_dirs)}")
            tar.extractall(work_dir, filter="data")
        archive.unlink()
        store_dir = work_dir / top_dirs.pop()
        provenance = {"source": store, "sha256": actual}
    else:
        raise ValueError(f"Not a store directory or a .tar.gz archive URL: {store}")

    metadata = json.loads((store_dir / "rgstore.json").read_text())
    provenance["rgstore"] = {
        key: metadata.get(key) for key in ("sequences_digest", "collections_digest", "aliases_digest", "modified")
    }
    return store_dir, provenance
