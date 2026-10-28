import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from clinvar_ingest.vrsify import store

URL = "https://example.org/refgetstore/genomic-test.tar.gz"


def make_archive(tmp_path, top_dirs=("genomic-test",)) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for top in top_dirs:
            root = tmp_path / "src" / top
            (root / "sequences" / "ab").mkdir(parents=True)
            (root / "rgstore.json").write_text(json.dumps({"sequences_digest": "seqs", "modified": "today"}))
            (root / "sequences" / "ab" / "abcd.seq").write_bytes(b"\x00\x01")
            tar.add(root, arcname=top)
    return buf.getvalue()


class FakeResponse:
    def __init__(self, body: bytes):
        self.raw = io.BytesIO(body)
        self.text = body.decode(errors="replace")

    def raise_for_status(self):
        pass

    def json(self):
        return json.loads(self.text)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def serve(monkeypatch):
    """Serve `archive` at URL and `sha256` at URL.sha256 through store's requests.get."""

    def install(archive: bytes, sha256: str | None = None):
        sha256 = sha256 or hashlib.sha256(archive).hexdigest()
        bodies = {URL: archive, URL + ".sha256": f"{sha256}  genomic-test.tar.gz\n".encode()}
        monkeypatch.setattr(store.requests, "get", lambda url, **_kwargs: FakeResponse(bodies[url]))

    return install


def test_archive_is_extracted_and_verified(tmp_path, serve):
    archive = make_archive(tmp_path)
    serve(archive)
    work = tmp_path / "work"
    work.mkdir()

    location, provenance = store.prepare_store(URL, work)
    store_dir = Path(location)

    assert store_dir.name == "genomic-test"
    assert (store_dir / "sequences" / "ab" / "abcd.seq").read_bytes() == b"\x00\x01"
    assert provenance == {
        "source": URL,
        "sha256": hashlib.sha256(archive).hexdigest(),
        "rgstore": {
            "sequences_digest": "seqs",
            "collections_digest": None,
            "aliases_digest": None,
            "modified": "today",
        },
    }
    # The archive itself is never written to disk
    assert [p.name for p in work.rglob("*.tar.gz")] == []


def test_checksum_mismatch_discards_the_extracted_store(tmp_path, serve):
    serve(make_archive(tmp_path), sha256="0" * 64)
    work = tmp_path / "work"
    work.mkdir()

    with pytest.raises(ValueError, match="expected 0000"):
        store.prepare_store(URL, work)
    assert list(work.iterdir()) == []


def test_archive_must_have_one_top_level_directory(tmp_path, serve):
    serve(make_archive(tmp_path, top_dirs=("one", "two")))
    work = tmp_path / "work"
    work.mkdir()

    with pytest.raises(ValueError, match="single top-level directory"):
        store.prepare_store(URL, work)
    assert list(work.iterdir()) == []


def test_local_store_directory_is_used_in_place(tmp_path):
    root = tmp_path / "local-store"
    root.mkdir()
    (root / "rgstore.json").write_text(json.dumps({"aliases_digest": "aliases"}))

    location, provenance = store.prepare_store(str(root), tmp_path)

    assert location == str(root)
    assert provenance["source"] == str(root.resolve())
    assert provenance["rgstore"]["aliases_digest"] == "aliases"


def test_remote_store_is_opened_by_url_without_downloading_it(tmp_path, monkeypatch):
    remote = "https://example.org/refgetstore/genomic-test"
    requested = []

    def get(url, **_kwargs):
        requested.append(url)
        return FakeResponse(json.dumps({"sequences_digest": "seqs", "modified": "today"}).encode())

    monkeypatch.setattr(store.requests, "get", get)

    location, provenance = store.prepare_store(remote, tmp_path)

    assert location == remote
    assert requested == [f"{remote}/rgstore.json"]
    assert provenance["source"] == remote
    assert provenance["remote"] is True
    assert provenance["rgstore"]["sequences_digest"] == "seqs"
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("location", "remote"),
    [
        ("https://example.org/refgetstore/genomic-2026-10-27", True),
        ("https://example.org/refgetstore/genomic-2026-10-27.tar.gz", False),
        ("gs://bucket/store.tar.gz", False),
        ("/data/refgetstore", False),
    ],
)
def test_is_remote_store(location, remote):
    assert store.is_remote_store(location) is remote
