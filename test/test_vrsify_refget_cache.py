"""
Prefetching a remote RefgetStore's sequence files into gtars's cache of it, against a
store served over HTTP from a local directory.
"""

import gzip
import json
import multiprocessing
import random
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

refget = pytest.importorskip("gtars.refget")
hgvs_uta = pytest.importorskip("hgvs.dataproviders.uta")

import bioutils.seqfetcher  # noqa: E402

from clinvar_ingest.vrsify import refget_cache, runner  # noqa: E402
from clinvar_ingest.vrsify.refget_cache import TEMP_PREFIX, cache_sequence_files  # noqa: E402
from clinvar_ingest.vrsify.translate import translate_record  # noqa: E402

# No record reads NC_000003.12, so a prefetched cache lacks its file
ACCESSIONS = {"NC_000001.11": 5000, "NC_000002.12": 3000, "NC_000003.12": 4000}


@pytest.fixture(scope="module")
def store_dir(tmp_path_factory):
    work = tmp_path_factory.mktemp("refget")
    rng = random.Random(320)  # noqa: S311 - reproducible test sequences
    fasta = work / "genome.fa"
    fasta.write_text("".join(f">{ac}\n{''.join(rng.choices('ACGT', k=n))}\n" for ac, n in ACCESSIONS.items()))
    store = refget.RefgetStore.on_disk(str(work / "store"))
    store.set_quiet(True)
    store.add_sequence_collection_from_fasta(str(fasta))
    for metadata in store.list_sequences():
        store.add_sequence_alias("refseq", metadata.name, metadata.sha512t24u)
    store.write()
    return work / "store"


@pytest.fixture(scope="module")
def digests(store_dir):
    store = refget.RefgetStore.open_local(str(store_dir))
    return {m.name: m.sha512t24u for m in store.list_sequences()}


class StoreHandler(SimpleHTTPRequestHandler):
    """Serves a directory; `faults[path]` lists what the next GETs of path do instead."""

    faults: Any
    gets: Any

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.gets[self.path] = self.gets.get(self.path, 0) + 1
        pending = self.faults.get(self.path) or []
        fault = pending[0] if pending else None
        self.faults[self.path] = pending[1:]
        if fault == "503":
            self.send_error(503)
        elif fault == "short":
            body = (Path(self.directory) / self.path.lstrip("/")).read_bytes()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body[: len(body) // 2])
            self.close_connection = True
        else:
            super().do_GET()


def _serve(directory, port, faults, gets):
    handler = type("Handler", (StoreHandler,), {"faults": faults, "gets": gets})
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), partial(handler, directory=directory))
    port.put(httpd.server_address[1])
    httpd.serve_forever()


@pytest.fixture
def server(store_dir):
    """
    The store at store_dir served over HTTP: (url, faults, gets). In a process of its
    own, because gtars holds the GIL through its requests, which a server thread in this
    process could then not answer.
    """
    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager:
        faults, gets, port = manager.dict(), manager.dict(), context.Queue()
        process = context.Process(target=_serve, args=(str(store_dir), port, faults, gets), daemon=True)
        process.start()
        try:
            yield f"http://127.0.0.1:{port.get(timeout=60)}", faults, gets
        finally:
            process.terminate()
            process.join()


@pytest.fixture(autouse=True)
def no_retry_pause(monkeypatch):
    monkeypatch.setattr(refget_cache, "RETRY_PAUSE_SECONDS", 0)


def open_remote(url, cache_dir):
    store = refget.RefgetStore.open_remote(str(cache_dir), url)
    store.set_quiet(True)
    store.get_sequence_metadata("")  # loads the sequence index
    return store


def seq_path(digest):
    return f"sequences/{digest[:2]}/{digest}.seq"


def test_prefetch_writes_the_store_files_and_skips_cached_ones(store_dir, digests, server, tmp_path):
    url, _, _ = server
    store = open_remote(url, tmp_path)
    requested = [f"SQ.{d}" for d in digests.values()]

    report = cache_sequence_files(store, url, tmp_path, requested, threads=4)

    assert report == {
        "requested": 3,
        "distinct": 3,
        "already_cached": 0,
        "cached": 3,
        "cached_bytes": sum((store_dir / seq_path(d)).stat().st_size for d in digests.values()),
        "failed_count": 0,
        "failed": [],
    }
    for digest in digests.values():
        assert (tmp_path / seq_path(digest)).read_bytes() == (store_dir / seq_path(digest)).read_bytes()
    assert not list(tmp_path.rglob(f"{TEMP_PREFIX}*"))
    again = cache_sequence_files(store, url, tmp_path, requested, threads=4)
    assert (again["already_cached"], again["cached"]) == (3, 0)


def test_prefetch_reports_digests_the_store_does_not_hold(digests, server, tmp_path):
    url, _, _ = server
    report = cache_sequence_files(open_remote(url, tmp_path), url, tmp_path, ["SQ.notadigest"], threads=2)
    assert report["failed_count"] == 1
    assert report["failed"][0][0] == "SQ.notadigest"


def test_transient_failures_are_retried(digests, server, tmp_path):
    url, faults, gets = server
    digest = digests["NC_000002.12"]
    faults[f"/{seq_path(digest)}"] = ["503", "short"]
    report = cache_sequence_files(open_remote(url, tmp_path), url, tmp_path, [digest], threads=1)
    assert (report["cached"], report["failed_count"]) == (1, 0)
    assert gets.get(f"/{seq_path(digest)}") == 3
    assert not list(tmp_path.rglob(f"{TEMP_PREFIX}*"))


def test_persistent_failures_are_reported_after_the_last_attempt(digests, server, tmp_path):
    url, faults, gets = server
    digest = digests["NC_000002.12"]
    faults[f"/{seq_path(digest)}"] = ["503"] * refget_cache.DOWNLOAD_ATTEMPTS
    report = cache_sequence_files(open_remote(url, tmp_path), url, tmp_path, [digest], threads=1)
    assert report["failed_count"] == 1
    assert gets.get(f"/{seq_path(digest)}") == refget_cache.DOWNLOAD_ATTEMPTS
    assert not (tmp_path / seq_path(digest)).exists()


def test_a_missing_file_is_not_retried(digests, server, tmp_path):
    url, _, gets = server
    digest = digests["NC_000002.12"]
    store = open_remote(url, tmp_path / "cache")
    # Fetched from a path the server has nothing under, while the index still lists it
    missing = f"{url}/missing"
    report = cache_sequence_files(store, missing, tmp_path / "cache", [digest], threads=1)
    assert report["failed_count"] == 1
    assert "404" in report["failed"][0][1]
    assert gets.get(f"/missing/{seq_path(digest)}") == 1


# Translation from a prefetched cache


RECORDS = [
    {
        "vrs_class": "Allele",
        "fmt": "spdi",
        "source": "NC_000001.11:100:A:T",
        "variation_type": "single nucleotide variant",
    },
    {"vrs_class": "Allele", "fmt": "hgvs", "source": "NC_000002.12:g.151_152del", "variation_type": "Deletion"},
    {"vrs_class": "Allele", "fmt": "hgvs", "source": "NC_000002.12:g.2000dup", "variation_type": "Duplication"},
]


@pytest.fixture
def restore_patches(monkeypatch):
    """Restore what a worker initializer patches once the test ends."""
    from ga4gh.vrs.dataproxy_refget import RefgetSequenceDataProxy

    monkeypatch.setattr(hgvs_uta, "connect", hgvs_uta.connect)
    monkeypatch.setattr(bioutils.seqfetcher, "fetch_seq", bioutils.seqfetcher.fetch_seq)
    monkeypatch.setattr(
        RefgetSequenceDataProxy, "_ensure_sequence_loaded", RefgetSequenceDataProxy._ensure_sequence_loaded
    )
    monkeypatch.delenv("GA4GH_VRS_REFGET_CACHE_DIR", raising=False)


def translate_all(source):
    runner._init_worker(source, 30)
    return [
        translate_record(r, runner._WORKER["allele_translators"], runner._WORKER["cnv_translator"]) for r in RECORDS
    ]


def write_input(path):
    with gzip.open(path, "wt") as fh:
        fh.writelines(json.dumps(r) + "\n" for r in RECORDS)
    return path


@pytest.mark.parametrize("reads", ["whole", "ranges"])
def test_workers_translate_from_the_prefetched_cache_as_from_the_store(
    store_dir, server, tmp_path, restore_patches, reads
):
    url, _, gets = server
    input_path = write_input(tmp_path / "vi.jsonl.gz")

    report, cache_dir = runner.prefetch_sequences([input_path], url, tmp_path / "cache", threads=4)
    assert (report["identifiers"], report["cached"], report["failed_count"]) == (2, 2, 0)
    assert len(list(cache_dir.rglob("*.seq"))) == 2  # of the store's 3
    fetched = sum(gets.values())
    prefetched = translate_all(runner.SequenceSource("refget", str(cache_dir), reads))
    local = translate_all(runner.SequenceSource("refget", str(store_dir), reads))

    assert all("id" in out for out in prefetched), prefetched
    assert prefetched == local
    # The workers read the cache as a local store, fetching nothing
    assert sum(gets.values()) == fetched


def test_prefetch_raises_when_a_sequence_cannot_be_fetched(digests, server, tmp_path, restore_patches):
    url, faults, _ = server
    faults[f"/{seq_path(digests['NC_000002.12'])}"] = ["503"] * refget_cache.DOWNLOAD_ATTEMPTS
    input_path = write_input(tmp_path / "vi.jsonl.gz")
    with pytest.raises(RuntimeError, match="Could not prefetch 1 of 2"):
        runner.prefetch_sequences([input_path], url, tmp_path / "cache", threads=4)


def test_workers_do_not_open_a_remote_store(server):
    url, _, _ = server
    with pytest.raises(ValueError, match="prefetch the remote store first"):
        runner._data_proxy(runner.SequenceSource("refget", url))


def test_prefetch_threads_must_be_positive():
    with pytest.raises(ValueError, match="at least 1"):
        runner._check_sequence_options(None, "whole", 0)
    runner._check_sequence_options(None, "ranges", 1)


@pytest.mark.parametrize("reads", ["whole", "ranges"])
def test_vrsify_prefetches_a_remote_store(store_dir, server, tmp_path, restore_patches, reads):
    url, _, _ = server
    input_path = write_input(tmp_path / "vi.jsonl.gz")
    output = tmp_path / "out.jsonl.gz"

    runner.vrsify(str(input_path), str(output), url, workers=1, work_dir=str(tmp_path / "work"), refget_reads=reads)

    provenance = json.loads(Path(runner.provenance_uri(str(output))).read_text())
    assert provenance["refget_store"]["remote"] is True
    assert (provenance["refget_prefetch"]["cached"], provenance["refget_prefetch"]["failed_count"]) == (2, 0)
    assert provenance["refget_cache"]["unfinished_fetches"] == 0
    with gzip.open(output, "rt") as fh:
        outputs = {row["in"]["source"]: row["out"] for row in map(json.loads, fh)}
    expected = translate_all(runner.SequenceSource("refget", str(store_dir), reads))
    assert [outputs[r["source"]] for r in RECORDS] == expected
