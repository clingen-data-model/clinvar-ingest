"""
Translation in a vrsify worker against a real RefgetStore, with UTA and network sequence
fetching disabled as the worker initializer disables them.
"""

import random

import pytest

hgvs_uta = pytest.importorskip("hgvs.dataproviders.uta")
refget = pytest.importorskip("gtars.refget")

import bioutils.seqfetcher  # noqa: E402

from clinvar_ingest.vrsify import offline, runner  # noqa: E402
from clinvar_ingest.vrsify.translate import translate_record  # noqa: E402

ACCESSION = "NC_000002.12"
LENGTH = 3000


@pytest.fixture(scope="module")
def store_dir(tmp_path_factory):
    work = tmp_path_factory.mktemp("refget")
    rng = random.Random(320)  # noqa: S311 - a reproducible test sequence
    sequence = list(rng.choices("ACGT", k=LENGTH))
    sequence[150] = "A"  # g.151
    fasta = work / "chr2.fa"
    fasta.write_text(f">{ACCESSION}\n" + "".join(sequence) + "\n")

    store = refget.RefgetStore.on_disk(str(work / "store"))
    store.set_quiet(True)
    store.add_sequence_collection_from_fasta(str(fasta))
    (digest,) = [m.sha512t24u for m in store.list_sequences()]
    store.add_sequence_alias("refseq", ACCESSION, digest)
    store.write()
    return work / "store"


@pytest.fixture(scope="module")
def seqrepo_dir(tmp_path_factory, store_dir):
    """A SeqRepo holding the same sequence as store_dir."""
    seqrepo = pytest.importorskip("biocommons.seqrepo")
    (sequence,) = [line for line in (store_dir.parent / "chr2.fa").read_text().splitlines() if not line.startswith(">")]
    root = tmp_path_factory.mktemp("seqrepo") / "2026-10-28"
    repo = seqrepo.SeqRepo(str(root), writeable=True)
    repo.store(sequence, [{"namespace": "NCBI", "alias": ACCESSION}])
    repo.commit()
    return root


@pytest.fixture
def restore_patches(monkeypatch):
    """Restore what a worker initializer patches once the test ends."""
    from ga4gh.vrs.dataproxy_refget import RefgetSequenceDataProxy

    monkeypatch.setattr(hgvs_uta, "connect", hgvs_uta.connect)
    monkeypatch.setattr(bioutils.seqfetcher, "fetch_seq", bioutils.seqfetcher.fetch_seq)
    monkeypatch.setattr(
        RefgetSequenceDataProxy, "_ensure_sequence_loaded", RefgetSequenceDataProxy._ensure_sequence_loaded
    )


@pytest.fixture
def worker(store_dir, restore_patches):
    runner._init_worker(runner.SequenceSource("refget", str(store_dir)), 30)
    return runner._WORKER


def translate(worker, record):
    return translate_record(record, worker["allele_translators"], worker["cnv_translator"])


@pytest.mark.parametrize(
    ("record", "vrs_type"),
    [
        ({"vrs_class": "Allele", "fmt": "hgvs", "source": f"{ACCESSION}:g.151A>T"}, "Allele"),
        ({"vrs_class": "Allele", "fmt": "spdi", "source": f"{ACCESSION}:150:A:T"}, "Allele"),
        (
            {
                "vrs_class": "CopyNumberChange",
                "fmt": "hgvs",
                "source": f"{ACCESSION}:g.1001_2000del",
                "variation_type": "Deletion",
            },
            "CopyNumberChange",
        ),
        (
            {
                "vrs_class": "CopyNumberChange",
                "fmt": "hgvs",
                "source": f"{ACCESSION}:g.(?_1001)_(2000_?)del",
                "variation_type": "Deletion",
            },
            "CopyNumberChange",
        ),
    ],
)
def test_genomic_expressions_translate_without_uta(worker, record, vrs_type):
    out = translate(worker, record)
    assert out.get("type") == vrs_type, out
    assert out["id"].startswith("ga4gh:")


def test_translators_hold_the_uta_placeholder(worker):
    translate(worker, {"vrs_class": "Allele", "fmt": "hgvs", "source": f"{ACCESSION}:g.151A>T"})
    translate(
        worker,
        {
            "vrs_class": "CopyNumberChange",
            "fmt": "hgvs",
            "source": f"{ACCESSION}:g.1001_2000del",
            "variation_type": "Deletion",
        },
    )
    # Fails if vrs-python stops looking up hgvs.dataproviders.uta.connect at call time,
    # which would let the translators connect to UTA again
    translators = [*worker["allele_translators"].values(), worker["cnv_translator"]]
    assert all(isinstance(t.hgvs_tools.uta_conn, offline.DisabledUTA) for t in translators)


def test_uta_use_raises(worker):
    tools = worker["cnv_translator"].hgvs_tools
    with pytest.raises(offline.RemoteServiceDisabledError, match="UTA access is disabled"):
        tools.uta_conn.get_tx_identity_info("NM_000551.3")
    tools.uta_conn.close()


def test_network_sequence_fetching_raises(worker):
    with pytest.raises(offline.RemoteServiceDisabledError, match="network sequence fetching is disabled"):
        bioutils.seqfetcher.fetch_seq("NC_000017.11", 0, 10)


def test_accession_missing_from_the_store_is_an_error(worker):
    out = translate(
        worker,
        {
            "vrs_class": "CopyNumberChange",
            "fmt": "hgvs",
            "source": "NC_000017.11:g.1001_2000del",
            "variation_type": "Deletion",
        },
    )
    assert "errors" in out
    assert "disabled" not in out["errors"]


RECORDS = [
    {"vrs_class": "Allele", "fmt": "hgvs", "source": f"{ACCESSION}:g.151A>T"},
    {"vrs_class": "Allele", "fmt": "spdi", "source": f"{ACCESSION}:150:A:T"},
    {"vrs_class": "Allele", "fmt": "hgvs", "source": f"{ACCESSION}:g.1001_1010del"},
    {
        "vrs_class": "CopyNumberChange",
        "fmt": "hgvs",
        "source": f"{ACCESSION}:g.1001_2000del",
        "variation_type": "Deletion",
    },
    {
        "vrs_class": "CopyNumberChange",
        "fmt": "hgvs",
        "source": f"{ACCESSION}:g.(?_1001)_(2000_?)del",
        "variation_type": "Deletion",
    },
]


def translate_all(source):
    runner._init_worker(source, 30)
    return [translate(runner._WORKER, record) for record in RECORDS]


def loaded_digests() -> set[str]:
    """The sequences this worker's data proxy loaded whole."""
    return runner._WORKER["cnv_translator"].data_proxy._sequence_proxy._loaded_digests


def test_range_reads_translate_as_whole_sequence_loads_do(store_dir, restore_patches):
    whole = translate_all(runner.SequenceSource("refget", str(store_dir)))
    assert all("id" in out for out in whole), whole
    assert loaded_digests()
    ranges = translate_all(runner.SequenceSource("refget", str(store_dir), reads="ranges"))
    assert ranges == whole
    # The sequence stays unloaded: gtars serves reads from the .seq file
    assert not loaded_digests()


def test_seqrepo_translates_as_the_refget_store_does(store_dir, seqrepo_dir, restore_patches):
    refget_outputs = translate_all(runner.SequenceSource("refget", str(store_dir)))
    seqrepo_outputs = translate_all(runner.SequenceSource("seqrepo", str(seqrepo_dir)))
    assert seqrepo_outputs == refget_outputs


def test_worker_reports_its_resource_use(worker):
    results, report = runner._translate_batch(['{"vrs_class": "Unknown"}'])
    assert results[0][2] == "null"
    assert report["pid"] > 0
    assert report["batch_end"] >= report["batch_start"] >= report["started"]
    assert report["cpu_seconds"] >= report["batch_cpu_seconds"] >= 0
    assert report["peak_rss_bytes"] > 0


def test_worker_stats_summarize_utilization():
    stats = runner._WorkerStats(run_start=1000.0)
    # 2 s of CPU (interpreter start, imports) before the worker initialized
    base = {
        "pid": 7,
        "started": 1000.0,
        "cpu_at_start": 2,
        "user_seconds": 0,
        "system_seconds": 0,
        "peak_rss_bytes": 2**20,
    }
    counters = {
        "voluntary_context_switches": 3,
        "involuntary_context_switches": 1,
        "major_page_faults": 0,
        "block_reads": 0,
    }
    # 8 s of CPU in a 10 s batch, then 4 s of CPU in a 5 s batch spanning two windows
    stats.add(base | counters | {"batch_start": 1000.0, "batch_end": 1010.0, "batch_cpu_seconds": 8, "cpu_seconds": 10})
    stats.add(base | counters | {"batch_start": 1018.0, "batch_end": 1023.0, "batch_cpu_seconds": 4, "cpu_seconds": 14})
    (summary,) = stats.summary()
    assert summary["lifetime_seconds"] == 23.0
    assert summary["busy_seconds"] == 15.0
    assert summary["cpu_seconds"] == 12.0
    assert summary["startup_cpu_seconds"] == 2.0
    assert summary["cpu_utilization_busy"] == 0.8
    assert summary["cpu_utilization_lifetime"] == round(12 / 23, 3)
    assert summary["cpu_seconds_per_window"] == [8.0, 1.6, 2.4]
