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


@pytest.fixture
def worker(store_dir, monkeypatch):
    # Restore the patched functions after each test
    monkeypatch.setattr(hgvs_uta, "connect", hgvs_uta.connect)
    monkeypatch.setattr(bioutils.seqfetcher, "fetch_seq", bioutils.seqfetcher.fetch_seq)
    runner._init_worker(str(store_dir), 30)
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
