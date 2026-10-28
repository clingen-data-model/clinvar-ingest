import pytest

from clinvar_ingest.vrsify.translate import is_genomic, translate_record


class FakeVariation:
    def __init__(self, dump: dict):
        self.dump = dump

    def model_dump(self, exclude_none: bool):
        assert exclude_none
        return self.dump


class FakeTranslator:
    """Records translate_from calls and returns a variation, or raises `error`."""

    def __init__(self, name: str, error: Exception | None = None):
        self.name = name
        self.error = error
        self.calls = []

    def translate_from(self, source, fmt, **kwargs):
        self.calls.append((source, fmt, kwargs))
        if self.error:
            raise self.error
        return FakeVariation({"type": self.name, "location": {"start": 1, "end": 2, "sequence": "A"}, "kwargs": kwargs})


@pytest.fixture
def translators():
    alleles = {version: FakeTranslator(f"Allele{version}") for version in ("38", "37", "36")}
    return alleles, FakeTranslator("CNV")


@pytest.mark.parametrize(
    ("fmt", "source", "expected"),
    [
        ("spdi", "NC_000001.11:100:A:T", True),
        ("gnomad", "1-100-A-T", True),
        ("hgvs", "NC_000001.11:g.100A>T", True),
        ("hgvs", "NC_012920.1:m.100A>G", True),
        ("hgvs", "NG_009385.2:g.(?_5001)_(40068_?)del", True),
        ("hgvs", "NM_000551.3:c.100A>T", False),
        ("hgvs", "LRG_556t1:c.(391+1_392-1)insN[?]", False),
        ("hgvs", "NP_000542.1:p.Arg34Ter", False),
        (None, "NC_000001.11:g.100A>T", False),
    ],
)
def test_is_genomic(fmt, source, expected):
    assert is_genomic(fmt, source) is expected


def test_allele_uses_assembly_version_and_drops_location_sequence(translators):
    alleles, cnv = translators
    out = translate_record(
        {"vrs_class": "Allele", "fmt": "gnomad", "source": "1-100-A-T", "assembly_version": "37"}, alleles, cnv
    )
    assert out == {"type": "Allele37", "location": {"start": 1, "end": 2}, "kwargs": {}}
    assert alleles["37"].calls == [("1-100-A-T", "gnomad", {})]


def test_allele_defaults_to_grch38(translators):
    alleles, cnv = translators
    out = translate_record({"vrs_class": "Allele", "fmt": "spdi", "source": "NC_000001.11:100:A:T"}, alleles, cnv)
    assert out["type"] == "Allele38"


@pytest.mark.parametrize(
    ("record", "copy_change"),
    [
        ({"copy_change_type": "loss", "variation_type": "Deletion"}, "loss"),
        ({"variation_type": "Duplication"}, "gain"),
        ({"variation_type": "copy number loss"}, "loss"),
    ],
)
def test_copy_number_change(translators, record, copy_change):
    alleles, cnv = translators
    source = "NC_000005.10:g.(?_177131798)_(177300213_?)del"
    out = translate_record({"vrs_class": "CopyNumberChange", "fmt": "hgvs", "source": source, **record}, alleles, cnv)
    assert out["kwargs"] == {"copy_change": copy_change}
    assert cnv.calls == [(source, "hgvs", {"copy_change": copy_change})]


def test_copy_number_count_passes_absolute_copies(translators):
    alleles, cnv = translators
    record = {
        "vrs_class": "CopyNumberCount",
        "fmt": "hgvs",
        "source": "NC_000001.10:g.215199578_223035427del",
        "absolute_copies": "1",
    }
    assert translate_record(record, alleles, cnv)["kwargs"] == {"copies": 1}


@pytest.mark.parametrize(
    "record",
    [
        {"vrs_class": "Unknown"},
        {"vrs_class": "Not Available", "issue": "haplotype and genotype variations are not supported."},
        {"vrs_class": "Haplotype", "source": "NM_000551.3:c.[1A>T;5C>G]"},
    ],
)
def test_untranslated_classes_are_null(translators, record):
    assert translate_record(record, *translators) is None


@pytest.mark.parametrize("vrs_class", ["Allele", "CopyNumberChange", "CopyNumberCount"])
def test_translated_classes_without_a_source_are_errors(translators, vrs_class):
    alleles, cnv = translators
    out = translate_record({"vrs_class": vrs_class, "fmt": "hgvs"}, alleles, cnv)
    assert out == {"errors": f"not translated: {vrs_class} record has no source expression"}
    assert not cnv.calls
    assert not any(t.calls for t in alleles.values())


def test_non_genomic_expressions_are_errors_without_translating(translators):
    alleles, cnv = translators
    record = {"vrs_class": "CopyNumberChange", "fmt": "hgvs", "source": "NM_021219.4:c.(67+1_68-1)_(394+1_395-1)del"}
    out = translate_record(record, alleles, cnv)
    assert out == {"errors": "not translated: non-genomic expression: NM_021219.4:c.(67+1_68-1)_(394+1_395-1)del"}
    assert not cnv.calls


def test_unknown_copy_change_is_an_error(translators):
    out = translate_record({"vrs_class": "CopyNumberChange", "fmt": "hgvs", "source": "NC_1.1:g.1_2del"}, *translators)
    assert out == {"errors": "ValueError: unknown copy change ''"}


def test_unknown_assembly_version_is_an_error(translators):
    record = {"vrs_class": "Allele", "fmt": "gnomad", "source": "1-100-A-T", "assembly_version": "35"}
    assert translate_record(record, *translators) == {"errors": "ValueError: unknown assembly_version '35'"}


def test_translation_failures_and_timeouts_become_errors():
    alleles = {"38": FakeTranslator("Allele", error=TimeoutError("record took longer than 300 s"))}
    out = translate_record(
        {"vrs_class": "Allele", "fmt": "spdi", "source": "NC_1.1:1:A:T"}, alleles, FakeTranslator("CNV")
    )
    assert out == {"errors": "TimeoutError: record took longer than 300 s"}
