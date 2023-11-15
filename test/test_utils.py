from clinvar_ingest.utils import extract


def test_extract():
    d = {"A": "A-1", "B": "B-1", "C": "C-1", "D": "D-1"}
    assert extract(d, "A") == "A-1"
    assert "A" not in d
    assert extract(d, "A") is None

    assert extract(d, "B") == "B-1"

    assert extract(d, "A", "B") is None
    assert extract(d, "A", "B", "C") == "C-1"
