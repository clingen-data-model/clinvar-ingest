from clinvar_ingest.utils import extract, extract_in, extract_oneof


def test_extract():
    d = {"A": "A-1", "B": "B-1", "C": "C-1", "D": "D-1"}
    assert extract(d, "A") == "A-1"
    assert "A" not in d
    assert extract(d, "A") is None

    assert extract(d, "B") == "B-1"

    assert extract_oneof(d, "A", "B") is None
    assert extract_oneof(d, "A", "B", "C") == ("C", "C-1")


def test_extract_in():
    """
    All of these lines were added by copilot, purely based on the function name being test_extract_in.

    I added 'def test_extract_in():', and the rest was added by copilot. Only one assert was wrong.
    """
    d = {"A": {"B": {"C": "C-1"}}}
    assert extract_in(d, "A", "B", "C") == "C-1"
    assert "C" not in d["A"]["B"]
    assert extract_in(d, "A", "B", "C") is None
    assert extract_in(d, "A", "B", "D") is None
    assert extract_in(d, "A", "B") == {}
    assert "B" not in d["A"]
    assert extract_in(d, "A", "B") is None
    # This was added by copilot but is wrong, B was already removed in extract_in(d, "A", "B").
    # assert extract_in(d, "A") == {"B": {}}
    # The following was added a correction to the above case.
    # It was also added by copilot, which somehow figured out the correct
    # expected result after I added the above comments.
    assert extract_in(d, "A") == {}
    assert extract_in(d, "A") is None
    assert extract_in(d, "B") is None
    assert extract_in(d, "A", "B", "C", "D") is None
    assert extract_in(d, "A", "B", "C", "D", "E") is None
