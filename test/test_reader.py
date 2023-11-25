import xmltodict

from clinvar_ingest.reader import _handle_text_nodes


def test_handle_text_nodes():
    inp = "<foo>bar</foo>"
    out = xmltodict.parse(inp, postprocessor=_handle_text_nodes)
    expected = {"foo": {"$": "bar"}}
    assert expected == out

    inp = "<foo bar='baz'>qux</foo>"
    out = xmltodict.parse(inp, postprocessor=_handle_text_nodes)
    expected = {"foo": {"@bar": "baz", "$": "qux"}}
    assert expected == out
