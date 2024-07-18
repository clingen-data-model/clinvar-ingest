from clinvar_ingest.reader import _parse_xml_document


def test_handle_text_nodes():
    inp = "<foo>bar</foo>"
    out = _parse_xml_document(inp)
    expected = {"foo": {"$": "bar"}}
    assert expected == out

    inp = "<foo bar='baz'>qux</foo>"
    out = _parse_xml_document(inp)
    expected = {"foo": {"@bar": "baz", "$": "qux"}}
    assert expected == out
