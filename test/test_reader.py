from clinvar_ingest.reader import textify_xmltext


def test_textify_xmlcdata():
    """
    Test the textify_xmlcdata function.

    There are only some structures that are valid xmltodict returns.
    A non-@ key will with a string as the value will always be the only such key.



    <root>
        <foo>bar</foo>
        <baz>qux</baz>
    </root>

    """
    inp = {"foo": "bar"}
    out = textify_xmltext(inp)
    expected = {"foo": {"#text": "bar"}}
    assert expected == out

    inp = {"foo": {"@bar": "baz"}}
    out = textify_xmltext(inp)
    expected = {"foo": {"@bar": "baz"}}
    assert expected == out

    inp = {"foo": {"bar": {"baz": "qux"}}}
    out = textify_xmltext(inp)
    expected = {"foo": {"bar": {"baz": {"#text": "qux"}}}}
    assert expected == out

    inp = {"foo": "bar", "baz": "qux"}
    out = textify_xmltext(inp)
    expected = {"foo": {"#text": "bar"}, "baz": {"#text": "qux"}}
    assert expected == out
