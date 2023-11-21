import xmltodict

from clinvar_ingest.model import Variation


def test_variation_descendant_tree():
    """
    Test the Variation.descendant_tree method.

    Generates a tree of Variation objects, and then tests that the
    descendant_tree method returns the expected tree.
    """
    with open("test/data/original-clinvar-variation-634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["InterpretedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    expected_tree = [
        "634266",
        ["633847", ["634864"], ["634875"], ["634882"]],
        ["633853", ["633853"]],
    ]
    assert expected_tree == descendant_tree


def test_variation_get_all_children():
    with open("test/data/original-clinvar-variation-634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["InterpretedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    children = Variation.get_all_children(descendant_tree)
    expected_children = ["633847", "633853"]
    assert expected_children == children


def test_variation_get_all_descendants():
    with open("test/data/original-clinvar-variation-634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["InterpretedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    descendants = Variation.get_all_descendants(descendant_tree)
    expected_descendants = ["633847", "633853", "634864", "634875", "634882", "633853"]
    assert expected_descendants == descendants
