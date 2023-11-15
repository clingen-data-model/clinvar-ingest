from clinvar_ingest.model import Variation, VariationArchive
from clinvar_ingest.reader import read_clinvar_xml


def test_read_original_clinvar_variation_2():
    filename = "test/data/original-clinvar-variation-2.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    assert 2 == len(objects)
    assert isinstance(objects[0], Variation)
    assert isinstance(objects[1], VariationArchive)
