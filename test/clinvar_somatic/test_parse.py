from clinvar_ingest.model.common import dictify
from clinvar_ingest.reader import read_clinvar_vcv_xml


def test_vcv_VCV000000002():
    filename = "test/data/clinvar_somatic/VCV000000002.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

    for obj in objects:
        print(dictify(obj))
    assert False
