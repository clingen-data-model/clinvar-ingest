from clinvar_ingest.model.rcv import RcvMapping
from clinvar_ingest.reader import _parse_xml_document
from clinvar_ingest.utils import ensure_list


def test_parse_10():
    filename = "test/data/rcv/RCV000000010.xml"
    with open(filename, "r", encoding="utf-8") as f:
        xml = f.read()
    root = _parse_xml_document(xml)
    release_set = root["ReleaseSet"]
    clinvar_set = release_set["ClinVarSet"]
    assert isinstance(clinvar_set, dict)

    rcv_map = RcvMapping.from_xml(clinvar_set)
    assert "rcv_mapping" == rcv_map.entity_type
    assert "RCV000000010" == rcv_map.rcv_accession
    assert "SCV000020153" == rcv_map.scv_accession
    assert "6212" == rcv_map.trait_set_id
    assert ["15692"] == [t["@ID"] for t in ensure_list(rcv_map.rcv_trait_set["Trait"])]
