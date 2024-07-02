from clinvar_ingest.model.rcv import RcvMapping
from clinvar_ingest.reader import _parse_xml_document


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
    assert ["SCV000020153"] == rcv_map.scv_accessions
    assert "6212" == rcv_map.trait_set_id


def test_parse_12():
    filename = "test/data/rcv/RCV000000012.xml"
    with open(filename, "r", encoding="utf-8") as f:
        xml = f.read()
    root = _parse_xml_document(xml)
    release_set = root["ReleaseSet"]
    clinvar_set = release_set["ClinVarSet"]
    assert isinstance(clinvar_set, dict)

    rcv_map = RcvMapping.from_xml(clinvar_set)
    assert "rcv_mapping" == rcv_map.entity_type
    assert "RCV000000012" == rcv_map.rcv_accession
    assert ["SCV000020155", "SCV001451119"] == rcv_map.scv_accessions
    assert "2" == rcv_map.trait_set_id
