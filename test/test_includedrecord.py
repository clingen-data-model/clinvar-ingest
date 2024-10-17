from clinvar_ingest.model.variation_archive import (
    ClinicalAssertion,
    RcvAccession,
    VariationArchiveClassification,
)
from clinvar_ingest.reader import read_clinvar_vcv_xml


def test_included_record():
    filename = "test/data/VCV000025511.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

    # Verify no scvs
    scvs = [o for o in objects if isinstance(o, ClinicalAssertion)]
    assert len(scvs) == 0

    # Verify no RCVs
    rcvs = [o for o in objects if isinstance(o, RcvAccession)]
    assert len(rcvs) == 0

    # Verify that the "empty" classifications in the XML are not
    # included as VariationArchiveClassifications
    classifications = [
        o for o in objects if isinstance(o, VariationArchiveClassification)
    ]
    assert len(classifications) == 0
