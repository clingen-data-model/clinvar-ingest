from clinvar_ingest.model.common import dictify
from clinvar_ingest.model_somatic.variation_archive import (
    StatementType,
    VariationArchiveClassification,
)
from clinvar_ingest.reader import read_clinvar_somatic_model_vcv_xml


def test_vcv_VCV000000002():
    filename = "test/data/clinvar_somatic/VCV000000002.xml"
    with open(filename) as f:
        objects = list(read_clinvar_somatic_model_vcv_xml(f))

    # for obj in objects:
    #     print(dictify(obj))

    vcv_classification = [
        o for o in objects if isinstance(o, VariationArchiveClassification)
    ]
    assert len(vcv_classification) == 1
    vcv_classification = vcv_classification[0]
    assert vcv_classification.entity_type == "variation_archive_classification"
    assert vcv_classification.date_created == "2017-01-30"
    assert vcv_classification.interp_description == "Pathogenic"
    assert vcv_classification.most_recent_submission == "2021-05-16"
    assert vcv_classification.num_submissions == 2
    assert vcv_classification.num_submitters == 2
    assert vcv_classification.review_status == "criteria provided, single submitter"
    assert vcv_classification.statement_type == StatementType.GermlineClassification
    print(dictify(vcv_classification))
    # assert False
