from clinvar_ingest.model.common import dictify
from clinvar_ingest.model.variation_archive import (
    ClinicalAssertion,
    RcvAccession,
    RcvAccessionClassification,
    StatementType,
    VariationArchive,
    VariationArchiveClassification,
)
from clinvar_ingest.reader import read_clinvar_vcv_xml


def test_vcv_VCV000000002():
    filename = "test/data/VCV000000002.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

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

    # VariationArchive
    vcv = [o for o in objects if isinstance(o, VariationArchive)]
    assert len(vcv) == 1
    vcv = vcv[0]
    assert vcv.entity_type == "variation_archive"
    assert vcv.date_created == "2017-01-30"
    assert vcv.date_last_updated == "2022-04-25"
    assert vcv.id == "VCV000000002"
    assert (
        vcv.name
        == "NM_014855.3(AP5Z1):c.80_83delinsTGCTGTAAACTGTAACTGTAAA (p.Arg27_Ile28delinsLeuLeuTer)"
    )
    assert vcv.num_submissions == 2
    assert vcv.num_submitters == 2
    assert vcv.record_status == "current"
    assert vcv.species == "Homo sapiens"
    assert vcv.variation_id == "2"
    assert vcv.version == "3"

    print(dictify(vcv_classification))

    # ClinicalAssertion
    scv = [o for o in objects if isinstance(o, ClinicalAssertion)]
    assert len(scv) == 2
    scv20155 = scv[0]
    scv2865972 = scv[1]

    # SCV scv20155
    assert scv20155.assertion_type == "variation to disease"
    assert scv20155.clinical_impact_assertion_type is None
    assert scv20155.clinical_impact_clinical_significance is None
    assert scv20155.date_created == "2013-04-04"
    assert scv20155.date_last_updated == "2017-01-30"
    assert scv20155.entity_type == "clinical_assertion"
    assert scv20155.id == "SCV000020155"
    assert scv20155.internal_id == "20155"
    assert scv20155.interpretation_comments == []
    assert scv20155.interpretation_date_last_evaluated == "2010-06-29"
    assert scv20155.interpretation_description is None
    assert (
        scv20155.local_key == "613653.0001_SPASTIC PARAPLEGIA 48, AUTOSOMAL RECESSIVE"
    )
    assert scv20155.record_status == "current"
    assert scv20155.review_status == "no assertion criteria provided"
    assert scv20155.statement_type == StatementType.GermlineClassification
    assert scv20155.submission_id == "3.2017-01-26"
    assert scv20155.submission_names == []
    assert scv20155.submitted_assembly is None
    assert scv20155.submitter_id == "3"
    assert (
        scv20155.title
        == "AP5Z1, 4-BP DEL/22-BP INS, NT80_SPASTIC PARAPLEGIA 48, AUTOSOMAL RECESSIVE"
    )
    assert scv20155.variation_archive_id == "VCV000000002"
    assert scv20155.variation_id == "2"
    assert scv20155.version == "3"

    # SCV scv2865972
    assert scv2865972.assertion_type == "variation to disease"
    assert scv2865972.clinical_impact_assertion_type is None
    assert scv2865972.clinical_impact_clinical_significance is None
    assert scv2865972.date_created == "2021-05-16"
    assert scv2865972.date_last_updated == "2021-05-16"
    assert scv2865972.entity_type == "clinical_assertion"
    assert scv2865972.id == "SCV001451119"
    assert scv2865972.internal_id == "2865972"
    assert scv2865972.interpretation_comments == []
    assert scv2865972.interpretation_date_last_evaluated is None
    assert scv2865972.interpretation_description is None
    assert (
        scv2865972.local_key
        == "NM_014855.3:c.80_83delinsTGCTGTAAACTGTAACTGTAAA|OMIM:613647"
    )
    assert scv2865972.record_status == "current"
    assert scv2865972.review_status == "criteria provided, single submitter"
    assert scv2865972.statement_type == StatementType.GermlineClassification
    assert scv2865972.submission_id == "507826.2020-11-14"
    assert scv2865972.submission_names == ["SUB8526155"]
    assert scv2865972.submitted_assembly == "GRCh37"
    assert scv2865972.submitter_id == "507826"
    assert scv2865972.title is None
    assert scv2865972.variation_archive_id == "VCV000000002"
    assert scv2865972.variation_id == "2"
    assert scv2865972.version == "1"

    # RCV
    rcvs = [o for o in objects if isinstance(o, RcvAccession)]
    assert len(rcvs) == 1
    rcv: RcvAccession = rcvs[0]
    # assert rcv.clinical_impact_assertion_type is None
    # assert rcv.clinical_impact_clinical_significance is None
    # assert rcv.date_last_evaluated is None
    assert rcv.entity_type == "rcv_accession"
    assert rcv.id == "RCV000000012"
    assert rcv.independent_observations is None
    assert (
        rcv.title
        == "NM_014855.3(AP5Z1):c.80_83delinsTGCTGTAAACTGTAACTGTAAA (p.Arg27_Ile28delinsLeuLeuTer) AND Hereditary spastic paraplegia 48"
    )
    assert rcv.trait_set_id == "2"
    assert rcv.variation_archive_id == "VCV000000002"
    assert rcv.version == 5

    # RCV Classification
    rcv_classifications = [
        o for o in objects if isinstance(o, RcvAccessionClassification)
    ]
    assert len(rcv_classifications) == 1
    rcv_classification: RcvAccessionClassification = rcv_classifications[0]
    assert rcv_classification.entity_type == "rcv_accession_classification"
    assert rcv_classification.statement_type == StatementType.GermlineClassification
    assert rcv_classification.clinical_impact_assertion_type is None
    assert rcv_classification.clinical_impact_clinical_significance is None
    assert rcv_classification.date_last_evaluated is None
    assert rcv_classification.num_submissions == 2
    assert rcv_classification.interp_description == "Pathogenic"
    assert rcv_classification.review_status == "criteria provided, single submitter"


def test_rcv_multi_classifications():
    assert False


def test_vcv_VCV000013961():
    filename = "test/data/multi-classification/VCV000013961.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

    assert False
