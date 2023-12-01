from clinvar_ingest.model import Gene, GeneAssociation, Variation, VariationArchive, ClinicalAssertion, \
    Submitter, Submission
from clinvar_ingest.reader import read_clinvar_xml


def test_read_original_clinvar_variation_2():
    """
    Test a SimpleAllele record
    """
    filename = "test/data/original-clinvar-variation-2.xml"
    #filename = "data/original-clinvar-variation-2.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    # gene, gene_association, variation, variation_archive
    assert 6 == len(objects)
    assert isinstance(objects[0], Gene)
    assert isinstance(objects[1], GeneAssociation)
    assert isinstance(objects[2], Variation)
    assert isinstance(objects[3], ClinicalAssertion)
    assert isinstance(objects[4], ClinicalAssertion)
    assert isinstance(objects[5], VariationArchive)

    variation = objects[2]

    # Test that extracted fields were there
    assert variation.id == "2"
    assert variation.subclass_type == "SimpleAllele"
    assert variation.protein_change == []

    # Test that included fields were not included in content
    assert "@VariationID" not in variation.content
    assert "GeneList" not in variation.content
    assert "SequenceLocation" in variation.content

    # Verify gene association
    gene = objects[0]
    gene_association = objects[1]
    assert gene.id == "9907"
    assert gene.hgnc_id == "HGNC:22197"
    assert gene.symbol == "AP5Z1"
    assert gene.full_name == "adaptor related protein complex 5 subunit zeta 1"

    assert gene_association.source == "submitted"
    assert gene_association.relationship_type == "within single gene"
    assert gene_association.gene_id == "9907"
    assert gene_association.variation_id == "2"

    # SCVs - TODO build out further
    scv = objects[3]
    assert scv.assertion_id == "20155"
    submitter = scv.submitter
    assert submitter.id == "3"
    assert submitter.current_name == 'OMIM'
    submission = scv.submission
    assert submission.id == "3"
    assert submission.submission_date == "2017-01-26"

    scv = objects[4]
    assert scv.assertion_id == "2865972"
    submitter = scv.submitter
    assert submitter.id == "507826"
    assert submitter.current_name == "Paris Brain Institute, Inserm - ICM"
    submission = scv.submission
    assert submission.id == "507826"
    assert submission.submission_date == "2020-11-14"


def test_read_original_clinvar_variation_634266():
    """
    Test a Genotype record
    """
    filename = "test/data/original-clinvar-variation-634266.xml"
    # filename = "data/original-clinvar-variation-634266.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    assert 6 == len(objects)
    assert isinstance(objects[0], Variation)
    assert isinstance(objects[1], ClinicalAssertion)
    assert isinstance(objects[2], ClinicalAssertion)
    assert isinstance(objects[3], ClinicalAssertion)
    assert isinstance(objects[4], ClinicalAssertion)
    assert isinstance(objects[5], VariationArchive)

    # Verify variation
    variation = objects[0]

    assert variation.id == "634266"
    assert variation.subclass_type == "Genotype"
    assert variation.protein_change == []

    assert variation.child_ids == ["633847", "633853"]
    assert variation.descendant_ids == [
        "633847",
        "633853",
        "634864",
        "634875",
        "634882",
        "633853",
    ]

    # Verify variation archive
    variation_archive = objects[5]
    assert variation_archive.id == "VCV000634266"
    assert variation_archive.name == "CYP2C19*12/*34"
    assert variation_archive.date_created == "2019-06-17"
    assert variation_archive.record_status == "current"
    assert variation_archive.species == "Homo sapiens"
    assert variation_archive.review_status == "practice guideline"
    assert variation_archive.interp_description == "drug response"
    assert variation_archive.num_submitters == 1
    assert variation_archive.num_submissions == 4
    assert variation_archive.date_last_updated == "2023-10-07"
    assert variation_archive.interp_type == "Clinical significance"
    assert variation_archive.interp_explanation is None
    assert variation_archive.interp_date_last_evaluated is None
    # assert variation_archive.interp_content
    # assert variation_archive.content
    assert variation_archive.variation_id == "634266"

    # Test that included fields were not included in content
    # assert "@VariationID" not in variation.content  # TODO - broken
    assert "GeneList" in variation.content

    # SCVs - TODO build out further
    scv = objects[1]
    assert scv.assertion_id == "1801318"
    submitter = scv.submitter
    assert submitter.id == "505961"
    assert submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    submission = scv.submission
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    scv = objects[2]
    assert scv.assertion_id == "1801467"
    submitter = scv.submitter
    assert submitter.id == "505961"
    assert submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    submission = scv.submission
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    scv = objects[3]
    assert scv.assertion_id == "1802126"
    submitter = scv.submitter
    assert submitter.id == "505961"
    assert submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    submission = scv.submission
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    scv = objects[4]
    assert scv.assertion_id == "1802127"
    submitter = scv.submitter
    assert submitter.id == "505961"
    assert submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    submission = scv.submission
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"


if __name__ == '__main__':
    test_read_original_clinvar_variation_2()
