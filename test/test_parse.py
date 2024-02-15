from clinvar_ingest.model import (
    ClinicalAssertion,
    Gene,
    GeneAssociation,
    Submission,
    Submitter,
    Trait,
    TraitSet,
    Variation,
    VariationArchive,
)
from clinvar_ingest.reader import read_clinvar_xml


def test_read_original_clinvar_variation_2():
    """
    Test a SimpleAllele record
    """
    filename = "test/data/original-clinvar-variation-2.xml"
    # filename = "data/original-clinvar-variation-2.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    # gene, gene_association, variation, variation_archive
    assert 12 == len(objects)
    assert isinstance(objects[0], Gene)
    assert isinstance(objects[1], GeneAssociation)
    assert isinstance(objects[2], Variation)
    assert isinstance(objects[3], Trait)
    assert isinstance(objects[4], TraitSet)
    assert isinstance(objects[5], Submitter)
    assert isinstance(objects[6], Submission)
    assert isinstance(objects[7], ClinicalAssertion)
    assert isinstance(objects[8], Submitter)
    assert isinstance(objects[9], Submission)
    assert isinstance(objects[10], ClinicalAssertion)
    assert isinstance(objects[11], VariationArchive)

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
    scv = objects[7]
    assert scv.assertion_id == "20155"
    submitter = objects[5]
    assert submitter.id == "3"
    assert submitter.current_name == "OMIM"
    submission = objects[6]
    assert submission.id == "3"
    assert submission.submission_date == "2017-01-26"

    scv = objects[10]
    assert scv.assertion_id == "2865972"
    submitter = objects[8]
    assert submitter.id == "507826"
    assert submitter.current_name == "Paris Brain Institute, Inserm - ICM"
    submission = objects[9]
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

    assert 22 == len(objects)
    assert isinstance(objects[0], Variation)
    assert isinstance(objects[1], Trait)
    assert isinstance(objects[2], TraitSet)
    assert isinstance(objects[3], Trait)
    assert isinstance(objects[4], TraitSet)
    assert isinstance(objects[5], Trait)
    assert isinstance(objects[6], TraitSet)
    assert isinstance(objects[7], Trait)
    assert isinstance(objects[8], TraitSet)

    assert isinstance(objects[9], Submitter)
    assert isinstance(objects[10], Submission)
    assert isinstance(objects[11], ClinicalAssertion)

    assert isinstance(objects[12], Submitter)
    assert isinstance(objects[13], Submission)
    assert isinstance(objects[14], ClinicalAssertion)

    assert isinstance(objects[15], Submitter)
    assert isinstance(objects[16], Submission)
    assert isinstance(objects[17], ClinicalAssertion)

    assert isinstance(objects[18], Submitter)
    assert isinstance(objects[19], Submission)
    assert isinstance(objects[20], ClinicalAssertion)
    assert isinstance(objects[21], VariationArchive)

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
    variation_archive = objects[21]
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
    # SCV 1
    scv = objects[11]
    assert scv.assertion_id == "1801318"
    submitter = objects[9]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = objects[10]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 2
    scv = objects[14]
    assert scv.assertion_id == "1801467"
    submitter = objects[12]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = objects[13]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 3
    scv = objects[17]
    assert scv.assertion_id == "1802126"
    submitter = objects[15]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = objects[16]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 4
    scv = objects[20]
    assert scv.assertion_id == "1802127"
    submitter = objects[18]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = objects[19]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"


if __name__ == "__main__":
    test_read_original_clinvar_variation_2()
