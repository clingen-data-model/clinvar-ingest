from clinvar_ingest.model.trait import (
    ClinicalAssertionTrait,
    ClinicalAssertionTraitSet,
    Trait,
    TraitSet,
)
from clinvar_ingest.model.variation_archive import (
    ClinicalAssertion,
    ClinicalAssertionObservation,
    Gene,
    GeneAssociation,
    Submission,
    Submitter,
    Variation,
    VariationArchive,
)
from clinvar_ingest.reader import read_clinvar_xml


def test_read_original_clinvar_variation_2():
    """
    Test a SimpleAllele record
    """
    filename = "test/data/original-clinvar-variation-2.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    # print("\n".join([str(dictify(o)) for o in objects]))
    assert 18 == len(objects)
    expected_types = [
        Variation,
        Gene,
        GeneAssociation,
        Trait,
        TraitSet,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        VariationArchive,
    ]
    for i, obj in enumerate(objects):
        assert isinstance(
            obj, expected_types[i]
        ), f"Expected {expected_types[i]} at index {i}, got {type(obj)}"

    variation = list(filter(lambda o: isinstance(o, Variation), objects))[0]

    # Test that extracted fields were there
    assert variation.id == "2"
    assert variation.subclass_type == "SimpleAllele"
    assert variation.protein_change == []

    # Test that included fields were not included in content
    assert "@VariationID" not in variation.content
    assert "GeneList" not in variation.content
    assert "SequenceLocation" in variation.content

    # Verify gene association
    gene = list(filter(lambda o: isinstance(o, Gene), objects))[0]
    gene_association = list(filter(lambda o: isinstance(o, GeneAssociation), objects))[
        0
    ]
    assert gene.id == "9907"
    assert gene.hgnc_id == "HGNC:22197"
    assert gene.symbol == "AP5Z1"
    assert gene.full_name == "adaptor related protein complex 5 subunit zeta 1"

    assert gene_association.source == "submitted"
    assert gene_association.relationship_type == "within single gene"
    assert gene_association.gene_id == "9907"
    assert gene_association.variation_id == "2"

    # SCVs - TODO build out further
    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[0]
    assert scv.assertion_id == "20155"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[0]
    assert submitter.id == "3"
    assert submitter.current_name == "OMIM"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[0]
    assert submission.id == "3"
    assert submission.submission_date == "2017-01-26"

    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[1]
    assert scv.assertion_id == "2865972"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[1]
    assert submitter.id == "507826"
    assert submitter.current_name == "Paris Brain Institute, Inserm - ICM"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[1]
    assert submission.id == "507826"
    assert submission.submission_date == "2020-11-14"


def test_read_original_clinvar_variation_634266():
    """
    Test a Genotype record
    """
    filename = "test/data/original-clinvar-variation-634266.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    assert 34 == len(objects)
    expected_types = [
        Variation,
        Trait,
        TraitSet,
        Trait,
        TraitSet,
        Trait,
        TraitSet,
        Trait,
        TraitSet,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,  # 30
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        VariationArchive,
    ]

    for i, obj in enumerate(objects):
        assert isinstance(
            obj, expected_types[i]
        ), f"Expected {expected_types[i]} at index {i}, got {type(obj)}"

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
    variation_archive = list(
        filter(lambda o: isinstance(o, VariationArchive), objects)
    )[0]
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
    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[0]
    assert scv.assertion_id == "1801318"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[0]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[0]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 2
    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[1]
    assert scv.assertion_id == "1801467"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[1]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[1]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 3
    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[2]
    assert scv.assertion_id == "1802126"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[2]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[2]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 4
    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[3]
    assert scv.assertion_id == "1802127"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[3]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[3]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"


def test_read_original_clinvar_variation_1264328():
    """
    This tests an IncludedRecord with no ClinicalAssertions.
    Exercises this bug fix: https://github.com/clingen-data-model/clinvar-ingest/issues/101
    """
    filename = "test/data/original-clinvar-variation-1264328.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    assert 6 == len(objects)
    assert isinstance(objects[0], Variation)
    assert isinstance(objects[1], Gene)
    assert isinstance(objects[2], GeneAssociation)
    assert isinstance(objects[3], Gene)
    assert isinstance(objects[4], GeneAssociation)
    assert isinstance(objects[5], VariationArchive)
    clinical_assertions = [obj for obj in objects if isinstance(obj, ClinicalAssertion)]
    assert 0 == len(clinical_assertions)


if __name__ == "__main__":
    test_read_original_clinvar_variation_2()
