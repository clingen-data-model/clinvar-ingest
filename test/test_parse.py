from clinvar_ingest.model.trait import (
    ClinicalAssertionTrait,
    ClinicalAssertionTraitSet,
    Trait,
    TraitMapping,
    TraitSet,
)
from clinvar_ingest.model.variation_archive import (
    ClinicalAssertion,
    ClinicalAssertionObservation,
    Gene,
    GeneAssociation,
    RcvAccession,
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
    assert len(objects) == 21
    expected_types = [
        Variation,
        Gene,
        GeneAssociation,
        TraitMapping,
        TraitMapping,
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
        RcvAccession,
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
    # Verify SCV traits are linked to VCV traits
    scv_trait_0: ClinicalAssertionTrait = list(
        filter(lambda o: isinstance(o, ClinicalAssertionTrait), objects)
    )[0]
    assert scv_trait_0.trait_id == "9580"
    scv_trait_1 = list(
        filter(lambda o: isinstance(o, ClinicalAssertionTrait), objects)
    )[1]
    assert scv_trait_1.trait_id == "9580"

    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[1]
    assert scv.assertion_id == "2865972"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[1]
    assert submitter.id == "507826"
    assert submitter.current_name == "Paris Brain Institute, Inserm - ICM"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[1]
    assert submission.id == "507826"
    assert submission.submission_date == "2020-11-14"

    # Rcv
    rcv: RcvAccession = list(filter(lambda o: isinstance(o, RcvAccession), objects))[0]
    assert rcv.id == "RCV000000012"
    assert rcv.variation_archive_id == "VCV000000002"
    assert rcv.variation_id == "2"
    assert rcv.date_last_evaluated is None
    assert rcv.version == 5
    assert (
        rcv.title
        == "NM_014855.3(AP5Z1):c.80_83delinsTGCTGTAAACTGTAACTGTAAA (p.Arg27_Ile28delinsLeuLeuTer) AND Hereditary spastic paraplegia 48"
    )
    assert rcv.trait_set_id == "2"
    assert rcv.review_status == "criteria provided, single submitter"
    assert rcv.interpretation == "Pathogenic"


def test_read_original_clinvar_variation_634266(log_conf):
    """
    Test a Genotype record
    """
    filename = "test/data/original-clinvar-variation-634266.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    assert len(objects) == 42
    expected_types = [
        Variation,
        TraitMapping,
        TraitMapping,
        TraitMapping,
        TraitMapping,
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
        RcvAccession,
        RcvAccession,
        RcvAccession,
        RcvAccession,
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
    scv0: ClinicalAssertion = list(
        filter(lambda o: isinstance(o, ClinicalAssertion), objects)
    )[0]
    assert scv0.assertion_id == "1801318"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[0]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[0]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 2
    scv2 = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[1]
    assert scv2.assertion_id == "1801467"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[1]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[1]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 3
    scv3 = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[2]
    assert scv3.assertion_id == "1802126"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[2]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[2]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # SCV 4
    scv4 = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[3]
    assert scv4.assertion_id == "1802127"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[3]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[3]
    assert submission.id == "505961"
    assert submission.submission_date == "2018-03-01"

    # Test a clinical_assertion_trait linked to a trait via medgen id

    # ClinicalAssertion ID="1801318"
    # Trait should be linked to 32268, medgen CN221265 via Preferred name
    scv0_trait_set_id = scv0.clinical_assertion_trait_set_id
    scv0_trait_set = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTraitSet)
            and o.id == scv0_trait_set_id,
            objects,
        )
    )[0]
    scv0_trait_ids = scv0_trait_set.clinical_assertion_trait_ids
    assert len(scv0_trait_ids) == 1
    scv0_traits: list[ClinicalAssertionTrait] = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTrait) and o.id in scv0_trait_ids,
            objects,
        )
    )
    assert len(scv0_traits) == 1
    assert scv0_traits[0].name == "Sertraline response"
    assert scv0_traits[0].trait_id == "32268"
    assert scv0_traits[0].medgen_id == "CN221265"

    # ClinicalAssertion ID="1801467"
    # Trait should be 16405, medgen CN077957 via Preferred name
    scv2_trait_set_id = scv2.clinical_assertion_trait_set_id
    scv2_trait_set = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTraitSet)
            and o.id == scv2_trait_set_id,
            objects,
        )
    )[0]
    scv2_trait_ids = scv2_trait_set.clinical_assertion_trait_ids
    assert len(scv2_trait_ids) == 1
    scv2_traits: list[ClinicalAssertionTrait] = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTrait) and o.id in scv2_trait_ids,
            objects,
        )
    )
    assert len(scv2_traits) == 1
    assert scv2_traits[0].name == "Voriconazole response"
    assert scv2_traits[0].trait_id == "16405"
    assert scv2_traits[0].medgen_id == "CN077957"

    # ClinicalAssertion ID="1802126"
    # Trait should be 32266, medgen CN221263 via Preferred name
    scv3_trait_set_id = scv3.clinical_assertion_trait_set_id
    scv3_trait_set = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTraitSet)
            and o.id == scv3_trait_set_id,
            objects,
        )
    )[0]
    scv3_trait_ids = scv3_trait_set.clinical_assertion_trait_ids
    assert len(scv3_trait_ids) == 1
    scv3_traits: list[ClinicalAssertionTrait] = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTrait) and o.id in scv3_trait_ids,
            objects,
        )
    )
    assert len(scv3_traits) == 1
    assert scv3_traits[0].name == "Citalopram response"
    assert scv3_traits[0].trait_id == "32266"
    assert scv3_traits[0].medgen_id == "CN221263"

    # ClinicalAssertion ID="1802127"
    # Trait should be 32267, medgen CN221264 via Preferred name
    scv4_trait_set_id = scv4.clinical_assertion_trait_set_id
    scv4_trait_set = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTraitSet)
            and o.id == scv4_trait_set_id,
            objects,
        )
    )[0]
    scv4_trait_ids = scv4_trait_set.clinical_assertion_trait_ids
    assert len(scv4_trait_ids) == 1
    scv4_traits: list[ClinicalAssertionTrait] = list(
        filter(
            lambda o: isinstance(o, ClinicalAssertionTrait) and o.id in scv4_trait_ids,
            objects,
        )
    )
    assert len(scv4_traits) == 1
    assert scv4_traits[0].name == "Escitalopram response"
    assert scv4_traits[0].trait_id == "32267"
    assert scv4_traits[0].medgen_id == "CN221264"


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


def test_read_original_clinvar_variation_10(log_conf):
    """
    This tests an IncludedRecord with no ClinicalAssertions.
    Exercises this bug fix:
    """
    filename = "test/data/original-clinvar-variation-10.xml"
    with open(filename) as f:
        objects = list(read_clinvar_xml(f))

    scv372036 = [o for o in objects if isinstance(o, ClinicalAssertion)][0]
    assert scv372036.assertion_id == "372036"
    scv372036_trait_set = [
        o
        for o in objects
        if isinstance(o, ClinicalAssertionTraitSet)
        and o.id == scv372036.clinical_assertion_trait_set_id
    ][0]

    # This one is an example of a SCV that was submitted only with a medgen id,
    # no name or other attributes on the submitted trait
    # The ClinicalAssertionTrait should be linked to the Trait
    # and copy its medgen id if there, but not the name
    """
    <TraitSet Type="Disease">
          <Trait Type="Disease">
            <XRef DB="MedGen" ID="C0392514" Type="CUI"/>
          </Trait>
        </TraitSet>
    """

    scv372036_trait_ids = scv372036_trait_set.clinical_assertion_trait_ids
    assert len(scv372036_trait_ids) == 1
    scv372036_traits: list[ClinicalAssertionTrait] = [
        o
        for o in objects
        if isinstance(o, ClinicalAssertionTrait) and o.id in scv372036_trait_ids
    ]
    assert len(scv372036_traits) == 1
    assert scv372036_traits[0].trait_id == "33108"
    assert scv372036_traits[0].medgen_id == "C0392514"
    # Name
    # assert scv372036_traits[0].name == "Hereditary hemochromatosis"
    assert scv372036_traits[0].name is None


if __name__ == "__main__":
    test_read_original_clinvar_variation_2()
