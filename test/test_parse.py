import html

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
    ClinicalAssertionVariation,
    Gene,
    GeneAssociation,
    RcvAccession,
    Submission,
    Submitter,
    Variation,
    VariationArchive,
    VariationArchiveClassification,
)
from clinvar_ingest.parse import clean_object
from clinvar_ingest.reader import read_clinvar_vcv_xml


def test_read_original_clinvar_variation_2():
    """
    Test a SimpleAllele record
    """
    filename = "test/data/VCV000000002.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

    # print("\n".join([str(dictify(o)) for o in objects]))
    assert len(objects) == 24
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
        ClinicalAssertionVariation,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        ClinicalAssertionVariation,
        RcvAccession,
        VariationArchiveClassification,
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
    assert "SequenceLocation" in variation.content["Location"]
    assert (
        variation.content["OtherNameList"]["Name"]["$"]
        == "AP5Z1, 4-BP DEL/22-BP INS, NT80"
    )

    # Verify gene association
    gene = list(filter(lambda o: isinstance(o, Gene), objects))[0]
    gene_association = list(filter(lambda o: isinstance(o, GeneAssociation), objects))[
        0
    ]
    assert gene.id == "9907"
    assert gene.hgnc_id == "HGNC:22197"
    assert gene.symbol == "AP5Z1"
    assert gene.full_name == "adaptor related protein complex 5 subunit zeta 1"
    assert gene.vcv_id == "VCV000000002"

    assert gene_association.source == "submitted"
    assert gene_association.relationship_type == "within single gene"
    assert gene_association.gene_id == "9907"
    assert gene_association.variation_id == "2"

    # SCVs - TODO build out further
    scv = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[0]
    assert scv.internal_id == "20155"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[0]
    assert submitter.id == "3"
    assert submitter.current_name == "OMIM"
    assert submitter.scv_id == "SCV000020155"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[0]
    assert submission.id == "3.2017-01-26"
    assert submission.submission_date == "2017-01-26"
    assert submission.scv_id == "SCV000020155"
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
    assert scv.internal_id == "2865972"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[1]
    assert submitter.id == "507826"
    assert submitter.current_name == "Paris Brain Institute, Inserm - ICM"
    assert submitter.scv_id == "SCV001451119"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[1]
    assert submission.id == "507826.2020-11-14"
    assert submission.submission_date == "2020-11-14"
    assert submission.scv_id == "SCV001451119"

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
    filename = "test/data/VCV000634266.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

    assert len(objects) == 71
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
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        ClinicalAssertionVariation,
        RcvAccession,
        RcvAccession,
        RcvAccession,
        RcvAccession,
        VariationArchiveClassification,
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
    assert variation_archive.num_submitters == 1
    assert variation_archive.num_submissions == 4
    assert variation_archive.date_last_updated == "2024-07-29"
    # assert variation_archive.interp_content
    # assert variation_archive.content
    assert variation_archive.variation_id == "634266"

    # Test that included fields were not included in content
    # assert "@VariationID" not in variation.content  # TODO - broken
    assert variation.content["Haplotype"][0]["@VariationID"] == "633847"
    assert (
        variation.content["Haplotype"][0]["SimpleAllele"][0]["GeneList"]["Gene"][
            "@Symbol"
        ]
        == "CYP2C19"
    )

    # test for variationarchiveclassification
    classification = [
        o for o in objects if isinstance(o, VariationArchiveClassification)
    ]
    assert len(classification) == 1
    classification = classification[0]
    assert classification.entity_type == "variation_archive_classification"
    assert classification.review_status == "practice guideline"
    assert classification.interp_description == "drug response"
    assert classification.date_last_evaluated is None

    # SCVs - TODO build out further
    # SCV 1
    scv0: ClinicalAssertion = list(
        filter(lambda o: isinstance(o, ClinicalAssertion), objects)
    )[0]
    assert scv0.internal_id == "1801318"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[0]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    assert submitter.scv_id == "SCV000921753"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[0]
    assert submission.id == "505961.2018-03-01"
    assert submission.submission_date == "2018-03-01"
    assert submission.scv_id == "SCV000921753"

    # SCV 2
    scv2 = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[1]
    assert scv2.internal_id == "1801467"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[1]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    assert submitter.scv_id == "SCV000921902"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[1]
    assert submission.id == "505961.2018-03-01"
    assert submission.submission_date == "2018-03-01"
    assert submission.scv_id == "SCV000921902"

    # SCV 3
    scv3 = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[2]
    assert scv3.internal_id == "1802126"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[2]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    assert submitter.scv_id == "SCV000922561"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[2]
    assert submission.id == "505961.2018-03-01"
    assert submission.submission_date == "2018-03-01"
    assert submission.scv_id == "SCV000922561"

    # SCV 4
    scv4 = list(filter(lambda o: isinstance(o, ClinicalAssertion), objects))[3]
    assert scv4.internal_id == "1802127"
    submitter = list(filter(lambda o: isinstance(o, Submitter), objects))[3]
    assert submitter.id == "505961"
    assert (
        submitter.current_name == "Clinical Pharmacogenetics Implementation Consortium"
    )
    assert submitter.scv_id == "SCV000922562"
    submission = list(filter(lambda o: isinstance(o, Submission), objects))[3]
    assert submission.id == "505961.2018-03-01"
    assert submission.submission_date == "2018-03-01"
    assert submission.scv_id == "SCV000922562"

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
    filename = "test/data/VCV001264328.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

    assert 18 == len(objects)
    # assert isinstance(objects[0], Variation)
    # assert isinstance(objects[1], Gene)
    # assert isinstance(objects[2], GeneAssociation)
    # assert isinstance(objects[3], Gene)
    # assert isinstance(objects[4], GeneAssociation)
    # assert isinstance(objects[5], VariationArchive)
    expected_types = [
        Variation,
        Gene,
        GeneAssociation,
        Gene,
        GeneAssociation,
        TraitMapping,
        Trait,
        TraitSet,
        Submitter,
        Submission,
        ClinicalAssertionObservation,
        ClinicalAssertionTrait,
        ClinicalAssertionTraitSet,
        ClinicalAssertion,
        ClinicalAssertionVariation,
        RcvAccession,
        VariationArchiveClassification,
        VariationArchive,
    ]

    for i, obj in enumerate(objects):
        assert isinstance(
            obj, expected_types[i]
        ), f"Expected {expected_types[i]} at index {i}, got {type(obj)}"

    clinical_assertions = [obj for obj in objects if isinstance(obj, ClinicalAssertion)]
    assert 1 == len(clinical_assertions)

    # test for variationarchiveclassification
    classification = [
        o for o in objects if isinstance(o, VariationArchiveClassification)
    ]
    assert len(classification) == 1
    classification = classification[0]
    assert classification.entity_type == "variation_archive_classification"
    assert classification.review_status == "criteria provided, single submitter"
    assert classification.interp_description == "Pathogenic"
    assert classification.date_last_evaluated == "2023-09-12"


def test_read_original_clinvar_variation_10():
    """
    This tests an IncludedRecord with no ClinicalAssertions.
    Exercises this bug fix:
    """
    filename = "test/data/VCV000000010.xml"
    with open(filename) as f:
        objects = list(read_clinvar_vcv_xml(f))

    scvs = [o for o in objects if isinstance(o, ClinicalAssertion)]
    assert len(scvs) == 50

    scv372036 = [o for o in scvs if o.internal_id == "372036"][0]
    assert scv372036.internal_id == "372036"
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

    classification = [
        o for o in objects if isinstance(o, VariationArchiveClassification)
    ]
    assert len(classification) == 1
    classification = classification[0]
    assert (
        classification.review_status
        == "criteria provided, multiple submitters, no conflicts"
    )
    assert (
        classification.interp_description
        == "Pathogenic/Likely pathogenic/Pathogenic, low penetrance; other"
    )
    assert classification.num_submissions == 50
    assert classification.num_submitters == 49
    assert classification.date_last_evaluated == "2024-03-26"

    # Check an observation with a trait
    # ClinicalAssertion ID="3442424"
    # SCV 3442424 has an observation with a traitset
    # pylint: disable=W0105

    """
    <TraitSet DateLastEvaluated="2019-06-26" Type="Finding">
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0000496"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0000545"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0000163"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0000929"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0001626"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0002564"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0002721"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0012647"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0002719"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0011968"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0002242"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0002577"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0000098"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0010674"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0002353"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0001290"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0001250"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0001560"/>
              </Trait>
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0001558"/>
              </Trait>
            </TraitSet>
    """
    scv = [
        o
        for o in objects
        if isinstance(o, ClinicalAssertion) and o.internal_id == "3442424"
    ]
    assert len(scv) == 1
    scv = scv[0]
    observations = [
        o
        for o in objects
        if isinstance(o, ClinicalAssertionObservation)
        and o.id in scv.clinical_assertion_observation_ids
    ]
    assert len(observations) == 5

    # Match on XRef (line 1768)
    # Submitted trait HP:0000836 should map to medgen trait C0020550
    """
    <TraitSet DateLastEvaluated="2020-05-05" Type="Finding">
              <Trait Type="Finding" ClinicalFeaturesAffectedStatus="present">
                <XRef DB="HP" ID="HP:0000836"/>
              </Trait>
            </TraitSet>
    """
    trait_mappings_medgen_C0020550 = [
        o for o in objects if isinstance(o, TraitMapping) and o.medgen_id == "C0020550"
    ]
    assert len(trait_mappings_medgen_C0020550) == 8

    """
    Get trait set ids from the vcv
    xq -x '//ClinVarVariationRelease/VariationArchive/InterpretedRecord/Interpretations/Interpretation/ConditionList/TraitSet/@ID' VCV000000010.xml
    """

    clinical_assertion_traits = [
        o for o in objects if isinstance(o, ClinicalAssertionTrait)
    ]
    # parse xrefs
    # for t in clinical_assertion_traits:
    #     t.xrefs = [Trait.XRef(**json.loads(xref)) for xref in t.xrefs]
    assert len(clinical_assertion_traits) > 0

    traits_HP_0000836 = [
        o
        for o in objects
        if isinstance(o, ClinicalAssertionTrait)
        and len(
            [xref for xref in o.xrefs if xref.db == "HP" and xref.id == "HP:0000836"]
        )
        > 0
    ]
    assert len(traits_HP_0000836) == 8

    # Check interpretation_comments
    # SCV001251532
    scv = [
        o
        for o in objects
        if isinstance(o, ClinicalAssertion) and o.id == "SCV001251532"
    ]
    assert len(scv) == 1
    scv001251532 = scv[0]
    assert len(scv001251532.interpretation_comments) == 1
    assert scv001251532.interpretation_comments[0]["text"] == html.unescape(
        "The HFE c.187C&gt;G (p.H63D) variant is a pathogenic variant seen in 10.8% of the human population in gnomAD. Indviduals with the p.H63D variant are considered carriers of hemochromatosis, although this variant is associated with less severe iron overload and reduced penetrance compared to another pathogenic HFE variant, c.845G&gt;A, p.C282Y (PMID: 19159930; 20301613)."
    )
    assert "type" not in scv001251532.interpretation_comments[0]


def test_clean_object():
    # dictionaries
    obj = {}
    assert clean_object(obj) is None

    obj = {"key": "value"}
    assert clean_object(obj) == {"key": "value"}

    obj = {"key": None, "empty_list": [], "empty_dict": {}, "empty_string": ""}
    assert clean_object(obj) is None

    obj = {"key": "value", "empty_list": [], "empty_dict": {}, "empty_string": ""}
    assert clean_object(obj) == {"key": "value"}

    # lists
    obj = []
    assert clean_object(obj) is None

    obj = [1, 2, 3]
    assert clean_object(obj) == [1, 2, 3]

    obj = [1, 2, None]
    assert clean_object(obj) == [1, 2]

    obj = ["", {}, None]
    assert clean_object(obj) is None

    obj = [{"key": "value"}, {}, None]
    assert clean_object(obj) == [{"key": "value"}]

    obj = [[[[[[[[[[[[[[[[[[[[[[[[{"key": [{}]}]]]]]]]]]]]]]]]]]]]]]]]]
    assert clean_object(obj) is None

    # strings
    obj = "string"
    assert clean_object(obj) == "string"

    obj = ""
    assert clean_object(obj) is None

    obj = None
    assert clean_object(obj) is None


if __name__ == "__main__":
    test_read_original_clinvar_variation_2()
