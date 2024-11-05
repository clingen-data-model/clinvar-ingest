from clinvar_ingest.model.common import dictify
from clinvar_ingest.model.trait import Trait, TraitMapping, TraitSet
from clinvar_ingest.model.variation_archive import VariationArchive
from clinvar_ingest.reader import _parse_xml_document
from clinvar_ingest.utils import ensure_list


def unordered_dict_list_equal(list1: list[dict], list2: list[dict]) -> bool:
    set1 = {tuple(elem.items()) for elem in list1}
    set2 = {tuple(elem.items()) for elem in list2}
    return len(list1) == len(list2) and set1 == set2


def distinct_dict_set(list1: list[dict]) -> list[dict]:
    """
    N^2 algorithm to remove duplicates from a list of dicts
    """
    outs = []
    for elem in list1:
        if elem not in outs:
            outs.append(elem)
    return outs


def test_trait_from_xml_32():
    filename = "test/data/VCV000000032.xml"
    with open(filename) as f:
        content = f.read()
    root = _parse_xml_document(content)

    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["ClassifiedRecord"]
    rcv_id = interp_record["RCVList"]["RCVAccession"]["@Accession"]
    interp = interp_record["Classifications"]["GermlineClassification"]
    interp_traitset = interp["ConditionList"]["TraitSet"]
    raw_trait = interp_traitset["Trait"]  # only 1 trait in this example

    trait: Trait = Trait.from_xml(raw_trait, rcv_id)

    assert trait.id == "9592"
    assert trait.type == "Disease"
    assert trait.name == "Primary hyperoxaluria type 3"
    assert trait.alternate_names == ["PH III", "Primary hyperoxaluria, type III"]
    assert trait.symbol is None
    assert trait.alternate_symbols == ["HP3", "HOGA1", "PH3"]
    assert trait.mode_of_inheritance is None
    assert trait.ghr_links is None
    assert trait.keywords is None
    assert trait.gard_id == 10738
    assert trait.medgen_id == "C3150878"
    assert trait.public_definition is None
    assert trait.rcv_id == "RCV000000049"

    assert trait.disease_mechanism == "loss of function"
    assert trait.disease_mechanism_id == 273
    assert trait.gene_reviews_short is None
    assert trait.attribute_content == []
    assert trait.content is not None

    expected_trait_xrefs = [
        {
            "db": "MONDO",
            "id": "MONDO:0013327",
            "type": None,
            "ref_field": "name",
            "ref_field_element": "Primary hyperoxaluria type 3",
        },
        {
            "db": "Genetic Alliance",
            "id": "Primary+Hyperoxaluria+Type+3/8596",
            "type": None,
            "ref_field": "alternate_names",
            "ref_field_element": "Primary hyperoxaluria, type III",
        },
        {
            "db": "OMIM",
            "id": "613616",
            "type": "MIM",
            "ref_field": "alternate_symbols",
            "ref_field_element": "HP3",
        },
        {
            "db": "Office of Rare Diseases",
            "id": "10738",
            "type": None,
            "ref_field": "gard_id",
            "ref_field_element": None,
        },
        {
            "db": "Genetic Testing Registry (GTR)",
            "id": "GTR000561373",
            "type": None,
            "ref_field": "disease_mechanism",
            "ref_field_element": None,
        },
        {
            "db": "Orphanet",
            "id": "416",
            "type": None,
            "ref_field": None,
            "ref_field_element": None,
        },
        {
            "db": "Orphanet",
            "id": "93600",
            "type": None,
            "ref_field": None,
            "ref_field_element": None,
        },
        {
            "db": "MedGen",
            "id": "C3150878",
            "type": None,
            "ref_field": None,
            "ref_field_element": None,
        },
        {
            "db": "MONDO",
            "id": "MONDO:0013327",
            "type": None,
            "ref_field": None,
            "ref_field_element": None,
        },
        {
            "db": "OMIM",
            "id": "613616",
            "type": "MIM",
            "ref_field": None,
            "ref_field_element": None,
        },
    ]
    assert unordered_dict_list_equal(
        expected_trait_xrefs, [dictify(x) for x in trait.xrefs]
    )


def test_trait_from_xml_6619():
    with open("test/data/VCV000222476.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["ClassifiedRecord"]
    rcv_id = interp_record["RCVList"]["RCVAccession"]["@Accession"]
    interp = interp_record["Classifications"]["GermlineClassification"]
    interp_traitset = interp["ConditionList"]["TraitSet"]
    raw_trait = interp_traitset["Trait"]  # only 1 trait in this example

    trait = Trait.from_xml(raw_trait, rcv_id)
    assert trait.id == "6619"
    assert trait.name == "Arrhythmogenic right ventricular cardiomyopathy"
    assert trait.rcv_id == "RCV000208233"

    # This trait has a preferred symbol
    assert trait.symbol == "ARVD"

    # This trait has multiple XRefs on Name(Preferred) and Name(Alternate)
    trait_xrefs = [dictify(x) for x in trait.xrefs]
    ## Name(Preferred)
    assert {
        "db": "Genetic Alliance",
        "id": "Arrhythmogenic+Right+Ventricular+Cardiomyopathy/587",
        "type": None,
        "ref_field": "name",
        "ref_field_element": "Arrhythmogenic right ventricular cardiomyopathy",
    } in trait_xrefs
    assert {
        "db": "MONDO",
        "id": "MONDO:0016587",
        "type": None,
        "ref_field": "name",
        "ref_field_element": "Arrhythmogenic right ventricular cardiomyopathy",
    } in trait_xrefs
    assert {
        "db": "SNOMED CT",
        "id": "281170005",
        "type": None,
        "ref_field": "name",
        "ref_field_element": "Arrhythmogenic right ventricular cardiomyopathy",
    } in trait_xrefs

    ## Name(Alternate)
    assert {
        "db": "Centre for Mendelian Genomics, University Medical Centre Ljubljana",
        "id": "CMGVARID00187",
        "type": None,
        "ref_field": "alternate_names",
        "ref_field_element": "Arrhythmogenic right ventricular dysplasia",
    } in trait_xrefs
    assert {
        "db": "OMIM",
        "id": "PS107970",
        "type": "Phenotypic series",
        "ref_field": "alternate_names",
        "ref_field_element": "Arrhythmogenic right ventricular dysplasia",
    } in trait_xrefs
    assert {
        "db": "SNOMED CT",
        "id": "253528005",
        "type": None,
        "ref_field": "alternate_names",
        "ref_field_element": "Arrhythmogenic right ventricular dysplasia",
    } in trait_xrefs

    # This trait also has a GeneReviews short attribute, with an xref
    assert trait.gene_reviews_short == "NBK1131"
    assert {
        "db": "GeneReviews",
        "id": "NBK1131",
        "type": None,
        "ref_field": "gene_reviews_short",
        "ref_field_element": None,
    } in trait_xrefs

    # And it has a mode of inheritance (no xref)
    assert trait.mode_of_inheritance == "Various modes of inheritance"


def test_trait_from_xml_3510():
    with open("test/data/VCV000004897.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["ClassifiedRecord"]
    rcv_id = interp_record["RCVList"]["RCVAccession"]["@Accession"]
    interp = interp_record["Classifications"]["GermlineClassification"]
    interp_traitset = interp["ConditionList"]["TraitSet"]
    raw_trait = interp_traitset["Trait"]  # only 1 trait in this example

    trait = Trait.from_xml(raw_trait, rcv_id)
    assert trait.id == "3510"
    assert trait.rcv_id == "RCV000005175"
    assert trait.gard_id == 5289

    # This trait has an attribute_content array because it has multiple GARD ids
    # The first GARD ID is the one that is used as the trait's GARD ID,
    # and the rest are stored in the attribute_content array
    assert len(trait.attribute_content) == 1
    assert trait.attribute_content == [
        {
            "Attribute": {
                "@Type": "GARD id",
                "@integerValue": "4260",
            },
            "XRef": {"@ID": "4260", "@DB": "Office of Rare Diseases"},
        }
    ]


def test_trait_from_xml_406155():
    with open("test/data/VCV000406155.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["ClassifiedRecord"]
    rcv_id = interp_record["RCVList"]["RCVAccession"]["@Accession"]
    interp = interp_record["Classifications"]["GermlineClassification"]
    interp_traitset = interp["ConditionList"]["TraitSet"]
    raw_trait = interp_traitset["Trait"]  # only 1 trait in this example

    trait = Trait.from_xml(raw_trait, rcv_id)
    assert trait.id == "1150"
    assert trait.rcv_id == "RCV000467463"

    # This trait has ghr_links
    assert trait.ghr_links == "MECP2-related severe neonatal encephalopathy"

    # And a public_definition (with xref)
    assert (
        trait.public_definition
        == "The spectrum of MECP2-related phenotypes in females ranges from classic Rett syndrome to variant Rett syndrome with a broader clinical phenotype (either milder or more severe than classic Rett syndrome) to mild learning disabilities; the spectrum in males ranges from severe neonatal encephalopathy to pyramidal signs, parkinsonism, and macroorchidism (PPM-X) syndrome to severe syndromic/nonsyndromic intellectual disability. Females: Classic Rett syndrome, a progressive neurodevelopmental disorder primarily affecting girls, is characterized by apparently normal psychomotor development during the first six to 18 months of life, followed by a short period of developmental stagnation, then rapid regression in language and motor skills, followed by long-term stability. During the phase of rapid regression, repetitive, stereotypic hand movements replace purposeful hand use. Additional findings include fits of screaming and inconsolable crying, autistic features, panic-like attacks, bruxism, episodic apnea and/or hyperpnea, gait ataxia and apraxia, tremors, seizures, and acquired microcephaly. Males: Severe neonatal-onset encephalopathy, the most common phenotype in affected males, is characterized by a relentless clinical course that follows a metabolic-degenerative type of pattern, abnormal tone, involuntary movements, severe seizures, and breathing abnormalities. Death often occurs before age two years."  # noqa: E501
    )
    assert {
        "db": "GeneReviews",
        "id": "NBK1497",
        "type": None,
        "ref_field": "public_definition",
        "ref_field_element": None,
    } in [dictify(x) for x in trait.xrefs]


def test_trait_set_from_xml_10():
    with open("test/data/VCV000000010.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["ClassifiedRecord"]
    rcv_accessions = interp_record["RCVList"]["RCVAccession"]
    interp = interp_record["Classifications"]["GermlineClassification"]
    interp_traitset = ensure_list(interp["ConditionList"]["TraitSet"])

    trait_set_id_to_rcv_id = {
        r["ClassifiedConditionList"]["@TraitSetID"]: r["@Accession"]
        for r in rcv_accessions
    }
    trait_sets = [
        TraitSet.from_xml(raw_traitset, trait_set_id_to_rcv_id[raw_traitset["@ID"]])
        for raw_traitset in interp_traitset
    ]
    assert len(trait_sets) == 10
    assert [ts.id for ts in trait_sets] == [
        "55473",
        "9460",
        "8589",
        "16994",
        "13451",
        "9590",
        "7",
        "21210",
        "2387",
        "1961",
    ]
    assert [ts.rcv_id for ts in trait_sets] == [
        "RCV001248831",
        "RCV000175607",
        "RCV001731265",
        "RCV004584302",
        "RCV000763144",
        "RCV000844708",
        "RCV000000026",
        "RCV000394716",
        "RCV002272003",
        "RCV000991133",
    ]
    assert [ts.type for ts in trait_sets] == [
        "Disease",
        "Disease",
        "Disease",
        "PhenotypeInstruction",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
    ]

    # test traits
    ts13451 = trait_sets[4]
    assert ts13451.id == "13451"
    assert len(ts13451.traits) == 6
    assert [t.id for t in ts13451.traits] == [
        "5535",
        "3370",
        "9587",
        "9601",
        "9582",
        "222",
    ]

    # test content
    assert ts13451.content == {"@ContributesToAggregateClassification": "true"}


def test_trait_mapping_10():
    with open("test/data/VCV000000010.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["ClassifiedRecord"]
    clinical_assertion_id_to_accession = {
        clinical_assertion["@ID"]: clinical_assertion["ClinVarAccession"]["@Accession"]
        for clinical_assertion in ensure_list(
            interp_record["ClinicalAssertionList"]["ClinicalAssertion"]
        )
    }
    trait_mappings_raw = ensure_list(interp_record["TraitMappingList"]["TraitMapping"])
    assert len(trait_mappings_raw) == 353

    trait_mappings_raw_distinct = distinct_dict_set(trait_mappings_raw)
    assert len(trait_mappings_raw_distinct) == 185

    trait_mappings = [
        TraitMapping.from_xml(raw, clinical_assertion_id_to_accession)
        for raw in trait_mappings_raw_distinct
    ]

    tm0 = [
        tm
        for tm in trait_mappings
        if tm.clinical_assertion_id == "SCV000607203"
        and tm.trait_type == "Finding"
        and tm.mapping_type == "XRef"
        and tm.mapping_value == "HP:0002087"
        and tm.mapping_ref == "HP"
    ]
    assert len(tm0) == 1
    tm0 = tm0[0]
    assert tm0.medgen_id == "C4025727"
    assert tm0.medgen_name == "Abnormality of the upper respiratory tract"


def test_traits_from_multiple_classifications():
    """
    This ensures the VCV traitsets from all classification types are included
    """
    filename = "test/data/VCV000013961.xml"
    with open(filename) as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    variation_archive_xml = release["VariationArchive"]
    vcv = VariationArchive.from_xml(variation_archive_xml)
    assert len(vcv.trait_sets) == 32
