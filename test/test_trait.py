import json
from typing import List

from clinvar_ingest.model import Trait, TraitSet, dictify
from clinvar_ingest.reader import _parse_xml_document
from clinvar_ingest.utils import ensure_list


def unordered_dict_list_equal(list1: List[dict], list2: List[dict]) -> bool:
    set1 = set([tuple(elem.items()) for elem in list1])
    set2 = set([tuple(elem.items()) for elem in list2])
    return len(list1) == len(list2) and set1 == set2


def test_trait_from_xml_32():
    filename = "test/data/original-clinvar-variation-32.xml"
    with open(filename) as f:
        content = f.read()
    root = _parse_xml_document(content)

    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["InterpretedRecord"]
    interp = interp_record["Interpretations"]["Interpretation"]
    interp_traitset = interp["ConditionList"]["TraitSet"]
    raw_trait = interp_traitset["Trait"]  # only 1 trait in this example

    trait = Trait.from_xml(raw_trait)

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

    assert trait.disease_mechanism == "loss of function"
    assert trait.disease_mechanism_id == 273
    assert trait.gene_reviews_short is None
    assert trait.attribute_content == []
    assert trait.content is not None

    assert unordered_dict_list_equal(
        dictify(trait.xrefs),
        [
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
        ],
    )


def test_trait_from_xml_6619():
    with open("test/data/original-clinvar-variation-222476.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["InterpretedRecord"]
    interp = interp_record["Interpretations"]["Interpretation"]
    interp_traitset = interp["ConditionList"]["TraitSet"]
    raw_trait = interp_traitset["Trait"]  # only 1 trait in this example

    trait = Trait.from_xml(raw_trait)
    assert trait.id == "6619"
    assert trait.name == "Arrhythmogenic right ventricular cardiomyopathy"

    # This trait has a preferred symbol
    assert trait.symbol == "ARVD"

    # This trait has multiple XRefs on Name(Preffered) and Name(Alternate)
    trait_xrefs = dictify(trait.xrefs)
    ## Name(Preffered)
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
    # test/data/original-clinvar-variation-4897.xml
    with open("test/data/original-clinvar-variation-4897.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["InterpretedRecord"]
    interp = interp_record["Interpretations"]["Interpretation"]
    interp_traitset = interp["ConditionList"]["TraitSet"]
    raw_trait = interp_traitset["Trait"]  # only 1 trait in this example

    trait = Trait.from_xml(raw_trait)
    assert trait.id == "3510"

    # This trait has an attribute_content array because it has multiple GARD ids
    assert len(trait.attribute_content) == 1
    assert trait.attribute_content == [
        json.dumps(
            {
                "Attribute": {
                    "@Type": "GARD id",
                    "@integerValue": "5289",
                },
                "XRef": {"@ID": "5289", "@DB": "Office of Rare Diseases"},
            }
        )
    ]


def test_trait_set_from_xml_10():
    with open("test/data/original-clinvar-variation-10.xml") as f:
        content = f.read()
    root = _parse_xml_document(content)
    release = root["ClinVarVariationRelease"]
    interp_record = release["VariationArchive"]["InterpretedRecord"]
    interp = interp_record["Interpretations"]["Interpretation"]
    interp_traitset = ensure_list(interp["ConditionList"]["TraitSet"])

    trait_sets = [TraitSet.from_xml(raw_traitset) for raw_traitset in interp_traitset]
    assert len(trait_sets) == 11
    assert [ts.id for ts in trait_sets] == [
        "55473",
        "7",
        "13451",
        "21210",
        "9460",
        "9590",
        "8589",
        "2387",
        "1961",
        "52490",
        "16994",
    ]
    assert [ts.type for ts in trait_sets] == [
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Disease",
        "Finding",
        "PhenotypeInstruction",
    ]

    # test traits
    ts13451 = trait_sets[2]
    assert ts13451.id == "13451"
    assert len(ts13451.traits) == 6
    assert [t.id for t in ts13451.traits] == [
        "3370",
        "222",
        "9601",
        "9582",
        "9587",
        "5535",
    ]

    # test content
    assert ts13451.content == json.dumps({"@ContributesToAggregateClinsig": "true"})
