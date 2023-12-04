from typing import List
from clinvar_ingest.model import Trait, dictify
from clinvar_ingest.reader import _parse_xml_document


def unordered_dict_list_equal(list1: List[dict], list2: List[dict]) -> bool:
    set1 = set([tuple(elem.items()) for elem in list1])
    set2 = set([tuple(elem.items()) for elem in list2])
    return len(list1) == len(list2) and set1 == set2
    # return set(list1) == set(list2)


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
    # assert dictify(trait.xrefs) == []
    assert trait.attribute_content is None
    assert trait.content is not None

    # assert set(dictify(trait.xrefs)) == set(
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


"""
def test_trait_from_xml_51():
    filename = "test/data/original-clinvar-variation-51.xml"
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
    assert trait.medgen_id is None
    assert trait.public_definition is None

    assert trait.disease_mechanism == "loss of function"
    assert trait.disease_mechanism_id == 273
    assert trait.gene_reviews_short is None
    assert trait.xrefs
    assert trait.attribute_content is None
    assert trait.content is not None
"""
