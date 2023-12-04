from clinvar_ingest.model import Trait, dictify
from clinvar_ingest.reader import _parse_xml_document


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
    assert trait.medgen_id is None
    assert trait.public_definition is None

    assert trait.disease_mechanism == "loss of function"
    assert trait.disease_mechanism_id == 273
    assert trait.gene_reviews_short is None
    assert dictify(trait.xrefs) == []
    assert trait.attribute_content is None
    assert trait.content is not None


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
