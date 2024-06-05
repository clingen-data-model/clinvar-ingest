import xmltodict

from clinvar_ingest.model.variation_archive import ClinicalAssertionVariation, Variation


def test_variation_descendant_tree():
    """
    Test the Variation.descendant_tree method.

    Generates a tree of Variation objects, and then tests that the
    descendant_tree method returns the expected tree.
    """
    with open("test/data/original-clinvar-variation-634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["InterpretedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    expected_tree = [
        "634266",
        ["633847", ["634864"], ["634875"], ["634882"]],
        ["633853", ["633853"]],
    ]
    assert expected_tree == descendant_tree


def test_variation_get_all_children():
    with open("test/data/original-clinvar-variation-634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["InterpretedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    children = Variation.get_all_children(descendant_tree)
    expected_children = ["633847", "633853"]
    assert expected_children == children


def test_variation_get_all_descendants():
    with open("test/data/original-clinvar-variation-634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["InterpretedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    descendants = Variation.get_all_descendants(descendant_tree)
    expected_descendants = ["633847", "633853", "634864", "634875", "634882", "633853"]
    assert expected_descendants == descendants


def test_clinical_assertion_variation_descendants():
    with open("test/data/original-clinvar-variation-2.xml") as inp:
        inp_xml = inp.read()

    inp = xmltodict.parse(inp_xml)
    interp_record = inp["ClinVarVariationRelease"]["VariationArchive"][
        "InterpretedRecord"
    ]
    clinical_assertion_list = interp_record["ClinicalAssertionList"][
        "ClinicalAssertion"
    ]
    assert isinstance(clinical_assertion_list, list)
    assert len(clinical_assertion_list) == 2

    scv0_variations = ClinicalAssertionVariation.extract_variations(
        clinical_assertion_list[0],
        clinical_assertion_list[0]["ClinVarAccession"]["@Accession"],
    )
    assert len(scv0_variations) == 1
    v0: ClinicalAssertionVariation = scv0_variations[0]
    assert v0.id == "SCV000020155.0"
    assert v0.content["Name"] == "AP5Z1, 4-BP DEL/22-BP INS, NT80"
    assert v0.clinical_assertion_id == "SCV000020155"
    assert v0.child_ids == []
    assert v0.descendant_ids == []
    # print(scv0_variations[0])


def test_clinical_assertion_variation_descendants_genotype():
    with open("test/data/original-clinvar-variation-634266.xml") as inp:
        inp_xml = inp.read()

    inp = xmltodict.parse(inp_xml)
    interp_record = inp["ClinVarVariationRelease"]["VariationArchive"][
        "InterpretedRecord"
    ]
    clinical_assertion_list = interp_record["ClinicalAssertionList"][
        "ClinicalAssertion"
    ]
    assert isinstance(clinical_assertion_list, list)
    assert len(clinical_assertion_list) == 4

    scv000921753 = clinical_assertion_list[0]
    assert scv000921753["ClinVarAccession"]["@Accession"] == "SCV000921753"

    scv000921753_variations = ClinicalAssertionVariation.extract_variations(
        scv000921753,
        "SCV000921753",
    )
    assert len(scv000921753_variations) == 7
    genotype: ClinicalAssertionVariation = scv000921753_variations[0]
    assert genotype.subclass_type == "Genotype"
    assert genotype.id == "SCV000921753.0"

    haplotypeA: ClinicalAssertionVariation = scv000921753_variations[1]
    assert haplotypeA.subclass_type == "Haplotype"
    assert haplotypeA.id == "SCV000921753.1"

    simplealleleAA: ClinicalAssertionVariation = scv000921753_variations[2]
    assert simplealleleAA.subclass_type == "SimpleAllele"
    assert simplealleleAA.id == "SCV000921753.2"

    haplotypeB: ClinicalAssertionVariation = scv000921753_variations[3]
    assert haplotypeB.subclass_type == "Haplotype"
    assert haplotypeB.id == "SCV000921753.3"

    simplealleleBA: ClinicalAssertionVariation = scv000921753_variations[4]
    assert simplealleleBA.subclass_type == "SimpleAllele"
    assert simplealleleBA.id == "SCV000921753.4"

    simplealleleBB: ClinicalAssertionVariation = scv000921753_variations[5]
    assert simplealleleBB.subclass_type == "SimpleAllele"
    assert simplealleleBB.id == "SCV000921753.5"

    simplealleleBC: ClinicalAssertionVariation = scv000921753_variations[6]
    assert simplealleleBC.subclass_type == "SimpleAllele"
    assert simplealleleBC.id == "SCV000921753.6"

    # Check direct children
    assert genotype.child_ids == [haplotypeA.id, haplotypeB.id]
    assert haplotypeA.child_ids == [simplealleleAA.id]
    assert simplealleleAA.child_ids == []
    assert haplotypeB.child_ids == [
        simplealleleBA.id,
        simplealleleBB.id,
        simplealleleBC.id,
    ]
    assert simplealleleBA.child_ids == []
    assert simplealleleBB.child_ids == []
    assert simplealleleBC.child_ids == []

    # Check descendants
    assert list(sorted(genotype.descendant_ids)) == list(
        sorted(
            [
                haplotypeA.id,
                simplealleleAA.id,
                haplotypeB.id,
                simplealleleBA.id,
                simplealleleBB.id,
                simplealleleBC.id,
            ]
        )
    )
    assert haplotypeA.descendant_ids == [simplealleleAA.id]
    assert simplealleleAA.descendant_ids == []
    assert haplotypeB.descendant_ids == [
        simplealleleBA.id,
        simplealleleBB.id,
        simplealleleBC.id,
    ]
    assert simplealleleBA.descendant_ids == []
    assert simplealleleBB.descendant_ids == []
    assert simplealleleBC.descendant_ids == []
