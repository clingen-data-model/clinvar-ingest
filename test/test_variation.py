import xmltodict

from clinvar_ingest.model.variation_archive import ClinicalAssertionVariation, Variation


def test_variation_descendant_tree_genotype_with_haplotypes():
    """
    Test the Variation.descendant_tree method.

    Generates a tree of Variation objects, and then tests that the
    descendant_tree method returns the expected tree.
    """
    with open("test/data/VCV000634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["ClassifiedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    expected_tree = [
        "634266",
        ["633847", ["634864"], ["634875"], ["634882"]],
        ["633853", ["633853"]],
    ]
    assert expected_tree == descendant_tree


def test_simple_test_descendant_tree_all_types():
    simple_allele1 = {
        "@VariationID": "SimpleAllele1",
    }
    simple_allele2 = {
        "@VariationID": "SimpleAllele2",
    }
    simple_allele3 = {
        "@VariationID": "SimpleAllele3",
    }
    haplotype1 = {
        "@VariationID": "Haplotype1",
        "SimpleAllele": [simple_allele1, simple_allele2],
    }
    haplotype2 = {
        "@VariationID": "Haplotype2",
        "SimpleAllele": [simple_allele3],
    }
    genotype = {
        "@VariationID": "Genotype1",
        "Haplotype": [haplotype1, haplotype2],
    }
    genotype_descendant_tree = Variation.descendant_tree({"Genotype": genotype})
    expected_genotype_descendant_tree = [
        "Genotype1",
        ["Haplotype1", ["SimpleAllele1"], ["SimpleAllele2"]],
        ["Haplotype2", ["SimpleAllele3"]],
    ]
    assert expected_genotype_descendant_tree == genotype_descendant_tree

    expected_genotype_only_descendant_tree = ["Genotype2"]
    genotype_only_descendant_tree = Variation.descendant_tree(
        {
            "Genotype": {
                "@VariationID": "Genotype2",
            }
        }
    )
    assert expected_genotype_only_descendant_tree == genotype_only_descendant_tree

    haplotype1_descendant_tree = Variation.descendant_tree({"Haplotype": haplotype1})
    expected_haplotype1_descendant_tree = [
        "Haplotype1",
        ["SimpleAllele1"],
        ["SimpleAllele2"],
    ]
    assert expected_haplotype1_descendant_tree == haplotype1_descendant_tree

    expected_haplotype_only_descendant_tree = ["Haplotype3"]
    haplotype_only_descendant_tree = Variation.descendant_tree(
        {
            "Haplotype": {
                "@VariationID": "Haplotype3",
            }
        }
    )
    assert expected_haplotype_only_descendant_tree == haplotype_only_descendant_tree

    haplotype2_descendant_tree = Variation.descendant_tree({"Haplotype": haplotype2})
    expected_haplotype2_descendant_tree = [
        "Haplotype2",
        ["SimpleAllele3"],
    ]
    assert expected_haplotype2_descendant_tree == haplotype2_descendant_tree

    simple_allele1_descendant_tree = Variation.descendant_tree(
        {"SimpleAllele": simple_allele1}
    )
    expected_simple_allele1_descendant_tree = ["SimpleAllele1"]
    assert expected_simple_allele1_descendant_tree == simple_allele1_descendant_tree


def test_variation_descendant_tree_haploytpe_with_alleles():
    """
    Test the Variation.descendant_tree method.

    Generates a tree of Variation objects, and then tests that the
    descendant_tree method returns the expected tree.
    """
    with open("test/data/VCV000040200.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["ClassifiedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    expected_tree = [
        "40200",
        ["7169"],
        ["440459"],
    ]
    assert expected_tree == descendant_tree


def test_variation_get_all_children():
    with open("test/data/VCV000634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["ClassifiedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    children = Variation.get_all_children(descendant_tree)
    expected_children = ["633847", "633853"]
    assert expected_children == children


def test_variation_get_all_descendants():
    with open("test/data/VCV000634266.xml") as inp:
        inp_xml = inp.read()
    inp = xmltodict.parse(inp_xml)
    inp = inp["ClinVarVariationRelease"]["VariationArchive"]["ClassifiedRecord"]

    descendant_tree = Variation.descendant_tree(inp)
    descendants = Variation.get_all_descendants(descendant_tree)
    expected_descendants = ["633847", "633853", "634864", "634875", "634882", "633853"]
    assert expected_descendants == descendants


def test_clinical_assertion_variation_descendants():
    with open("test/data/VCV000000002.xml") as inp:
        inp_xml = inp.read()

    inp = xmltodict.parse(inp_xml)
    interp_record = inp["ClinVarVariationRelease"]["VariationArchive"][
        "ClassifiedRecord"
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


def test_clinical_assertion_variation_descendants_genotype():
    with open("test/data/VCV000634266.xml") as inp:
        inp_xml = inp.read()

    inp = xmltodict.parse(inp_xml)
    interp_record = inp["ClinVarVariationRelease"]["VariationArchive"][
        "ClassifiedRecord"
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
    assert sorted(genotype.descendant_ids) == sorted(
        [
            haplotypeA.id,
            simplealleleAA.id,
            haplotypeB.id,
            simplealleleBA.id,
            simplealleleBB.id,
            simplealleleBC.id,
        ]
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
