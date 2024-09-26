from clinvar_ingest.model.variation_archive import ClinicalAssertionVariation, Variation
from clinvar_ingest.reader import read_clinvar_vcv_xml


def test_parse(log_conf):
    """
    This test evaluates the case where a Genotype contains both a SimpleAllele and a
    Haplotype (which contains SimpleAlleles)
    """
    filename = "test/data/VCV000424711.xml"
    with open(filename, "r", encoding="utf-8") as f:
        objects = list(read_clinvar_vcv_xml(f))

    assert len(objects) == 18

    clinical_assertion_variations: list[ClinicalAssertionVariation] = [
        o for o in objects if o.entity_type == "clinical_assertion_variation"  # type: ignore
    ]
    assert len(clinical_assertion_variations) == 5

    genotypes = [
        o for o in clinical_assertion_variations if o.subclass_type == "Genotype"
    ]
    haplotypes = [
        o for o in clinical_assertion_variations if o.subclass_type == "Haplotype"
    ]
    simple_alleles = [
        o for o in clinical_assertion_variations if o.subclass_type == "SimpleAllele"
    ]

    assert len(genotypes) == 1
    assert len(haplotypes) == 1
    assert len(simple_alleles) == 3

    scv_id = "SCV000196710"

    # Check child objects of the genotype
    genotype = genotypes[0]
    assert genotype.subclass_type == "Genotype"
    assert genotype.clinical_assertion_id == scv_id
    assert genotype.child_ids == [f"{scv_id}.1", f"{scv_id}.2"]

    genotype_child1 = [
        o for o in clinical_assertion_variations if o.id == genotype.child_ids[0]
    ][0]
    assert genotype_child1.subclass_type == "SimpleAllele"

    genotype_child2 = [
        o for o in clinical_assertion_variations if o.id == genotype.child_ids[1]
    ][0]

    assert genotype_child2.subclass_type == "Haplotype"

    # Check child objects of the haplotype
    haplotype = genotype_child2
    assert haplotype.child_ids == [f"{scv_id}.3", f"{scv_id}.4"]
    haplotype_child1 = [
        o for o in clinical_assertion_variations if o.id == haplotype.child_ids[0]
    ][0]
    assert haplotype_child1.subclass_type == "SimpleAllele"

    haplotype_child2 = [
        o for o in clinical_assertion_variations if o.id == haplotype.child_ids[1]
    ][0]
    assert haplotype_child2.subclass_type == "SimpleAllele"

    # Check descendant_ids
    assert genotype.descendant_ids == [
        genotype_child1.id,
        genotype_child2.id,
        haplotype_child1.id,
        haplotype_child2.id,
    ]

    assert haplotype.descendant_ids == [haplotype_child1.id, haplotype_child2.id]

    # Check the Variation objects have correct descendants and children too
    variation: Variation = [o for o in objects if isinstance(o, Variation)][0]
    assert variation.id == "424711"
    assert variation.child_ids == ["192373", "189364"]
    assert variation.descendant_ids == [
        "192373",
        "189364",
        "242614",
        "242615",
    ]
