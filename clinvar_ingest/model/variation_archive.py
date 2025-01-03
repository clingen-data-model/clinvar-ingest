"""
Data model for ClinVar Variation XML files.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import re
from enum import StrEnum

from clinvar_ingest.model.common import (
    Model,
    dictify,
    int_or_none,
    model_copy,
    sanitize_date,
)
from clinvar_ingest.model.trait import (
    ClinicalAssertionTraitSet,
    Trait,
    TraitMapping,
    TraitSet,
)
from clinvar_ingest.utils import (
    ensure_list,
    extract,
    extract_oneof,
    flatten1,
    get,
    make_counter,
)

_logger = logging.getLogger("clinvar_ingest")


class StatementType(StrEnum):
    GermlineClassification = "GermlineClassification"
    SomaticClinicalImpact = "SomaticClinicalImpact"
    OncogenicityClassification = "OncogenicityClassification"


@dataclasses.dataclass
class Submitter(Model):
    id: str
    current_name: str
    current_abbrev: str
    all_names: list[str]
    all_abbrevs: list[str]
    org_category: str
    scv_id: str
    content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "submitter"

    @staticmethod
    def from_xml(
        inp: dict,
        scv_id: str,
    ):
        _logger.debug(f"Submitter.from_xml(inp={json.dumps(inp)})")
        current_name = extract(inp, "@SubmitterName")
        current_abbrev = extract(inp, "@OrgAbbreviation")
        return Submitter(
            id=extract(inp, "@OrgID"),
            current_name=current_name,
            current_abbrev=current_abbrev,
            org_category=extract(inp, "@OrganizationCategory"),
            all_names=[] if not current_name else [current_name],
            all_abbrevs=[] if not current_abbrev else [current_abbrev],
            scv_id=scv_id,
            content=inp,
        )

    def disassemble(self):
        yield self


@dataclasses.dataclass
class Submission(Model):
    id: str
    submitter_id: str
    additional_submitter_ids: list[str]
    submission_date: str
    scv_id: str
    content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "submission"

    @staticmethod
    def from_xml(
        inp: dict,
        submitter: Submitter,
        additional_submitters: list[Submitter],
        scv_id: str,
    ):
        _logger.debug(
            f"Submission.from_xml(inp={json.dumps(inp)}, {submitter=}, "
            f"{additional_submitters=})"
        )
        submission_date = sanitize_date(extract(inp, "@SubmissionDate"))
        return Submission(
            id=f"{submitter.id}.{submission_date}",
            submitter_id=submitter.id,
            additional_submitter_ids=[s.id for s in additional_submitters],
            submission_date=submission_date,
            scv_id=scv_id,
            # TODO is this overly broad? The `inp` here is the ClinicalAssertion node
            content=inp,
        )

    def disassemble(self):
        yield self


# TODO some ClinicalAssertionTraitSets come from ObservedIn elements,
# but not link is retained between the Observation and its TraitSets
@dataclasses.dataclass
class ClinicalAssertionObservation(Model):
    id: str
    # This is redudant information, so don't inclue the whole TraitSet here, just the id
    # TODO this is actually referring to a TraitSet which can be nested under the ObservedIn element
    # That TraitSet should come out as a ClinicalAssertionTraitSet, and the id should go here
    clinical_assertion_trait_set: ClinicalAssertionTraitSet | None
    content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "clinical_assertion_observation"

    @staticmethod
    def from_xml(inp: dict):
        raise NotImplementedError

    def disassemble(self):
        self_copy = model_copy(self)
        trait_set = self_copy.clinical_assertion_trait_set
        del self_copy.clinical_assertion_trait_set
        if trait_set is not None:
            self_copy.clinical_assertion_trait_set_id = trait_set.id
            yield from trait_set.disassemble()
        else:
            self_copy.clinical_assertion_trait_set_id = None
        yield self_copy


@dataclasses.dataclass
class ClinicalAssertion(Model):
    internal_id: str
    id: str
    title: str
    local_key: str
    version: str
    assertion_type: str
    date_created: str
    date_last_updated: str
    submitted_assembly: str
    record_status: str
    # (CHANGED) Moved to ClinicalAssertion.Classification.ReviewStatus.$
    review_status: str
    # (CHANGED) Moved to ClinicalAssertion.Classification.@DateLastEvaluated
    interpretation_date_last_evaluated: str
    # (CHANGED) Moved to ClinicalAssertion.Classification.<StatementType>.$
    interpretation_description: str
    interpretation_comments: list[dict]
    submitter_id: str
    submitters: list[Submitter]
    submission: Submission
    submission_id: str
    submission_names: list[str]
    variation_id: str
    variation_archive_id: str
    content: dict

    clinical_assertion_observations: list[ClinicalAssertionObservation]
    clinical_assertion_trait_set: ClinicalAssertionTraitSet | None
    clinical_assertion_variations: list[ClinicalAssertionVariation]

    # (NEW) From ClinicalAssertion.Classification [<StatementType>]
    statement_type: StatementType
    # Only for SomaticClinicalImpact
    clinical_impact_assertion_type: str
    clinical_impact_clinical_significance: str

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content", "interpretation_comments"]

    def __post_init__(self):
        self.entity_type = "clinical_assertion"

    @staticmethod
    def from_xml(
        inp: dict,
        normalized_traits: list[Trait],
        trait_mappings: list[TraitMapping],
        variation_id: str,
        variation_archive_id: str,
    ):
        # TODO
        # if _logger.isEnabledFor(logging.DEBUG):
        _logger.debug(f"ClinicalAssertion.from_xml(inp={json.dumps(inp)})")
        obj_id = extract(inp, "@ID")
        raw_accession = extract(inp, "ClinVarAccession")
        scv_accession = extract(raw_accession, "@Accession")
        clinvar_submission = extract(inp, "ClinVarSubmissionID")
        # Do not extract Classification, leave behind remainder in content
        classification_raw = inp["Classification"]
        additional_submitters = [
            Submitter.from_xml(a, scv_accession)
            for a in ensure_list(
                extract(inp, "AdditionalSubmitters", "SubmitterDescription") or []
            )
        ]

        submitter = Submitter.from_xml(raw_accession, scv_accession)
        submitters = [submitter, *additional_submitters]
        submission = Submission.from_xml(
            inp, submitter, additional_submitters, scv_accession
        )

        trait_set_counter = make_counter()
        assertion_trait_set = extract(inp, "TraitSet")
        if assertion_trait_set is not None:
            assertion_trait_set = ClinicalAssertionTraitSet.from_xml(
                assertion_trait_set,
                normalized_traits=normalized_traits,
                trait_mappings=trait_mappings,
            )
            assertion_trait_set.id = scv_accession
            for i, t in enumerate(assertion_trait_set.traits):
                t.id = f"{scv_accession}.{i}"

        observed_ins = ensure_list(extract(inp, "ObservedInList", "ObservedIn") or [])
        observations = [
            ClinicalAssertionObservation(
                id=f"{scv_accession}.{i}",
                clinical_assertion_trait_set=(
                    ClinicalAssertionTraitSet.from_xml(
                        extract(o, "TraitSet"),
                        normalized_traits=normalized_traits,
                        trait_mappings=trait_mappings,
                    )
                    if "TraitSet" in o
                    else None
                ),
                content=o,
            )
            for i, o in enumerate(observed_ins)
        ]
        # The ClinicalAssertion TraitSet and Traits have synthetic ids.
        # Go back and replace them with the accession.<index> for TraitSet,
        # and <TraitSet.id>.<index> for Traits
        # e.g. SCV000000001 has 2 ClinicalAssertion TraitSets, each with 2 Traits:
        # TraitSets: SCV000000001.0, SCV000000001.1
        # Traits: SCV000000001.0.0, SCV000000001.0.1, SCV000000001.1.0, SCV000000001.1.1
        for _, observation in enumerate(observations):
            obs_trait_set = observation.clinical_assertion_trait_set
            if obs_trait_set is not None:
                obs_trait_set.id = f"{scv_accession}.{next(trait_set_counter)}"
                for j, t in enumerate(obs_trait_set.traits):
                    t.id = f"{obs_trait_set.id}.{j}"
        # Extract all variations and add top level to the assertion
        # (with child_ids and descendant_ids pointing to others)
        submitted_variations = ClinicalAssertionVariation.extract_variations(
            inp, scv_accession
        )
        _logger.debug(
            f"scv {scv_accession} had submitted_variations: {submitted_variations}"
        )

        interpretation_comments = []
        for raw_comment in ensure_list(extract(classification_raw, "Comment") or []):
            comment = {"text": extract(raw_comment, "$")}
            if "@Type" in raw_comment:
                comment["type"] = extract(raw_comment, "@Type")
            interpretation_comments.append(comment)

        submission_names = ensure_list(
            extract(inp, "SubmissionNameList", "SubmissionName") or []
        )

        # Classification fields
        review_status = extract(classification_raw, "ReviewStatus", "$")
        interpretation_date_last_evaluated = sanitize_date(
            extract(classification_raw, "@DateLastEvaluated")
        )
        statement_type = None
        clinical_impact_assertion_type = None
        clinical_impact_clinical_significance = None
        interpretation_description = None

        for st in StatementType:
            if st.value in classification_raw:
                if statement_type is not None:
                    raise ValueError(
                        f"Multiple statement types found! {scv_accession}: {classification_raw}"
                    )
                statement_type = st
                # The node, e.g. GermlineClassification
                cls = classification_raw[st.value]
                # In SCVs it's the inner text, not under a Description node like in VCVs
                interpretation_description = extract(cls, "$")
                clinical_impact_assertion_type = extract(
                    cls, "@ClinicalImpactAssertionType"
                )
                clinical_impact_clinical_significance = extract(
                    cls, "@ClinicalImpactClinicalSignificance"
                )

        return ClinicalAssertion(
            internal_id=obj_id,
            id=scv_accession,
            title=extract(clinvar_submission, "@title"),
            local_key=extract(clinvar_submission, "@localKey"),
            version=extract(raw_accession, "@Version"),
            assertion_type=extract(extract(inp, "Assertion"), "$"),
            date_created=sanitize_date(extract(inp, "@DateCreated")),
            date_last_updated=sanitize_date(extract(inp, "@DateLastUpdated")),
            submitted_assembly=extract(clinvar_submission, "@submittedAssembly"),
            record_status=extract(extract(inp, "RecordStatus"), "$"),
            review_status=review_status,
            interpretation_date_last_evaluated=interpretation_date_last_evaluated,
            interpretation_description=interpretation_description,
            interpretation_comments=interpretation_comments,
            submitter_id=submitter.id,
            submitters=submitters,
            submission=submission,
            submission_id=submission.id,
            submission_names=[sn["$"] for sn in submission_names],
            variation_id=variation_id,
            variation_archive_id=variation_archive_id,
            clinical_assertion_observations=observations,
            clinical_assertion_trait_set=assertion_trait_set,
            clinical_assertion_variations=submitted_variations,
            statement_type=statement_type,
            clinical_impact_assertion_type=clinical_impact_assertion_type,
            clinical_impact_clinical_significance=clinical_impact_clinical_significance,
            content=inp,
        )

    def disassemble(self):
        self_copy: ClinicalAssertion = model_copy(self)

        for submitter in self_copy.submitters:
            for subobj in submitter.disassemble():
                yield subobj
        del self_copy.submitters

        for subobj in self_copy.submission.disassemble():
            yield subobj
        del self_copy.submission

        for obs in self_copy.clinical_assertion_observations:
            for subobj in obs.disassemble():
                yield subobj
        self_copy.clinical_assertion_observation_ids = [
            obs.id for obs in self_copy.clinical_assertion_observations
        ]
        del self_copy.clinical_assertion_observations

        if self_copy.clinical_assertion_trait_set is not None:
            for subobj in self_copy.clinical_assertion_trait_set.disassemble():
                yield subobj
            self_copy.clinical_assertion_trait_set_id = re.split(
                "\\.", self_copy.clinical_assertion_trait_set.id
            )[0]
        del self_copy.clinical_assertion_trait_set

        # Make a local reference to the variations and delete the field from the
        # object since it is yielded before the variations
        clinical_assertion_variations = self_copy.clinical_assertion_variations
        del self_copy.clinical_assertion_variations

        yield self_copy

        # Yield variations after the assertion since they reference it, not the other way around
        for variation in clinical_assertion_variations:
            for subobj in variation.disassemble():
                yield subobj


@dataclasses.dataclass
class Gene(Model):
    hgnc_id: str
    id: str
    symbol: str
    full_name: str
    vcv_id: str

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return []

    def __post_init__(self):
        self.entity_type = "gene"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        raise NotImplementedError

    def disassemble(self):
        yield self


@dataclasses.dataclass
class GeneAssociation(Model):
    source: str
    variation_id: str
    gene: Gene
    relationship_type: str
    content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.gene_id = self.gene.id
        self.entity_type = "gene_association"

    @staticmethod
    def from_xml(inp: dict):
        raise NotImplementedError

    def disassemble(self):
        self_copy = model_copy(self)
        yield self_copy.gene
        del self_copy.gene
        yield self_copy


@dataclasses.dataclass
class ClinicalAssertionVariation(Model):
    id: str
    clinical_assertion_id: str
    variation_type: str
    subclass_type: str
    descendant_ids: list[str]
    child_ids: list[str]

    content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "clinical_assertion_variation"

    @staticmethod
    def extract_variations(inp: dict, assertion_accession: str):
        """
        Accepts a ClinicalAssertion XML dict. Returns a list of
        ClinicalAssertionVariation objects, with child_ids and descendant_ids
        filled in appropriately. Removes these variations from the ClinicalAssertion dict.
        Generates synthetic ids for each variation, an index incremented for each and
        appended to the assertion_accession.

        e.g. a Genotype with 2 Haplotypes, each with 2 SimpleAlleles, on assertion SCV01
        Genotype
            Haplotype1
                SimpleAllele1
                SimpleAllele2
            Haplotype2
                SimpleAllele3
                SimpleAllele4
        ->
        [
            # Genotype
            ClinicalAssertionVariation(
                id=SCV01.1,
                child_ids=[SCV01.2, SCV01.3],
                descendant_ids=[SCV01.2, SCV01.3, SCV01.4, SCV01.5, SCV01.6, SCV01.7]),
            # Haplotype1
            ClinicalAssertionVariation(
                id=SCV01.2,
                child_ids=[SCV01.4, SCV01.5],
                descendant_ids=[SCV01.4, SCV01.5]),
            # SimpleAllele1
            ClinicalAssertionVariation(id=SCV01.3, child_ids=[], descendant_ids=[]),
            # SimpleAllele2
            ClinicalAssertionVariation(id=SCV01.4, child_ids=[], descendant_ids=[]),
            # Haplotype2
            ClinicalAssertionVariation(
                id=SCV01.5,
                child_ids=[SCV01.6, SCV01.7],
                descendant_ids=[SCV01.6, SCV01.7]),
            # SimpleAllele3
            ClinicalAssertionVariation(id=SCV01.6, child_ids=[], descendant_ids=[]),
            # SimpleAllele4
            ClinicalAssertionVariation(id=SCV01.7, child_ids=[], descendant_ids=[]),
        ]
        """
        buffer = []

        class Counter:
            def __init__(self):
                self.counter = 0

            def get_and_increment(self):
                v = self.counter
                self.counter += 1
                return v

        counter = Counter()

        def extract_and_accumulate_descendants(inp: dict) -> list[Variation]:
            _logger.debug(
                f"extract_and_accumulate_descendants(inp={json.dumps(dictify(inp))})"
            )
            variants = []
            if "SimpleAllele" in inp:
                variants += [
                    ("SimpleAllele", o)
                    for o in ensure_list(extract(inp, "SimpleAllele"))
                ]
            if "Haplotype" in inp:
                variants += [
                    ("Haplotype", o) for o in ensure_list(extract(inp, "Haplotype"))
                ]
            if "Genotype" in inp:
                variants += [("Genotype", o) for o in [extract(inp, "Genotype")]]
            if len(variants) == 0:
                return []

            outputs = []
            for subclass_type, variant_input in variants:
                variation = ClinicalAssertionVariation(
                    id=f"{assertion_accession}.{counter.get_and_increment()}",
                    clinical_assertion_id=assertion_accession,
                    variation_type=extract(extract(variant_input, "VariantType"), "$")
                    or extract(extract(variant_input, "VariationType"), "$"),
                    subclass_type=subclass_type,
                    descendant_ids=[],  # Fill in later
                    child_ids=[],  # Fill in later
                    content={},  # Fill in later
                )
                # Add to arrays first before recursing so that the variations
                # are in the buffer in the order encountered, not the order finished
                # (pre-order traversal order)
                # This is works because later steps just update fields on the objects.
                buffer.append(variation)
                outputs.append(variation)

                # Recursion
                children = extract_and_accumulate_descendants(variant_input)
                # Update fields based on accumulated descendants
                variation.child_ids = [c.id for c in children]
                direct_children = variation.child_ids
                _logger.debug(f"{direct_children=}")
                non_child_descendants = flatten1([c.child_ids or [] for c in children])
                _logger.debug(f"{non_child_descendants=}")
                variation.descendant_ids = direct_children + non_child_descendants
                variation.content = variant_input

            return outputs

        v = extract_and_accumulate_descendants(inp)
        _logger.debug(f"extract_and_accumulate_descendants returned: {v}")
        if len(v) > 1:
            raise RuntimeError(f"Expected 1 or fewer variations, got {len(v)}: {v}")
        return buffer

    def disassemble(self):
        yield self


@dataclasses.dataclass
class Variation(Model):
    id: str
    name: str
    variation_type: str
    subclass_type: str
    allele_id: str
    protein_change: list[str]
    num_chromosomes: int
    gene_associations: list[GeneAssociation]

    content: dict

    child_ids: list[str]
    descendant_ids: list[str]

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "variation"

    @staticmethod
    def from_xml(inp: dict, variation_archive_id: str):
        _logger.debug(f"Variation.from_xml(inp={json.dumps(inp)})")
        descendant_tree = Variation.descendant_tree(inp)
        # _logger.info(f"descendant_tree: {descendant_tree}")
        child_ids = Variation.get_all_children(descendant_tree)
        # _logger.info(f"child_ids: {child_ids}")
        descendant_ids = Variation.get_all_descendants(descendant_tree)
        # _logger.info(f"descendant_ids: {descendant_ids}")
        if "SimpleAllele" in inp:
            subclass_type = "SimpleAllele"
            inp = extract(inp, "SimpleAllele")
        elif "Haplotype" in inp:
            subclass_type = "Haplotype"
            inp = extract(inp, "Haplotype")
        elif "Genotype" in inp:
            subclass_type = "Genotype"
            inp = extract(inp, "Genotype")
        else:
            raise RuntimeError("Unknown variation type: " + json.dumps(inp))
        obj = Variation(
            # VariationID is at the VariationArchive and the
            # SimpleAllele/Haplotype/Genotype level
            id=extract(inp, "@VariationID"),
            name=extract(extract(inp, "Name"), "$"),
            variation_type=extract(
                extract_oneof(inp, "VariantType", "VariationType")[1], "$"
            ),
            subclass_type=subclass_type,
            allele_id=extract(inp, "@AlleleID"),
            protein_change=[
                pc["$"] for pc in ensure_list(extract(inp, "ProteinChange") or [])
            ],
            num_chromosomes=int_or_none(extract(inp, "@NumberOfChromosomes")),
            gene_associations=[],
            child_ids=child_ids,
            descendant_ids=descendant_ids,
            content=inp,
        )
        obj.gene_associations = [
            GeneAssociation(
                source=extract(g, "@Source"),
                variation_id=obj.id,
                gene=Gene(
                    hgnc_id=extract(g, "@HGNC_ID"),
                    id=extract(g, "@GeneID"),
                    symbol=extract(g, "@Symbol"),
                    full_name=extract(g, "@FullName"),
                    vcv_id=variation_archive_id,
                ),
                relationship_type=extract(g, "@RelationshipType"),
                content=g,
            )
            for g in ensure_list(extract(extract(inp, "GeneList"), "Gene") or [])
        ]
        return obj

    @staticmethod
    def descendant_tree(inp: dict, caller: bool = False):  # noqa: PLR0912
        """
        Accepts xmltodict parsed XML for a SimpleAllele, Haplotype, or Genotype.
        Returns a tree of child ids. Each level is a list, where the first element
        is the parent id, and the rest are children, each which is also a list following
        the same layout. Any list with a single element is a leaf node.

        [genotype_id,
            [haplotype_id1,
                [simpleallele_id11]
                [simpleallele_id12]]
            [haplotype_id2,
                [simpleallele_id21]]]
        """
        outputs = []
        if "SimpleAllele" in inp:
            simple_alleles = ensure_list(inp["SimpleAllele"])
            for sa in simple_alleles:
                node = [sa["@VariationID"]]
                if caller:
                    outputs.append(node)
                else:
                    outputs.extend(node)

        if "Haplotype" in inp:
            haplotypes = ensure_list(inp["Haplotype"])
            # List of Haplotype IDs, and recursive call on each Haplotype object
            for h in haplotypes:
                node = [h["@VariationID"]]
                desc_tree = Variation.descendant_tree(h, True)
                if desc_tree:
                    node.extend(desc_tree)
                # When processing a single haplotype with alleles and no owning context,
                # the caller is None, and we return as a list
                #      [haplotype_id1,
                #          [simpleallele_id11]
                #          [simpleallele_id12]]
                #
                # When the caller is not None, we are in a dependent context,
                # and we return a list of lists
                #      [[haplotype_id1,
                #           [simpleallele_id11]
                #           [simpleallele_id12]]
                #        [haplotype_id2,
                #           [simpleallele_id21]]]
                if caller:
                    outputs.append(node)
                else:
                    outputs.extend(node)

        if "Genotype" in inp:
            genotypes = ensure_list(inp["Genotype"])
            if len(genotypes) > 1:
                _logger.error(f"Multiple genotypes not supported: {json.dumps(inp)}")
                raise RuntimeError("Multiple genotypes not supported")
            if len(outputs) > 0:
                _logger.error(
                    f"Genotype cannot coexist with other variation type: {json.dumps(inp)}"
                )
                raise RuntimeError("Genotype cannot coexist with other variation type")
            g = genotypes[0]
            node = [g["@VariationID"]]
            desc_tree = Variation.descendant_tree(g, True)
            if desc_tree:
                node.extend(desc_tree)
            outputs = node

        return outputs

    @staticmethod
    def get_all_descendants(descendant_tree: list):
        """
        Accepts a descendant_tree. Returns a list of ids descending from the root.
        (non inclusive of root)
        """
        if len(descendant_tree) == 0:
            return []
        _, *children = descendant_tree
        child_ids = [c[0] for c in children]
        grandchildren = [
            grandchild
            for child in children
            for grandchild in Variation.get_all_descendants(child)
        ]
        _logger.debug(f"{child_ids=}, {grandchildren=}")
        return child_ids + grandchildren

    @staticmethod
    def get_all_children(descendant_tree: list):
        """
        Accepts a descendant_tree. Returns the first level children.
        """
        if descendant_tree is None or len(descendant_tree) == 0:
            return []
        _, *children = descendant_tree
        return [child[0] for child in children]

    def disassemble(self):
        self_copy = model_copy(self)

        # Yield self before gene associations since they refer to the variation
        gene_associations = self_copy.gene_associations
        del self_copy.gene_associations
        yield self_copy

        for ga in gene_associations:
            yield from ga.disassemble()


@dataclasses.dataclass
class RcvAccessionClassification(Model):
    # TODO Add RCV_ID as a field to link this back to the VariationArchive
    # maybe another name? Use a field name that exists elsewhere.
    rcv_id: str
    statement_type: StatementType
    review_status: str

    # Description
    num_submissions: int | None
    date_last_evaluated: str
    interp_description: str

    # Only for SomaticClinicalImpact
    clinical_impact_assertion_type: str
    clinical_impact_clinical_significance: str

    content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "rcv_accession_classification"

    @staticmethod
    def from_xml_single(inp: dict, statement_type: StatementType, rcv_id: str):
        """
        The input is a single Classification node contents.
        Either the value of a GermlineClassification, SomaticClinicalImpact,
        or OncogenicityClassification entry. The statement_type is the key
        from the original `Classifications` XML/dict, indicating the type.
        """
        # Don't extract Description because there's a chance more XML attributes could be added.
        # TODO some SomaticClinicalImpact classifications have more than 1 Description element, causing this
        # get() call to return an array rather than a single {} dict. The `"Description": [..]` key-value is then
        # placed in the resulting `content` field of the classification, since it is non-empty, and the date_last_evaluated,
        # num_submissions, interp_description, clinical_impact_assertion_type, and clinical_impact_clinical_significance
        # fields are null.
        # This is rare, and we are deferring handling this until it can be discussed with ClinVar.  Possibly it will
        # just have to be handled downstream. Or we can make a 'description' field that is an array of dicts/records.
        raw_description = get(inp, "Description") or {}
        return RcvAccessionClassification(
            rcv_id=rcv_id,
            statement_type=statement_type,
            review_status=extract(inp, "ReviewStatus", "$"),
            num_submissions=int_or_none(extract(raw_description, "@SubmissionCount")),
            interp_description=extract(raw_description, "$"),
            date_last_evaluated=sanitize_date(
                extract(raw_description, "@DateLastEvaluated")
            ),
            clinical_impact_assertion_type=extract(
                raw_description, "@ClinicalImpactAssertionType"
            ),
            clinical_impact_clinical_significance=extract(
                raw_description,
                "@ClinicalImpactClinicalSignificance",
            ),
            content=inp,
        )

    @staticmethod
    def from_xml(inp: dict, rcv_id: str):
        outputs: list[RcvAccessionClassification] = []

        # StrEnum objects to values dict
        statement_types = {o: o.value for o in StatementType}
        for statement_type, statement_type_str in statement_types.items():
            if statement_type_str in inp:
                outputs.append(
                    RcvAccessionClassification.from_xml_single(
                        inp[statement_type_str], statement_type, rcv_id
                    )
                )
        return outputs

    def disassemble(self):
        yield self


@dataclasses.dataclass
class RcvAccession(Model):
    id: str
    variation_id: int
    independent_observations: int
    variation_archive_id: str
    version: int | None
    title: str
    trait_set_id: str

    content: dict

    classifications: list[RcvAccessionClassification]

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "rcv_accession"

    @staticmethod
    def from_xml(
        inp: dict,
        variation_id: int,
        variation_archive_id: str,
    ):
        """
        OLD:

        <RCVAccession
            Title="CYP2C19*12/*34 AND Sertraline response"
            ReviewStatus="practice guideline"
            Interpretation="drug response"
            SubmissionCount="1"
            Accession="RCV000783230"
            Version="8">
        <InterpretedConditionList TraitSetID="20745">
            <InterpretedCondition DB="MedGen" ID="CN221265">
                Sertraline response
            </InterpretedCondition>
        </InterpretedConditionList>
        </RCVAccession>

        NEW:

        <RCVAccession Title="CYP2C19*12/*34 AND Sertraline response"
                      Accession="RCV000783230" Version="10">
            <ClassifiedConditionList TraitSetID="20745">
                <ClassifiedCondition DB="MedGen" ID="CN221265">Sertraline response</ClassifiedCondition>
            </ClassifiedConditionList>
            <RCVClassifications>
                <GermlineClassification>
                    <ReviewStatus>practice guideline</ReviewStatus>
                    <Description SubmissionCount="1">drug response</Description>
                </GermlineClassification>
            </RCVClassifications>
        </RCVAccession>

        SomaticClinicalImpact:
        <RCVAccession Title="NM_024426.6(WT1):c.1400G&gt;T (p.Arg467Leu) AND Acute myeloid leukemia"
                      Accession="RCV003883245" Version="1">
            <ClassifiedConditionList TraitSetID="6288">
                <ClassifiedCondition DB="MedGen" ID="C0023467">Acute myeloid leukemia</ClassifiedCondition>
            </ClassifiedConditionList>
            <RCVClassifications>
                <SomaticClinicalImpact>
                    <ReviewStatus>no assertion criteria provided</ReviewStatus>
                    <Description
                        ClinicalImpactAssertionType="prognostic"
                        ClinicalImpactClinicalSignificance="poor outcome"
                        DateLastEvaluated="2024-01-24"
                        SubmissionCount="1">
                        Tier I - Strong
                    </Description>
                </SomaticClinicalImpact>
            </RCVClassifications>
        </RCVAccession>
        """
        # org.broadinstitute.monster.clinvar.parsers.VCV.scala : 259 : parseRawRcv
        rcv_id = extract(inp, "@Accession")
        rcv_classifications_raw = extract(inp, "RCVClassifications") or {}

        # TODO independentObservations always null?
        return RcvAccession(
            independent_observations=extract(inp, "@independentObservations"),
            variation_id=variation_id,
            id=rcv_id,
            variation_archive_id=variation_archive_id,
            version=int_or_none(extract(inp, "@Version")),
            title=extract(inp, "@Title"),
            trait_set_id=extract(inp, "ClassifiedConditionList", "@TraitSetID"),
            classifications=RcvAccessionClassification.from_xml(
                rcv_classifications_raw, rcv_id
            ),
            content=inp,
        )

    def disassemble(self):
        self_copy = model_copy(self)

        for c in self_copy.classifications:
            yield from c.disassemble()
        del self_copy.classifications

        yield self_copy


@dataclasses.dataclass
class VariationArchiveClassification(Model):
    vcv_id: str
    statement_type: StatementType
    review_status: str

    num_submitters: int | None
    num_submissions: int | None
    date_created: str
    date_last_evaluated: str
    interp_description: str
    interp_explanation: str
    most_recent_submission: str

    # Only for SomaticClinicalImpact
    clinical_impact_assertion_type: str
    clinical_impact_clinical_significance: str

    content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "variation_archive_classification"

    @staticmethod
    def from_xml_single(inp: dict, statement_type: StatementType, vcv_id: str):
        """
        The input is a single Classification node contents.
        Either the value of a GermlineClassification, SomaticClinicalImpact,
        or OncogenicityClassification entry. The statement_type is the key
        from the original `Classifications` XML/dict, indicating the type.
        """
        interp_description = extract(inp, "Description")

        """
        explanation example:
        <Explanation DataSource="ClinVar" Type="public">Pathogenic(1); Uncertain significance(2)</Explanation>
        """
        # Get the explanation inner text but leave the attributes behind
        interp_explanation = extract(get(inp, "Explanation"), "$")
        return VariationArchiveClassification(
            vcv_id=vcv_id,
            statement_type=statement_type,
            review_status=extract(inp, "ReviewStatus", "$"),
            num_submitters=int_or_none(extract(inp, "@NumberOfSubmitters")),
            num_submissions=int_or_none(extract(inp, "@NumberOfSubmissions")),
            date_created=sanitize_date(extract(inp, "@DateCreated")),
            interp_description=extract(interp_description, "$"),
            interp_explanation=interp_explanation,
            most_recent_submission=sanitize_date(extract(inp, "@MostRecentSubmission")),
            date_last_evaluated=sanitize_date(extract(inp, "@DateLastEvaluated")),
            clinical_impact_assertion_type=extract(
                interp_description, "@ClinicalImpactAssertionType"
            ),
            clinical_impact_clinical_significance=extract(
                interp_description,
                "@ClinicalImpactClinicalSignificance",
            ),
            content=inp,
        )

    @staticmethod
    def from_xml(inp: dict, vcv_id: str):
        outputs: list[VariationArchiveClassification] = []

        # StrEnum objects to values dict
        statement_types = {o: o.value for o in StatementType}
        for statement_type, statement_type_str in statement_types.items():
            if statement_type_str in inp:
                outputs.append(
                    VariationArchiveClassification.from_xml_single(
                        inp[statement_type_str], statement_type, vcv_id
                    )
                )
        return outputs

    def disassemble(self):
        yield self


@dataclasses.dataclass
class VariationArchive(Model):
    id: str
    name: str
    version: str
    variation: Variation
    record_status: str
    species: str
    num_submitters: int | None
    num_submissions: int | None

    date_created: str
    date_last_updated: str
    most_recent_submission: str

    content: dict

    trait_sets: list[TraitSet]
    trait_mappings: list[TraitMapping]
    clinical_assertions: list[ClinicalAssertion]
    rcv_accessions: list[RcvAccession]
    classifications: list[VariationArchiveClassification]

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content", "interp_content"]

    def __post_init__(self):
        self.variation_id = self.variation.id
        self.entity_type = "variation_archive"

    @staticmethod
    def from_xml(inp: dict):
        _logger.debug(f"VariationArchive.from_xml(inp={json.dumps(inp)})")
        vcv_accession = extract(inp, "@Accession")

        # TODO don't include empty classifications from IncludedRecord
        # Find a submitted Haplotype with a SimpleAllele IncludedRecord with a non-empty Classification

        record_type = (
            "ClassifiedRecord" if "ClassifiedRecord" in inp else "IncludedRecord"
        )
        interp_record = inp[record_type]

        variation = Variation.from_xml(interp_record, vcv_accession)
        rcv_accessions = [
            RcvAccession.from_xml(
                r, variation_id=variation.id, variation_archive_id=vcv_accession
            )
            for r in ensure_list(
                extract(interp_record, "RCVList", "RCVAccession") or []
            )
        ]
        raw_clinical_assertions = ensure_list(
            extract(
                extract(interp_record, "ClinicalAssertionList"),
                "ClinicalAssertion",
            )
            or []
        )
        clinical_assertion_id_to_accession = {
            clinical_assertion["@ID"]: clinical_assertion["ClinVarAccession"][
                "@Accession"
            ]
            for clinical_assertion in raw_clinical_assertions
        }
        trait_mappings = [
            TraitMapping.from_xml(tm, clinical_assertion_id_to_accession)
            for tm in ensure_list(
                extract(
                    extract(
                        interp_record,
                        "TraitMappingList",
                    ),
                    "TraitMapping",
                )
                or []
            )
        ]
        # Collect TraitSet dicts from each Classification type
        if record_type == "ClassifiedRecord":
            raw_classifications = extract(interp_record, "Classifications")
        else:
            # IncludedRecord classifications are added by ClinVar in the XML, however
            # they are always "empty", saying the number of submissions is 0 and there
            # is no classification for the variant. We will ignore these.
            # e.g.:
            # {"@VariationID": "267462", "@VariationType": "Deletion", "@RecordType": "included", "IncludedRecord": {"Classifications": {
            # "GermlineClassification": {"@NumberOfSubmissions": "0", "@NumberOfSubmitters": "0", "ReviewStatus": {"$": "no classification for the single variant"}, "Description": {"$": "no classification for the single variant"}}, "SomaticClinicalImpact": {"@NumberOfSubmissions": "0", "@NumberOfSubmitters": "0", "ReviewStatus": {"$": "no classification for the single variant"}, "Description": {"$": "no classification for the single variant"}},
            # "OncogenicityClassification": {"@NumberOfSubmissions": "0", "@NumberOfSubmitters": "0", "ReviewStatus": {"$": "no classification for the single variant"}, "Description": {"$": "no classification for the single variant"}}}, "SubmittedClassificationList": {"SCV": {"@Accession": "SCV000328413", "@Version": "2"}}, "ClassifiedVariationList": {"ClassifiedVariation": {"@VariationID": "267444", "@Accession": "VCV000267444", "@Version": "4"}}}}
            raw_classifications = {}
        raw_classification_types = {r.value for r in StatementType}.intersection(
            set(raw_classifications.keys())
        )
        raw_trait_sets = flatten1(
            [
                ensure_list(
                    extract(
                        raw_classifications[raw_classification_type],
                        "ConditionList",
                        "TraitSet",
                    )
                    or []
                )
                for raw_classification_type in raw_classification_types
            ]
        )

        trait_set_id_to_rcv_id = {r.trait_set_id: r.id for r in rcv_accessions}
        trait_sets = [
            TraitSet.from_xml(ts, trait_set_id_to_rcv_id[get(ts, "@ID")])
            for ts in raw_trait_sets
        ]

        # Classifications is a single node containing multiple Classification subclass nodes
        # e.g. "Classifications": {
        #  "GermlineClassification": {...},
        #  "SomaticClinicalImpact": {...}}
        classifications = VariationArchiveClassification.from_xml(
            raw_classifications, vcv_accession
        )

        return VariationArchive(
            id=vcv_accession,
            name=extract(inp, "@VariationName"),
            version=extract(inp, "@Version"),
            variation=variation,
            clinical_assertions=[
                ClinicalAssertion.from_xml(
                    ca,
                    normalized_traits=flatten1([ts.traits for ts in trait_sets]),
                    trait_mappings=trait_mappings,
                    variation_id=variation.id,
                    variation_archive_id=vcv_accession,
                )
                for ca in raw_clinical_assertions
            ],
            date_created=sanitize_date(extract(inp, "@DateCreated")),
            date_last_updated=sanitize_date(extract(inp, "@DateLastUpdated")),
            most_recent_submission=sanitize_date(extract(inp, "@MostRecentSubmission")),
            record_status=extract(extract(inp, "RecordStatus"), "$"),
            species=extract(extract(inp, "Species"), "$"),
            num_submitters=int_or_none(extract(inp, "@NumberOfSubmitters")),
            num_submissions=int_or_none(extract(inp, "@NumberOfSubmissions")),
            trait_sets=trait_sets,
            trait_mappings=trait_mappings,
            rcv_accessions=rcv_accessions,
            classifications=classifications,
            content=inp,
        )

    def disassemble(self):
        self_copy = model_copy(self)
        for val in self_copy.variation.disassemble():
            yield val
        del self_copy.variation
        for tm in self_copy.trait_mappings:
            for val in tm.disassemble():
                yield val
        del self_copy.trait_mappings
        for ts in self_copy.trait_sets:
            for val in ts.disassemble():
                yield val
        del self_copy.trait_sets
        for clinical_assertion in self_copy.clinical_assertions:
            for sub_obj in clinical_assertion.disassemble():
                yield sub_obj
        del self_copy.clinical_assertions

        for rcv in self_copy.rcv_accessions:
            for sub_obj in rcv.disassemble():
                yield sub_obj
        del self_copy.rcv_accessions

        # TODO classifications
        for classification in self_copy.classifications:
            for sub_obj in classification.disassemble():
                yield sub_obj
        del self_copy.classifications

        yield self_copy
