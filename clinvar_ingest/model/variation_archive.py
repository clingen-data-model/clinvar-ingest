"""
Data model for ClinVar Variation XML files.
"""

# TODO https://github.com/jpvanhal/inflection does good conversion
# between PascalCase and snake_case for entity_type. If Model names are
# reliable we could generate entity_type strings.
from __future__ import annotations

import dataclasses
import json
import logging
from typing import List

from clinvar_ingest.model.common import Model, int_or_none, model_copy, sanitize_date
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


@dataclasses.dataclass
class Submitter(Model):
    id: str
    current_name: str
    current_abbrev: str
    all_names: List[str]
    all_abbrevs: List[str]
    org_category: str
    content: dict

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "submitter"

    @staticmethod
    def from_xml(inp: dict):
        _logger.debug(f"Submitter.from_xml(inp={json.dumps(inp)})")
        current_name = extract(inp, "@SubmitterName")
        current_abbrev = extract(inp, "@OrgAbbreviation")
        obj = Submitter(
            id=extract(inp, "@OrgID"),
            current_name=current_name,
            current_abbrev=current_abbrev,
            org_category=extract(inp, "@OrganizationCategory"),
            all_names=[] if not current_name else [current_name],
            all_abbrevs=[] if not current_abbrev else [current_abbrev],
            content=inp,
        )
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class Submission(Model):
    id: str
    submitter_id: str
    additional_submitter_ids: List[str]
    submission_date: str
    content: dict

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "submission"

    @staticmethod
    def from_xml(
        inp: dict,
        submitter: Submitter = {},
        additional_submitters: list = [Submitter],
    ):
        _logger.debug(
            f"Submission.from_xml(inp={json.dumps(inp)}, {submitter=}, "
            f"{additional_submitters=})"
        )
        obj = Submission(
            id=f"{submitter.id}",  # TODO - FIX w/ Date
            submitter_id=submitter.id,
            additional_submitter_ids=list(filter("id", additional_submitters)),
            submission_date=sanitize_date(extract(inp, "@SubmissionDate")),
            content=inp,
        )
        return obj

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
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "clinical_assertion_observation"

    @staticmethod
    def from_xml(inp: dict):
        raise NotImplementedError()

    def disassemble(self):
        self_copy = model_copy(self)
        trait_set = self_copy.clinical_assertion_trait_set
        del self_copy.clinical_assertion_trait_set
        if trait_set is not None:
            setattr(self_copy, "clinical_assertion_trait_set_id", trait_set.id)
            for subobj in trait_set.disassemble():
                yield subobj
        else:
            setattr(self_copy, "clinical_assertion_trait_set_id", None)
        yield self_copy


@dataclasses.dataclass
class ClinicalAssertion(Model):
    assertion_id: str
    title: str
    local_key: str
    assertion_accession: str
    version: str
    assertion_type: str
    date_created: str
    date_last_updated: str
    submitted_assembly: str
    record_status: str
    review_status: str
    interpretation_date_last_evaluated: str
    interpretation_description: str
    submitter: Submitter
    submission: Submission
    content: dict

    clinical_assertion_observations: List[ClinicalAssertionObservation]
    clinical_assertion_trait_set: ClinicalAssertionTraitSet | None

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "clinical_assertion"

    @staticmethod
    def from_xml(
        inp: dict,
        normalized_traits: List[Trait] = [],
        trait_mappings: List[TraitMapping] = [],
    ):
        _logger.debug(f"ClinicalAssertion.from_xml(inp={json.dumps(inp)})")
        obj_id = extract(inp, "@ID")
        raw_accession = extract(inp, "ClinVarAccession")
        scv_accession = extract(raw_accession, "@Accession")
        clinvar_submission = extract(inp, "ClinVarSubmissionID")
        interpretation = extract(inp, "Interpretation")
        additional_submitters = list(
            map(
                Submitter.from_xml,
                ensure_list(
                    extract(
                        raw_accession, "AdditionalSubmitters", "SubmitterDescription"
                    )
                    or []
                ),
            )
        )
        submitter = Submitter.from_xml(raw_accession)
        submission = Submission.from_xml(inp, submitter, additional_submitters)

        trait_set_counter = make_counter()
        assertion_trait_set = extract(inp, "TraitSet")
        if assertion_trait_set is not None:
            assertion_trait_set = ClinicalAssertionTraitSet.from_xml(
                assertion_trait_set,
                normalized_traits=normalized_traits,
                trait_mappings=trait_mappings,
            )
            # The ClinicalAssertion TraitSet and Traits have synthetic ids.
            # Replace them with just the accession.<index>
            assertion_trait_set.id = f"{scv_accession}.{next(trait_set_counter)}"
            for i, t in enumerate(assertion_trait_set.traits):
                t.id = f"{scv_accession}.{i + 1}"

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
        for i, observation in enumerate(observations):
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
        _logger.info(f"submitted_variations: {submitted_variations}")

        obj = ClinicalAssertion(
            assertion_id=obj_id,
            title=extract(clinvar_submission, "@title"),
            local_key=extract(clinvar_submission, "@localKey"),
            assertion_accession=scv_accession,
            version=extract(raw_accession, "@Version"),
            assertion_type=extract(extract(inp, "Assertion"), "$"),
            date_created=sanitize_date(extract(inp, "@DateCreated")),
            date_last_updated=sanitize_date(extract(inp, "@DateLastUpdated")),
            submitted_assembly=extract(clinvar_submission, "@submittedAssembly"),
            record_status=extract(extract(inp, "RecordStatus"), "$"),
            review_status=extract(extract(inp, "ReviewStatus"), "$"),
            interpretation_date_last_evaluated=sanitize_date(
                extract(interpretation, "@DateLastEvaluated")
            ),
            interpretation_description=extract(
                extract(interpretation, "Description"), "$"
            ),
            submitter=submitter,
            submission=submission,
            clinical_assertion_observations=observations,
            clinical_assertion_trait_set=assertion_trait_set,
            content=inp,
        )
        return obj

    def disassemble(self):
        self_copy = model_copy(self)

        for subobj in self_copy.submitter.disassemble():
            yield subobj
        del self_copy.submitter

        for subobj in self_copy.submission.disassemble():
            yield subobj
        del self_copy.submission

        for obs in self_copy.clinical_assertion_observations:
            for subobj in obs.disassemble():
                yield subobj
        setattr(
            self_copy,
            "clinical_assertion_observation_ids",
            [obs.id for obs in self_copy.clinical_assertion_observations],
        )
        del self_copy.clinical_assertion_observations

        if self_copy.clinical_assertion_trait_set is not None:
            for subobj in self_copy.clinical_assertion_trait_set.disassemble():
                yield subobj
            setattr(
                self_copy,
                "clinical_assertion_trait_set_id",
                self_copy.clinical_assertion_trait_set.id,
            )
        del self_copy.clinical_assertion_trait_set

        yield self_copy


@dataclasses.dataclass
class Gene(Model):
    hgnc_id: str
    id: str
    symbol: str
    full_name: str

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return []

    def __post_init__(self):
        self.entity_type = "gene"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        raise NotImplementedError()

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
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.gene_id = self.gene.id
        self.entity_type = "gene_association"

    @staticmethod
    def from_xml(inp: dict):
        raise NotImplementedError()

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
    descendant_ids: List[str]
    child_ids: List[str]

    content: dict

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "clinical_assertion_variation"

    @staticmethod
    def extract_variations(inp: dict, assertion_accession: str):
        """
        Accepts a ClinicalAssertion XML dict. Returns a list of ClinicalAssertionVariation objects.
        """
        # descendant_tree = Variation.descendant_tree(inp)
        # descendant_ids = Variation.get_all_descendants(descendant_tree)
        # child_ids = Variation.get_all_children(descendant_tree)
        buffer = []

        class Counter:
            def __init__(self):
                self.counter = 0

            def get_and_increment(self):
                self.counter += 1
                return self.counter

            # # Dunder method for int conversion
            # def __int__(self):
            #     return self.counter

            # def __str__(self):
            #     return str(self.counter)

        counter = Counter()

        def extract_and_accumulate_descendants(inp: dict) -> List[Variation]:
            if "SimpleAllele" in inp:
                subclass_type = "SimpleAllele"
                inputs = ensure_list(extract(inp, "SimpleAllele"))
            elif "Haplotype" in inp:
                subclass_type = "Haplotype"
                inputs = ensure_list(extract(inp, "Haplotype"))
            elif "Genotype" in inp:
                subclass_type = "Genotype"
                inputs = [extract(inp, "Genotype")]
            else:
                return []
                # raise RuntimeError("Unknown variation type: " + json.dumps(inp))

            outputs = []
            for inp in inputs:
                variation = ClinicalAssertionVariation(
                    id=f"{assertion_accession}.{counter.get_and_increment()}",
                    clinical_assertion_id=assertion_accession,
                    variation_type=extract(inp, "VariantType", "$")
                    or extract(inp, "VariationType", "$"),
                    subclass_type=subclass_type,
                    descendant_ids=[],  # Fill in later
                    child_ids=[],  # Fill in later
                    content={},  # Fill in later
                )
                children = extract_and_accumulate_descendants(inp)
                variation.child_ids = [c.id for c in children]
                direct_children = variation.child_ids
                _logger.info(f"{direct_children=}")
                non_child_descendants = flatten1([c.child_ids or [] for c in children])
                _logger.info(f"{non_child_descendants=}")
                variation.descendant_ids = direct_children + non_child_descendants

                buffer.append(variation)
                outputs.append(variation)
            return outputs

        v = extract_and_accumulate_descendants(inp)
        _logger.info(f"extract_and_accumulate_descendants returned: {v}")
        assert len(v) <= 1, f"Expected 1 or fewer variations, got {len(v)}: {v}"
        return buffer

    # @staticmethod
    # def from_xml(inp: dict, assertion_accession: str):
    #     _logger.debug(f"ClinicalAssertionVariation.from_xml(inp={json.dumps(inp)})")
    #     # descendant_tree = Variation.descendant_tree(inp)
    #     # descendant_ids = Variation.get_all_descendants(descendant_tree)
    #     # child_ids = Variation.get_all_children(descendant_tree)

    #     if "SimpleAllele" in inp:
    #         subclass_type = "SimpleAllele"
    #         inp = extract(inp, "SimpleAllele")
    #     elif "Haplotype" in inp:
    #         subclass_type = "Haplotype"
    #         inp = extract(inp, "Haplotype")
    #     elif "Genotype" in inp:
    #         subclass_type = "Genotype"
    #         inp = extract(inp, "Genotype")
    #     else:
    #         raise RuntimeError("Unknown variation type: " + json.dumps(inp))

    #     assertion_accession = extract(inp, "@Accession")
    #     all_descendants = ClinicalAssertionVariation.extract_variations(
    #         inp, assertion_accession
    #     )

    def disassemble(self):
        # self_copy = model_copy(self)
        # yield self_copy
        # TODO
        raise NotImplementedError()


@dataclasses.dataclass
class Variation(Model):
    id: str
    name: str
    variation_type: str
    subclass_type: str
    allele_id: str
    protein_change: List[str]
    num_chromosomes: int
    num_copies: int
    gene_associations: List[GeneAssociation]

    content: dict

    child_ids: List[str]
    descendant_ids: List[str]

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "variation"

    @staticmethod
    def from_xml(inp: dict):
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
            num_copies=int_or_none(extract(inp, "@NumberOfCopies")),
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
                ),
                relationship_type=extract(g, "@RelationshipType"),
                content=g,
            )
            for g in ensure_list(extract(extract(inp, "GeneList"), "Gene") or [])
        ]
        return obj

    @staticmethod
    def descendant_tree(inp: dict):
        """
        Accepts xmltodict parsed XML for a SimpleAllele, Haplotype, or Genotype.
        Returns a tuple tree of child ids.

        (genotype_id,
            (haplotype_id1,
                (simpleallele_id11, None)
                (simpleallele_id12, None)))
            (haplotype_id2,
                (simpleallele_id21, None))
        """
        if "SimpleAllele" in inp:
            inp = inp["SimpleAllele"]
            return [inp["@VariationID"]]
        elif "Haplotype" in inp:
            inp = inp["Haplotype"]
            return [
                inp["@VariationID"],
                *[
                    Variation.descendant_tree({"SimpleAllele": simple_allele})
                    for simple_allele in ensure_list(inp["SimpleAllele"])
                ],
            ]
        elif "Genotype" in inp:
            inp = inp["Genotype"]
            if "SimpleAllele" in inp:
                return [
                    inp["@VariationID"],
                    *[
                        Variation.descendant_tree({"SimpleAllele": simpleAllele})
                        for simpleAllele in ensure_list(inp["SimpleAllele"])
                    ],
                ]
            else:
                return [
                    inp["@VariationID"],
                    *[
                        Variation.descendant_tree({"Haplotype": haplotype})
                        for haplotype in ensure_list(inp["Haplotype"])
                    ],
                ]
        else:
            raise RuntimeError("Unknown variation type: " + json.dumps(inp))

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
            for gaobj in ga.disassemble():
                yield gaobj


@dataclasses.dataclass
class RcvAccession(Model):
    independent_observations: int
    variation_id: int
    id: str
    variation_archive_id: int
    date_last_evaluated: str
    version: int
    title: str
    trait_set_id: str
    review_status: str
    interpretation: str

    content: dict

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "rcv_accession"

    @staticmethod
    def from_xml(
        inp: dict,
        variation_id: int = None,
        variation_archive_id=None,
    ) -> RcvAccession:
        """
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
        """
        # org.broadinstitute.monster.clinvar.parsers.VCV.scala : 259 : parseRawRcv

        # TODO take the TraitSetID as given in the XML. If missing, ignore it, don't
        # try to find the matching one in the ClinicalAssertion Traits
        trait_set_id = get(inp, "InterpretedConditionList", "@TraitSetID")

        # TODO independentObservations always null?
        obj = RcvAccession(
            independent_observations=extract(inp, "@independentObservations"),
            variation_id=variation_id,
            id=extract(inp, "@Accession"),
            variation_archive_id=variation_archive_id,
            date_last_evaluated=extract(inp, "@DateLastEvaluated"),
            version=int_or_none(extract(inp, "@Version")),
            title=extract(inp, "@Title"),
            trait_set_id=trait_set_id,
            review_status=extract(inp, "@ReviewStatus"),
            interpretation=extract(inp, "@Interpretation"),
            content=inp,
        )
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class VariationArchive(Model):
    id: str
    name: str
    version: str
    variation: Variation
    clinical_assertions: List[ClinicalAssertion]
    date_created: str
    record_status: str
    species: str
    review_status: str
    interp_description: str
    num_submitters: str
    num_submissions: str
    date_last_updated: str
    interp_type: str
    interp_explanation: str
    interp_date_last_evaluated: str
    interp_content: dict
    content: str

    trait_sets: List[TraitSet]
    trait_mappings: List[TraitMapping]

    rcv_accessions: List[RcvAccession]

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["content", "interp_content"]

    def __post_init__(self):
        self.variation_id = self.variation.id
        self.entity_type = "variation_archive"

    @staticmethod
    def from_xml(inp: dict):
        _logger.debug(f"VariationArchive.from_xml(inp={json.dumps(inp)})")
        interp_record = inp.get("InterpretedRecord", inp.get("IncludedRecord"))
        interpretations = extract(interp_record, "Interpretations")
        interpretation = interpretations["Interpretation"]
        variation = Variation.from_xml(interp_record)
        trait_mappings = [
            TraitMapping.from_xml(tm)
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
        trait_sets = [
            TraitSet.from_xml(ts)
            for ts in ensure_list(
                extract(
                    interpretation,
                    "ConditionList",
                    "TraitSet",
                )
                or []
            )
        ]
        _id = extract(inp, "@Accession")
        obj = VariationArchive(
            id=_id,
            name=extract(inp, "@VariationName"),
            version=extract(inp, "@Version"),
            variation=variation,
            clinical_assertions=[
                ClinicalAssertion.from_xml(
                    ca,
                    normalized_traits=flatten1([ts.traits for ts in trait_sets]),
                    trait_mappings=trait_mappings,
                )
                for ca in ensure_list(
                    extract(
                        extract(interp_record, "ClinicalAssertionList"),
                        "ClinicalAssertion",
                    )
                    or []
                )
            ],
            date_created=sanitize_date(extract(inp, "@DateCreated")),
            date_last_updated=sanitize_date(extract(inp, "@DateLastUpdated")),
            record_status=extract(extract(inp, "RecordStatus"), "$"),
            species=extract(extract(inp, "Species"), "$"),
            review_status=extract(extract(interp_record, "ReviewStatus"), "$"),
            interp_type=extract(interpretation, "@Type"),
            interp_description=extract(extract(interpretation, "Description"), "$"),
            interp_explanation=extract(extract(interpretation, "Explanation"), "$"),
            # num_submitters and num_submissions are at top and interp level
            num_submitters=int_or_none(extract(interpretation, "@NumberOfSubmitters")),
            num_submissions=int_or_none(
                extract(interpretation, "@NumberOfSubmissions")
            ),
            interp_date_last_evaluated=sanitize_date(
                extract(interpretation, "@DateLastEvaluated")
            ),
            trait_sets=trait_sets,
            trait_mappings=trait_mappings,
            rcv_accessions=[
                RcvAccession.from_xml(
                    r,
                    variation_id=variation.id,
                    variation_archive_id=_id,
                )
                for r in ensure_list(
                    extract(interp_record, "RCVList", "RCVAccession") or []
                )
            ],
            interp_content=interpretation,
            content=inp,
        )
        return obj

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

        yield self_copy
