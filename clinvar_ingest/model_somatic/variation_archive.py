import dataclasses
import json
import logging
import re
from enum import StrEnum

from clinvar_ingest.model.common import Model, int_or_none, model_copy, sanitize_date
from clinvar_ingest.model.trait import (
    ClinicalAssertionTraitSet,
    Trait,
    TraitMapping,
    TraitSet,
)
from clinvar_ingest.model.variation_archive import (
    ClinicalAssertion,
    ClinicalAssertionObservation,
    ClinicalAssertionVariation,
    Submission,
    Submitter,
    Variation,
)
from clinvar_ingest.utils import ensure_list, extract, make_counter

_logger = logging.getLogger("clinvar_ingest")


class StatementType(StrEnum):
    GermlineClassification = "GermlineClassification"
    SomaticClinicalImpact = "SomaticClinicalImpact"
    OncogenicityClassification = "OncogenicityClassification"


@dataclasses.dataclass
class ClinicalAssertionSomatic(Model):
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
        self.entity_type = "clinical_assertion_somatic"

    @staticmethod
    def from_xml(
        inp: dict,
        normalized_traits: list[Trait] = [],
        trait_mappings: list[TraitMapping] = [],
        variation_id: str = None,
        variation_archive_id: str = None,
    ):
        _logger.debug(f"ClinicalAssertion.from_xml(inp={json.dumps(inp)})")
        obj_id = extract(inp, "@ID")
        raw_accession = extract(inp, "ClinVarAccession")
        scv_accession = extract(raw_accession, "@Accession")
        clinvar_submission = extract(inp, "ClinVarSubmissionID")
        classification_raw = extract(inp, "Classification")
        additional_submitters = [
            Submitter.from_xml(a, scv_accession)
            for a in ensure_list(
                extract(inp, "AdditionalSubmitters", "SubmitterDescription") or []
            )
        ]

        submitter = Submitter.from_xml(raw_accession, scv_accession)
        submitters = [submitter] + additional_submitters
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
        _logger.info(f"review_status: {review_status}")
        statement_type = None
        # The VariationArchiveClassification can be used to extract the few fields we care about
        _classification = VariationArchiveClassification.from_xml(classification_raw)
        if len(_classification) != 1:
            raise ValueError(
                f"Expected a single Classification node in SCV {scv_accession},"
                f" got: {_classification}"
            )
        _classification = _classification[0]
        statement_type = _classification.statement_type
        clincal_impact_assertion_type = _classification.clinical_impact_assertion_type
        clinical_impact_clinical_significance = (
            _classification.clinical_impact_clinical_significance
        )
        # In SCVs it's just the inner text, not a Description node like in VCVs
        interpretation_description = extract(classification_raw, "$")

        obj = ClinicalAssertionSomatic(
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
            interpretation_date_last_evaluated=sanitize_date(
                extract(classification_raw, "@DateLastEvaluated")
            ),
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
            clinical_impact_assertion_type=clincal_impact_assertion_type,
            clinical_impact_clinical_significance=clinical_impact_clinical_significance,
            content=inp,
        )
        return obj

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
                re.split(r"\.", self_copy.clinical_assertion_trait_set.id)[0],
            )
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
class RcvAccessionSomatic(Model):
    independent_observations: int
    variation_id: int
    id: str
    variation_archive_id: str
    date_last_evaluated: str
    version: int | None
    title: str
    trait_set_id: str
    review_status: str
    interpretation: str
    submission_count: int | None

    content: dict

    # New in Somatic Classification XML model
    statement_type: StatementType
    # Only for SomaticClinicalImpact
    clincal_impact_assertion_type: str | None
    clinical_impact_clinical_significance: str | None

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content"]

    def __post_init__(self):
        self.entity_type = "rcv_accession_somatic"

    # @staticmethod
    # def classifications_from_xml(rcv_classifications_raw: list[dict]) -> list[dict]:

    @staticmethod
    def from_xml(
        inp: dict,
        variation_id: int = None,
        variation_archive_id: str = None,
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

        # Ensure only 1 classification type is present by checking each StatementType
        statement_types: list[StatementType] = []
        for st in StatementType:
            if str(st) in rcv_classifications_raw:
                statement_types.append(st)
        if len(statement_types) > 1:
            raise ValueError(
                f"Expected a single StatementType node in RCVClassifications in RCV {rcv_id},"
                f" got: {statement_types}"
            )
        if len(statement_types) == 0:
            raise ValueError(
                f"Did not find a StatementType node in RCVClassifications in RCV {rcv_id}"
            )

        # extract classification information from the RCV Classification record
        statement_type = statement_types[0]
        classification_node = extract(rcv_classifications_raw, str(statement_type))
        description_node = extract(classification_node, "Description")

        # TODO independentObservations always null?
        obj = RcvAccessionSomatic(
            independent_observations=extract(inp, "@independentObservations"),
            variation_id=variation_id,
            id=rcv_id,
            variation_archive_id=variation_archive_id,
            date_last_evaluated=extract(description_node, "@DateLastEvaluated"),
            version=int_or_none(extract(inp, "@Version")),
            title=extract(inp, "@Title"),
            trait_set_id=extract(inp, "ClassifiedConditionList", "@TraitSetID"),
            review_status=extract(classification_node, "ReviewStatus", "$"),
            interpretation=extract(description_node, "$"),
            submission_count=int_or_none(extract(inp, "@SubmissionCount")),
            statement_type=statement_type,
            clincal_impact_assertion_type=extract(
                description_node, "@ClinicalImpactAssertionType"
            ),
            clinical_impact_clinical_significance=extract(
                description_node, "@ClinicalImpactClinicalSignificance"
            ),
            content=inp,
        )
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class VariationArchiveClassification(Model):
    # TODO Add VCV_ID as a field to link this back to the VariationArchive
    # maybe another name? Use a field name that exists elsewhere.
    statement_type: StatementType
    review_status: str

    num_submitters: int | None
    num_submissions: int | None
    date_created: str
    interp_description: str
    most_recent_submission: str

    # Only for SomaticClinicalImpact
    clinical_impact_assertion_type: str
    clinical_impact_clinical_significance: str

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return []

    def __post_init__(self):
        self.entity_type = "variation_archive_classification"

    @staticmethod
    def from_xml_single(inp: dict, statement_type: StatementType):
        """
        The input is a single Classification node contents.
        Either the value of a GermlineClassification, SomaticClinicalImpact,
        or OncogenicityClassification entry. The statement_type is the key
        from the original `Classifications` XML/dict, indicating the type.
        """
        interp_description = extract(inp, "Description")
        return VariationArchiveClassification(
            statement_type=statement_type,
            review_status=extract(inp, "ReviewStatus", "$"),
            num_submitters=int_or_none(extract(inp, "@NumberOfSubmitters")),
            num_submissions=int_or_none(extract(inp, "@NumberOfSubmissions")),
            date_created=sanitize_date(extract(inp, "@DateCreated")),
            interp_description=extract(interp_description, "$"),
            most_recent_submission=sanitize_date(extract(inp, "@MostRecentSubmission")),
            clinical_impact_assertion_type=extract(
                interp_description, "@ClinicalImpactAssertionType"
            ),
            clinical_impact_clinical_significance=extract(
                interp_description,
                "@ClinicalImpactClinicalSignificance",
            ),
        )

    @staticmethod
    def from_xml(inp: dict):
        outputs: list[VariationArchiveClassification] = []

        if "GermlineClassification" in inp:
            outputs.append(
                VariationArchiveClassification.from_xml_single(
                    inp["GermlineClassification"], StatementType.GermlineClassification
                )
            )
        if "SomaticClinicalImpact" in inp:
            outputs.append(
                VariationArchiveClassification.from_xml_single(
                    inp["SomaticClinicalImpact"], StatementType.SomaticClinicalImpact
                )
            )
        if "OncogenicityClassification" in inp:
            outputs.append(
                VariationArchiveClassification.from_xml_single(
                    inp["OncogenicityClassification"],
                    StatementType.OncogenicityClassification,
                )
            )

        return outputs

    def disassemble(self):
        yield self


@dataclasses.dataclass
class VariationArchiveSomatic(Model):
    id: str
    name: str
    version: str
    variation: Variation
    clinical_assertions: list[ClinicalAssertionSomatic]
    date_created: str
    record_status: str
    species: str
    num_submitters: int | None
    num_submissions: int | None
    date_last_updated: str
    content: dict

    trait_sets: list[TraitSet]
    trait_mappings: list[TraitMapping]

    rcv_accessions: list[RcvAccessionSomatic]
    classifications: list[VariationArchiveClassification]

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["content", "interp_content"]

    def __post_init__(self):
        self.variation_id = self.variation.id
        self.entity_type = "variation_archive_somatic"

    @staticmethod
    def from_xml(inp: dict):
        _logger.debug(f"VariationArchive.from_xml(inp={json.dumps(inp)})")
        vcv_accession = extract(inp, "@Accession")

        interp_record = inp.get("ClassifiedRecord", inp.get("IncludedRecord"))

        variation = Variation.from_xml(interp_record, vcv_accession)
        rcv_accessions = [
            RcvAccessionSomatic.from_xml(
                r, variation_id=variation.id, variation_archive_id=vcv_accession
            )
            for r in ensure_list(
                extract(interp_record, "RCVList", "RCVAccession") or []
            )
        ]
        # TODO update ClinicalAssertion to include Classification
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
        # trait_set_id_to_rcv_id = {r.trait_set_id: r.id for r in rcv_accessions}
        # trait_sets = [
        #     TraitSet.from_xml(ts, trait_set_id_to_rcv_id[get(ts, "@ID")])
        #     for ts in ensure_list(
        #         extract(
        #             interpretation,
        #             "ConditionList",
        #             "TraitSet",
        #         )
        #         or []
        #     )
        # ]

        # TODO Classifications
        # Classifications is a single node containing multiple Classification subclass nodes
        # e.g. "Classifications": {
        #  "GermlineClassification": {...},
        #  "SomaticClinicalImpact": {...}}
        classifications_xml = extract(interp_record, "Classifications")
        classifications = VariationArchiveClassification.from_xml(classifications_xml)

        obj = VariationArchiveSomatic(
            id=vcv_accession,
            name=extract(inp, "@VariationName"),
            version=extract(inp, "@Version"),
            variation=variation,
            clinical_assertions=[
                ClinicalAssertionSomatic.from_xml(
                    ca,
                    # TODO
                    normalized_traits=[],  # flatten1([ts.traits for ts in trait_sets]),
                    trait_mappings=trait_mappings,
                    variation_id=variation.id,
                    variation_archive_id=vcv_accession,
                )
                for ca in raw_clinical_assertions
            ],
            date_created=sanitize_date(extract(inp, "@DateCreated")),
            date_last_updated=sanitize_date(extract(inp, "@DateLastUpdated")),
            record_status=extract(extract(inp, "RecordStatus"), "$"),
            species=extract(extract(inp, "Species"), "$"),
            num_submitters=int_or_none(extract(inp, "@NumberOfSubmitters")),
            num_submissions=int_or_none(extract(inp, "@NumberOfSubmissions")),
            # trait_sets=trait_sets,
            trait_sets=[],
            # trait_mappings=trait_mappings,
            trait_mappings=[],
            rcv_accessions=rcv_accessions,
            classifications=classifications,
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

        # TODO classifications
        for classification in self_copy.classifications:
            for sub_obj in classification.disassemble():
                yield sub_obj
        del self_copy.classifications

        yield self_copy
