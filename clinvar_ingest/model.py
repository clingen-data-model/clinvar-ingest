"""
Data model for ClinVar Variation XML files.
"""

# TODO https://github.com/jpvanhal/inflection does good conversion
# between PascalCase and snake_case for entity_type. If Model names are
# reliable we could generate entity_type strings.

import dataclasses
import json
import logging
from abc import ABCMeta, abstractmethod
from typing import List, Union

from clinvar_ingest.utils import ensure_list, extract, extract_in, extract_oneof, get

_logger = logging.getLogger(__name__)


class Model(object, metaclass=ABCMeta):

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        """
        Constructs an instance of this class using the XML structure parsed into a dict.

        The strategy of passing the input may differ based on the case. e.g. variation, the
        type is based on the tag (SimpleAllele|Haplotype|Genotype), so the tag is included
        above the value passed as {type: value}. For others, the tag may be unnecessary and
        only the content+attributes are based.
        """
        raise NotImplementedError()

    @abstractmethod
    def disassemble(self):
        """
        Decomposes this instance into instances of contained Model classes, and itself.
        An object referred to by another will be returned before the other.
        """
        raise NotImplementedError()


@dataclasses.dataclass
class Submitter(Model):
    id: str
    release_date: str
    current_name: str
    current_abbrev: str
    all_names: List[str]
    all_abbrevs: List[str]
    org_category: str
    content: dict

    def __post_init__(self):
        self.entity_type = "submitter"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.info(
            f"Submitter.from_xml(inp={json.dumps(inp)}, {jsonify_content=})"
        )
        current_name = extract(inp, "@SubmitterName")
        current_abbrev = extract(inp, "@OrgAbbreviation")
        obj = Submitter(
            id=extract(inp, "@OrgID"),
            current_name=current_name,
            current_abbrev=current_abbrev,
            release_date="",  # TODO - Fix
            org_category=extract(inp, "@OrganizationCategory"),
            all_names=[] if not current_name else [current_name],
            all_abbrevs=[] if not current_abbrev else [current_abbrev],
            content=inp
        )
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class Submission(Model):
    id: str
    submitter_id: str
    release_date: str
    additional_submitter_ids: List[str]
    submission_date: str
    content: dict

    def __post_init__(self):
        self.entity_type = "submission"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True, submitter: Submitter = {}, additional_submitters: list = [Submitter]):
        _logger.info(
            f"Submission.from_xml(inp={json.dumps(inp)}, {jsonify_content=}, {submitter=}, "
            f"{additional_submitters=})"
        )
        obj = Submission(
            id=f"{submitter.id}",  # TODO - FIX w/ Date
            release_date=submitter.release_date,
            submitter_id=submitter.id,
            additional_submitter_ids=list(filter('id', additional_submitters)),
            submission_date=extract(inp, "@SubmissionDate"),
            content=inp
        )
        return obj

    def disassemble(self):
        yield self


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

    def __post_init__(self):
        self.entity_type = "clinical_assertion"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.info(
            f"ClinicalAssertion.from_xml(inp={json.dumps(inp)}, {jsonify_content=})"
        )
        raw_accession = extract(inp, "ClinVarAccession")
        clinvar_submission = extract(inp, "ClinVarSubmissionID")
        interpretation = extract(inp, "Interpretation")
        additional_submitters = list(
            map(
                Submitter.from_xml,
                ensure_list(extract_in(raw_accession, 'AdditionalSubmitters', 'SubmitterDescription') or [])
            )
        )
        submitter = Submitter.from_xml(raw_accession)
        submission = Submission.from_xml(inp, jsonify_content, submitter, additional_submitters)
        obj = ClinicalAssertion(
            assertion_id=extract(inp, "@ID"),
            title=extract(clinvar_submission, "@title"),
            local_key=extract(clinvar_submission, "@localKey"),
            assertion_accession=extract(raw_accession, "@Accession"),
            version=extract(raw_accession, "@Version"),
            assertion_type=extract(extract(inp, "Assertion"), "$"),
            date_created=extract(inp, "@DateCreated"),
            date_last_updated=extract(inp, "@DateLastUpdated"),
            submitted_assembly=extract(clinvar_submission, "@submittedAssembly"),
            record_status=extract(extract(inp, "RecordStatus"), "$"),
            review_status=extract(extract(inp, "ReviewStatus"), "$"),
            interpretation_date_last_evaluated=extract(interpretation, "@DateLastEvaluated"),
            interpretation_description=extract(extract(interpretation, "Description"), "$"),
            submitter=submitter,
            submission=submission,
            content=inp
        )
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class Gene(Model):
    hgnc_id: str
    id: str
    symbol: str
    full_name: str

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

    def __post_init__(self):
        self.gene_id = self.gene.id
        self.entity_type = "gene_association"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        raise NotImplementedError()

    def disassemble(self):
        self_copy = model_copy(self)
        yield self_copy.gene
        del self_copy.gene
        yield self_copy


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

    def __post_init__(self):
        self.entity_type = "variation"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.info(f"Variation.from_xml(inp={json.dumps(inp)}, {jsonify_content=})")
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
            # VariationID is at the VariationArchive and the SimpleAllele/Haplotype/Genotype level
            id=extract(inp, "@VariationID"),
            name=extract(extract(inp, "Name"), "$"),
            variation_type=extract(
                extract_oneof(inp, "VariantType", "VariationType")[1], "$"
            ),
            subclass_type=subclass_type,
            allele_id=extract_in(inp, "@AlleleID"),
            protein_change=ensure_list(extract_in(inp, "ProteinChange") or []),
            num_copies=int_or_none(extract_in(inp, "@NumberOfCopies")),
            num_chromosomes=int_or_none(extract_in(inp, "@NumberOfChromosomes")),
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

        if jsonify_content:
            obj.content = json.dumps(inp)
            for ga in obj.gene_associations:
                ga.content = json.dumps(ga.content)
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
        print(f"{child_ids=}, {grandchildren=}")
        return child_ids + grandchildren

    @staticmethod
    def get_all_children(descendant_tree: tuple):
        """
        Accepts a descendant_tree. Returns the first level children.
        """
        if descendant_tree is None or len(descendant_tree) == 0:
            return []
        _, *children = descendant_tree
        return [child[0] for child in children]

    def disassemble(self):
        self_copy = model_copy(self)
        for ga in self_copy.gene_associations:
            for gaobj in ga.disassemble():
                yield gaobj
        del self_copy.gene_associations
        yield self_copy


@dataclasses.dataclass
class Trait(Model):
    id: str
    disease_mechanism_id: int
    name: str
    attribute_content: List[str]
    mode_of_inheritance: str
    release_date: str
    ghr_links: str
    keywords: List[str]
    gard_id: int
    medgen_id: str
    public_definition: str
    type: str
    symbol: str
    disease_mechanism: str
    alternate_symbols: List[str]
    gene_reviews_short: str
    alternate_names: List[str]
    xrefs: List[str]

    content: str

    class XRef:
        def __init__(
            self,
            db: str,
            id: str,
            type: str,
            ref_field: str = None,
            ref_field_element: str = None,
        ):
            """
            ref_field and ref_field_element are used to differentiate between XRefs which may
            apply to similar values but which originate from different nodes in the XML.

            ref_field: the field name which differentiates this XRef
            ref_field_element: the field value which differentiates this XRef

            e.g. ref_field="alternate_symbol", ref_field_element="MC1DN26"
            """
            self.db = db
            self.id = id
            self.type = type
            self.ref_field = ref_field
            self.ref_field_element = ref_field_element

    def __post_init__(self):
        self.entity_type = "trait"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.info(f"Trait.from_xml(inp={json.dumps(inp)})")
        id = extract(inp, "@ID")
        names = ensure_list(extract(inp, "Name") or [])
        preferred_names = [
            n for n in names if get(n, "ElementValue", "@Type") == "Preferred"
        ]
        if len(preferred_names) > 1:
            raise RuntimeError(f"Trait {id} has multiple preferred names")
        preferred_name = None
        if len(preferred_names) == 1:
            preferred_name = preferred_names[0]["ElementValue"]["$"]

        alternate_names = [
            n for n in names if get(n, "ElementValue", "@Type") == "Alternate"
        ]
        alternate_name_strs = [get(n, "ElementValue", "$") for n in alternate_names]

        # XRefs which are within Name objects (type=Preferred)
        preferred_name_xrefs = [
            Trait.XRef(
                db=n["XRef"]["@DB"],
                id=n["XRef"]["@ID"],
                type=get(n, "XRef", "@Type"),
                ref_field="name",
                ref_field_element=n["ElementValue"]["$"],
            )
            for n in preferred_names
            if "XRef" in n
        ]
        _logger.info(
            "preferred_name_xrefs: " + json.dumps(dictify(preferred_name_xrefs))
        )
        # XRefs which are within Name objects (type=Alternate)
        alternate_name_xrefs = [
            Trait.XRef(
                db=n["XRef"]["@DB"],
                id=n["XRef"]["@ID"],
                type=get(n, "XRef", "@Type"),
                ref_field="alternate_names",
                ref_field_element=n["ElementValue"]["$"],
            )
            for n in alternate_names
            if "XRef" in n
        ]
        _logger.info(
            "alternate_name_xrefs: " + json.dumps(dictify(alternate_name_xrefs))
        )

        # XRefs from Trait AttributeSet
        attribute_set_key_map = {
            "public definition": "public_definition",
            "GARD ID": "gard_id",
            "keyword": "keywords",
            "disease mechanism": "disease_mechanism",
            "mode of inheritance": "mode_of_inheritance",
            "GeneReviews short": "gene_reviews_short",
            "Genetics Home Reference (GHR) links": "ghr_links",
        }
        attribute_set_xrefs = []
        # Map of attribute key (values in attribute_set_key_map) to their values
        parsed_attributes = {}
        attribute_set = ensure_list(inp.get("AttributeSet", {}))
        for attr_name, attr_key in attribute_set_key_map.items():
            filtered_attrs = [
                attr
                for attr in attribute_set
                if get(attr, "Attribute", "@Type") == attr_name
            ]
            for f_attr in filtered_attrs:
                for xref in ensure_list(f_attr.get("XRef", [])):
                    attribute_set_xrefs.append(
                        Trait.XRef(
                            db=xref.get("@DB"),
                            id=xref.get("@ID"),
                            type=xref.get("@Type", None),
                            ref_field=attr_key,
                            ref_field_element=get(f_attr, "Attribute", "$"),
                        )
                    )
        _logger.info("attribute_set_xrefs: " + json.dumps(dictify(attribute_set_xrefs)))

        top_xrefs = [
            # XRefs which are at the top level of Trait objects
            Trait.XRef(
                db=x["@DB"],
                id=x["@ID"],
                type=get(x, "@Type"),
                ref_field=None,
                ref_field_element=None,
            )
            for x in ensure_list(inp.get("XRef", None) or [])
        ]
        _logger.info("top_xrefs: " + json.dumps(dictify(top_xrefs)))

        obj = Trait(
            id=id,
            disease_mechanism_id=None,
            name=preferred_name,
            attribute_content=None,
            mode_of_inheritance=None,
            release_date=None,
            ghr_links=None,
            keywords=None,
            gard_id=None,
            medgen_id=None,
            public_definition=None,
            type=None,
            symbol=None,
            disease_mechanism=None,
            alternate_symbols=None,
            gene_reviews_short=None,
            alternate_names=alternate_name_strs,
            xrefs=None,
            content=None,
        )
        if jsonify_content:
            obj.content = json.dumps(inp)
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class TraitSet(Model):
    id: str
    type: str
    traits: List[Trait]

    content: str

    def __post_init__(self):
        self.trait_ids = [t.id for t in self.traits]
        self.entity_type = "trait_set"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.info(f"TraitSet.from_xml(inp={json.dumps(inp)})")
        obj = TraitSet(
            id=extract(inp, "@ID"),
            type=extract(inp, "@Type"),
            traits=[
                Trait.from_xml(t, jsonify_content=jsonify_content)
                for t in ensure_list(extract(inp, "Trait"))
            ],
            content=inp,
        )
        if jsonify_content:
            obj.content = json.dumps(inp)
            for t in obj.traits:
                t.content = json.dumps(t.content)
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class TraitMapping(Model):
    clinical_assertion_id: str
    trait_type: str
    mapping_type: str
    mapping_value: str
    mapping_ref: str
    medgen_name: str
    medgen_id: str

    def __post_init__(self):
        self.entity_type = "trait_mapping"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        return TraitMapping(
            clinical_assertion_id=extract(inp, "@ClinicalAssertionID"),
            trait_type=extract(inp, "@TraitType"),
            mapping_type=extract(inp, "@MappingType"),
            mapping_value=extract(inp, "@MappingValue"),
            mapping_ref=extract(inp, "@MappingRef"),
            medgen_name=extract_in(inp, "MedGen", "@Name"),
            medgen_id=extract_in(inp, "MedGen", "@CUI"),
        )

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

    def __post_init__(self):
        self.variation_id = self.variation.id
        self.entity_type = "variation_archive"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.info(
            f"VariationArchive.from_xml(inp={json.dumps(inp)}, {jsonify_content=})"
        )
        interp_record = inp.get("InterpretedRecord", inp.get("IncludedRecord"))
        interp = extract(interp_record, "Interpretations")["Interpretation"]
        obj = VariationArchive(
            id=extract(inp, "@Accession"),
            name=extract(inp, "@VariationName"),
            version=extract(inp, "@Version"),
            variation=Variation.from_xml(
                interp_record, jsonify_content=jsonify_content
            ),
            clinical_assertions=list(
                map(
                    ClinicalAssertion.from_xml,
                    extract(extract(interp_record, "ClinicalAssertionList"), "ClinicalAssertion"),
                )
            ),
            date_created=extract(inp, "@DateCreated"),
            date_last_updated=extract(inp, "@DateLastUpdated"),
            record_status=extract(extract(inp, "RecordStatus"), "$"),
            species=extract(extract(inp, "Species"), "$"),
            review_status=extract(extract(interp_record, "ReviewStatus"), "$"),
            interp_type=extract_in(interp, "@Type"),
            interp_description=extract(extract_in(interp, "Description"), "$"),
            interp_explanation=extract_in(extract_in(interp, "Explanation"), "$"),
            # num_submitters and num_submissions are at top and interp level
            num_submitters=int_or_none(extract_in(interp, "@NumberOfSubmitters")),
            num_submissions=int_or_none(extract_in(interp, "@NumberOfSubmissions")),
            interp_date_last_evaluated=extract_in(interp, "@DateLastEvaluated"),
            trait_sets=[
                TraitSet.from_xml(ts, jsonify_content=jsonify_content)
                for ts in ensure_list(
                    extract_in(
                        interp_record,
                        "Interpretations",
                        "Interpretation",
                        "ConditionList",
                        "TraitSet",
                    )
                )
            ],
            interp_content=interp,
            content=inp,
        )
        if jsonify_content:
            obj.content = json.dumps(inp)
            obj.interp_content = json.dumps(interp)
        return obj

    def disassemble(self):
        self_copy = model_copy(self)
        for val in self_copy.variation.disassemble():
            yield val
        for clinical_assertion in self.clinical_assertions:
            for sub_obj in clinical_assertion.disassemble():
                yield sub_obj
        del self_copy.clinical_assertions
        del self_copy.variation
        yield self_copy


def model_copy(obj):
    """
    Create a copy of the given object. Significantly faster than copy.deepcopy().

    Args:
        obj: The object to be copied.

    Returns:
        A new instance of the same class as the input object, with the same attribute values.

    Example:
        >>> class Foo:
        ...     def __init__(self, a, b):
        ...         self.a = a
        ...         self.b = b
        >>> foo = Foo(1, 2)
        >>> foo_copy = model_copy(foo)
        >>> foo_copy.a
        1
        >>> foo_copy.b
        2
        >>> foo_copy.a = 3
        >>> foo_copy.a
        3
        >>> foo.a
        1
    """
    cls = type(obj)
    fields = dataclasses.fields(cls)
    kwargs = {f.name: getattr(obj, f.name) for f in fields}
    return cls(**kwargs)


def int_or_none(s: Union[str, None]) -> Union[int, None]:
    if s is None:
        return None
    return int(s)


def dictify(obj):
    """
    Recursively dictify Python objects into dicts. Objects may be Model instances.
    """
    if getattr(obj, "__slots__", None):
        return {k: getattr(obj, k, None) for k in obj.__slots__}
    if isinstance(obj, dict):
        return {k: dictify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [dictify(v) for v in obj]
    # Replaced isinstance(obj, Model) with this because of interactive class reloading
    if getattr(obj, "__dict__", None):
        return dictify(vars(obj))
    return obj
