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

from clinvar_ingest.utils import ensure_list, extract, extract_in, extract_oneof

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
        _logger.info(f"Variation.from_xml(inp={json.dumps(inp)})")
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
            for g in ensure_list(extract(extract(inp, "GeneList"), "Gene"))
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
class VariationArchive(Model):
    id: str
    name: str
    version: str
    variation: Variation
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
