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

from clinvar_ingest.utils import extract, extract_in, extract_oneof

_logger = logging.getLogger(__name__)


class Model(object, metaclass=ABCMeta):
    @staticmethod
    def from_xml(inp: dict):
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
class Variation(Model):
    id: str
    name: str
    variation_type: str
    subclass_type: str
    content: dict

    def __post_init__(self):
        self.entity_type = "variation"

    @staticmethod
    def from_xml(inp: dict):
        _logger.info(f"Variation.from_xml(inp={json.dumps(inp)})")
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
        return Variation(
            # VariationID is at the VariationArchive and the SimpleAllele/Haplotype/Genotype level
            id=extract(inp, "@VariationID"),
            name=extract(inp, "Name"),
            variation_type=extract_oneof(inp, "VariantType", "VariationType")[1],
            subclass_type=subclass_type,
            content=inp,
        )

    def disassemble(self):
        yield self


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
    content: dict

    def __post_init__(self):
        self.entity_type = "variation_archive"
        self.variation_id = self.variation.id
        self.interp_content = None

    @staticmethod
    def from_xml(inp: dict):
        _logger.info(f"VariationArchive.from_xml(inp={json.dumps(inp)})")
        interp_record = inp.get("InterpretedRecord", inp.get("IncludedRecord"))
        interp = interp_record.get("Interpretations", {}).get("Interpretation", {})
        return VariationArchive(
            id=extract(inp, "@Accession"),
            name=extract(inp, "@VariationName"),
            version=extract(inp, "@Version"),
            variation=Variation.from_xml(interp_record),
            date_created=extract(inp, "@DateCreated"),
            date_last_updated=extract(inp, "@DateLastUpdated"),
            record_status=extract(inp, "RecordStatus"),
            species=extract(inp, "Species"),
            review_status=extract(interp_record, "ReviewStatus"),
            interp_type=extract_in(interp, "@Type"),
            interp_description=extract_in(interp, "Description"),
            interp_explanation=extract_in(extract_in(interp, "Explanation"), "#text"),
            # num_submitters and num_submissions are at top and interp level
            num_submitters=extract_in(interp, "@NumberOfSubmitters"),
            num_submissions=extract_in(interp, "@NumberOfSubmissions"),
            interp_date_last_evaluated=extract_in(interp, "@DateLastEvaluated"),
            content=inp,
        )

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
