"""
Data model for ClinVar Variation XML files.
"""

# TODO https://github.com/jpvanhal/inflection does good conversion
# between PascalCase and snake_case for entity_type. If Model names are
# reliable we could generate entity_type strings.

import json
import logging
from abc import ABCMeta, abstractmethod

from clinvar_ingest.utils import extract

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


class Variation(Model):
    def __init__(
        self,
        id: str,
        name: str,
        variation_type: str,
        subclass_type: str,
        content: dict = None,
    ):
        self.id = id
        self.name = name
        self.variation_type = variation_type
        self.subclass_type = subclass_type
        self.entity_type = "variation"
        self.content = content

    @staticmethod
    def from_xml(inp: dict):
        _logger.info(f"Variation.from_xml(inp={json.dumps(inp)})")
        if "SimpleAllele" in inp:
            subclass_type = "SimpleAllele"
            inp = inp["SimpleAllele"]
        elif "Haplotype" in inp:
            subclass_type = "Haplotype"
            inp = inp["Haplotype"]
        elif "Genotype" in inp:
            subclass_type = "Genotype"
            inp = inp["Genotype"]
        else:
            raise RuntimeError("Unknown variation type: " + json.dumps(inp))
        return Variation(
            id=extract(inp, "@VariationID"),
            name=extract(inp, "Name"),
            variation_type=extract(inp, "VariantType", "VariationType"),
            subclass_type=subclass_type,
            content=inp,
        )

    def disassemble(self):
        yield self


class VariationArchive(Model):
    def __init__(
        self,
        id: str,
        name: str,
        version: str,
        variation: Variation,
        content: dict = None,
    ):
        self.id = id
        self.name = name
        self.version = version
        self.variation = variation
        self.entity_type = "variation_archive"
        self.content = content

    @staticmethod
    def from_xml(inp: dict):
        _logger.info(f"VariationArchive.from_xml(inp={json.dumps(inp)})")
        return VariationArchive(
            id=extract(inp, "@Accession"),
            name=extract(inp, "@VariationName"),
            version=extract(inp, "@Version"),
            variation=Variation.from_xml(
                extract(inp, "InterpretedRecord", "IncludedRecord")
                # inp.get("InterpretedRecord", inp.get("IncludedRecord"))
            ),
            content=inp,
        )

    def disassemble(self):
        for val in self.variation.disassemble():
            yield val
        del self.variation
        yield self


def dictify(obj):
    """
    Recursively dictify Python objects into dicts. Objects may be Model instances.
    """
    _logger.debug(f"dictify(obj={obj})")
    if getattr(obj, "__slots__", None):
        return {k: getattr(obj, k, None) for k in obj.__slots__}
    else:
        if isinstance(obj, dict):
            return {k: dictify(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [dictify(v) for v in obj]
        elif isinstance(obj, Model):
            return dictify(vars(obj))
        else:
            return obj
