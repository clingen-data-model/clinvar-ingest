from abc import ABCMeta, abstractmethod
import logging
import json

_logger = logging.getLogger(__name__)


class Model(object, metaclass=ABCMeta):
    @staticmethod
    def from_xml(inp: dict):
        """
        Constructs an instance of this class using the XML structure parsed into a dict.

        """
        raise NotImplementedError()

    @abstractmethod
    def disassemble(self):
        raise NotImplementedError()


class Variation(Model):
    def __init__(self, id: str, name: str, variation_type: str, subclass_type: str):
        self.id = id
        self.name = name
        self.variation_type = variation_type
        self.subclass_type = subclass_type
        self.entity_type = "Variation"

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
            id=inp["@VariationID"],
            name=inp["Name"],
            variation_type=inp.get("VariantType", inp.get("VariationType")),
            subclass_type=subclass_type,
        )

    def disassemble(self):
        yield self


class VariationArchive(Model):
    def __init__(self, id: str, name: str, version: str, variation: Variation):
        self.id = id
        self.name = name
        self.version = version
        self.variation = variation
        self.entity_type = "VariationArchive"

    @staticmethod
    def from_xml(inp: dict):
        _logger.info(f"VariationArchive.from_xml(inp={json.dumps(inp)})")
        return VariationArchive(
            id=inp["Accession"],
            name=inp["VariationName"],
            version=inp["Version"],
            variation=Variation.from_xml(
                inp.get("InterpretedRecord", inp.get("IncludedRecord"))
            ),
        )

    def disassemble(self):
        for val in self.variation.disassemble():
            yield val
        del self.variation
        yield self


def dictify(obj):
    _logger.info(f"dictify(obj={obj})")
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
