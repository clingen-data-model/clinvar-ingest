import logging

_logger = logging.getLogger(__name__)


class VariationArchive(object):
    __slots__ = ["id", "name"]

    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name

    @staticmethod
    def from_xml(inp: dict):
        return VariationArchive(id=inp["VariationID"], name=inp["VariationName"])


def dictify_slot_class(obj):
    return {k: getattr(obj, k, None) for k in obj.__slots__}
