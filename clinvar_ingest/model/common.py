import dataclasses
import logging
import re
from abc import ABCMeta, abstractmethod
from typing import Any

_logger = logging.getLogger("clinvar_ingest")


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

    @staticmethod
    @abstractmethod
    def jsonifiable_fields() -> list[str]:
        """
        List of field names which can be serialized to JSON upon the object's serialization
        for output. Field names which map to lists of objects will have each item in the
        list serialized to JSON individually, and the list will remain a list.

        Example:
            >>> class Foo(Model):
            ...     def __init__(self, a, b, c):
            ...         self.a = a
            ...         self.b = b
            ...         self.c = c
            ...     @staticmethod
            ...     def jsonifiable_fields():
            ...         return ["a", "c"]
            >>> foo = Foo(1, 2, 3)
            >>> foo.a = {"x": 1}
            >>> foo.b = {"y": 2}
            >>> foo.c = [{"z1": 3}, {"z2": 4}]
            >>> foo_dict = dictify(foo)
            >>> foo_dict["a"]
            '{"x": 1}'
            >>> foo_dict["b"]
            {'y': 2}
            >>> foo_dict["c"]
            ['{"z1": 3}', '{"z2": 4}']
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__dict__.__repr__()})"


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


def int_or_none(s: str | None) -> int | None:
    if s is None:
        return None
    return int(s)


def sanitize_date(s: str) -> str:
    """
    Parse a string which starts with a valid date in format YYYY-MM-DD.

    This function is permissive and discards trailing input because ClinVar has
    some dates like '2018-06-21-05:00'

    See: https://github.com/clingen-data-model/clinvar-ingest/issues/99
    """
    if not s:
        return s
    pattern_str = r"^(\d{4}-\d{2}-\d{2})"
    date_pattern = re.compile(pattern_str)
    match = date_pattern.match(s)
    if match:
        if match.span()[1] != len(s):
            _logger.warning(
                f"Trailing content trimmed from date."
                f" Date {match.group(1)} was followed by {s[match.span()[1]:]}"
            )
        return match.group(1)
    else:
        raise ValueError(f"Invalid date: {s}, must match {pattern_str}")


def dictify(
    obj,
) -> dict | list[dict | Any]:  # recursive type truncated at 2nd level
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
