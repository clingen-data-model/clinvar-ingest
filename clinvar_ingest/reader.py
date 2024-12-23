"""
Module for iterating over XML files and calling Model constructors
on appropriate elements.
"""

import logging
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from enum import StrEnum
from typing import Any, TextIO

import xmltodict

from clinvar_ingest.model.common import Model
from clinvar_ingest.model.rcv import RcvMapping
from clinvar_ingest.model.variation_archive import VariationArchive

_logger = logging.getLogger("clinvar_ingest")

QUEUE_STOP_VALUE = -1


def construct_model(tag, item):
    _logger.debug(f"construct_model: {tag=}, {item=}")
    if tag == "VariationArchive":
        _logger.debug("Returning new VariationArchive")
        return VariationArchive.from_xml(item)
    if tag == "ClinVarSet":
        _logger.debug("Returning new ClinVarSet")
        return RcvMapping.from_xml(item)
    raise ValueError(f"Unexpected tag: {tag} {item=}")


def make_item_cb(output_queue, keep_going):
    def item_cb(tag, item):
        if not keep_going["value"]:
            return False
        _logger.debug(type(tag))
        _logger.debug(f"{tag=}")
        current_tag = tag[-1]
        tagname = current_tag[0]
        attributes = current_tag[1]
        _logger.debug(f"tagname: {tagname}")
        _logger.debug(f"attributes: {attributes}")
        for attr_k, attr_v in attributes.items():
            item[attr_k] = attr_v
        obj = construct_model(tagname, item)
        if obj is not None:
            output_queue.put(obj)
        return True

    return item_cb


def _parse(file, item_cb, output_queue, depth=2):
    """
    Parsing target. Loads entire file, calls `item_cb` on every object at `depth`
    """
    xmltodict.parse(file, item_depth=depth, item_callback=item_cb)
    output_queue.put(QUEUE_STOP_VALUE)


class ElementTreeEvent(StrEnum):
    """
    Enum for ElementTree events
    """

    START = "start"
    END = "end"
    START_NS = "start-ns"
    END_NS = "end-ns"


def get_clinvar_rcv_xml_releaseinfo(file) -> dict:
    """
    Parses top level release info from RCV XML file.
    Returns dict with {"release_date"} key.
    """
    release_date = None
    _logger.debug(f"{file=}")
    for event, elem in ET.iterparse(
        file, events=[ElementTreeEvent.START, ElementTreeEvent.END]
    ):
        if event == ElementTreeEvent.START and elem.tag == "ReleaseSet":
            release_date = elem.attrib["Dated"]
        else:
            break
    if release_date is None:
        raise ValueError("Root element ReleaseSet not found!")
    return {"release_date": release_date}


def get_clinvar_vcv_xml_releaseinfo(file) -> dict:
    """
    Parses top level release info from file.
    Returns dict with {"release_date"} key.
    """
    release_date = None
    _logger.debug(f"{file=}")
    for event, elem in ET.iterparse(
        file, events=[ElementTreeEvent.START, ElementTreeEvent.END]
    ):
        if event == ElementTreeEvent.START and elem.tag == "ClinVarVariationRelease":
            release_date = elem.attrib["ReleaseDate"]
        else:
            break
    if release_date is None:
        raise ValueError("Root element ClinVarVariationRelease not found!")
    return {"release_date": release_date}


def _handle_text_nodes(path, key, value) -> tuple[Any, Any]:  # noqa: ARG001
    """
    Takes a path, key, value, returns a tuple of new (key, value)

    If the value looks like an XML text node, put it in a key "$".

    Used as a postprocessor for xmltodict.parse.
    """
    if isinstance(value, str) and not key.startswith("@"):
        if key == "#text":
            return ("$", value)
        return (key, {"$": value})
    return (key, value)


def _parse_xml_document(doc_str: str | bytes):
    """
    Reads an XML document from a string.
    """
    return xmltodict.parse(doc_str, postprocessor=_handle_text_nodes)


def read_clinvar_rcv_xml(reader: TextIO, disassemble=True) -> Iterator[Model]:
    tag_we_care_about = "ClinVarSet"
    return _read_clinvar_xml(reader, tag_we_care_about, disassemble)


def read_clinvar_vcv_xml(reader: TextIO, disassemble=True) -> Iterator[Model]:
    tag_we_care_about = "VariationArchive"
    return _read_clinvar_xml(reader, tag_we_care_about, disassemble)


def _read_clinvar_xml(
    reader: TextIO, tag_we_care_about: str, disassemble=True
) -> Iterator[Model]:
    """
    Generator function that reads a ClinVar Variation XML file and outputs objects.
    Accepts `reader` as a readable TextIO/BytesIO object, or a filename.
    """
    unclosed = 0
    for event, elem in ET.iterparse(reader, events=["start", "end"]):
        # https://docs.python.org/3/library/xml.etree.elementtree.html#element-objects
        # tag text attrib

        # Sanity checks to make sure we only parse at the correct depth.
        # For depth=1 (first level inside the root, unclosed should == 1)
        # ET sends an end event for self-closed tags like e.g. <br/>, so this should work.
        if event == "start":
            unclosed += 1
        elif event == "end":
            unclosed -= 1
        else:
            raise ValueError(f"Unexpected event: {event}. Element: {ET.tostring(elem)}")

        if event == "end" and elem.tag == tag_we_care_about:
            if unclosed != 1:
                _logger.warning(
                    f"Found a {tag_we_care_about} at a depth other than 1:"
                    f" {unclosed}, element: {ET.tostring(elem)}"
                )
            else:
                elem_d = _parse_xml_document(ET.tostring(elem))
                if not isinstance(elem_d, dict):
                    raise RuntimeError(
                        f"xmltodict returned non-dict type: ({type(elem_d)}) {elem_d}"
                    )
                if len(elem_d.keys()) > 1:
                    raise RuntimeError(
                        f"parsed dict had more than 1 key: ({elem_d.keys()}) {elem_d}"
                    )
                tag, contents = next(iter(elem_d.items()))
                model_obj = construct_model(tag, contents)
                if disassemble:
                    yield from model_obj.disassemble()
                else:
                    yield model_obj
            elem.clear()
