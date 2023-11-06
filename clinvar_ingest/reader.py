import xmltodict
import threading
import queue
import logging

import clinvar_ingest.model as model

logger = logging.getLogger(__name__)


def construct_model(tag, item):
    logger.debug(f"construct_model: {tag=}, {item=}")
    if tag == "VariationArchive":
        logger.debug("Returning new VariationArchive")
        return model.VariationArchive.from_xml(item)


def make_item_cb(output_queue):
    def item_cb(tag, item):
        logger.info(type(tag))
        logger.info(f"{tag=}")
        current_tag = tag[-1]
        tagname = current_tag[0]
        attributes = current_tag[1]
        logger.info(f"tagname: {tagname}")
        logger.info(f"attributes: {attributes}")
        # print(f"item: {item}")
        for attr_k, attr_v in attributes.items():
            item[attr_k] = attr_v
        obj = construct_model(tagname, item)
        if obj is not None:
            output_queue.put(obj)
        return True

    return item_cb


def _parse(file, item_cb, output_queue):
    xmltodict.parse(file, item_depth=2, item_callback=item_cb)
    output_queue.put(None)


def read_clinvar_xml(file):
    """
    Generator function that reads a ClinVar Variation XML file and outputs objects
    """
    logger.info(f"{file=}")
    output_queue = queue.Queue()
    parser_thread = threading.Thread(
        target=_parse,
        name="ParserThread",
        args=(file, make_item_cb(output_queue), output_queue),
    )
    parser_thread.start()

    while True:
        obj = output_queue.get()
        logger.info(f"Got object from output queue: {str(obj)}")
        if obj is None:
            break

        for subobj in obj.disassemble():
            yield subobj

        # yield obj

    parser_thread.join()
