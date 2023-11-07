import xmltodict
import threading
import queue
import logging

import clinvar_ingest.model as model

logger = logging.getLogger(__name__)

QUEUE_STOP_VALUE = -1


def construct_model(tag, item):
    logger.debug(f"construct_model: {tag=}, {item=}")
    if tag == "VariationArchive":
        logger.debug("Returning new VariationArchive")
        return model.VariationArchive.from_xml(item)


def make_item_cb(output_queue, keep_going):
    def item_cb(tag, item):
        if not keep_going["value"]:
            return False
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
    output_queue.put(QUEUE_STOP_VALUE)


def read_clinvar_xml(file, keep_going: dict, disassemble=True):
    """
    Generator function that reads a ClinVar Variation XML file and outputs objects

    keep_going is a dict with a "value" key that when true keeps this loop and thread going.
    """
    logger.info(f"{file=}")
    output_queue = queue.Queue()
    parser_thread = threading.Thread(
        target=_parse,
        name="ParserThread",
        args=(file, make_item_cb(output_queue, keep_going), output_queue),
    )
    parser_thread.start()

    while keep_going["value"]:
        try:
            obj = output_queue.get(timeout=1)
        except queue.Empty as e:
            if not parser_thread.is_alive():
                raise RuntimeError("Parser thread died!") from e
            continue
        logger.info(f"Got object from output queue: {str(obj)}")
        if obj == QUEUE_STOP_VALUE:
            break
        if disassemble:
            for subobj in obj.disassemble():
                yield subobj
        else:
            yield obj

    # if not keep_going["value"]:
    #     parser_thread.inter

    parser_thread.join()
