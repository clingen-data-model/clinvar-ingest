"""
Last run:
python partition_testing.py  984.58s user 5.19s system 99% cpu 16:37.82 total

16 minutes isn't too bad. Within the 60 minute max range. Then if we can split the parses
into 10 parallel jobs, it could speed up the entire workflow, and get the parse step
well under 60 minutes as well, meaning we don' need the async step completion checks,
which is causing failures by having a container without active requests be terminated.
BackgroundTasks are not active requests and make the container look idle.
"""

import gzip
import logging
from pathlib import Path
import time
from typing import Any, Iterator, TextIO, Tuple
import urllib.request
import requests
import xml.etree.ElementTree as ET

from google.cloud import storage
import xmltodict

from clinvar_ingest.cloud.gcs import (
    blob_writer,
    http_upload,
    http_download_curl,
    http_download_requests,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("copy_testing.ipynb")


# This is the url for a ClinVarVariationRelease xml file:
file_baseurl = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/VCV_xml_old_format/"
file_name = "ClinVarVariationRelease_2024-02.xml.gz"
file_url = file_baseurl + file_name

local_file = "./ClinVarVariationRelease_2024-02.xml.gz"

expected_file_size = 3298023159

# storage_client = storage.Client()

bucket_path = "kyle-test"


file_path = "/Users/kferrite/dev/data/ClinVarVariationRelease_2024-0107.xml.gz"


# Parse this file path as xml using ElementTree, element b


def _handle_text_nodes(path, key, value) -> Tuple[Any, Any]:
    """
    Takes a path, key, value, returns a tuple of new (key, value)

    If the value looks like an XML text node, put it in a key "$".

    Used as a postprocessor for xmltodict.parse.
    """
    if isinstance(value, str) and not key.startswith("@"):
        if key == "#text":
            return ("$", value)
        else:
            return (key, {"$": value})
    return (key, value)


def _parse_xml_document(doc_str: str):
    """
    Reads an XML document from a string.
    """
    return xmltodict.parse(doc_str, postprocessor=_handle_text_nodes)


def split_clinvar_xml(
    reader: TextIO,
    output_dir: Path,
    partitions=10,
) -> Iterator[ET.Element]:
    """
    Generator function that reads a ClinVar Variation XML file and outputs objects.
    Accepts `reader` as a readable TextIO/BytesIO object, or a filename.
    """
    ROOT_ELEMENT = "ClinVarVariationRelease"
    N = int(1e6)
    ct = 0
    output_paths = [output_dir / f"part_{i}.xml.gz" for i in range(partitions)]
    output_files = [gzip.open(p, "wb", compresslevel=1) for p in output_paths]

    def a_file():
        if getattr(a_file, "i", None) is None:
            a_file.i = 0
        some_f = output_files[a_file.i]
        a_file.i = (a_file.i + 1) % len(output_files)
        return some_f

    prior_ct = 0
    prior_log_ts = time.time()

    try:

        # Write XML roots to output files
        # for f in output_files:
        #     f.write('<?xml version="1.0"?>\n')
        #     f.write("<ClinVarVariation>\n")

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
                raise ValueError(
                    f"Unexpected event: {event}. Element: {ET.tostring(elem)}"
                )

            if event == "start" and elem.tag == ROOT_ELEMENT:

                # Write XML root elements to output files
                for f_out in output_files:
                    # f.write('<?xml version="1.0"?>\n')
                    f_out.write(ET.tostring(elem))

                release_date = elem.attrib["ReleaseDate"]
                _logger.info(f"Parsing release date: {release_date}")

            elif event == "end" and elem.tag == "VariationArchive":

                if unclosed != 1:
                    _logger.warning(
                        f"Found a VariationArchive at a depth other than 1:"
                        f" {unclosed}, element: {ET.tostring(elem)}"
                    )
                else:
                    # elem_s = element_to_string(elem)
                    a_file().write(ET.tostring(elem))

                elem.clear()

                ct = ct + 1
                if ct >= N:
                    break

            # log progress
            now = time.time()
            elapsed = now - prior_log_ts
            if elapsed > 10:
                elapsed_ct = ct - prior_ct
                _logger.info(
                    f"Processed {elapsed_ct} elements in {elapsed:.2f} seconds."
                )
                prior_log_ts = now
                prior_ct = ct

        # Return written files
        return output_paths
    except Exception as e:
        _logger.error(f"Error occurred: {e}")
        raise e
    finally:
        for _f in output_files:
            _f.write(f"</{ROOT_ELEMENT}>\n".encode("utf-8"))
            _f.close()


import xml.etree.ElementTree as ET


def write_element(file, element):
    file.write(ET.tostring(element, encoding="unicode"))


# with open("output.xml", "w") as f:
#     f.write('<?xml version="1.0"?>\n')  # Write the XML declaration

#     root = ET.Element("root")
#     write_element(f, root)  # Write the root element start tag

#     for i in range(10):  # Replace with your actual data
#         child = ET.Element("child")
#         child.text = f"This is child {i}"
#         write_element(f, child)  # Write the child element

#     f.write("</root>\n")  # Write the root element end tag

output_dir = Path("/Users/kferrite/dev/clinvar-ingest/misc/bin/clinvar_split")
if not output_dir.exists():
    output_dir.mkdir(parents=True, exist_ok=True)

with gzip.open(file_path, "rb") as f:
    output_paths = split_clinvar_xml(
        f,
        output_dir=output_dir,
        partitions=10,
    )
