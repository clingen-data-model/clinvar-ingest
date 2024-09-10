"""
This module is meant to be used as a CLI to read a ClinVarVariationRelease XML
file and write a smaller number of VariationArchive objects to output files.

Example:
python test/data/clinvar-somatic/filter.py \
    -i clinvar-new/ClinVarVCVRelease_2024-08.xml.gz \
    -o clinvar-new \
    -v VCV000000002,VCV000000010,VCV000000032,VCV000000051,VCV000004897,VCV000040200,VCV000222476,VCV000406155,VCV000424711,VCV000634266,VCV001264328
"""

import argparse
import gzip
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_args(argv) -> dict:
    """
    Parses options: --input-filename (-i), --output-filename (-o)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-filename", "-i", required=True, type=str)
    parser.add_argument("--output-directory", "-o", required=True, type=str)
    # Accept a list of VCV accession strings from a comma separated string arg
    parser.add_argument("--vcv-accessions", "-v", required=True, type=str)
    parsed = vars(parser.parse_args(argv))

    # Split the comma separated string into a set of accession strings
    if parsed["vcv_accessions"]:
        parsed["vcv_accessions"] = set(parsed["vcv_accessions"].split(","))

    # Make sure the output directory exists or can be created
    if not os.path.exists(parsed["output_directory"]):
        os.makedirs(parsed["output_directory"])

    return parsed


def main(opts):
    """
    Main entrypoint for CLI.
    """
    with gzip.open(opts["input_filename"], "rt") as f_in:
        # Use ElementTree to iterate over f_in, and when a VariationArchive
        # element is completed, write it to f_out.
        opening_tag = None
        closing_tag = "</ClinVarVariationRelease>"
        item_count = 0
        item_limit = 1e12
        found_accessions = set()
        for event, elem in ET.iterparse(f_in, events=["start", "end"]):
            if event == "end" and elem.tag == "VariationArchive":
                # Check if the VCV accession is in the filter list
                accession = elem.attrib["Accession"]

                if accession in opts["vcv_accessions"]:
                    found_accessions.add(accession)
                    print(
                        f"Found accession: {accession}, remaining: {opts['vcv_accessions'] - found_accessions}"
                    )

                    # Open file, write opening tag, write element, write closing tag
                    assert opening_tag
                    elem_s = ET.tostring(elem)
                    filename = Path(opts["output_directory"]) / (accession + ".xml")
                    with open(filename, "wb") as f_out:
                        f_out.write(opening_tag)
                        f_out.write(b"\n")
                        f_out.write(elem_s.rstrip())
                        f_out.write(b"\n")
                        f_out.write(closing_tag.encode("utf-8"))
                        f_out.write(b"\n")

                # Clear completed VariationArchive element
                elem.clear()

            elif event == "start" and elem.tag == "ClinVarVariationRelease":
                # opening_attributes = " ".join(
                #     f'{key}="{value}"' for key, value in elem.attrib.items()
                # )
                # opening_tag = f"<{elem.tag} {opening_attributes}>"
                opening_tag = ET.tostring(elem).split(b"\n")[0]

            elif event == "end" and elem.tag == "ClinVarVariationRelease":
                break

            item_count += 1
            if item_count >= item_limit:
                break


if __name__ == "__main__":
    options = parse_args(sys.argv[1:])
    sys.exit(main(options))
